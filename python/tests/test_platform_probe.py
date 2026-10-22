"""Check a copied probe with installed native SDK and an actual local router."""

import importlib.util
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import time
from types import SimpleNamespace
import unittest
import uuid

import aos_sdk as aos
import test_zenoh_session as router_env
from process_support import process_alive

SCRIPT = Path(__file__).resolve().parents[2] / "scripts/platform_probe.py"
SPEC = importlib.util.spec_from_file_location("platform_probe", SCRIPT)
probe = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(probe)
SOURCE_SHA = "0" * 40  # A test fixture label, never release provenance.


def records(path):
    return [json.loads(line) for line in path.read_text().splitlines()]


class EvidenceContractTest(unittest.TestCase):
    def test_security_profile_domain_inherits_across_child_process_boundary(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            profile = root / "peer.json"
            profile.write_text(json.dumps({"domain_id": 29}))
            arguments = ["client", "--security-profile", str(profile),
                         "--run-id", "secure_domain", "--source-sha", SOURCE_SHA,
                         "--output", str(root / "output")]
            args = probe.parse_args(arguments)
            self.assertIsNone(args.domain_id)
            child = probe.child_command(args, "_client", root / "child")
            child_args = probe.parse_args(child[4:])
            self.assertIsNone(child_args.domain_id)
            self.assertEqual(child_args.security_profile, profile.resolve())
            explicit = probe.parse_args(arguments + ["--domain-id", "29"])
            self.assertEqual(explicit.domain_id, 29)
            native = probe.parse_args(["client", "--config", str(profile),
                                      "--run-id", "native_domain", "--source-sha", SOURCE_SHA,
                                      "--output", str(root / "native")])
            self.assertEqual(native.domain_id, 72)

    def test_mixed_source_requires_an_explicit_matching_peer(self):
        args = SimpleNamespace(run_id="mixed", source_sha="a" * 40)
        origin = {"run_id": "mixed", "round": 1, "source_sha": "b" * 40,
                  "probe_sha256": probe.digest(SCRIPT)}
        with self.assertRaisesRegex(RuntimeError, "source SHA"):
            probe.check_origin(origin, args, 1)
        args.peer_source_sha = "b" * 40
        probe.check_origin(origin, args, 1)
        origin["source_sha"] = "c" * 40
        with self.assertRaisesRegex(RuntimeError, "source SHA"):
            probe.check_origin(origin, args, 1)

    def test_inline_interfaces_load_without_repository_resources(self):
        for definition in probe.DEFINITIONS.values():
            with self.subTest(name=definition["name"]):
                self.assertEqual(aos.Interface.load(definition).definition, definition)

    def test_success_cannot_be_inferred_from_cancel_acceptance(self):
        origin = {"instance_id": "provider"}
        request = {"request_id": "request", "steps": 10}
        result = {"status": 4, "result": {
            "origin": origin, "request_id": "request", "completed": 10, "total": 55,
        }}
        probe.check_result(result, request, origin, canceled=False)
        with self.assertRaisesRegex(RuntimeError, "terminal"):
            probe.check_result(result, request, origin, canceled=True)
        result["status"] = 5
        with self.assertRaisesRegex(RuntimeError, "terminal outcome"):
            probe.check_result(result, request, origin, canceled=True)
        result["result"].update(completed=2, total=3)
        probe.check_result(result, request, origin, canceled=True)
        result["result"]["origin"] = {"instance_id": "stale"}
        with self.assertRaisesRegex(RuntimeError, "another provider"):
            probe.check_result(result, request, origin, canceled=True)


@unittest.skipUnless(os.environ.get("AOS_ZENOHD"), "AOS_ZENOHD is not set")
class PlatformProcessProbeTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        router_env.setUpModule()

    @classmethod
    def tearDownClass(cls):
        router_env.tearDownModule()

    def run_probe(self, mode, directory, *extra):
        # Copy only one file; use a third directory as cwd. No examples,
        # interfaces, checkout imports, or PYTHONPATH are available to children.
        copied = directory / "copied-probe.py"
        shutil.copy2(SCRIPT, copied)
        unrelated = directory / "unrelated"
        unrelated.mkdir()
        endpoint = re.search(r"tcp/127\.0\.0\.1:\d+",
                             router_env.session_config.read_text()).group()
        config = directory / "client.json"
        config.write_text(json.dumps({
            "mode": "client",
            "connect": {"endpoints": [endpoint], "timeout_ms": 3000, "exit_on_failure": True},
            "listen": {"endpoints": []},
            "scouting": {"multicast": {"enabled": False},
                         "gossip": {"enabled": False, "autoconnect": []}},
            "timestamping": {"enabled": True},
        }))
        output = directory / "evidence"
        command = [
            sys.executable, "-I", str(copied), mode,
            "--config", str(config), "--output", str(output),
            "--run-id", "test_" + uuid.uuid4().hex, "--source-sha", SOURCE_SHA,
            "--build-label", "baseline", "--overall-timeout", "40", *extra,
        ]
        result = subprocess.run(command, cwd=unrelated, capture_output=True,
                                text=True, timeout=50)
        return result, output

    def test_two_process_rounds_with_real_results_and_restart(self):
        with tempfile.TemporaryDirectory() as temporary:
            result, output = self.run_probe("local", Path(temporary))
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertEqual(json.loads((output / "local-0.json").read_text())["status"], "passed")
            client = records(output / "client/_client-0.jsonl")
            providers = [records(output / f"provider/_provider-{round_}.jsonl")
                         for round_ in (1, 2)]
            client_pid = next(row["pid"] for row in client if row["event"] == "sdk_ready")
            self.assertEqual(sum(row["event"] == "sdk_ready" for row in client), 1)
            origins = []
            for round_, provider in enumerate(providers, 1):
                origin = next(row["origin"] for row in provider if row["event"] == "provider_ready")
                origins.append(origin)
                self.assertNotEqual(origin["pid"], client_pid)
                for event in ("topic_received", "service_verified", "action_success_verified",
                              "action_cancel_verified", "round_verified"):
                    rows = [row for row in client if row["event"] == event and row["round"] == round_]
                    self.assertEqual(len(rows), 1, event)
                    self.assertTrue(rows[0]["isolated"])
                    self.assertEqual(rows[0]["build_label"], "baseline")
                for row in client:
                    if row["event"] in ("action_success_verified", "action_cancel_verified") \
                            and row["round"] == round_:
                        matching = [p for p in provider if p["event"] == "action_completed"
                                    and p["goal_id"] == row["goal_id"]]
                        self.assertEqual(len(matching), 1)
                        self.assertEqual(matching[0]["status"], row["response"]["status"])
                        self.assertEqual(matching[0]["result"], row["response"]["result"])
            self.assertNotEqual(origins[0]["pid"], origins[1]["pid"])
            self.assertNotEqual(origins[0]["node_id"], origins[1]["node_id"])
            self.assertEqual(sum(row["event"] == "old_provider_gone" for row in client), 1)
            self.assertEqual(sum(row["event"] == "final_provider_gone" for row in client), 1)
            for origin in origins:
                self.assertFalse(process_alive(origin["pid"]), origin)

    def test_missing_provider_fails_with_bounded_cleanup(self):
        with tempfile.TemporaryDirectory() as temporary:
            started = time.monotonic()
            result, output = self.run_probe("client", Path(temporary),
                                            "--stage-timeout", "0.2", "--overall-timeout", "5")
            self.assertNotEqual(result.returncode, 0)
            self.assertLess(time.monotonic() - started, 10)
            summary = json.loads((output / "client-0.json").read_text())
            self.assertEqual(summary["status"], "failed")
            events = records(output / "client-0.jsonl")
            started_children = [row["child_pid"] for row in events if row["event"] == "process_started"]
            self.assertEqual(len(started_children), 1)
            self.assertFalse(process_alive(started_children[0]), started_children)
            worker = records(output / "_client-0.jsonl")
            self.assertFalse(any(row["event"] == "round_verified" for row in worker))

    def test_idle_provider_is_stopped_at_overall_deadline(self):
        with tempfile.TemporaryDirectory() as temporary:
            started = time.monotonic()
            result, output = self.run_probe("provider", Path(temporary),
                                            "--overall-timeout", "0.5")
            self.assertNotEqual(result.returncode, 0)
            self.assertLess(time.monotonic() - started, 8)
            events = records(output / "provider-0.jsonl")
            children = [row["child_pid"] for row in events if row["event"] == "process_started"]
            self.assertEqual(len(children), 1)
            self.assertEqual(events[-1]["status"], "failed")
            self.assertIn("TimeoutExpired", events[-1]["error"])
            for pid in children:
                self.assertFalse(process_alive(pid), pid)


if __name__ == "__main__":
    unittest.main()
