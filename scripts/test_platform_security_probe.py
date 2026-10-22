"""Check secure-probe source declarations without requiring installed SDK or network I/O."""

import argparse
import contextlib
import copy
import importlib.util
import io
from pathlib import Path
import tempfile
import unittest
from unittest import mock

SPEC = importlib.util.spec_from_file_location(
    "security_probe", Path(__file__).with_name("platform_security_probe.py"))
probe = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(probe)

A, B, C = "a" * 40, "b" * 40, "c" * 40
SOURCE_GATES = ("same_run_and_probe", "stable_source_bindings",
                "declared_peer_sources_match", "router_source_matches_endpoint")


def source_fixture(*, mixed=False, declared=True, router_role="provider"):
    def record(role, hostname, source, peer):
        row = dict(event="finished", run_id="secure_run", direction="a_to_b", role=role,
                   hostname=hostname, pid=42,
                   binding=dict(source_sha=source, probe_sha256="1" * 64, base_probe_sha256="2" * 64,
                                cli_path="/" + hostname + "/aos", cli_sha256="3" * 64,
                                wheel_sha256="4" * 64, native_sha256="5" * 64))
        if declared:
            row["expected_peer_source_sha"] = peer
        return row
    provider_sha = B if mixed else A
    client = record("client", "client_host", A, provider_sha)
    provider = record("provider", "provider_host", provider_sha, A)
    first, second = copy.deepcopy(provider), copy.deepcopy(provider)
    first["role"], second["role"] = "_provider-1", "_provider-2"
    router = copy.deepcopy(client if router_role == "client" else provider)
    router["role"] = "router"
    # These records cover provenance only; they must never pass the business gates.
    return [[client], [first], [second], [provider], [router]]


class SourceTests(unittest.TestCase):
    def verify(self, rows):
        with mock.patch.object(probe, "events", side_effect=rows):
            return probe.verify("client", "provider", "router")

    def assert_sources_pass(self, result):
        self.assertEqual({name: result["checks"][name] for name in SOURCE_GATES},
                         dict.fromkeys(SOURCE_GATES, True))
        self.assertEqual(result["status"], "failed")
        self.assertFalse(result["checks"]["initial:phase_verified"])

    def test_default_same_source_remains_required(self):
        self.assert_sources_pass(self.verify(source_fixture(declared=False)))
        mixed = self.verify(source_fixture(mixed=True, declared=False))
        self.assertFalse(mixed["checks"]["declared_peer_sources_match"])

    def test_explicit_reciprocal_sources_preserve_actual_builds(self):
        for router_role in ("client", "provider"):
            with self.subTest(router_role=router_role):
                result = self.verify(source_fixture(mixed=True, router_role=router_role))
                self.assert_sources_pass(result)
                self.assertEqual(result["source_bindings"]["client"],
                                 dict(source_sha=A, expected_peer_source_sha=B))
                self.assertEqual(result["source_bindings"]["provider"],
                                 dict(source_sha=B, expected_peer_source_sha=A))
                self.assertEqual(result["router_source_roles"], [router_role])

    def test_one_sided_or_wrong_peer_declaration_is_rejected(self):
        for peer in (None, C):
            with self.subTest(peer=peer):
                rows = source_fixture(mixed=True)
                for group in rows[1:]:
                    if peer is None:
                        group[0].pop("expected_peer_source_sha")
                    else:
                        group[0]["expected_peer_source_sha"] = peer
                self.assertFalse(self.verify(rows)["checks"]["declared_peer_sources_match"])

    def test_each_role_source_and_declaration_must_stay_stable(self):
        for index in range(5):
            for field in ("source_sha", "expected_peer_source_sha", "native_sha256"):
                with self.subTest(stream=index, field=field):
                    rows = source_fixture(mixed=True)
                    changed = copy.deepcopy(rows[index][0])
                    target = changed if field == "expected_peer_source_sha" else changed["binding"]
                    target[field] = C if field != "native_sha256" else "6" * 64
                    rows[index].append(changed)
                    self.assertFalse(self.verify(rows)["checks"]["stable_source_bindings"])

    def test_run_direction_and_script_hashes_still_match_in_every_stream(self):
        for index in range(5):
            for field in ("run_id", "direction", "probe_sha256", "base_probe_sha256"):
                with self.subTest(stream=index, field=field):
                    rows = source_fixture(mixed=True)
                    target = rows[index][0] if field in ("run_id", "direction") else rows[index][0]["binding"]
                    target[field] = "different"
                    self.assertFalse(self.verify(rows)["checks"]["same_run_and_probe"])

    def test_missing_supervisor_cannot_establish_source_consistency(self):
        rows = source_fixture(mixed=True)
        rows[3] = []
        self.assertFalse(self.verify(rows)["checks"]["same_run_and_probe"])

    def test_router_must_match_known_host_artifacts_and_peer_declaration(self):
        for field in ("hostname", "cli_sha256", "expected_peer_source_sha"):
            with self.subTest(field=field):
                rows = source_fixture(mixed=True)
                target = rows[4][0]["binding"] if field == "cli_sha256" else rows[4][0]
                target[field] = C
                result = self.verify(rows)
                self.assertFalse(result["checks"]["router_source_matches_endpoint"])
                self.assertEqual(result["router_source_roles"], [])

    def test_partial_source_or_peer_sha_is_rejected(self):
        for field in ("source_sha", "expected_peer_source_sha"):
            rows = source_fixture(mixed=True)
            target = rows[0][0]["binding"] if field == "source_sha" else rows[0][0]
            target[field] = "abc123"
            self.assertFalse(self.verify(rows)["checks"]["stable_source_bindings"])


class ArgumentTests(unittest.TestCase):
    def args(self, output, peer=None):
        return argparse.Namespace(binding=Path("binding.json"), output=output,
                                  run_id="secure_run", direction="a_to_b", domain_id=74,
                                  timeout=180, stage_timeout=15, peer_source_sha=peer)

    def test_metadata_records_default_or_explicit_peer_without_rewriting_binding(self):
        for peer in (None, B):
            with self.subTest(peer=peer), tempfile.TemporaryDirectory() as temporary:
                args = self.args(Path(temporary), peer)
                build = dict(source_sha=A)
                with mock.patch.object(probe, "binding", return_value=build), contextlib.redirect_stdout(io.StringIO()):
                    evidence = probe.Evidence(args, "client")
                    self.assertEqual(evidence.binding, dict(source_sha=A))
                    self.assertEqual(evidence.metadata["expected_peer_source_sha"], peer or A)
                    evidence.close()

    def test_provider_child_inherits_only_explicit_peer_argument(self):
        for peer in (None, B):
            command = probe.child_command(self.args(Path("output"), peer), "_provider", Path("output"), generation=1)
            if peer is None:
                self.assertNotIn("--peer-source-sha", command)
            else:
                self.assertEqual(command[command.index("--peer-source-sha") + 1], B)

    def test_cli_rejects_partial_peer_sha(self):
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as error:
            probe.main(["policy", "--peer-source-sha", "abc123"])
        self.assertEqual(error.exception.code, 2)


if __name__ == "__main__":
    unittest.main()
