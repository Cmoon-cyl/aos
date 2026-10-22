"""Fresh-prefix installation and optional real run/launch process checks.

Set AOS_CLI and AOS_ZENOHD for process checks. No shared router or existing
business directory is used. TLS routing generation is an offline check; native
TLS/ACL and cross-host acceptance belong to the final C21 orchestration.
"""
import hashlib
from process_support import console_process_options, interrupt_process

import json
import os
from pathlib import Path
import queue
import signal
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import unittest

import aos_sdk as aos

ROOT = Path(__file__).resolve().parents[2]
INSTALLER = ROOT / "scripts/install_memory_package.py"


class InstallationFixture(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="aos-memory-install-")
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name).resolve()
        self.prefix = self.directory / "installed"
        self.data = self.directory / "business"
        self.config = self.directory / "isolated.json"
        self.config.write_text(json.dumps({"mode": "peer", "connect": {"endpoints": []},
            "listen": {"endpoints": []}, "scouting": {"multicast": {"enabled": False}}}))
        self.environment = {key: value for key, value in os.environ.items()
                            if not key.startswith("AOS_") and key not in ("PYTHONPATH", "PYTHONHOME")}

    def install(self, *arguments, expected=0):
        result = subprocess.run([sys.executable, "-I", str(INSTALLER),
            "--prefix", str(self.prefix), "--work-dir", str(self.data), *arguments],
            cwd=self.directory, env=self.environment, capture_output=True, text=True, timeout=20)
        self.assertEqual(result.returncode, expected, result.stdout + result.stderr)
        return json.loads(result.stdout) if expected == 0 else result

    def request(self, name):
        return json.loads((self.prefix / "share/aos_memory/requests" / f"{name}.json").read_text())


class MemoryInstallationTest(InstallationFixture):
    def test_fresh_prefix_uses_installed_python_resources_and_does_not_open_business_data(self):
        report = self.install("--config", str(self.config), "--domain-id", "2021", "--namespace", "/memory_test")
        self.assertEqual(report["python"], sys.executable)
        self.assertFalse(self.data.exists())
        self.assertFalse(report["credentials_validated"])
        package = aos.PackageIndex([self.prefix]).package("aos_memory")
        self.assertEqual(package["manifest"]["name"], "aos_memory")
        for relative, expected in report["files"].items():
            self.assertEqual(hashlib.sha256((self.prefix / relative).read_bytes()).hexdigest(), expected)
        run = self.request("run-server")
        self.assertEqual(run["readiness"]["node"], "/memory_test/server")
        self.assertEqual(run["context"]["domain_id"], 2021)
        self.assertEqual(len(run["readiness"]["endpoints"]), 5)
        self.assertEqual(run["readiness"]["lifecycle"], {"target": "active", "autostart": True})
        entry = self.prefix / "lib/aos_memory/provider"
        if os.name == "nt":
            selected = package["manifest"]["entries"]["provider"]
            self.assertEqual(selected["interpreter"], sys.executable)
            self.assertEqual(selected["interpreter_args"], ["-I", "-u"])
            self.assertTrue((self.prefix / selected["executable"]).is_file())
        else:
            self.assertIn(" -I -u ", entry.read_text())
        manifest = self.prefix / "share/aos/resource_index/packages/aos_memory.json"
        before = manifest.read_bytes()
        self.install("--config", str(self.config), expected=2)
        self.assertEqual(manifest.read_bytes(), before)

    def test_secure_requests_keep_three_profiles_and_the_control_identity_explicit(self):
        profiles = {}
        for role in ("supervisor", "provider", "consumer"):
            path = self.directory / f"{role}.json"
            path.write_text(json.dumps({"schema_version": 1, "domain_id": 2021,
                "router_endpoints": ["tls/127.0.0.1:12345"],
                "root_ca_certificate": "/unopened/ca.pem", "certificate": f"/unopened/{role}.pem",
                "private_key": f"/unopened/{role}.key"}))
            profiles[role] = str(path)
        report = self.install(*[item for role, path in profiles.items() for item in (f"--{role}-profile", path)])
        self.assertTrue(report["security_selected"])
        self.assertFalse(report["credentials_validated"])
        run = self.request("run-server")
        self.assertEqual(run["security_profile"], profiles["supervisor"])
        self.assertEqual(run["client_node"], {"name": "memory_supervisor", "namespace": "/memory_control"})
        self.assertEqual(run["context"], {"domain_id": 2021, "zenoh_config": None})
        pair = json.loads((self.prefix / "share/aos_memory/launch/pair.json").read_text())
        for child in pair["processes"]:
            self.assertEqual(child["env"]["AOS_SECURITY_PROFILE"], profiles[child["id"]])
        self.assertEqual(pair["processes"][1]["restart"]["policy"], "never")
        self.assertFalse(self.data.exists())
        self.assertFalse(any(path.suffix in (".pem", ".key") for path in self.prefix.rglob("*")))


@unittest.skipUnless(os.environ.get("AOS_CLI") and os.environ.get("AOS_ZENOHD"),
                     "AOS_CLI and AOS_ZENOHD are required for real process checks")
class MemoryInstalledProcessTest(InstallationFixture):
    @classmethod
    def setUpClass(cls):
        import test_zenoh_session as router_env
        cls.router_env = router_env
        try:
            router_env.setUpModule()
        except BaseException:
            router_env.tearDownModule()
            raise

    @classmethod
    def tearDownClass(cls):
        cls.router_env.tearDownModule()

    def setUp(self):
        super().setUp()
        self.binary = str(Path(os.environ["AOS_CLI"]).resolve(strict=True))
        self.install("--config", str(self.router_env.session_config), "--domain-id", "2021")

    def start(self, group, request):
        events = queue.Queue()
        errors = self.directory / (group + "-" + str(time.time_ns()) + ".stderr")
        error_file = errors.open("wb")
        try:
            process = subprocess.Popen([self.binary, group, "start", "--request", "-"],
                cwd=self.directory, env=self.environment, stdin=subprocess.PIPE,
                stdout=subprocess.PIPE, stderr=error_file, text=True, start_new_session=os.name != "nt", **console_process_options())
        finally:
            error_file.close()
        process.stdin.write(json.dumps(request))
        process.stdin.close()
        process.stdin = None
        seen = []

        def read():
            with process.stdout:
                for line in process.stdout:
                    try:
                        value = json.loads(line)
                    except ValueError:
                        value = {"invalid_json": line}
                    seen.append(value)
                    events.put(value)
            events.put(None)

        reader = threading.Thread(target=read, name="memory-launch-output")
        reader.start()

        def cleanup():
            if process.poll() is None:
                interrupt_process(process)
                try:
                    process.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    # This is the group created above, never an existing process.
                    process.kill() if os.name == "nt" else os.killpg(process.pid, signal.SIGKILL)
                    process.wait(timeout=5)
            reader.join(5)
            self.assertFalse(reader.is_alive())

        self.addCleanup(cleanup)
        return {"process": process, "events": events, "seen": seen, "reader": reader, "errors": errors}

    def wait(self, running, predicate, timeout=20):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                value = running["events"].get(timeout=max(0, deadline - time.monotonic()))
            except queue.Empty:
                break
            if value is None:
                break
            self.assertNotIn("invalid_json", value)
            if predicate(value):
                return value
        self.fail(f"Expected event not observed; {running['seen']!r}; stderr={running['errors'].read_text()}")

    def business_completed(self, running):
        return self.wait(running, lambda event: event.get("event") == "process_stdout" and
            event.get("details", {}).get("value", {}).get("event") == "demo_completed")

    def stop(self, running):
        interrupt_process(running["process"])
        summary = self.wait(running, lambda event: event.get("event") == "summary")
        running["process"].wait(timeout=15)
        running["reader"].join(5)
        self.assertEqual(summary["data"]["outcome"], "stopped", summary)
        self.assertEqual(summary["data"]["cleanup_errors"], [])
        self.assertIn(running["process"].returncode, (0, 130))

    def committed(self):
        connection = sqlite3.connect(self.data / "memory.sqlite3")
        try:
            self.assertEqual(connection.execute("SELECT commit_seq,records,requests FROM metadata").fetchone(), (3, 3, 3))
        finally:
            connection.close()
        exports = list((self.data / "exports").glob("*.jsonl"))
        self.assertEqual(len(exports), 1)
        self.assertEqual(len(exports[0].read_text().splitlines()), 3)

    def test_launch_pair_runs_real_business_and_keeps_provider_until_explicit_stop(self):
        running = self.start("launch", self.request("launch-pair"))
        self.business_completed(running)
        self.assertIsNone(running["process"].poll())
        self.stop(running)
        self.committed()

    def test_separate_run_server_and_one_shot_run_consumer_use_the_installed_package(self):
        server = self.start("run", self.request("run-server"))
        self.wait(server, lambda event: event.get("event") == "ready")
        consumer = self.start("run", self.request("run-consumer"))
        self.business_completed(consumer)
        summary = self.wait(consumer, lambda event: event.get("event") == "summary")
        self.assertEqual(summary["data"]["outcome"], "completed", summary)
        self.assertEqual(summary["data"]["cleanup_errors"], [])
        self.assertEqual(consumer["process"].wait(timeout=10), 0)
        self.stop(server)
        self.committed()


if __name__ == "__main__":
    unittest.main()
