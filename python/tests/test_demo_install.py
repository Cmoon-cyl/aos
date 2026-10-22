"""Install the public demo and prove lifecycle, dependency and business output."""
import json
import os
from pathlib import Path
import subprocess
import sys

import test_memory_install as memory


class DemoInstallationTest(memory.InstallationFixture):
    setUpClass = classmethod(memory.MemoryInstalledProcessTest.setUpClass.__func__)
    tearDownClass = classmethod(memory.MemoryInstalledProcessTest.tearDownClass.__func__)
    start = memory.MemoryInstalledProcessTest.start
    wait = memory.MemoryInstalledProcessTest.wait
    stop = memory.MemoryInstalledProcessTest.stop

    def test_public_demo_installs_and_runs_from_a_unicode_space_prefix(self):
        self.prefix = self.directory / "demo space \u6d4b\u8bd5"
        self.binary = str(Path(os.environ["AOS_CLI"]).resolve(strict=True))
        result = subprocess.run([sys.executable, "-I", "-X", "utf8",
            str(memory.ROOT / "scripts/install_demo_package.py"),
            "--prefix", str(self.prefix), "--config", str(self.router_env.session_config),
            "--domain-id", "2021", "--namespace", "/installed_demo"],
            cwd=self.directory, env=self.environment, capture_output=True, text=True, encoding="utf8", timeout=20)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        report = json.loads(result.stdout)
        self.assertEqual(report["status"], "installed")
        running = self.start("launch", {"prefixes": [str(self.prefix)], "package": "aos_demo", "resource": "pair"})
        event = self.wait(running, lambda event: event.get("event") == "process_stdout" and
            event.get("details", {}).get("value", {}).get("event") == "business_state" and
            event["details"]["value"]["message"]["state"] == "ready")
        self.assertEqual(event["details"]["value"]["message"]["received"], 3)
        self.stop(running)
        stopped = [event["details"]["value"] for event in running["seen"]
                   if event.get("event") == "process_stdout" and
                   event.get("details", {}).get("value", {}).get("event") == "program_stopped"]
        self.assertEqual(len(stopped), 2)
        self.assertTrue(all(record["executor_stopped"] for record in stopped))
