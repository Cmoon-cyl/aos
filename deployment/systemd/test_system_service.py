"""Pure preflight/ownership tests; real Linux tests live in verify_systemd.py."""
import argparse
import importlib.util
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location("system_service", ROOT / "scripts/system_service.py")
SERVICE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(SERVICE)


class SystemServiceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.aos = self.root / 'aos $NOT_EXPANDED%'
        self.aos.write_text("#!/bin/sh\nexit 0\n")
        self.aos.chmod(0o700)
        self.request = self.root / 'request "$VALUE"%.json'
        self.plan = {"schema_version": 1, "processes": [{"id": "provider"}]}
        self.write_request()
        self.args = SERVICE.parser().parse_args([
            "render", "--name", "test", "--aos", str(self.aos),
            "--request", str(self.request),
        ])

    def write_request(self, **other):
        self.request.write_text(json.dumps({"prefixes": [str(self.root)], "plan": self.plan, **other}))

    def test_unit_escaping_and_single_restart_owner(self):
        unit, record = SERVICE.render(self.args)
        self.assertIn("ExecStart=:\"", unit)
        self.assertIn('$NOT_EXPANDED%%', unit)
        self.assertIn(r'\"$VALUE\"%%', unit)
        self.assertIn(f"WorkingDirectory={self.root}\n", unit)
        self.assertIn("KillMode=mixed\n", unit)
        self.assertIn("RestartPreventExitStatus=1 2 3 4 129 130 131 143\n", unit)
        self.assertIn("SuccessExitStatus=129 130 131 143\n", unit)
        self.assertIn("TimeoutStopSec=48s\n", unit)
        self.assertNotIn("Environment=", unit)
        self.assertNotIn("ExecStop=", unit)
        self.assertEqual(record["graceful_budget_seconds"], 48)
        self.assertEqual(record["stdout_stall_seconds"], 30)
        self.assertEqual(record["shutdown_output_seconds"], 30)
        self.assertEqual(record["probe_close_seconds"], 1)
        self.assertEqual(record["terminal_output_seconds"], 2)

    def test_stop_budget_uses_real_plan_and_explicit_forced_fallback(self):
        self.plan["shutdown"] = {"sigint_timeout": 17, "sigterm_timeout": 22}
        self.write_request()
        unit, record = SERVICE.render(self.args)
        self.assertIn("TimeoutStopSec=77s", unit)
        self.args.stop_timeout = 1
        with self.assertRaisesRegex(SERVICE.ServiceError, "allow-forced-stop"):
            SERVICE.render(self.args)
        self.args.allow_forced_stop = True
        _, record = SERVICE.render(self.args)
        self.assertTrue(record["forced_stop"])

    def test_path_plan_and_changes_are_tracked_without_content_copy(self):
        source = self.root / "launch.json"
        source.write_text(json.dumps({**self.plan, "shutdown": {"sigint_timeout": 0, "sigterm_timeout": 2}}))
        self.request.write_text(json.dumps({"prefixes": [str(self.root)], "path": str(source)}))
        _, record = SERVICE.render(self.args)
        self.assertEqual(record["stop_timeout_seconds"], 40)
        self.assertEqual(SERVICE.changed_inputs(record), [])
        source.write_text("{}")
        self.assertEqual(SERVICE.changed_inputs(record), [str(source)])
        self.assertNotIn("processes", json.dumps(record))

    def test_no_implicit_prefixes_or_relative_sources(self):
        for request in ({"plan": self.plan}, {"prefixes": ["relative"], "plan": self.plan},
                        {"prefixes": [str(self.root)], "path": "launch.json"},
                        {"prefixes": [str(self.root)], "plan": self.plan, "path": "/tmp/x"}):
            with self.subTest(request=request):
                self.request.write_text(json.dumps(request))
                with self.assertRaises(SERVICE.ServiceError):
                    SERVICE.render(self.args)

    def test_duplicate_nonfinite_boolean_duration_and_newline_rejected(self):
        for value in (b'{"a":1,"a":2}', b'{"a":NaN}', b'[]', '{"a":1}'.encode("utf-16")):
            with self.assertRaises(SERVICE.ServiceError):
                SERVICE.strict_json(value)
        for value in (True, float("inf"), -1, 10 ** 400):
            with self.assertRaises(SERVICE.ServiceError):
                SERVICE.seconds(value, "timeout")
        with self.assertRaises(SERVICE.ServiceError):
            SERVICE.quote("a\n[Service]\nExecStart=x")
        self.assertEqual(SERVICE.directory_value('/tmp/a "b"$c% d'), '/tmp/a "b"$c%% d')
        with self.assertRaises(SERVICE.ServiceError):
            SERVICE.directory_value("/tmp/space ")
        with self.assertRaises(SERVICE.ServiceError):
            SERVICE.unit_name("../unrelated")

    def test_restart_limit_cannot_be_disabled_or_fast(self):
        for key, value in (("restart_delay", 0), ("start_limit_burst", 0),
                           ("start_limit_burst", 6), ("start_limit_interval", 20)):
            with self.subTest(key=key, value=value):
                args = argparse.Namespace(**vars(self.args))
                setattr(args, key, value)
                with self.assertRaises(SERVICE.ServiceError):
                    SERVICE.render(args)

    def test_owned_modified_symlink_and_foreign_record_refuse_mutation(self):
        with mock.patch.dict(os.environ, {"XDG_CONFIG_HOME": str(self.root / "config"),
                                         "XDG_STATE_HOME": str(self.root / "state")}):
            unit, record = SERVICE.render(self.args)
            path, owner = SERVICE.locations("test")
            path.parent.mkdir(parents=True)
            owner.parent.mkdir(parents=True)
            SERVICE.exclusive_write(path, unit.encode())
            SERVICE.exclusive_write(owner, json.dumps(record).encode())
            SERVICE.owned("test", manager=False)
            path.write_text(unit + "# User modification\n")
            with self.assertRaisesRegex(SERVICE.ServiceError, "changed after"):
                SERVICE.owned("test", manager=False)
            path.unlink()
            path.symlink_to(self.request)
            with self.assertRaises(SERVICE.ServiceError):
                SERVICE.owned("test", manager=False)

    def test_render_does_not_replace_existing_file(self):
        self.args.output = str(self.root / "owned.service")
        SERVICE.execute(self.args)
        original = Path(self.args.output).read_bytes()
        with self.assertRaises(FileExistsError):
            SERVICE.execute(self.args)
        self.assertEqual(Path(self.args.output).read_bytes(), original)

    def test_cgroup_path_cannot_escape_mount(self):
        self.assertIsNone(SERVICE.cgroup_mount("/user.slice/../../unrelated"))
        self.assertIsNone(SERVICE.cgroup_mount("relative"))


if __name__ == "__main__":
    unittest.main()
