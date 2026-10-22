"""Installed-SDK diagnostics, with isolated peers and real child-process deadlines."""

import copy
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest import mock

import aos_sdk as aos
import aos_sdk.doctor as doctor


class DoctorTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)
        self.config = self.directory / "peer.json5"
        self.config.write_text(
            """{
              // This comment must never be echoed: PRIVATE_CONFIG_CONTENT
              mode: "peer",
              connect: { endpoints: [] },
              listen: { endpoints: ["tcp/127.0.0.1:0"] },
              scouting: { multicast: { enabled: false }, gossip: { enabled: false } },
            }""",
            encoding="utf-8",
        )
        self.environment = mock.patch.dict(os.environ, {
            "AOS_ZENOH_CONFIG": str(self.config), "AOS_DOMAIN_ID": "23",
        })
        self.environment.start()
        self.addCleanup(self.environment.stop)

    def test_real_isolated_context_and_graph_do_not_claim_router_or_security(self):
        report = aos.doctor_report(timeout=3)
        self.assertEqual(report["status"], "ok", report)
        self.assertEqual(report["config"]["source"], "environment")
        self.assertTrue(report["config"]["exists"])
        self.assertEqual(report["config"]["sha256"], hashlib.sha256(self.config.read_bytes()).hexdigest())
        self.assertEqual(report["config"]["size_bytes"], len(self.config.read_bytes()))
        self.assertEqual(report["domain"]["value"], 23)
        self.assertTrue(report["native"]["available"])
        self.assertEqual(report["native"]["path"], aos._native.__file__)
        self.assertEqual(report["sdk"]["native_version"], aos.__version__)
        self.assertEqual(report["context"]["status"], "ok")
        self.assertEqual(report["graph"]["status"], "ok")
        self.assertEqual(report["graph"]["entity_count"], 0)
        self.assertEqual(report["router"]["status"], "unknown")
        self.assertIsNone(report["router"]["ids"])
        for key in ("authentication", "authorization", "encryption"):
            self.assertEqual(report["security"][key], "unknown")
        self.assertEqual(report["shutdown"]["status"], "ok")
        self.assertEqual(report["telemetry"]["status"], "ok")
        snapshot = report["telemetry"]["snapshot"]
        self.assertNotEqual(snapshot["process_id"], os.getpid())
        self.assertEqual(snapshot["active_resources"], {})
        self.assertFalse(snapshot["log_capture_enabled"])
        self.assertGreater(snapshot["events_total"], 0)
        self.assertIn("subprocess", report["telemetry"]["scope"])
        self.assertTrue(report["cleanup"]["reaped"])
        self.assertFalse(report["cleanup"]["forced"])
        self.assertNotIn("PRIVATE_CONFIG_CONTENT", json.dumps(report))
        self.assertIn("doctor_report", aos.__all__)

    def test_explicit_options_override_environment(self):
        with mock.patch.dict(os.environ, {
            "AOS_ZENOH_CONFIG": str(self.directory / "missing"),
            "AOS_DOMAIN_ID": "invalid-private-environment",
        }):
            report = aos.doctor_report(config=self.config, domain_id=7, timeout=3)
        self.assertEqual(report["status"], "ok", report)
        self.assertEqual(report["config"]["source"], "argument")
        self.assertEqual(report["domain"], {"source": "argument", "status": "ok", "value": 7})

    def test_empty_context_environment_matches_unset_before_session_open(self):
        # Exercise parent selection and child validation without connecting to
        # the built-in router endpoint, which this test does not own.
        original_popen = subprocess.Popen
        fixture = """
import json, runpy, sys
module = runpy.run_path(sys.argv[1])
options = json.loads(sys.argv[2])
report = module["_report"](options)
module["_config_and_domain"](report, options)
if not report["failures"]:
    report["status"] = "ok"
module["_emit"](report, "complete")
"""

        def start_config_probe(command, **kwargs):
            return original_popen(
                [sys.executable, "-I", "-c", fixture, doctor.__file__, command[-1]], **kwargs,
            )

        environment = {key: value for key, value in os.environ.items()
                       if key not in ("AOS_ZENOH_CONFIG", "AOS_DOMAIN_ID", "AOS_SECURITY_PROFILE")}
        reports = []
        for selected in ({}, {"AOS_ZENOH_CONFIG": "", "AOS_DOMAIN_ID": ""}):
            with self.subTest(environment=selected), \
                 mock.patch.dict(os.environ, environment | selected, clear=True), \
                 mock.patch.object(doctor.subprocess, "Popen", start_config_probe):
                report = aos.doctor_report(timeout=3)
                self.assertEqual(dict(os.environ), environment | selected)
            reports.append(report)
            self.assertEqual(report["status"], "ok", report)
            self.assertEqual(report["config"]["source"], "built_in")
            self.assertIsNone(report["config"]["path"])
            self.assertEqual(report["domain"], {"source": "default", "status": "ok", "value": 0})
            self.assertEqual(report["context"]["status"], "not_run")
        self.assertEqual(reports[0]["config"], reports[1]["config"])
        self.assertEqual(reports[0]["domain"], reports[1]["domain"])

    def test_launch_identity_is_excluded_only_from_the_diagnostic_subprocess(self):
        bootstrap = self.directory / "private-process.json"
        bootstrap.write_text("PRIVATE_INVALID_BOOTSTRAP", encoding="utf-8")
        with mock.patch.dict(os.environ, {"AOS_PROCESS_CONFIG": str(bootstrap)}):
            # Ordinary business Contexts still validate the inherited bootstrap.
            with self.assertRaises(aos.InvalidArgumentError):
                aos.Context()
            report = aos.doctor_report(config=self.config, domain_id=7, timeout=3)
            self.assertEqual(os.environ["AOS_PROCESS_CONFIG"], str(bootstrap))
        self.assertEqual(report["status"], "ok", report)
        self.assertEqual(report["domain"]["value"], 7)
        self.assertTrue(report["deployment"]["process_config_present"])
        self.assertFalse(report["deployment"]["used_by_probe"])
        self.assertEqual(report["deployment"]["validation"], "not_run")
        self.assertNotIn(str(bootstrap), json.dumps(report))
        self.assertNotIn("PRIVATE_INVALID_BOOTSTRAP", json.dumps(report))

    def test_config_and_domain_failures_skip_context_but_check_native(self):
        cases = [
            (self.directory / "missing", "23", "CONFIG_NOT_FOUND"),
            (self.directory, "23", "CONFIG_NOT_FILE"),
            (self.config, "invalid-private-environment", "INVALID_DOMAIN_ID"),
        ]
        for config, domain, code in cases:
            with self.subTest(code=code), mock.patch.dict(os.environ, {"AOS_DOMAIN_ID": domain}):
                report = aos.doctor_report(config=config, timeout=3)
            self.assertEqual(report["status"], "error", report)
            self.assertTrue(report["native"]["available"])
            self.assertIn(code, [failure["code"] for failure in report["failures"]])
            self.assertEqual(report["context"]["status"], "not_run")
            self.assertNotIn("invalid-private-environment", json.dumps(report))

    def test_config_parser_exception_does_not_expose_file_contents(self):
        self.config.write_text('{ password: "PRIVATE_PARSER_CONTENT", broken', encoding="utf-8")
        report = aos.doctor_report(timeout=3)
        self.assertEqual(report["status"], "error", report)
        self.assertEqual(report["failures"][0]["code"], "CONFIG_INVALID")
        self.assertEqual(report["failures"][0]["stage"], "context")
        self.assertNotIn("PRIVATE_PARSER_CONTENT", json.dumps(report))
        self.assertEqual(report["shutdown"]["status"], "not_run")

    def test_cli_help_and_json_output_have_no_import_warning(self):
        help_result = subprocess.run(
            [sys.executable, "-I", "-m", "aos_sdk.doctor", "--help"],
            capture_output=True, text=True, timeout=5,
        )
        self.assertEqual(help_result.returncode, 0, help_result.stderr)
        self.assertIn("--timeout", help_result.stdout)
        self.assertEqual(help_result.stderr, "")
        result = subprocess.run(
            [sys.executable, "-I", "-m", "aos_sdk.doctor", "--timeout", "3"],
            capture_output=True, text=True, timeout=5,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout)["status"], "ok")
        self.assertEqual(result.stderr, "")

    def test_direct_file_entry_reports_missing_native_without_importing_package_first(self):
        package = self.directory / "aos_sdk"
        package.mkdir()
        shutil.copyfile(doctor.__file__, package / "doctor.py")
        (package / "__init__.py").write_text(
            "raise ImportError('PRIVATE_NATIVE_LOADER_CONTENT')\n", encoding="utf-8",
        )
        result = subprocess.run(
            [sys.executable, "-I", str(package / "doctor.py"), "--timeout", "3"],
            capture_output=True, text=True, timeout=5,
        )
        report = json.loads(result.stdout)
        self.assertEqual(result.returncode, 1)
        self.assertFalse(report["native"]["available"])
        self.assertEqual(report["failures"][0]["code"], "NATIVE_IMPORT_FAILED")
        self.assertEqual(report["context"]["status"], "not_run")
        self.assertEqual(report["telemetry"]["status"], "not_run")
        self.assertIsNone(report["telemetry"]["snapshot"])
        self.assertNotIn("PRIVATE_NATIVE_LOADER_CONTENT", result.stdout + result.stderr)
        self.assertTrue(report["cleanup"]["reaped"])

    def test_total_timeout_kills_and_reaps_blocked_open_graph_and_shutdown(self):
        original_popen = subprocess.Popen
        threads_before = set(threading.enumerate())
        script = """
import json, runpy, sys, time
module = runpy.run_path(sys.argv[1])
report = module["_report"](json.loads(sys.argv[2]))
stage = sys.argv[3]
report[stage]["status"] = "running"
module["_emit"](report, stage)
while True:
    time.sleep(60)
"""
        for stage in ("context", "graph", "shutdown", "telemetry"):
            children = []

            def start_fixture(command, **kwargs):
                child = original_popen(
                    [sys.executable, "-I", "-c", script, doctor.__file__, command[-1], stage],
                    **kwargs,
                )
                children.append(child)
                return child

            with self.subTest(stage=stage), mock.patch.object(doctor.subprocess, "Popen", start_fixture):
                started = time.monotonic()
                report = aos.doctor_report(timeout=0.8)
                elapsed = time.monotonic() - started
            self.assertEqual(report["status"], "timeout", report)
            self.assertEqual(report["probe"]["stage"], stage)
            self.assertEqual(report[stage]["status"], "timeout")
            self.assertTrue(report["cleanup"]["forced"])
            self.assertTrue(report["cleanup"]["reaped"])
            self.assertIsNotNone(children[0].poll())
            # Account for scheduler jitter without allowing an unbounded cleanup.
            self.assertLess(elapsed, 1.1)
        self.assertEqual(set(threading.enumerate()), threads_before)

    def test_child_crash_is_structured_and_raw_output_is_not_forwarded(self):
        original_popen = subprocess.Popen

        def crash(command, **kwargs):
            return original_popen([
                sys.executable, "-I", "-c",
                "print('PRIVATE_RAW_NATIVE_OUTPUT', flush=True); raise SystemExit(7)",
            ], **kwargs)

        with mock.patch.object(doctor.subprocess, "Popen", crash):
            report = aos.doctor_report(timeout=3)
        self.assertEqual(report["status"], "error")
        self.assertEqual(report["probe"]["returncode"], 7)
        self.assertEqual(report["failures"][0]["code"], "PROBE_EXITED")
        self.assertNotIn("PRIVATE_RAW_NATIVE_OUTPUT", json.dumps(report))
        self.assertTrue(report["cleanup"]["reaped"])

    def test_graph_failure_still_shuts_down_and_never_calls_business_methods(self):
        calls = []
        reports = []

        class Context:
            effective_config = {"domain_id": {"value": 0, "source": "cli"}}

            def __init__(self, **kwargs):
                calls.append("open")

            def ok(self):
                calls.append("ok")
                return True

            def graph(self, *, timeout):
                calls.append("graph")
                raise aos.CommunicationError("PRIVATE_GRAPH_ERROR")

            def shutdown(self):
                calls.append("shutdown")

            def telemetry_snapshot(self):
                calls.append("telemetry")
                return {"active_resources": {}}

        options = {
            "timeout": 3, "probe_deadline": time.monotonic() + 3,
            "config": str(self.config), "config_source": "argument",
            "domain_id": 0, "domain_source": "argument",
            "domain_environment": "", "process_config_present": False,
            "security_profile": None, "security_profile_explicit": False,
        }

        def emit(report, stage):
            report["probe"]["stage"] = stage
            reports.append(copy.deepcopy(report))

        with mock.patch.object(doctor, "_native", return_value=SimpleNamespace(Context=Context)), \
             mock.patch.object(doctor, "_emit", side_effect=emit), \
             mock.patch.object(sys, "path", list(sys.path)):
            doctor._probe(options)
        report = reports[-1]
        self.assertEqual(calls, ["open", "ok", "graph", "shutdown", "telemetry"])
        self.assertEqual(report["status"], "error")
        self.assertEqual(report["failures"][0]["code"], "COMMUNICATION_FAILED")
        self.assertEqual(report["failures"][0]["stage"], "graph")
        self.assertEqual(report["shutdown"]["status"], "ok")
        self.assertEqual(report["telemetry"]["status"], "ok")
        self.assertNotIn("PRIVATE_GRAPH_ERROR", json.dumps(report))

    def test_invalid_timeout_does_not_start_probe(self):
        for value in (0, -1, True, float("nan"), float("inf"), 86401):
            with self.subTest(value=value), mock.patch.object(doctor.subprocess, "Popen") as spawn:
                with self.assertRaises(ValueError):
                    aos.doctor_report(timeout=value)
                spawn.assert_not_called()


if __name__ == "__main__":
    unittest.main()
