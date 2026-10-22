"""Fail-closed evidence gates; synthetic fixtures are not runtime evidence."""

from copy import deepcopy
import contextlib
import importlib.util
import io
import json
import os
from pathlib import Path
import tempfile
import sys
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import zipfile


ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("platform_soak_probe", ROOT / "scripts/platform_soak_probe.py")
probe = importlib.util.module_from_spec(spec)
spec.loader.exec_module(probe)


def config():
    return probe.load_json(ROOT / "config/platform-soak-smoke.json")


def evidence(configuration):
    """Minimal deliberately synthetic successful local-run shape."""
    counters = {key: 3 for key in (*probe.business_keys(configuration), "parameter_ok", "lifecycle_ok", "churn_closed")}
    counters.update(large_sequence_gaps=1, large_received=99)
    if probe.active(configuration, "churn"):
        counters.update(churn_closed=configuration["workload"]["churn"]["rounds"],
                        churn_timeout=configuration["workload"]["churn"]["rounds"])
    histogram = {"count": 3, "p99_upper_seconds": .1}
    worker = {"context_closed": True, "cleanup_errors": [], "pending_threads": [], "counters": counters,
              "epochs": {"0": counters, "1": counters}, "measurement_counters": dict(counters),
              "measurement_latencies": {key: histogram for key in ("service_ok", "action_accepted", "action_cancel_accepted")}}
    binding = {"source_sha": "a" * 40, "artifact_sha256": {name: "b" * 64 for name in ("wheel", "cli", "router")}}
    exit_record = {"reaped": True, "forced": False, "log_complete": True, "returncode": 0}
    result = {"finished_normally": True, "interrupted": False, "measurement_seconds": configuration["duration_seconds"],
            "config": deepcopy(configuration), "run_id": "synthetic-smoke", "protocol": probe.PROTOCOL,
            "platform": "synthetic", "hardware": {"system": "Darwin", "machine": "arm64", "hostname": "synthetic-mac"},
            "scope": {"kind": "local", "system": "Darwin", "machine": "arm64", "hostname": "synthetic-mac"},
            "configured_client_threads": probe.configured_client_threads(configuration),
            "raw_evidence": {"path": probe.RAW_MANIFEST, "sha256": "0" * 64},
            "binding": binding, "binding_after": deepcopy(binding), "harness_sha256": "c" * 64,
            "harness_after": "c" * 64, "harness_source": {"sha": "d" * 40, "status": ""},
            "input_config_sha256": "e" * 64, "config_after": "e" * 64,
            "resources": {"samples": 35, "failures": [], "unavailable_counts": {}, "peaks": {}, "trends": {}},
            "workers": {"provider": deepcopy(worker), "client": deepcopy(worker)},
            "faults": [{"old_process_reaped": True, "port_closed": True, "old_pid": 1, "new_pid": 2, "epoch": 1, "recovered": True,
                        "recovery_seconds": 2, "recovery_timeout_seconds": 15}],
            "bag": {"status": {"resources_closed": True, "counters": {"written_total": 10}},
                    "manifest": {"clean_close": True}},
            "boundaries": {name: {"passed": True} for name in ("sigint", "context_close")},
            "process_exits": {name: deepcopy(exit_record) for name in ("provider", "client", "boundary_sigint", "boundary_context_close")},
            "disk_bytes": 8192, "failures": []}
    for role, scopes in probe.INFLIGHT_SCOPES.items():
        result["workers"][role]["inflight"] = {name: {"current": 0, "peak": 1, "measurement_peak": 1,
            "started": 3, "finished": 3, "measurement_started": 3, "measurement_finished": 3}
            for name, component in scopes.items() if probe.active(configuration, component)}
    return result


def baseline_rounds(configuration):
    rounds = []
    for index in range(configuration["rounds"]):
        value = evidence(configuration)
        value.update(run_id="synthetic-round-" + str(index), warmup_seconds=300, faults=[])
        value["workers"]["client"]["measurement_counters"]["churn_closed"] = 100
        value["workers"]["client"]["measurement_counters"]["churn_timeout"] = 100
        histogram = {"count": 3, "p50_upper_seconds": .01, "p95_upper_seconds": .05, "p99_upper_seconds": .1}
        for key in (*probe.business_keys(configuration), "action_get_result_terminal", "action_cancel_terminal"):
            value["workers"]["client"]["measurement_latencies"][key] = deepcopy(histogram)
        if probe.transport(configuration) == "mtls":
            value["security"] = {"credentials_deleted": True, "policy_unchanged": True,
                "profile_inputs_unchanged": True, "routers": {"router_0": {"ready": {"locators": ["tls/127.0.0.1:1"]},
                "port_closed": True, "session_closed": True}}}
        rounds.append(value)
    return rounds


def save_synthetic_run(folder, value):
    """Real temporary files for hash tests; none contain a native/runtime result."""
    folder.mkdir(parents=True)
    value = deepcopy(value)
    probe.write_json(folder / "run.json", value)
    for role, worker in value["workers"].items():
        probe.write_json(folder / (role + "-final.json"), worker)
    probe.write_json(folder / "bag-final.json", value["bag"])
    (folder / "resources.jsonl").write_text('{"synthetic":true}\n')
    (folder / "client.log").write_text("synthetic log\n")
    value["raw_evidence"] = probe.seal_raw_evidence(folder, value["run_id"])
    path = folder / "result.json"
    probe.write_json(path, value)
    return path, value


def save_synthetic_baseline(folder, configuration):
    rounds, files = [], []
    for index, value in enumerate(baseline_rounds(configuration)):
        path, value = save_synthetic_run(folder / f"round-{index:02d}", value)
        files.append({"path": str(path.relative_to(folder)), "sha256": probe.digest(path)})
        rounds.append(value)
    result = {"config": configuration, "binding": rounds[0]["binding"], "round_evidence": files,
              "scope": rounds[0]["scope"], "harness_sha256": rounds[0]["harness_sha256"]}
    path = folder / "baseline-result.json"
    probe.write_json(path, result)
    return path, result


class GateTest(unittest.TestCase):
    def assert_rejected(self, configuration, value, code):
        gate = probe.evaluate(configuration, value)
        self.assertEqual(gate["status"], "failed", gate)
        self.assertFalse(gate["continuous_24h_passed"])
        self.assertIn(code, [item["code"] for item in gate["failures"]])

    def test_smoke_cannot_claim_continuous_24_hours(self):
        configuration = config()
        value = evidence(configuration)
        gate = probe.evaluate(configuration, value)
        self.assertEqual(gate["status"], "smoke_passed", gate)
        self.assertFalse(gate["continuous_24h_passed"])
        self.assertFalse(gate["c20_acceptance_passed"])

    def test_smoke_accepts_all_100_measured_churn_cycles(self):
        configuration = config()
        configuration["workload"]["churn"]["rounds"] = 100
        value = evidence(configuration)
        measured = value["workers"]["client"]["measurement_counters"]
        self.assertEqual((measured["churn_closed"], measured["churn_timeout"]), (100, 100))
        self.assertEqual(probe.evaluate(configuration, value)["status"], "smoke_passed")

    def test_smoke_rejects_each_incomplete_measured_churn_count(self):
        configuration = config()
        configuration["workload"]["churn"]["rounds"] = 100
        for key, code in (("churn_closed", "CHURN_INCOMPLETE"),
                          ("churn_timeout", "CHURN_TIMEOUT_INCOMPLETE")):
            with self.subTest(counter=key):
                value = evidence(configuration)
                client = value["workers"]["client"]
                client["measurement_counters"][key] = 99
                self.assertEqual(client["counters"][key], 100)
                self.assert_rejected(configuration, value, code)

    def test_smoke_cumulative_churn_cannot_replace_measured_cycles(self):
        configuration = config()
        configuration["workload"]["churn"]["rounds"] = 100
        value = evidence(configuration)
        client = value["workers"]["client"]
        client["measurement_counters"].update(churn_closed=0, churn_timeout=0)
        self.assertEqual((client["counters"]["churn_closed"], client["counters"]["churn_timeout"]), (100, 100))
        self.assert_rejected(configuration, value, "CHURN_INCOMPLETE")
        self.assert_rejected(configuration, value, "CHURN_TIMEOUT_INCOMPLETE")

    def test_inactive_churn_does_not_require_churn_evidence(self):
        for case in ("small_topic", "small_topic_ceiling", "large_topic", "service", "action"):
            with self.subTest(case=case):
                configuration = config()
                configuration["workload_case"] = case
                value = evidence(configuration)
                client = value["workers"]["client"]
                for counters in (client["counters"], client["measurement_counters"]):
                    counters.pop("churn_closed", None)
                    counters.pop("churn_timeout", None)
                self.assertEqual(probe.evaluate(configuration, value)["status"], "smoke_passed")

    def test_unfrozen_24_hour_template_refuses_to_start(self):
        configuration = probe.load_json(ROOT / "config/platform-soak-unfrozen.json")
        with self.assertRaises(probe.ProbeError) as raised:
            probe.validate_config(configuration)
        self.assertEqual(raised.exception.code, "UNFROZEN_THRESHOLD")

    def test_formal_run_cannot_relabel_short_measurement(self):
        configuration = config()
        value = evidence(configuration)
        configuration.update(mode="soak", duration_seconds=probe.DAY,
                             freeze={"source_sha": value["binding"]["source_sha"],
                                     "artifact_sha256": value["binding"]["artifact_sha256"]})
        self.assert_rejected(configuration, value, "LESS_THAN_24_HOURS")

    def test_candidate_changes_cannot_inherit_a_run(self):
        for key in ("source_sha", "artifact_sha256"):
            with self.subTest(key=key):
                configuration = config()
                value = evidence(configuration)
                value["binding_after"][key] = "changed"
                self.assert_rejected(configuration, value, "ARTIFACT_CHANGED")

    def test_interruption_and_sampling_gap_are_distinct_failures(self):
        configuration = config()
        value = evidence(configuration)
        value["interrupted"] = True
        self.assert_rejected(configuration, value, "INTERRUPTED")
        value["interrupted"] = False
        value["resources"]["failures"].append({"code": "SAMPLING_INTERRUPTED", "gap_seconds": 100})
        self.assert_rejected(configuration, value, "SAMPLING_INTERRUPTED")

    def test_frozen_candidate_cannot_be_replaced_even_if_begin_and_end_match(self):
        for field, code in (("source_sha", "SOURCE_MISMATCH"), ("artifact_sha256", "ARTIFACT_MISMATCH")):
            with self.subTest(field=field):
                configuration = config()
                value = evidence(configuration)
                configuration.update(mode="soak", duration_seconds=probe.DAY,
                                     freeze=deepcopy(value["binding"]))
                value["measurement_seconds"] = probe.DAY
                value["binding"][field] = "replacement"
                value["binding_after"] = deepcopy(value["binding"])
                self.assert_rejected(configuration, value, code)

    def test_expected_fault_error_storm_cannot_hide_an_unexpected_failure(self):
        metrics = probe.Metrics("test")
        for _ in range(100):
            metrics.error("fault", RuntimeError("expected"), expected=True)
        metrics.error("business", probe.ProbeError("WRONG_RESULT", "bad value"))
        snapshot = metrics.snapshot()
        self.assertEqual(snapshot["counters"]["expected_fault_errors"], 100)
        self.assertEqual(snapshot["counters"]["unexpected_errors"], 1)
        self.assertEqual(snapshot["errors"][-1]["code"], "WRONG_RESULT")
        self.assertLessEqual(len(snapshot["errors"]), 32)

    def test_old_epoch_business_does_not_prove_recovery(self):
        configuration = config()
        value = evidence(configuration)
        del value["workers"]["client"]["epochs"]["1"]["action_canceled"]
        self.assert_rejected(configuration, value, "ROUTER_RECOVERY_FAILED")

    def test_duplicate_business_execution_fails_even_when_results_succeed(self):
        configuration = config()
        value = evidence(configuration)
        value["workers"]["provider"]["counters"]["service_duplicate_execution"] = 1
        self.assert_rejected(configuration, value, "BUSINESS_REPLAY")

    def test_clean_file_without_resource_cleanup_cannot_pass(self):
        configuration = config()
        value = evidence(configuration)
        value["bag"]["status"]["resources_closed"] = False
        self.assert_rejected(configuration, value, "BAG_EVIDENCE_MISSING")
        value = evidence(configuration)
        value["process_exits"]["client"]["forced"] = True
        self.assert_rejected(configuration, value, "PROCESS_CLEANUP_FAILED")

    def test_resource_and_latency_thresholds_are_enforced(self):
        configuration = config()
        value = evidence(configuration)
        value["resources"]["peaks"]["client:rss_bytes"] = configuration["thresholds"]["rss_bytes_per_process"] + 1
        self.assert_rejected(configuration, value, "RESOURCE_PEAK")
        value = evidence(configuration)
        configuration["thresholds"]["service_latency_p99_seconds"] = .05
        self.assert_rejected(configuration, value, "LATENCY_THRESHOLD")

    def test_config_or_script_changed_mid_run_cannot_pass(self):
        for field, code in (("config_after", "CONFIG_CHANGED"), ("harness_after", "HARNESS_CHANGED")):
            with self.subTest(field=field):
                configuration = config()
                value = evidence(configuration)
                value[field] = "different"
                self.assert_rejected(configuration, value, code)

    def test_queue_bytes_include_reserved_copies_and_unknown_is_not_zero(self):
        stats = {"queued": 1, "depth": 2, "bytes": {"queued_bytes": 90, "reserved_bytes": 20, "queue_limit_bytes": 100}}
        self.assertEqual(probe.queue_violations("test", stats, True), ["test:queue_byte_bound"])
        stats["bytes"] = None
        self.assertEqual(probe.queue_violations("test", stats, True), ["test:queue_bytes_unavailable"])
        self.assertEqual(probe.queue_violations("test", stats, False), [])

    def test_request_ledger_distinguishes_parallel_sequences_from_replays(self):
        ledger = probe.SequenceLedger(16)
        for sequence in (1, 8, 16, 2):
            self.assertFalse(ledger.mark(sequence))
        self.assertTrue(ledger.mark(8))
        with self.assertRaises(probe.ProbeError):
            ledger.mark(17)

    def test_online_trend_and_histogram_preserve_growth_and_tail(self):
        trend = probe.Trend()
        for seconds, value in ((0, 100), (1800, 150), (3600, 200)):
            trend.add(seconds, value)
        self.assertAlmostEqual(trend.report()["growth_per_hour"], 100)
        histogram = probe.Histogram()
        for _ in range(99):
            histogram.add(.001)
        histogram.add(2)
        self.assertGreaterEqual(histogram.report()["p99_upper_seconds"], .001)
        self.assertEqual(histogram.report()["maximum_seconds"], 2)

    def test_payload_is_the_actual_requested_serialized_size(self):
        for size in (1024, 256 * 1024):
            message = probe.payload("run", "provider", 1, 9, size)
            probe.validate_payload(message, "run", "provider", size)
            self.assertEqual(len(json.dumps(message, separators=(",", ":")).encode()), size)
            message["value"] += 1
            with self.assertRaises(probe.ProbeError):
                probe.validate_payload(message, "run", "provider", size)


class InflightTest(unittest.TestCase):
    def test_real_overlaps_crossing_measurement_and_exception_cleanup(self):
        phase = ["warming"]
        metrics = probe.Metrics("synthetic", lambda: phase[0])
        entered = threading.Barrier(5)
        release = threading.Event()

        def operation():
            with metrics.track_inflight("service_calls"):
                entered.wait(timeout=3)
                release.wait(timeout=3)

        threads = [threading.Thread(target=operation) for _ in range(4)]
        for thread in threads:
            thread.start()
        try:
            entered.wait(timeout=3)
            before = metrics.snapshot()["inflight"]["service_calls"]
            self.assertEqual((before["current"], before["peak"], before["measurement_peak"]), (4, 4, 0))
            phase[0] = "measuring"
            self.assertEqual(metrics.snapshot()["inflight"]["service_calls"]["measurement_peak"], 4)
        finally:
            release.set()
            for thread in threads:
                thread.join(timeout=3)
        self.assertTrue(all(not thread.is_alive() for thread in threads))
        with self.assertRaisesRegex(RuntimeError, "synthetic failure"):
            with metrics.track_inflight("service_calls"):
                raise RuntimeError("synthetic failure")
        final = metrics.snapshot()["inflight"]["service_calls"]
        self.assertEqual((final["current"], final["started"], final["finished"]), (0, 5, 5))
        self.assertEqual((final["peak"], final["measurement_peak"]), (4, 4))
        self.assertEqual((final["measurement_started"], final["measurement_finished"]), (1, 5))

    def worker(self):
        worker = object.__new__(probe.Worker)
        worker.config = config()
        worker.config["workload"]["action"]["concurrency"] = 8
        worker.metrics = probe.Metrics("client", lambda: "measuring")
        worker.control = SimpleNamespace(read=lambda: {"epoch": 0})
        worker.stop = threading.Event()
        worker.next_sequence = probe.Counter()
        worker.sequence_lock = threading.Lock()
        worker.manifest = {"run_id": "synthetic"}
        worker.provider_instance = "synthetic-provider"
        return worker

    def test_service_call_and_action_request_use_observed_scopes_without_padding(self):
        worker = self.worker()

        def service(request, timeout):
            self.assertEqual(worker.metrics.snapshot()["inflight"]["service_calls"]["current"], 1)
            worker.stop.set()
            return request

        worker.service = SimpleNamespace(call=service)
        worker.call_services()
        self.assertEqual(worker.metrics.snapshot()["inflight"]["service_calls"]["finished"], 1)
        worker = self.worker()
        worker.aos = SimpleNamespace(GoalStatus=SimpleNamespace(UNKNOWN=0, SUCCEEDED=4, CANCELED=5))

        def send_goal(request, feedback_callback, timeout):
            self.assertEqual(worker.metrics.snapshot()["inflight"]["action_requests"]["current"], 1)
            value = {**request, "instance": worker.provider_instance, "completed": request["steps"],
                     "total": request["steps"] * (request["steps"] + 1) // 2}
            feedback_callback({"feedback": value})
            result = {"status": 4, "result": value}
            goal = SimpleNamespace(accepted=True, get_result_async=lambda **_: SimpleNamespace(result=lambda _: result))
            worker.stop.set()
            return SimpleNamespace(result=lambda _: goal)

        worker.action = SimpleNamespace(send_goal_async=send_goal)
        worker.call_actions()
        final = worker.metrics.snapshot()
        self.assertEqual(final["counters"]["action_succeeded"], 1)
        self.assertEqual((final["inflight"]["action_requests"]["current"],
                          final["inflight"]["action_requests"]["peak"],
                          final["inflight"]["action_requests"]["measurement_peak"]), (0, 1, 1))
        self.assertEqual(worker.config["workload"]["action"]["concurrency"], 8)

    def test_uncertain_action_keeps_load_slot_during_fault_cooldown(self):
        worker = self.worker()
        worker.config["workload"]["action"]["duration_seconds"] = 30
        worker.config["operation_timeout_seconds"] = 5
        worker.control.expected_fault = lambda *_: True
        worker.aos = SimpleNamespace(WaitTimeoutError=TimeoutError,
                                    NoResponseError=ConnectionError,
                                    CommunicationError=ConnectionError)
        now = [100.0]
        waits = []

        def send_goal(*_args, **_kwargs):
            now[0] += 5
            raise TimeoutError("provider execution is unknown")

        def wait(seconds):
            waits.append((seconds, worker.metrics.snapshot()["inflight"]["action_requests"]["current"]))
            now[0] += seconds
            worker.stop.set()
            return True

        worker.action = SimpleNamespace(send_goal_async=send_goal)
        with patch.object(probe.time, "monotonic", side_effect=lambda: now[0]), \
                patch.object(worker.stop, "wait", side_effect=wait):
            worker.call_actions()
        self.assertEqual(waits, [(30, 1), (0, 0)])
        final = worker.metrics.snapshot()
        self.assertEqual(final["counters"]["action_sent"], 1)
        self.assertEqual(final["counters"]["expected_fault_errors"], 1)
        self.assertEqual(final["inflight"]["action_requests"]["finished"], 1)

    def test_missing_unclosed_or_unmeasured_inflight_cannot_pass(self):
        configuration = config()
        for field, replacement, code in (("current", 1, "INFLIGHT_NOT_CLOSED"),
                ("finished", 2, "INFLIGHT_NOT_CLOSED"), ("measurement_peak", 0, "INFLIGHT_EVIDENCE_INVALID")):
            with self.subTest(field=field):
                value = evidence(configuration)
                value["workers"]["provider"]["inflight"]["action_executions"][field] = replacement
                self.assertIn(code, [failure["code"] for failure in probe.evaluate(configuration, value)["failures"]])
        value = evidence(configuration)
        del value["workers"]["client"]["inflight"]
        self.assertIn("INFLIGHT_EVIDENCE_MISSING", [failure["code"] for failure in probe.evaluate(configuration, value)["failures"]])
        value = evidence(configuration)
        self.assertEqual(probe.evaluate(configuration, value)["status"], "smoke_passed")
        self.assertLess(value["workers"]["client"]["inflight"]["action_requests"]["measurement_peak"],
                        configuration["workload"]["action"]["concurrency"])


class RawEvidenceTest(unittest.TestCase):
    def test_offline_evaluate_checks_files_and_summary_hash_boundary(self):
        with tempfile.TemporaryDirectory() as directory:
            path, value = save_synthetic_run(Path(directory) / "run", evidence(config()))
            manifest = probe.load_json(path.parent / probe.RAW_MANIFEST)
            self.assertEqual(manifest["excluded"], [probe.RAW_MANIFEST, "result.json"])
            self.assertFalse(set(probe.RAW_EXCLUSIONS) & {item["path"] for item in manifest["files"]})
            self.assertEqual(probe.load_verified_run(path), value)
            with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                self.assertEqual(probe.main(["evaluate", "--result", str(path)]), 0)
            # A result cannot prove its own hash: an outer baseline/freeze pins
            # its bytes. Its copied worker values still must agree with raw data.
            value["workers"]["client"]["counters"]["service_ok"] += 1
            probe.write_json(path, value)
            with self.assertRaises(probe.ProbeError) as caught:
                probe.load_verified_run(path)
            self.assertEqual(caught.exception.code, "RAW_SUMMARY_MISMATCH")

    def test_raw_mutations_removals_additions_and_manifest_edits_are_rejected(self):
        for mutation, code in (("resource", "RAW_EVIDENCE_CHANGED"), ("same_size_log", "RAW_EVIDENCE_CHANGED"),
                               ("remove", "RAW_EVIDENCE_INVENTORY"), ("add", "RAW_EVIDENCE_INVENTORY"),
                               ("manifest", "RAW_EVIDENCE_CHANGED")):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as directory:
                path, _ = save_synthetic_run(Path(directory) / "run", evidence(config()))
                if mutation == "resource":
                    (path.parent / "resources.jsonl").write_text("changed\n")
                elif mutation == "same_size_log":
                    log = path.parent / "client.log"
                    log.write_bytes(b"X" * log.stat().st_size)
                elif mutation == "remove":
                    (path.parent / "client.log").unlink()
                elif mutation == "add":
                    (path.parent / "unexpected.json").write_text("{}")
                else:
                    (path.parent / probe.RAW_MANIFEST).write_text("{}")
                with self.assertRaises(probe.ProbeError) as caught:
                    probe.load_verified_run(path)
                self.assertEqual(caught.exception.code, code)
                with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                    self.assertEqual(probe.main(["evaluate", "--result", str(path)]), 1)

    def test_manifest_cannot_exclude_raw_files_or_follow_external_links(self):
        with tempfile.TemporaryDirectory() as directory:
            path, value = save_synthetic_run(Path(directory) / "run", evidence(config()))
            manifest_path = path.parent / probe.RAW_MANIFEST
            manifest = probe.load_json(manifest_path)
            manifest["excluded"].append("resources.jsonl")
            probe.write_json(manifest_path, manifest)
            value["raw_evidence"]["sha256"] = probe.digest(manifest_path)
            probe.write_json(path, value)
            with self.assertRaises(probe.ProbeError) as caught:
                probe.load_verified_run(path)
            self.assertEqual(caught.exception.code, "RAW_EVIDENCE_INVALID")
            linked = path.parent / "linked"
            if os.name == "nt":
                # Do not require Developer Mode or symlink privileges to test
                # the guard. A real directory would otherwise pass inventory.
                linked.mkdir()
                is_symlink = Path.is_symlink
                link_metadata = patch.object(
                    Path, "is_symlink", lambda item: item == linked or is_symlink(item))
            else:
                linked.symlink_to(Path(directory), target_is_directory=True)
                link_metadata = contextlib.nullcontext()
            with link_metadata, self.assertRaises(probe.ProbeError) as caught:
                probe.seal_raw_evidence(path.parent, value["run_id"])
            self.assertEqual(caught.exception.code, "RAW_EVIDENCE_PATH")


class ArtifactBindingTest(unittest.TestCase):
    def test_installed_file_tamper_and_source_mismatch_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            folder = Path(directory)
            package = folder / "site/aos_sdk/__init__.py"
            package.parent.mkdir(parents=True)
            package.write_bytes(b"# installed package\n")
            native = package.with_name("_native.so")
            native.write_bytes(b"test-only binary fixture")
            wheel = folder / "candidate.whl"
            with zipfile.ZipFile(wheel, "w") as archive:
                for item in (package, native):
                    archive.writestr("aos_sdk/" + item.name, item.read_bytes())
            cli, router = folder / "aos", folder / "zenohd"
            cli.write_bytes(b"test-only CLI fixture")
            router.write_bytes(b"test-only router fixture")
            source_sha = "a" * 40
            proof = {"status": "passed", "tracked_changes": "", "source_sha": source_sha,
                     "final_source_sha": source_sha, "final_source_status": "", "commands": [{"returncode": 0}],
                     "wheel": {"path": wheel.name, "sha256": probe.digest(wheel)},
                     "cli": {"sha256": probe.digest(cli)}, "zenohd": {"sha256": probe.digest(router)}}
            path = folder / "provenance.json"
            probe.write_json(path, proof)
            binding = probe.verify_artifacts(path, source_sha, cli, router, package=package, native=native)
            self.assertEqual(binding["source_sha"], source_sha)
            with self.assertRaises(probe.ProbeError) as raised:
                probe.verify_artifacts(path, "f" * 40, cli, router, package=package, native=native)
            self.assertEqual(raised.exception.code, "SOURCE_MISMATCH")
            native.write_bytes(b"changed installed binary")
            with self.assertRaises(probe.ProbeError) as raised:
                probe.verify_artifacts(path, source_sha, cli, router, package=package, native=native)
            self.assertEqual(raised.exception.code, "INSTALLED_MISMATCH")


class BaselineGateTest(unittest.TestCase):
    def config(self):
        return probe.load_json(ROOT / "config/platform-baseline-local.json")

    def rounds(self, configuration):
        return baseline_rounds(configuration)

    def test_baseline_constraints_reject_short_or_reduced_profile(self):
        for key, value, code in (("warmup_seconds", 299, "BASELINE_DURATION"),
                                 ("duration_seconds", 899, "BASELINE_DURATION"),
                                 ("rounds", 2, "BASELINE_DURATION")):
            with self.subTest(key=key):
                configuration = self.config()
                configuration[key] = value
                with self.assertRaises(probe.ProbeError) as caught:
                    probe.validate_config(configuration)
                self.assertEqual(caught.exception.code, code)
        configuration = self.config()
        configuration["workload"]["action"]["concurrency"] = 7
        with self.assertRaises(probe.ProbeError) as caught:
            probe.validate_config(configuration)
        self.assertEqual(caught.exception.code, "BASELINE_WORKLOAD")

    def test_three_valid_rounds_are_baseline_only(self):
        configuration = self.config()
        rounds = self.rounds(configuration)
        one = probe.evaluate(configuration, rounds[0])
        self.assertEqual(one["status"], "baseline_round_passed", one)
        self.assertFalse(one["baseline_passed"])
        gate = probe.baseline_gate(configuration, rounds)
        self.assertTrue(gate["baseline_passed"], gate)
        self.assertFalse(gate["continuous_24h_passed"])
        self.assertFalse(gate["c20_acceptance_passed"])

    def test_partial_duplicated_or_changed_candidate_rounds_fail(self):
        configuration = self.config()
        rounds = self.rounds(configuration)
        self.assertFalse(probe.baseline_gate(configuration, rounds[:2])["baseline_passed"])
        for field, replacement in (("binding", {"source_sha": "different"}),
                                   ("run_id", rounds[0]["run_id"]), ("harness_sha256", "changed"),
                                   ("warmup_seconds", 299), ("measurement_seconds", 899)):
            with self.subTest(field=field):
                changed = deepcopy(rounds)
                changed[1][field] = replacement
                self.assertFalse(probe.baseline_gate(configuration, changed)["baseline_passed"])

    def test_measured_churn_cannot_be_replaced_by_warmup_counts(self):
        configuration = self.config()
        rounds = self.rounds(configuration)
        rounds[0]["workers"]["client"]["measurement_counters"]["churn_closed"] = 99
        gate = probe.baseline_gate(configuration, rounds)
        self.assertFalse(gate["baseline_passed"])
        self.assertIn("CHURN_INCOMPLETE", [item["code"] for item in gate["failures"][0]["failures"]])

    def test_single_cases_and_ceiling_do_not_silently_enable_mixed_load(self):
        configuration = self.config()
        configuration["workload_case"] = "small_topic_ceiling"
        with self.assertRaises(probe.ProbeError):
            probe.validate_config(configuration)
        configuration["workload"]["small_topic"]["hz"] = 1000
        probe.validate_config(configuration)
        self.assertTrue(probe.active(configuration, "small_topic"))
        for component in ("large_topic", "service", "action", "bag", "management", "churn"):
            self.assertFalse(probe.active(configuration, component))
        self.assertEqual(probe.business_keys(configuration), ["small_fast_0_received", "small_fast_1_received"])

    def test_saved_round_hash_and_aggregate_binding_cannot_change(self):
        configuration = self.config()
        with tempfile.TemporaryDirectory() as directory:
            folder = Path(directory)
            path, result = save_synthetic_baseline(folder, configuration)
            self.assertTrue(probe.evaluate_baseline(path)[0]["baseline_passed"])
            result["binding"] = {"source_sha": "relabelled"}
            probe.write_json(path, result)
            self.assertFalse(probe.evaluate_baseline(path)[0]["baseline_passed"])
            (folder / result["round_evidence"][0]["path"]).write_text("{}")
            with self.assertRaises(probe.ProbeError) as caught:
                probe.evaluate_baseline(path)
            self.assertEqual(caught.exception.code, "BASELINE_MISMATCH")

    def test_summary_retains_all_rounds_and_does_not_pool_quantiles(self):
        configuration = self.config()
        rounds = self.rounds(configuration)
        for index, value in enumerate(rounds):
            value["workers"]["client"]["measurement_latencies"]["service_ok"] = {"count": 100, "p99_upper_seconds": index + 1}
        summary = probe.summarize_rounds(rounds)
        self.assertEqual(len(summary["measured_rounds"]), 3)
        self.assertEqual(summary["across_round_ranges"]["latency_seconds/service_ok/p99_upper_seconds"]["maximum"], 3)
        self.assertEqual(summary["threshold_proposal"]["status"], "not_frozen")
        self.assertIsNone(summary["threshold_proposal"]["values"])


class FreezeMatrixTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temporary = tempfile.TemporaryDirectory(prefix="aos-synthetic-matrix-")
        folder = Path(cls.temporary.name)
        baselines = []
        first = None
        for case in probe.CASES:
            for network in probe.TRANSPORTS:
                configuration = probe.load_json(ROOT / "config/platform-baseline-local.json")
                configuration.update(workload_case=case, transport=network)
                configuration["workload"]["small_topic"]["hz"] = 1000 if case.endswith("_ceiling") else 100
                path, record = save_synthetic_baseline(folder / (case + "-" + network), configuration)
                first = first or record
                baselines.append({"scope_id": "synthetic-mac", "path": str(path), "sha256": probe.digest(path)})
        cls.binding, cls.scope = first["binding"], first["scope"]
        cls.harness_sha256 = first["harness_sha256"]
        cls.configuration = probe.load_json(ROOT / "config/platform-soak-unfrozen.json")
        cls.configuration["thresholds"] = {key: (True if key == "require_queue_byte_stats" else 1_000_000)
                                            for key in cls.configuration["thresholds"]}
        cls.configuration["freeze"] = {
            "id": "synthetic-not-runtime-evidence", "source_sha": cls.binding["source_sha"],
            "artifact_sha256": cls.binding["artifact_sha256"], "harness_sha256": cls.harness_sha256,
            "baseline_matrix": {"schema_version": 1, "local": [{"id": "synthetic-mac",
                **{key: cls.scope[key] for key in ("system", "machine", "hostname")},
                "artifact_sha256": cls.binding["artifact_sha256"]}], "cross_machine": []},
            "baseline_evidence": baselines,
        }

    @classmethod
    def tearDownClass(cls):
        cls.temporary.cleanup()

    def verify(self, configuration):
        return probe.verify_freeze(configuration, self.binding, scope=self.scope, harness_sha256=self.harness_sha256)

    def assert_rejected(self, configuration, code):
        with self.assertRaises(probe.ProbeError) as caught:
            self.verify(configuration)
        self.assertEqual(caught.exception.code, code)

    def test_all_14_cells_are_required_for_a_declared_local_host(self):
        configuration = deepcopy(self.configuration)
        probe.validate_config(configuration)
        result = self.verify(configuration)
        self.assertEqual(len(result["verified_cells"]), 14)
        self.assertFalse(result["cross_machine_covered"])
        self.assertFalse(result["c20_acceptance_passed"])
        configuration["freeze"]["baseline_evidence"].pop()
        self.assert_rejected(configuration, "BASELINE_MATRIX_INCOMPLETE")

    def test_duplicate_cell_cannot_replace_a_missing_case_or_transport(self):
        configuration = deepcopy(self.configuration)
        configuration["freeze"]["baseline_evidence"][-1] = deepcopy(configuration["freeze"]["baseline_evidence"][0])
        self.assert_rejected(configuration, "BASELINE_MATRIX_DUPLICATE")

    def test_mac_evidence_cannot_fill_a_declared_linux_scope(self):
        configuration = deepcopy(self.configuration)
        matrix = configuration["freeze"]["baseline_matrix"]
        linux = deepcopy(matrix["local"][0])
        linux.update(id="synthetic-linux", system="Linux", hostname="synthetic-linux")
        matrix["local"].append(linux)
        aliases = [{**item, "scope_id": "synthetic-linux"} for item in configuration["freeze"]["baseline_evidence"]]
        configuration["freeze"]["baseline_evidence"].extend(aliases)
        self.assert_rejected(configuration, "BASELINE_SCOPE_MISMATCH")

    def test_cross_machine_scope_is_explicit_and_never_satisfied_by_local_reports(self):
        configuration = deepcopy(self.configuration)
        matrix = configuration["freeze"]["baseline_matrix"]
        linux = deepcopy(matrix["local"][0])
        linux.update(id="synthetic-linux", system="Linux", hostname="synthetic-linux")
        matrix["local"].append(linux)
        matrix["cross_machine"] = [{"id": "mac-to-linux", "provider_id": "synthetic-mac", "client_id": "synthetic-linux"}]
        self.assert_rejected(configuration, "CROSS_MACHINE_BASELINE_UNSUPPORTED")

    def test_formal_freeze_cannot_omit_scope_or_change_harness(self):
        configuration = deepcopy(self.configuration)
        del configuration["freeze"]["baseline_matrix"]
        with self.assertRaises(probe.ProbeError) as caught:
            probe.validate_config(configuration)
        self.assertEqual(caught.exception.code, "UNFROZEN_MATRIX")
        configuration = deepcopy(self.configuration)
        configuration["freeze"]["harness_sha256"] = "f" * 64
        self.assert_rejected(configuration, "HARNESS_MISMATCH")


class NativeTlsGateTest(unittest.TestCase):
    def test_policy_enumerates_owned_nodes_and_real_mixed_operations(self):
        configuration = probe.load_json(ROOT / "config/platform-baseline-local.json")
        policy = probe.security_policy(configuration, "/unique_run")
        provider, client = policy["subjects"]
        self.assertEqual(len(provider["nodes"]), 2)
        self.assertEqual(len(client["nodes"]), 104)
        self.assertEqual(len({(node["namespace"], node["name"]) for node in client["nodes"]}), 104)
        self.assertTrue(any(item["operations"] == ["parameters_read", "parameters_write"] for item in client["systems"]))
        self.assertTrue(any(item["operations"] == ["lifecycle_read", "lifecycle_change"] for item in client["systems"]))
        self.assertTrue(any(item["node"]["name"] == "aos_bag_recorder" and item["operations"] == ["feedback", "status"] for item in client["endpoints"]))
        self.assertTrue(all("*" not in item["name"] and item["name"].startswith("/unique_run/") for subject in policy["subjects"] for item in subject["endpoints"]))

    def test_tls_port_readiness_without_native_report_cannot_pass(self):
        configuration = config()
        configuration["transport"] = "mtls"
        value = evidence(configuration)
        self.assertIn("NATIVE_TLS_EVIDENCE_MISSING", [item["code"] for item in probe.evaluate(configuration, value)["failures"]])
        value["security"] = {"credentials_deleted": True, "policy_unchanged": True,
                             "profile_inputs_unchanged": True,
                             "routers": {"router_0": {"ready": {"locators": ["tls/127.0.0.1:1234"]},
                                                      "port_closed": True, "session_closed": True}}}
        self.assertEqual(probe.evaluate(configuration, value)["status"], "smoke_passed")
        value["security"]["routers"]["router_0"]["ready"]["locators"] = ["tcp/127.0.0.1:1234"]
        self.assertIn("NATIVE_TLS_EVIDENCE_MISSING", [item["code"] for item in probe.evaluate(configuration, value)["failures"]])
        value["security"]["credentials_deleted"] = False
        self.assertIn("TLS_FIXTURE_CLEANUP", [item["code"] for item in probe.evaluate(configuration, value)["failures"]])

    def test_native_readiness_line_is_visible_before_owned_process_exits(self):
        with tempfile.TemporaryDirectory() as directory:
            child = (
                'import json,socket,time; listener=socket.socket(); listener.bind(("127.0.0.1",0)); '
                'listener.listen(); print(json.dumps({"event":"router_ready","data":'
                '{"locators":["tls/127.0.0.1:1"],"test_port":listener.getsockname()[1]}}), flush=True); '
                'time.sleep(30)'
            )
            process = probe.OwnedProcess("test-only-readiness", [sys.executable, "-I", "-c",
                'import subprocess,sys,time; subprocess.Popen([sys.executable,"-I","-c",sys.argv[1]]); time.sleep(30)',
                child],
                directory, 4096)
            try:
                observed = probe.wait_until(lambda: probe.jsonl_events(process.log), 2,
                                            "TEST_READINESS", "small log line remained buffered")
                self.assertEqual(observed[0]["event"], "router_ready")
                port = observed[0]["data"]["test_port"]
                self.assertTrue(probe.tcp_ready(port))
                self.assertIsNone(process.process.poll())
            finally:
                result = process.stop()
            self.assertTrue(result["reaped"])
            self.assertTrue(result["log_complete"])
            self.assertEqual(result["forced"], os.name == "nt")
            self.assertTrue(process.process.stdout.closed)
            self.assertFalse(probe.tcp_ready(port))


if __name__ == "__main__":
    unittest.main()
