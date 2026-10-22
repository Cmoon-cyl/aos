"""Offline adapter contract checks; no SDK import, network, native or MCAP claim.

Run with stdlib unittest. The fake Context and publisher only test ownership,
bounds, loss accounting, and the same live/replay parser. Real Core/Topic/bag
and live-provider zero-call acceptance are explicitly separate gates.
"""

import os
import ast
import copy
from contextlib import redirect_stdout
import importlib.util
import io
import json
from pathlib import Path
import sys
import tempfile
import threading
import time
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import patch
import uuid

ROOT = Path(__file__).resolve().parents[2]
SOURCE = ROOT / "examples/memory_server/request_observer.py"
spec = importlib.util.spec_from_file_location("request_observer_offline", SOURCE)
observer = importlib.util.module_from_spec(spec)
spec.loader.exec_module(observer)


class FakeContext:
    def __init__(self):
        self.context_id = str(uuid.uuid4())
        self.enabled = False
        self.capacity = 0
        self.records = []
        self.events = self.dropped = 0
        self.lock = threading.Lock()
        self.fail_drain = False

    def telemetry_snapshot(self):
        with self.lock:
            return {"context_id": self.context_id, "process_id": 123, "log_capture_enabled": self.enabled,
                "events_total": self.events, "log_capacity": self.capacity, "log_queued": len(self.records),
                "log_dropped_total": self.dropped, "log_write_failures_total": 0,
                "rejected_events_total": 0, "metric_keys_dropped_total": 0}

    def enable_log_capture(self, capacity):
        with self.lock:
            self.capacity, self.enabled = capacity, True

    def disable_log_capture(self):
        with self.lock:
            self.enabled = False

    def drain_log_records(self):
        with self.lock:
            if self.fail_drain:
                raise RuntimeError("offline injected capture failure")
            records, self.records = self.records, []
            return records

    def emit(self, **fields):
        with self.lock:
            self.events += 1
            record = {"schema_version": 1, "process_id": 123, "context_id": self.context_id,
                "sequence": self.events, "system_time_unix_ns": "1792537200123456789",
                "steady_time_ns": 123456789, "identity_trust": "unauthenticated",
                "component": "service", "event": "zenoh.service.query_received", "outcome": "ok", **fields}
            if self.enabled:
                self.records.append(record)
                while len(self.records) > self.capacity:
                    self.records.pop(0)
                    self.dropped += 1
            return copy.deepcopy(record)


class FakeTransport:
    def __init__(self, *, gate=None, fail=False, context=None):
        self.context = context or FakeContext()
        self.gate = gate
        self.fail = fail
        self.entered = threading.Event()
        self.published = []
        self.closed = False

    def publish(self, batch):
        self.entered.set()
        if self.gate is not None:
            if not self.gate.wait(3):
                raise TimeoutError("offline publisher gate")
        if self.fail:
            raise RuntimeError("offline publication failure")
        self.published.append(copy.deepcopy(batch))

    def close(self):
        self.closed = True


class ObserverFixture(unittest.TestCase):
    def exporter(self, source=None, transport=None, **options):
        source = source or FakeContext()
        transport = transport or FakeTransport()
        exporter = observer.RequestTelemetryExporter(source, topic="/test/request_telemetry",
            name="exporter", namespace="/test", _transport_factory=lambda **_: transport,
            poll_interval=0.01, **options)
        self.addCleanup(exporter.close)
        self.assertTrue(transport.entered.wait(1))
        return source, transport, exporter

    def wait_for(self, condition):
        deadline = time.monotonic() + 2
        while not condition() and time.monotonic() < deadline:
            threading.Event().wait(0.005)
        self.assertTrue(condition())


class ExporterTest(ObserverFixture):
    def test_real_metadata_shape_preserves_service_action_ids_and_missing_fields(self):
        source, transport, exporter = self.exporter()
        service = source.emit(request_id="e57a517e-6d69-4f80-8867-b45c6f333c41",
            endpoint="/memory/put", correlation_source="incoming_attachment", bytes=123)
        source.emit(component="topic", event="publish")
        action = source.emit(component="action", event="goal.succeeded", goal_id="33" * 16,
            endpoint="/memory/export", system_time_unix_ns=None, duration_ns=999)
        report = exporter.close()
        records = [row for batch in transport.published for row in batch["records"]]
        self.assertEqual(records, [service, action])
        self.assertNotIn("request_id", records[1])
        self.assertEqual([row["sequence"] for row in records], [1, 3])
        self.assertEqual(report["statistics"]["filtered_records_total"], 1)
        self.assertEqual(report["statistics"]["queue_dropped_records_total"], 0)
        self.assertEqual(report["end_to_end_delivery"], "unknown")
        self.assertTrue(report["cleanup_complete"] and report["worker_stopped"])
        self.assertFalse(source.enabled)
        self.assertFalse(report["source_current_read_failed"])
        self.assertEqual(report["source_current"]["log_queued"], 0)
        for batch in transport.published:
            observer.validate_batch(batch)

    def test_existing_capture_pending_records_and_known_flush_are_rejected(self):
        for mode in ("enabled", "pending", "flush", "unknown_executor"):
            with self.subTest(mode=mode):
                source = FakeContext()
                options = {}
                if mode in ("enabled", "pending"):
                    source.enable_log_capture(10)
                    source.emit()
                    if mode == "pending":
                        source.disable_log_capture()
                else:
                    options["known_executors"] = [SimpleNamespace(_flush_logs_on_spin=True) if mode == "flush" else object()]
                before = source.telemetry_snapshot()
                with self.assertRaises(ValueError):
                    observer.RequestTelemetryExporter(source, topic="/test/events", name="x", namespace="/test",
                        _transport_factory=lambda **_: self.fail("rejected ownership must not open transport"), **options)
                self.assertEqual(source.telemetry_snapshot(), before)

    def test_second_owner_rejected_and_completed_owner_released(self):
        source, transport, first = self.exporter(known_executors=[SimpleNamespace(_flush_logs_on_spin=False)])
        with self.assertRaises(ValueError):
            observer.RequestTelemetryExporter(source, topic="/test/events", name="x", namespace="/test",
                _transport_factory=lambda **_: FakeTransport())
        first.close()
        _, _, second = self.exporter(source=source)
        self.assertNotEqual(first.stream_id, second.stream_id)

    def test_export_context_must_be_different_and_uncaptured(self):
        for same in (False, True):
            source = FakeContext()
            target = source if same else FakeContext()
            if not same:
                target.enable_log_capture(10)
            with self.assertRaises(ValueError):
                observer.RequestTelemetryExporter(source, topic="/test/events", name="x", namespace="/test",
                    _transport_factory=lambda **_: FakeTransport(context=target))
            self.assertFalse(source.enabled)

    def test_record_and_byte_batch_limits_preserve_order(self):
        gate = threading.Event()
        source, transport, exporter = self.exporter(transport=FakeTransport(gate=gate), capture_capacity=256,
            batch_records=3, max_batch_bytes=4096, pending_records=256, pending_bytes=256 * 1024)
        originals = [source.emit(endpoint="/" + "x" * 500, request_id="r" * 128) for _ in range(20)]
        gate.set()
        exporter.close()
        self.assertEqual([row for batch in transport.published for row in batch["records"]], originals)
        self.assertTrue(all(len(batch["records"]) <= 3 and len(observer.encoded(batch)) <= 4096
                            for batch in transport.published))

    def test_source_eviction_and_adapter_overflow_are_distinct(self):
        gate = threading.Event()
        source, transport, exporter = self.exporter(transport=FakeTransport(gate=gate), capture_capacity=8,
            pending_records=2, pending_bytes=4096)
        for _ in range(20):
            source.emit()
        gate.set()
        report = exporter.close()
        self.assertEqual(report["source_after"]["log_dropped_total"], 12)
        self.assertEqual(report["statistics"]["queue_dropped_records_total"], 6)
        self.assertGreater(report["statistics"]["queue_dropped_bytes_total"], 0)
        self.assertEqual(sum(len(batch["records"]) for batch in transport.published), 2)

    def test_invalid_records_do_not_publish_payload_or_reconstruct_identifiers(self):
        source, transport, exporter = self.exporter()
        source.emit(payload="private business text")
        source.emit(request_id=None)
        source.emit(endpoint="/" + "界" * 200)
        source.emit(component="action", event="goal.completed", goal_id="known_goal")
        report = exporter.close()
        records = [row for batch in transport.published for row in batch["records"]]
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["goal_id"], "known_goal")
        self.assertEqual(report["statistics"]["invalid_records_total"], 3)
        self.assertNotIn("private business text", json.dumps(transport.published))

    def test_publish_failure_counts_dropped_records_without_retry(self):
        source, transport, exporter = self.exporter(transport=FakeTransport(fail=True))
        source.emit()
        source.emit(component="action", goal_id="failed_transport_goal")
        report = exporter.close()
        self.assertEqual(report["statistics"]["publish_failed_records_total"], 2)
        self.assertEqual(report["statistics"]["publish_submitted_total"], 0)
        self.assertGreaterEqual(report["statistics"]["publish_errors_total"], 1)
        self.assertFalse(report["worker_failed"])
        self.assertEqual(transport.published, [])

    def test_shutdown_deadline_does_not_claim_cleanup_or_release_active_owner(self):
        gate = threading.Event()
        source, transport, exporter = self.exporter(transport=FakeTransport(gate=gate))
        started = time.monotonic()
        report = exporter.close(timeout=0.01)
        self.assertLess(time.monotonic() - started, 0.5)
        self.assertFalse(report["worker_stopped"])
        self.assertFalse(report["cleanup_complete"])
        with self.assertRaises(ValueError):
            observer.RequestTelemetryExporter(source, topic="/test/events", name="x", namespace="/test",
                _transport_factory=lambda **_: FakeTransport())
        gate.set()
        self.assertTrue(exporter.close()["worker_stopped"])

    def test_worker_capture_failure_is_visible_and_closes_transport(self):
        source, transport, exporter = self.exporter()
        source.fail_drain = True
        source.emit()
        self.wait_for(lambda: exporter.snapshot()["worker_stopped"])
        report = exporter.snapshot()
        self.assertTrue(report["worker_failed"])
        self.assertEqual(report["statistics"]["source_errors_total"], 1)
        self.assertTrue(transport.closed)
        self.assertFalse(source.enabled)
        self.assertEqual(report["source_current"]["log_queued"], 1)

    def test_enabling_export_capture_terminates_exporter_instead_of_recursing(self):
        source, transport, exporter = self.exporter()
        transport.context.enable_log_capture(10)
        source.emit()
        self.wait_for(lambda: exporter.snapshot()["worker_stopped"])
        self.assertEqual(exporter.snapshot()["statistics"]["export_capture_errors_total"], 1)
        self.assertTrue(exporter.snapshot()["worker_failed"])


class ObserverTest(ObserverFixture):
    def batches(self):
        source, transport, exporter = self.exporter()
        source.emit(component="action", event="goal.completed", goal_id="original_goal")
        exporter.close()
        return [batch for batch in transport.published if batch["records"]]

    def test_same_parser_for_live_and_replay_never_interprets_endpoint_as_call(self):
        batches = self.batches()
        class Subscription:
            def __init__(self, values):
                self.values = iter(values)
            def recv(self, _timeout):
                return next(self.values)
        live, replay = [], []
        for output in (live, replay):
            report = observer.observe(Subscription(copy.deepcopy(batches)), count=len(batches), timeout=1,
                max_output_bytes=observer.MAX_BATCH_BYTES, timeout_error=TimeoutError, output=output.append)
            self.assertEqual(report["reason"], "count")
        self.assertEqual(live, replay)
        self.assertEqual(json.loads(live[0])["batch"]["records"][0]["goal_id"], "original_goal")
        # Static guard complements the fake parser check. It does not replace
        # live-provider/spy zero-call evidence during actual bag replay.
        tree = ast.parse(SOURCE.read_text())
        calls = {node.func.attr for node in ast.walk(tree) if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)}
        self.assertTrue(calls.isdisjoint({"create_client", "create_service", "ActionClient", "ActionServer",
                                         "send_goal_async", "get_result_async", "cancel_goal_async"}))

    def test_observer_budget_stops_output_and_first_sequence_is_not_assumed_loss(self):
        batch = self.batches()[0]
        batch["batch_sequence"] = 100
        class Subscription:
            def recv(self, _timeout):
                return copy.deepcopy(batch)
        lines = []
        report = observer.observe(Subscription(), count=2, timeout=1, max_output_bytes=len(observer.encoded(batch)) + 100,
            timeout_error=TimeoutError, output=lines.append)
        self.assertEqual(report["reason"], "output_limit")
        self.assertEqual(len(lines), 1)
        self.assertEqual(report["observed_batch_gaps"], 0)
        self.assertEqual(report["before_first_batch_delivery"], "unknown")

    def test_forged_or_oversized_diagnostic_batches_are_rejected(self):
        batch = self.batches()[0]
        for change in (lambda value: value.update(command="call"),
                       lambda value: value["records"][0].update(context_id=str(uuid.uuid4())),
                       lambda value: value["records"].extend(copy.deepcopy(value["records"]) * 33)):
            value = copy.deepcopy(batch)
            change(value)
            with self.assertRaises(ValueError):
                observer.validate_batch(value)


class InstallationOfflineTest(unittest.TestCase):
    def test_default_is_off_and_explicit_install_connects_both_real_programs(self):
        installer_spec = importlib.util.spec_from_file_location("memory_installer_offline", ROOT / "scripts/install_memory_package.py")
        installer = importlib.util.module_from_spec(installer_spec)
        installer_spec.loader.exec_module(installer)
        # Stand-in package metadata only: this deliberately does not validate
        # production Interface compilation or claim a working wheel install.
        sdk = ModuleType("aos_sdk")
        sdk.__version__ = "0.0.0-offline"
        sdk._native = SimpleNamespace(__file__=str(SOURCE))
        sdk.Interface = SimpleNamespace(load=lambda path: json.loads(Path(path).read_text()))
        class Index:
            def __init__(self, prefixes):
                self.prefix = prefixes[0]
            def package(self, name, **_):
                return {"manifest": json.loads((self.prefix / "share/aos/resource_index/packages" / f"{name}.json").read_text())}
        sdk.PackageIndex = Index
        with tempfile.TemporaryDirectory() as temporary, patch.dict(sys.modules, {"aos_sdk": sdk}):
            directory = Path(temporary).resolve()
            config = directory / "never-opened-config.json"
            config.write_text("{}")
            for enabled in (False, True):
                prefix = directory / ("enabled" if enabled else "default")
                data = directory / (prefix.name + "-business")
                with redirect_stdout(io.StringIO()) as output:
                    result = installer.main(["--prefix", str(prefix), "--work-dir", str(data),
                        "--config", str(config), *(["--export-request-telemetry"] if enabled else [])])
                self.assertEqual(result, 0)
                self.assertFalse(data.exists())
                report = json.loads(output.getvalue())
                self.assertEqual(report["request_telemetry_enabled"], enabled)
                manifest = report["package"]["manifest"]
                self.assertIn("request_observer", manifest["entries"])
                self.assertEqual(manifest["resources"]["request_telemetry"]["path"], "share/aos_memory/interfaces/RequestTelemetry.json")
                self.assertTrue((prefix / "lib/aos_memory/request_observer.py").exists())
                if os.name == "nt":
                    entry = manifest["entries"]["request_observer"]
                    self.assertEqual(entry["interpreter"], sys.executable)
                    self.assertEqual(entry["interpreter_args"], ["-I", "-u"])
                else:
                    self.assertIn(" -I -u ", (prefix / "lib/aos_memory/request_observer").read_text())
                pair = json.loads((prefix / "share/aos_memory/launch/pair.json").read_text())
                for process, node in zip(pair["processes"], ("server", "consumer")):
                    arguments = process["args"]
                    self.assertEqual("--telemetry-topic" in arguments, enabled)
                    if enabled:
                        self.assertEqual(arguments[:2], ["--telemetry-topic", f"/memory/{node}/request_telemetry"])

    def test_schema_numbers_are_canonical_and_default_scope_opens_nothing(self):
        schema = json.loads((ROOT / "examples/memory_server/interfaces/RequestTelemetry.json").read_text())
        def check(value):
            if type(value) is int:
                self.assertEqual(int(float(value)), value, "schema hash must not round an integer constraint")
            elif isinstance(value, dict):
                for child in value.values():
                    check(child)
            elif isinstance(value, list):
                for child in value:
                    check(child)
        check(schema)
        with observer.exporter_from_args(SimpleNamespace(telemetry_topic=None), object(), {}) as result:
            self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()
