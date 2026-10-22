"""Installed-wheel observability checks using real loopback transport."""

import json
import os
import subprocess
import sys
import threading
import unittest

import aos_sdk as aos

from test_platform_foundation import PlatformTestCase, ROOT


class TelemetryTest(PlatformTestCase):
    def test_default_capture_is_off_and_closed_resource_counters_remain_readable(self):
        node = self.node("metrics")
        interface = aos.Interface.load(ROOT / "interfaces/String.json")
        publisher = node.create_publisher(interface, "message")
        publisher.publish({"data": "PRIVATE_PAYLOAD"})
        snapshot = self.context.telemetry_snapshot()
        self.assertEqual(snapshot["schema_version"], 1)
        self.assertEqual(snapshot["process_id"], os.getpid())
        self.assertGreater(snapshot["events_total"], 0)
        self.assertGreaterEqual(snapshot["active_resources"]["node"], 1)
        self.assertGreaterEqual(snapshot["active_resources"]["topic"], 1)
        self.assertFalse(snapshot["log_capture_enabled"])
        self.assertEqual(snapshot["log_queued"], 0)
        self.assertEqual(self.context.drain_log_records(), [])
        self.assertEqual(self.context.flush_logs_to_stderr(), 0)
        self.assertTrue(any(
            metric["event"] == "zenoh.topic.publish" and metric["count"] >= 1
            for metric in snapshot["metrics"]
        ))
        self.context.shutdown()
        closed = self.context.telemetry_snapshot()
        self.assertEqual(closed["context_id"], snapshot["context_id"])
        self.assertEqual(closed["active_resources"], {})
        self.assertGreaterEqual(closed["events_total"], snapshot["events_total"])

    def test_bounded_capture_strict_capacity_and_metadata_only_records(self):
        publisher = self.context.create_publisher("/captured")
        self.context.enable_log_capture()
        self.assertEqual(self.context.telemetry_snapshot()["log_capacity"], 256)
        for capacity in (0, 4097, -1, True, False, None, 1.5, "2"):
            with self.subTest(capacity=capacity), self.assertRaises(aos.InvalidArgumentError):
                self.context.enable_log_capture(capacity)
        self.context.enable_log_capture(2)
        for _ in range(5):
            publisher.publish(b"PRIVATE_PAYLOAD")
        snapshot = self.context.telemetry_snapshot()
        self.assertEqual(snapshot["log_queued"], 2)
        self.assertGreaterEqual(snapshot["log_dropped_total"], 3)
        self.context.disable_log_capture()
        publisher.publish(b"PRIVATE_PAYLOAD_AFTER_DISABLE")
        records = self.context.drain_log_records()
        self.assertEqual(len(records), 2)
        self.assertEqual(self.context.drain_log_records(), [])
        self.assertEqual([item["sequence"] for item in records],
                         sorted(item["sequence"] for item in records))
        for record in records:
            self.assertEqual(record["identity_trust"], "unauthenticated")
            self.assertEqual(record["context_id"], snapshot["context_id"])
            self.assertEqual(record["process_id"], os.getpid())
            self.assertGreater(record["bytes"], 0)
            self.assertIsInstance(record["system_time_unix_ns"], str)
            self.assertIsInstance(record["steady_time_ns"], int)
            self.assertNotIn("payload", record)
            self.assertNotIn("error", record)
            self.assertNotIn("config", record)
        self.assertNotIn("PRIVATE_", json.dumps(records))
        self.assertFalse(self.context.telemetry_snapshot()["log_capture_enabled"])

    def test_service_wire_events_share_request_id_and_exclude_payload_and_error(self):
        schema = {
            "type": "object", "properties": {"data": {"type": "string"}},
            "required": ["data"], "additionalProperties": False,
        }
        interface = aos.Interface.load({
            "name": "telemetry/srv/Echo", "kind": "srv",
            "request": schema, "response": schema,
        })
        provider = self.node("provider")
        consumer = self.node("consumer")
        service = provider.create_service(interface, "/echo", lambda value: value)
        client = consumer.create_client(interface, "/echo")
        self.assertTrue(client.wait_for_service(2))
        self.context.enable_log_capture(256)
        future = client.call_async({"data": "PRIVATE_REQUEST"}, timeout=2)
        request = service._native.recv(2)
        request.reply({"data": "PRIVATE_RESPONSE"})
        self.assertEqual(future.result(3), {"data": "PRIVATE_RESPONSE"})
        records = self.context.drain_log_records()
        expected = {
            "zenoh.service.query_submitted": "outgoing_attachment",
            "zenoh.service.request_received": "incoming_attachment",
            "zenoh.service.reply_sent": "reply_attachment",
            "zenoh.service.reply_received": "reply_attachment",
            "zenoh.service.call_finished": "query_context",
        }
        matched = [record for record in records if record["event"] in expected]
        self.assertEqual({record["event"] for record in matched}, set(expected))
        self.assertEqual(len({record["request_id"] for record in matched}), 1)
        for record in matched:
            self.assertEqual(record["correlation_source"], expected[record["event"]])
            self.assertEqual(record["outcome"], "ok")
        self.assertNotIn("PRIVATE_", json.dumps(records))

        failed = client.call_async({"data": "PRIVATE_FAILED_REQUEST"}, timeout=2)
        service._native.recv(2).reply_error("PRIVATE_PROVIDER_ERROR")
        with self.assertRaises(aos.ServiceError):
            failed.result(3)
        error_records = self.context.drain_log_records()
        self.assertTrue(any(record["outcome"] == "remote" for record in error_records))
        self.assertNotIn("PRIVATE_", json.dumps(error_records))
        self.assertGreater(self.context.telemetry_snapshot()["failures_total"], 0)

    def test_explicit_flush_writes_jsonl_only_to_stderr_and_default_stays_silent(self):
        script = """
import json, sys
import aos_sdk as aos
with aos.Context() as context:
    publisher = context.create_publisher("/child")
    if sys.argv[1] == "capture":
        context.enable_log_capture(2)
    publisher.publish(b"PRIVATE_CHILD_PAYLOAD")
    count = context.flush_logs_to_stderr()
    print(json.dumps({"count": count, "snapshot": context.telemetry_snapshot()}))
"""
        for capture in (False, True):
            with self.subTest(capture=capture):
                child = subprocess.run(
                    [sys.executable, "-I", "-c", script, "capture" if capture else "default"],
                    cwd=self.directory, capture_output=True, text=True, timeout=8,
                )
                self.assertEqual(child.returncode, 0, child.stderr)
                result = json.loads(child.stdout)
                records = [json.loads(line) for line in child.stderr.splitlines()]
                self.assertEqual(result["count"], len(records))
                self.assertEqual(bool(records), capture)
                self.assertEqual(result["snapshot"]["log_queued"], 0)
                self.assertNotIn("PRIVATE_CHILD_PAYLOAD", child.stdout + child.stderr)
                for record in records:
                    self.assertEqual(record["identity_trust"], "unauthenticated")

    @unittest.skipUnless(os.name == "posix", "file-descriptor failure injection requires POSIX")
    def test_failed_stderr_counts_loss_and_executor_keeps_dispatching(self):
        script = """
import json, os
import aos_sdk as aos
with aos.Context() as context:
    node = aos.Node("failed_writer", context=context)
    publisher = context.create_publisher("/child")
    context.enable_log_capture(4)
    publisher.publish(b"PRIVATE_CHILD_PAYLOAD")
    queued = context.telemetry_snapshot()["log_queued"]
    stderr = os.dup(2)
    reader, writer = os.pipe()
    os.close(reader)
    os.dup2(writer, 2)
    os.close(writer)
    failed = False
    callbacks = []
    try:
        try:
            context.flush_logs_to_stderr()
        except OSError:
            failed = True
        first = context.telemetry_snapshot()
        publisher.publish(b"PRIVATE_SECOND_PAYLOAD")
        with aos.SingleThreadedExecutor(context=context, flush_logs=True) as executor:
            executor.add_node(node)
            node.create_timer(0.001, lambda: callbacks.append(True))
            executor.spin_once(1)
        after = context.telemetry_snapshot()
    finally:
        os.dup2(stderr, 2)
        os.close(stderr)
    print(json.dumps({
        "failed": failed, "queued": queued, "first": first,
        "after": after, "callbacks": callbacks,
    }))
"""
        child = subprocess.run(
            [sys.executable, "-I", "-c", script], cwd=self.directory,
            capture_output=True, text=True, timeout=8,
        )
        self.assertEqual(child.returncode, 0, child.stderr)
        result = json.loads(child.stdout)
        self.assertTrue(result["failed"])
        self.assertEqual(result["first"]["log_write_failures_total"], 1)
        self.assertGreaterEqual(result["first"]["log_dropped_total"], result["queued"])
        self.assertGreater(result["after"]["log_write_failures_total"], 1)
        self.assertEqual(result["after"]["log_queued"], 0)
        self.assertEqual(result["callbacks"], [True])
        self.assertEqual(child.stderr, "")

    def test_executor_flush_is_opt_in_and_runs_on_the_spin_caller(self):
        native = self.context
        for enabled in (False, True):
            with self.subTest(enabled=enabled):
                flush_threads = []
                callback_threads = []
                called = threading.Event()

                class ObservedContext:
                    def ok(self):
                        return native.ok()

                    def flush_logs_to_stderr(self):
                        flush_threads.append(threading.get_ident())
                        return native.flush_logs_to_stderr()

                executor = aos.MultiThreadedExecutor(
                    1, context=ObservedContext(), flush_logs=enabled,
                )
                node = self.node("flush_on" if enabled else "flush_off")
                executor.add_node(node)

                def callback():
                    callback_threads.append(threading.get_ident())
                    called.set()

                node.create_timer(0.001, callback)
                try:
                    executor.spin_once(1)
                    self.assertTrue(called.wait(2))
                    self.assertNotEqual(callback_threads[0], threading.get_ident())
                    self.assertEqual(bool(flush_threads), enabled)
                    self.assertTrue(all(
                        ident == threading.get_ident() for ident in flush_threads
                    ))
                finally:
                    self.assertTrue(executor.shutdown(timeout_sec=3))

    def test_executor_rejects_non_boolean_flush_option(self):
        for executor_type in (aos.SingleThreadedExecutor, aos.MultiThreadedExecutor):
            for value in (None, 0, 1, "true"):
                with self.subTest(executor=executor_type, value=value), \
                     self.assertRaises(aos.InvalidArgumentError):
                    executor_type(flush_logs=value)


if __name__ == "__main__":
    unittest.main()
