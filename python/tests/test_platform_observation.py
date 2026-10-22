"""Real-router subprocess acceptance for publication, hooks and measurements."""

import json
import os
from pathlib import Path
import queue
import shlex
import signal
import subprocess
import sys
import threading
import time
import unittest

import aos_sdk as aos
import test_platform_cli as support
from process_support import console_process_options, interrupt_process, process_alive

ROOT = Path(__file__).resolve().parents[2]


class PlatformObservationTest(unittest.TestCase):
    setUp = support.PlatformCliTest.setUp
    peers = support.PlatformCliTest.peers
    spin = support.PlatformCliTest.spin

    def call(self, group, verb, request, *, expected=0, environment=None):
        process = subprocess.run(
            [self.cli_path, group, verb, "--request", "-"],
            input=json.dumps(request), capture_output=True, text=True,
            env=self.environment | (environment or {}), timeout=12,
        )
        self.assertEqual(process.returncode, expected, process.stdout + process.stderr)
        return [json.loads(line) for line in process.stdout.splitlines()]

    def start(self, group, verb, request, *, environment=None):
        process = subprocess.Popen(
            [self.cli_path, group, verb, "--request", "-"],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, env=self.environment | (environment or {}), **console_process_options(),
        )
        process.stdin.write(json.dumps(request))
        process.stdin.close()
        process.stdin = None
        events, incoming = [], queue.Queue()

        def read():
            for line in process.stdout:
                try:
                    value = json.loads(line)
                except json.JSONDecodeError:
                    value = {"invalid_stdout": line}
                events.append(value)
                incoming.put(value)

        reader = threading.Thread(target=read, name="observation-cli-reader")
        reader.start()

        def close():
            if process.poll() is None:
                interrupt_process(process)
                try:
                    process.wait(5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(5)
            reader.join(5)
            self.assertFalse(reader.is_alive())
            process.stdout.close()
            process.stderr.close()

        self.addCleanup(close)
        return process, events, incoming, reader

    def finish(self, handle, expected=0):
        process, events, _, reader = handle
        process.wait(12)
        reader.join(3)
        self.assertFalse(reader.is_alive())
        self.assertEqual(process.returncode, expected, repr(events) + process.stderr.read())
        self.assertTrue(all("event" in event and "data" in event for event in events), events)
        return events

    def wait_event(self, handle, name):
        deadline = time.monotonic() + 5
        while True:
            event = handle[2].get(timeout=max(0.01, deadline - time.monotonic()))
            if event.get("event") == name:
                return event["data"]
            self.assertLess(time.monotonic(), deadline, repr(event))

    def publisher(self, node, topic):
        return node.create_publisher(aos.Interface.load(ROOT / "interfaces/String.json"), topic)

    @staticmethod
    def summary(events):
        return next(event["data"] for event in reversed(events) if event["event"] == "summary")

    def test_default_publication_is_one_without_subscribers_and_invalid_modes_fail_early(self):
        _, _, _, namespace = self.peers()
        request = {"topic_name": namespace + "/alone", "message_type": "aos_examples/msg/String",
                   "values": {"data": "one"}}
        environment = {"AOS_INTERFACE_PATH": str(ROOT / "interfaces/String.json")}
        started = time.monotonic()
        events = self.call("topic", "pub", request, environment=environment)
        self.assertLess(time.monotonic() - started, 3)
        self.assertEqual(len([event for event in events if event["event"] == "published"]), 1)
        self.assertEqual(self.summary(events)["submitted_count"], 1)
        self.assertEqual(self.summary(events)["requested_subscribers"], 0)
        for extra in ({"once": False}, {"once": True, "times": 1},
                      {"times": 2, "follow": True}, {"timeout": 1, "match_timeout": 1}):
            with self.subTest(extra=extra):
                events = self.call("topic", "pub", request | extra, expected=2,
                                   environment=environment | {"AOS_ZENOH_CONFIG": "/missing-config"})
                self.assertEqual(events[-1]["data"]["code"], "INVALID_ARGUMENT")
                self.assertNotIn("session configuration", events[-1]["data"]["message"])

    def test_explicit_subscriber_count_and_exact_publication_count(self):
        _, _, node, namespace = self.peers()
        interface = aos.Interface.load(ROOT / "interfaces/String.json")
        subscriptions = [node.create_subscription(interface, "three") for _ in range(2)]
        events = self.call("topic", "pub", {
            "topic_name": namespace + "/three", "message_type": interface.type_name,
            "values": {"data": "exact"}, "times": 3, "rate": 50,
            "wait_for_subscribers": 2, "match_timeout": 3, "linger": 0.02,
        })
        self.assertEqual(self.summary(events)["submitted_count"], 3)
        self.assertGreaterEqual(self.summary(events)["observed_subscribers"], 2)
        for subscription in subscriptions:
            self.assertEqual([subscription.recv(1) for _ in range(3)], [{"data": "exact"}] * 3)
        events = self.call("topic", "pub", {
            "topic_name": namespace + "/absent", "message_type": interface.type_name,
            "values": {"data": "not submitted"}, "wait_for_subscribers": 1, "match_timeout": 0.05,
        }, expected=4)
        self.assertEqual(self.summary(events)["submitted_count"], 0)
        self.assertEqual(self.summary(events)["stop_reason"], "match_timeout")

    def test_float_counts_are_rejected_before_network_open(self):
        cases = [
            ("topic", "pub", {"topic_name": "/never_sent",
                             "message_type": "aos_examples/msg/String",
                             "values": {"data": "invalid"}, "times": 3.0}),
            ("topic", "pub", {"topic_name": "/never_sent",
                             "message_type": "aos_examples/msg/String",
                             "values": {"data": "invalid"}, "wait_for_subscribers": 2.0}),
            ("topic", "echo", {"topic_name": "/never_opened", "count": 3.0}),
            ("topic", "hz", {"topic_name": "/never_opened", "window": 3.0}),
            ("service", "call", {"service_name": "/never_called",
                                "service_type": "aos_examples/srv/AddTwoInts",
                                "values": {"a": 1, "b": 2}, "times": 3.0}),
            ("doctor", "report", {"logging": {"stderr": True, "capacity": 3.0}}),
        ]
        for group, verb, request in cases:
            with self.subTest(group=group, verb=verb, request=request):
                records = self.call(group, verb, request, expected=2, environment={
                    "AOS_ZENOH_CONFIG": "/missing-config-for-integer-validation",
                })
                error = records[-1].get("data", records[-1].get("error"))
                self.assertEqual(error["code"], "INVALID_ARGUMENT")
                self.assertIn("integer", error["message"])
                self.assertNotEqual(error["phase"], "session_open")

    def test_continuous_publisher_accepts_late_subscriber_and_interrupt_reports_count(self):
        _, _, node, namespace = self.peers()
        interface = aos.Interface.load(ROOT / "interfaces/String.json")
        handle = self.start("topic", "pub", {
            "topic_name": namespace + "/late", "message_type": interface.type_name,
            "values": {"data": "late"}, "follow": True, "rate": 40,
        }, environment={"AOS_INTERFACE_PATH": str(ROOT / "interfaces/String.json")})
        self.wait_event(handle, "published")
        subscriber = node.create_subscription(interface, "late")
        self.assertEqual(subscriber.recv(3), {"data": "late"})
        interrupt_process(handle[0])
        summary = self.summary(self.finish(handle, expected=130))
        self.assertEqual(summary["stop_reason"], "interrupted")
        self.assertGreaterEqual(summary["submitted_count"], 1)

    def test_filter_hook_projection_order_and_hook_wins_same_message_count(self):
        _, _, node, namespace = self.peers()
        publisher = self.publisher(node, "order")
        handle = self.start("topic", "echo", {
            "topic_name": namespace + "/order", "timeout": 3, "count": 2,
            "filter": {"path": "/data", "op": "ne", "value": "skip"},
            "fields": ["/omitted"],
            "hook": {"kind": "condition", "condition": {"path": "/data", "op": "eq", "value": "ready"}},
        })
        publisher.wait_for_subscribers(3)
        for value in ("skip", "working", "ready"):
            publisher.publish({"data": value})
        events = self.finish(handle)
        summary = self.summary(events)
        self.assertEqual((summary["received"], summary["filtered"]), (3, 2))
        self.assertEqual(summary["stop_reason"], "hook_matched")
        self.assertEqual(summary["matched"]["message"], {"data": "ready"})
        self.assertEqual(summary["matched"]["projection"], {})
        self.assertEqual(summary["projection_missing_fields"], 2)
        self.assertEqual([event["data"]["message"] for event in events if event["event"] == "message"], [{}, {}])
        # Closing observation does not close the provider's publisher.
        publisher.publish({"data": "still running"})

    def test_missing_filter_fields_and_explicit_follow_timeout_are_observable(self):
        _, _, node, namespace = self.peers()
        publisher = self.publisher(node, "missing")
        handle = self.start("topic", "echo", {
            "topic_name": namespace + "/missing", "follow": True, "timeout": 0.2,
            "filter": {"path": "/absent", "op": "gt", "value": 0},
        })
        publisher.wait_for_subscribers(3)
        publisher.publish({"data": "valid but filtered"})
        summary = self.summary(self.finish(handle, expected=4))
        self.assertEqual(summary["stop_reason"], "total_timeout")
        self.assertEqual((summary["received"], summary["filtered"], summary["filter_missing_fields"]), (1, 0, 1))

    def test_script_receives_complete_input_and_reports_errors_and_timeout(self):
        _, _, node, namespace = self.peers()
        for index, (script, hook_timeout, expected, reason) in enumerate([
            ('import json,sys; v=json.load(sys.stdin); print(json.dumps({"decision":"matched","reason":v["message"]["data"]}))',
             1, 0, "hook_matched"),
            ('print("not json")', 1, 1, "hook_error"),
            ('import json; print(json.dumps({"decision":"error","reason":"explicit failure"}))', 1, 1, "hook_error"),
            ('import time; time.sleep(5)', 0.05, 1, "hook_error"),
        ]):
            with self.subTest(index=index):
                topic = "script_" + str(index)
                publisher = self.publisher(node, topic)
                handle = self.start("topic", "echo", {
                    "topic_name": namespace + "/" + topic, "timeout": 3,
                    "fields": ["/absent"],
                    "hook": {"kind": "script", "executable": sys.executable,
                             "argv": ["-I", "-c", script], "timeout": hook_timeout},
                })
                publisher.wait_for_subscribers(3)
                publisher.publish({"data": "unprojected"})
                events = self.finish(handle, expected)
                summary = self.summary(events)
                self.assertEqual(summary["stop_reason"], reason)
                self.assertFalse(summary["provider_stopped"])
                if expected == 0:
                    self.assertEqual(summary["matched"]["hook"]["reason"], "unprojected")
                else:
                    self.assertIsNone(summary["matched"])

    def test_filtered_messages_reset_idle_timeout_without_satisfying_count(self):
        _, _, node, namespace = self.peers()
        publisher = self.publisher(node, "idle")
        handle = self.start("topic", "echo", {
            "topic_name": namespace + "/idle", "timeout": 3, "idle_timeout": 0.15,
            "count": 1, "filter": {"path": "/data", "op": "eq", "value": "never"},
        })
        publisher.wait_for_subscribers(3)
        for _ in range(8):
            publisher.publish({"data": "filtered"})
            time.sleep(0.04)
            self.assertIsNone(handle[0].poll())
        summary = self.summary(self.finish(handle, expected=4))
        self.assertEqual(summary["stop_reason"], "idle_timeout")
        self.assertEqual((summary["received"], summary["filtered"]), (8, 0))

    def test_script_is_stopped_by_total_deadline_and_sigint(self):
        _, _, node, namespace = self.peers()
        script = ('import pathlib,sys,time,json,os; json.load(sys.stdin); '
                  'pathlib.Path(sys.argv[1]).write_text(str(os.getpid())); time.sleep(30)')
        for index, (verb, interrupted) in enumerate((("echo", False), ("hz", False), ("echo", True))):
            publisher = self.publisher(node, "interrupt_" + str(index))
            marker = self.directory / ("hook-pid-" + str(index))
            handle = self.start("topic", verb, {
                "topic_name": namespace + "/interrupt_" + str(index),
                "timeout": 10 if interrupted else 5,
                "hook": {"kind": "script", "executable": sys.executable,
                         "argv": ["-I", "-c", script, str(marker)], "timeout": 15},
            })
            publisher.wait_for_subscribers(3)
            publisher.publish({"data": "begin"})
            deadline = time.monotonic() + 4
            while not marker.exists():
                self.assertLess(time.monotonic(), deadline)
                time.sleep(0.005)
            pid = int(marker.read_text())
            if interrupted:
                interrupt_process(handle[0])
            expected = 130 if interrupted else (4 if verb == "echo" else 0)
            events = self.finish(handle, expected=expected)
            summary = self.summary(events)
            self.assertEqual(summary["stop_reason"], "interrupted" if interrupted else "total_timeout")
            if verb == "hz":
                self.assertFalse(any(event["event"] == "error" for event in events), events)
            self.assertFalse(process_alive(pid))
            self.assertFalse(summary["provider_stopped"])

    def test_doctor_selected_python_keeps_explicit_config_with_empty_environment(self):
        config = self.directory / "doctor-peer.json5"
        config.write_text(json.dumps({
            "mode": "peer", "connect": {"endpoints": []}, "listen": {"endpoints": []},
            "scouting": {"multicast": {"enabled": False}, "gossip": {"enabled": False}},
        }))
        result = self.call("doctor", "report", {
            "python": sys.executable, "timeout": 3,
            "context_config": {"cli": {"zenoh_config": str(config)}},
        }, environment={"AOS_ZENOH_CONFIG": "", "AOS_DOMAIN_ID": ""})[0]["data"]
        selected = result["python_native_module"]
        self.assertEqual(selected["status"], "ok", selected)
        report = selected["report"]
        self.assertEqual(report["config"]["source"], "argument")
        self.assertEqual(report["config"]["path"], str(config))
        self.assertEqual(report["domain"], {"source": "argument", "status": "ok", "value": 0})
        self.assertEqual(report["shutdown"]["status"], "ok")

    def test_doctor_checks_selected_python_and_bounds_slow_interpreter(self):
        self.peers()
        result = self.call("doctor", "report", {"python": sys.executable, "timeout": 3})[0]["data"]
        self.assertEqual(result["python_native_module"]["status"], "ok", result)
        self.assertTrue(result["python_native_module"]["report"]["native"]["available"])
        self.assertEqual(result["security"]["authentication"], "unknown")
        environment = self.directory / "empty-python"
        subprocess.run([sys.executable, "-I", "-m", "venv", "--without-pip", str(environment)],
                       check=True, capture_output=True, text=True, timeout=15)
        result = self.call("doctor", "report", {
            "python": str(environment / ("Scripts/python.exe" if os.name == "nt" else "bin/python")), "timeout": 3,
        })[0]["data"]
        self.assertEqual(result["status"], "error")
        self.assertEqual(result["python_native_module"]["report"]["failures"][0]["code"], "SDK_NOT_INSTALLED")
        marker = self.directory / "doctor-pid"
        executable = Path(os.environ["AOS_PROCESS_FIXTURE"])
        fixture_env = {"AOS_PROCESS_FIXTURE_MARKER": str(marker), "AOS_PROCESS_FIXTURE_MODE": "hold"}
        started = time.monotonic()
        # The shared budget includes Graph discovery and OS child startup.
        # Leave enough time to start the marker-writing child so this case
        # proves cleanup of an executing child, not just expiry before startup.
        result = self.call("doctor", "report", {"python": str(executable), "timeout": 5}, environment=fixture_env)[0]["data"]
        self.assertLess(time.monotonic() - started, 6.5)
        self.assertEqual(result["python_native_module"]["status"], "timeout")
        self.assertGreater(result["python_native_module"]["budget_seconds"], 1)
        self.assertTrue(marker.is_file(), result)
        self.assertFalse(process_alive(int(marker.read_text())))
        marker.unlink()
        handle = self.start("doctor", "report", {"python": str(executable), "timeout": 10}, environment=fixture_env)
        deadline = time.monotonic() + 3
        while not marker.exists():
            self.assertLess(time.monotonic(), deadline)
            time.sleep(0.005)
        pid = int(marker.read_text())
        interrupt_process(handle[0])
        self.assertEqual(handle[0].wait(3), 130)
        handle[3].join(3)
        self.assertFalse(handle[3].is_alive())
        self.assertFalse(handle[1][-1]["ok"])
        self.assertEqual(handle[1][-1]["error"]["code"], "INTERRUPTED")
        self.assertFalse(process_alive(pid))

    @unittest.skipUnless(os.name == "posix", "requires inherited POSIX SIGCHLD disposition")
    def test_doctor_reaps_children_when_parent_ignores_sigchld(self):
        self.peers()
        launcher = (
            "import os,signal,sys; signal.signal(signal.SIGCHLD,signal.SIG_IGN); "
            "os.execv(sys.argv[1],sys.argv[1:])"
        )
        process = subprocess.run(
            [sys.executable, "-I", "-c", launcher, self.cli_path, "doctor", "report", "--request", "-"],
            input=json.dumps({"python": sys.executable, "timeout": 5}),
            capture_output=True, text=True, env=self.environment, timeout=10,
        )
        self.assertEqual(process.returncode, 0, process.stdout + process.stderr)
        result = json.loads(process.stdout)["data"]["python_native_module"]
        self.assertEqual(result["status"], "ok", result)
        self.assertTrue(result["report"]["cleanup"]["reaped"], result)

    def test_slow_hook_reports_real_queue_overflow(self):
        _, _, node, namespace = self.peers()
        publisher = self.publisher(node, "overflow")
        marker = self.directory / "hook-started"
        script = ('import pathlib,sys,time,json; json.load(sys.stdin); '
                  'pathlib.Path(sys.argv[1]).write_text("ready"); time.sleep(.25); '
                  'print(json.dumps({"decision":"continue"}))')
        handle = self.start("topic", "echo", {
            "topic_name": namespace + "/overflow", "qos": {"depth": 1}, "timeout": 3,
            "hook": {"kind": "script", "executable": sys.executable,
                     "argv": ["-I", "-c", script, str(marker)]},
        })
        publisher.wait_for_subscribers(3)
        publisher.publish({"data": "begin"})
        deadline = time.monotonic() + 3
        while not marker.exists():
            self.assertLess(time.monotonic(), deadline)
            time.sleep(0.01)
        for index in range(100):
            publisher.publish({"data": str(index)})
        summary = self.summary(self.finish(handle, expected=3))
        self.assertEqual(summary["stop_reason"], "hook_overflow")
        self.assertGreater(summary["queue"]["dropped_total"], 0)


    def test_native_hooks_bound_input_output_and_inherited_descendant_pipes(self):
        _, _, node, namespace = self.peers()
        fixture = os.environ["AOS_PROCESS_FIXTURE"]
        for mode in ("echo", "hold", "tree_exit", "flood"):
            with self.subTest(mode=mode):
                marker = self.directory / ("native-hook-" + mode)
                publisher = self.publisher(node, "native_" + mode)
                handle = self.start("topic", "echo", {
                    "topic_name": namespace + "/native_" + mode, "timeout": 5,
                    "hook": {"kind": "script", "executable": fixture,
                             "timeout": .5, "max_output_bytes": 16384},
                }, environment={"AOS_PROCESS_FIXTURE_MARKER": str(marker), "AOS_PROCESS_FIXTURE_MODE": mode})
                publisher.wait_for_subscribers(3)
                started = time.monotonic()
                publisher.publish({"data": "x" * 200000})
                summary = self.summary(self.finish(handle, 0 if mode == "echo" else 1))
                self.assertLess(time.monotonic() - started, 3)
                self.assertTrue(marker.exists(), summary)
                self.assertFalse(process_alive(int(marker.read_text())))
                self.assertEqual(summary["stop_reason"], "hook_matched" if mode == "echo" else "hook_error")
                if mode == "echo":
                    self.assertGreater(int(summary["matched"]["hook"]["reason"]), 200000)

    def test_structured_logging_keeps_business_stdout_and_exposes_bounded_loss(self):
        _, context, node, namespace = self.peers()
        shape = {"type": "object", "properties": {"data": {"type": "string"}},
                 "required": ["data"], "additionalProperties": False}
        interface = aos.Interface.load({
            "name": "aos_tests/srv/LogIsolation", "kind": "srv",
            "request": shape, "response": shape,
        })
        node.create_service(interface, "logged", lambda request: request)
        self.spin(context, node)
        secret = "payload-must-not-be-in-diagnostic-records"
        request = {
            "service_name": namespace + "/logged", "service_type": "aos_tests/srv/LogIsolation",
            "values": {"data": secret}, "times": 2, "timeout": 3,
            "logging": {"stderr": True, "capacity": 128},
        }
        process = subprocess.run(
            [self.cli_path, "service", "call", "--request", "-"],
            input=json.dumps(request), capture_output=True, text=True,
            env=self.environment, timeout=12,
        )
        self.assertEqual(process.returncode, 0, process.stdout + process.stderr)
        events = [json.loads(line) for line in process.stdout.splitlines()]
        self.assertEqual([value["data"]["response"] for value in events
                          if value["event"] == "response"], [request["values"]] * 2)
        records = [json.loads(line) for line in process.stderr.splitlines()]
        self.assertTrue(records)
        self.assertNotIn(secret, process.stderr)
        submitted = {row["request_id"] for row in records
                     if row["event"] == "zenoh.service.query_submitted"}
        finished = {row["request_id"] for row in records
                    if row["event"] == "zenoh.service.call_finished"}
        self.assertGreaterEqual(len(submitted), 2)
        self.assertEqual(submitted, finished)
        summary = records[-1]
        self.assertEqual(summary["event"], "telemetry_summary")
        self.assertEqual(summary["telemetry"]["active_resources"], {})
        self.assertEqual(summary["telemetry"]["log_dropped_total"], 0)
        process = subprocess.run(
            [self.cli_path, "doctor", "report", "--request", "-"],
            input=json.dumps({"logging": {"stderr": True, "capacity": 1}}),
            capture_output=True, text=True, env=self.environment, timeout=12,
        )
        self.assertEqual(process.returncode, 0, process.stdout + process.stderr)
        self.assertTrue(json.loads(process.stdout)["ok"])
        summary = json.loads(process.stderr.splitlines()[-1])
        self.assertGreater(summary["telemetry"]["log_dropped_total"], 0)
        self.assertEqual(summary["telemetry"]["log_queued"], 0)

    def test_measurements_use_receive_intervals_and_payload_bytes_and_refuse_unproved_delay(self):
        _, _, node, namespace = self.peers()
        values = [{"data": value} for value in ("一", "二二", "三三三", "四四四四")]
        expected_bytes = sum(len(json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode()) for value in values)
        for verb in ("hz", "bw", "delay"):
            with self.subTest(verb=verb):
                publisher = self.publisher(node, verb)
                handle = self.start("topic", verb, {
                    "topic_name": namespace + "/" + verb, "count": 4,
                    "timeout": 3, "report_interval": 0.05, "window": 4,
                })
                publisher.wait_for_subscribers(3)
                for value in values:
                    publisher.publish(value)
                    time.sleep(0.08)
                summary = self.summary(self.finish(handle))
                statistics = summary["statistics"]
                self.assertEqual(statistics["total_payload_bytes"], expected_bytes)
                self.assertEqual(statistics["hz"]["status"], "available")
                self.assertTrue(6 < statistics["hz"]["value"] < 20, statistics)
                self.assertEqual(statistics["bw"]["status"], "available")
                self.assertEqual(statistics["delay"]["status"], "unavailable")
                self.assertNotIn("value", statistics["delay"])

    def test_explicit_service_repetition_is_sequential_and_timeout_never_retries(self):
        _, context, node, namespace = self.peers()
        interface = aos.Interface.load(ROOT / "interfaces/AddTwoInts.json")
        calls = []

        def add(request):
            calls.append(("add", time.monotonic()))
            return {"sum": request["a"] + request["b"]}

        def slow(request):
            calls.append(("slow", time.monotonic()))
            time.sleep(0.2)
            return {"sum": request["a"] + request["b"]}

        node.create_service(interface, "add", add)
        node.create_service(interface, "slow", slow)
        self.spin(context, node)
        request = {"service_name": namespace + "/add", "service_type": interface.type_name,
                   "values": {"a": 20, "b": 22}, "times": 3, "interval": 0.03, "timeout": 2}
        events = self.call("service", "call", request)
        responses = [event["data"] for event in events if event["event"] == "response"]
        self.assertEqual([value["response"] for value in responses], [{"sum": 42}] * 3)
        self.assertEqual(len({value["request_id"] for value in responses}), 3)
        self.assertEqual(len(calls), 3)
        self.assertTrue(all(right[1] - left[1] >= 0.025 for left, right in zip(calls, calls[1:])))
        events = self.call("service", "call", request | {
            "service_name": namespace + "/slow", "timeout": 0.05,
        }, expected=4)
        self.assertEqual(self.summary(events)["started_count"], 1)
        self.assertEqual(len([kind for kind, _ in calls if kind == "slow"]), 1)
        self.assertFalse(next(event["data"] for event in events if event["event"] == "call_error")["retry"])

    def test_interrupt_inflight_service_does_not_retry_or_cancel_provider(self):
        _, context, node, namespace = self.peers()
        interface = aos.Interface.load(ROOT / "interfaces/AddTwoInts.json")
        entered, release, completed = threading.Event(), threading.Event(), threading.Event()
        calls = []

        def delayed(request):
            calls.append(request)
            entered.set()
            release.wait(5)
            completed.set()
            return {"sum": request["a"] + request["b"]}

        node.create_service(interface, "interrupt_call", delayed)
        self.spin(context, node)
        handle = self.start("service", "call", {
            "service_name": namespace + "/interrupt_call",
            "service_type": interface.type_name,
            "values": {"a": 20, "b": 22}, "times": 3, "timeout": 8,
        })
        try:
            self.assertTrue(entered.wait(4))
            began = time.monotonic()
            interrupt_process(handle[0])
            summary = self.summary(self.finish(handle, expected=130))
            self.assertLess(time.monotonic() - began, 1.5)
            self.assertEqual(summary["stop_reason"], "interrupted")
            self.assertEqual(summary["started_count"], 1)
            self.assertEqual(summary["completed_count"], 0)
            self.assertFalse(completed.is_set())
        finally:
            release.set()
        self.assertTrue(completed.wait(3))
        self.assertEqual(len(calls), 1)


if __name__ == "__main__":
    unittest.main()
