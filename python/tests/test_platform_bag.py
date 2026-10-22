"""Installed-wheel bag controls and real CLI/SDK data, without a Python writer."""
import hashlib
from process_support import console_process_options, interrupt_process, read_line

import json
import os
from pathlib import Path
import queue
import select
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import uuid
import zlib
from unittest import mock

import aos_sdk as aos
from test_platform_foundation import PlatformTestCase, ROOT
from test_stage1_cli import RouterProcess


class BagSdkTest(PlatformTestCase):
    def test_close_rejects_unclean_reports_and_preserves_the_primary_error(self):
        recorder = aos.BagRecorder(
            self.context, self.directory / "cleanup-report", all_topics=True,
        )
        self.addCleanup(recorder.close)
        cleanup = {"kind": "io", "message": "node cleanup failed", "file_offset": None}
        primary = {"kind": "corrupt", "message": "source failed", "file_offset": 123}
        for error in (None, primary):
            report = {"status": {
                "state": "stopped", "error": error, "cleanup_error": cleanup,
                "resources_closed": False,
            }}
            with self.subTest(error=error), mock.patch.object(
                recorder, "wait", return_value=report,
            ):
                with self.assertRaises(aos.BagError) as caught:
                    recorder.close()
                expected = error or cleanup
                self.assertEqual(caught.exception.kind, expected["kind"])
                self.assertEqual(caught.exception.file_offset, expected["file_offset"])
                self.assertIs(caught.exception.report, report)
                self.assertEqual(str(caught.exception), expected["message"])

    def test_null_qos_keeps_bag_default_and_cleanup_preserves_body_error(self):
        recorder = aos.BagRecorder(
            self.context, self.directory / "null-qos", topics=["/null_qos"], qos=None,
        )
        self.addCleanup(recorder.close)
        provider = self.node("null_qos_provider")
        publisher = provider.create_publisher(
            aos.Interface.load(ROOT / "interfaces/String.json"), "/null_qos",
        )
        self.addCleanup(publisher.destroy)
        self.wait_for(lambda: len(recorder.status["active_channels"]) == 1)
        self.assertEqual(recorder.status["active_channels"][0]["record_qos"], {
            "depth": 256, "reliability": "best_effort", "durability": "volatile",
            "history": "keep_last",
        })
        body = ValueError("application failure")
        cleanup = aos.BagError("cleanup failure")
        with mock.patch.object(recorder, "close", side_effect=cleanup):
            with self.assertRaises(ValueError) as caught:
                with recorder:
                    raise body
        self.assertIs(caught.exception, body)
        self.assertIs(body.bag_cleanup_error, cleanup)
        self.assertIn("cleanup failure", body.__notes__[0])

    def recording(self, count=4, interval=0.02):
        path = self.directory / f"recording-{time.monotonic_ns()}"
        interface = aos.Interface.load(ROOT / "interfaces/String.json")
        recorder = aos.BagRecorder(
            self.context, path, topics=["/bag/source"], qos=aos.QoSProfile(depth=64),
        )
        self.addCleanup(recorder.close)
        provider = self.node(f"provider_{time.monotonic_ns()}")
        publisher = provider.create_publisher(interface, "/bag/source")
        self.wait_for(lambda: len(recorder.status["active_channels"]) == 1)
        for index in range(count):
            publisher.publish({"data": str(index)})
            time.sleep(interval)
        self.wait_for(lambda: recorder.status["counters"]["written_total"] == count)
        report = recorder.close()
        self.assertEqual(report["status"]["state"], "finished")
        self.assertTrue(report["manifest"]["clean_close"])
        self.assertIsNone(report["status"]["counters"]["end_to_end_lost_total"])
        provider.destroy_node()
        return path, interface

    def test_offline_source_and_paused_replay_keep_context_and_types(self):
        path, interface = self.recording()
        info = aos.inspect_bag(path)
        self.assertEqual(info["messages"], 4)
        self.assertEqual(info["channels"][0]["interface"], interface.definition)
        self.assertEqual(info["channels"][0]["type_hash"], interface.type_hash)
        self.assertEqual(info["channels"][0]["record_qos"]["depth"], 64)
        consumer = self.node("consumer")
        subscription = consumer.create_subscription(interface, "/replay/bag/source")
        with aos.BagPlayer(self.context, path, namespace="/replay", start_paused=True,
                           subscriber_wait_ms=1000) as player:
            self.assertEqual(player.status["state"], "ready")
            self.assertEqual(player.start()["state"], "paused")
            before = player.status
            time.sleep(0.05)
            self.assertEqual(player.status["position_ns"], before["position_ns"])
            with self.assertRaises(aos.WaitTimeoutError):
                subscription.recv(0.01)
            self.assertEqual(player.seek(0)["epoch"], 1)
            self.assertEqual(player.set_rate(2.0)["rate"], 2.0)
            self.assertEqual(player.resume()["state"], "playing")
            received = [subscription.recv(2)["data"] for _ in range(4)]
            self.assertEqual(received, ["0", "1", "2", "3"])
            self.assertEqual(player.wait(2)["status"]["state"], "finished")
        self.assertTrue(self.context.ok())
        self.assertEqual(aos.inspect_bag(path)["messages"], 4)

    def test_wait_releases_gil_for_other_thread_stop_and_preserves_timeout(self):
        recorder = aos.BagRecorder(self.context, self.directory / "wait", all_topics=True)
        self.addCleanup(recorder.close)
        with self.assertRaises(aos.BagError) as caught:
            recorder.wait(0)
        self.assertEqual(caught.exception.kind, "timeout")
        results = queue.Queue()
        started = threading.Event()

        def wait():
            started.set()
            try:
                results.put(recorder.wait(2))
            except BaseException as error:
                results.put(error)

        waiter = threading.Thread(target=wait)
        waiter.start()
        self.assertTrue(started.wait(1))
        time.sleep(0.05)
        recorder.stop()
        waiter.join(1)
        self.assertFalse(waiter.is_alive(), "native wait retained the GIL or did not close")
        report = results.get(timeout=1)
        self.assertIsInstance(report, dict, report)
        self.assertEqual(report["status"]["state"], "finished")
        self.assertTrue(self.context.ok())

    def test_strict_input_and_recovery_error_attributes(self):
        path, _ = self.recording()
        with self.assertRaises(aos.InvalidArgumentError):
            aos.BagRecorder(self.context, path, all_topics=True)
        with self.assertRaises(aos.InvalidArgumentError):
            aos.BagRecorder(self.context, self.directory / "invalid", all_topics=True,
                            queue_messages=3.0)
        with aos.BagPlayer(self.context, path, namespace="/replay", start_paused=True) as player:
            player.start()
            for bad in (True, -1, 3.0, 2**64):
                with self.subTest(offset=bad), self.assertRaises(aos.InvalidArgumentError):
                    player.seek(bad)
            with self.assertRaises(aos.InvalidArgumentError) as caught:
                player.set_rate(0)
            self.assertEqual(caught.exception.kind, "invalid_argument")
        shard = next(path.glob("*.mcap"))
        broken = self.directory / "truncated.mcap.partial"
        broken.write_bytes(shard.read_bytes()[:-128])
        digest = hashlib.sha256(broken.read_bytes()).hexdigest()
        recovered = self.directory / "recovered"
        result = aos.recover_bag(broken, recovered)
        self.assertTrue(result["info"]["manifest"]["incomplete"])
        self.assertEqual(hashlib.sha256(broken.read_bytes()).hexdigest(), digest)
        with self.assertRaises(aos.BagError) as caught:
            aos.inspect_bag(recovered)
        self.assertEqual(caught.exception.kind, "incomplete")
        self.assertEqual(aos.inspect_bag(recovered, allow_incomplete=True)["messages"], 4)


class BagCliTest(unittest.TestCase):
    def setUp(self):
        self.binary = Path(os.environ["AOS_CLI"]).resolve()
        self.assertTrue(self.binary.is_file())
        temporary = tempfile.TemporaryDirectory(prefix="aos-bag-cli-")
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        self.environment = {key: value for key, value in os.environ.items()
                            if not key.startswith("AOS_")}

    def start(self, verb, request, *, read_output=True, stdout=subprocess.PIPE):
        path = self.directory / f"request-{time.monotonic_ns()}.json"
        path.write_text(json.dumps(request))
        process = subprocess.Popen(
            [str(self.binary), "bag", verb, "--request", str(path)],
            stdin=subprocess.PIPE, stdout=stdout, stderr=subprocess.PIPE,
            text=True, env=self.environment, bufsize=1, **console_process_options(),
        )
        events = queue.Queue()
        trace = []

        def read():
            for line in process.stdout:
                try:
                    event = json.loads(line)
                except BaseException as error:
                    events.put(error)
                    return
                trace.append(event)
                events.put(event)

        reader = threading.Thread(target=read) if read_output else None
        if reader:
            reader.start()

        def close():
            if process.poll() is None:
                process.kill()
            process.wait(5)
            if reader:
                reader.join(5)
                self.assertFalse(reader.is_alive())
            process.stdin.close()
            if process.stdout:
                process.stdout.close()
            process.stderr.close()

        self.addCleanup(close)
        return process, events, trace

    def wait_for(self, predicate, timeout=5):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if predicate():
                return
            time.sleep(0.01)
        self.fail("condition did not become true before its steady deadline")

    def clean_manifest(self, path):
        manifest = path / "manifest.json"
        if not manifest.exists():
            return False
        return json.loads(manifest.read_text())["clean_close"]

    def event(self, events, name, predicate=lambda event: True, timeout=5):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            event = events.get(timeout=max(0.001, deadline - time.monotonic()))
            self.assertIsInstance(event, dict, event)
            if event["event"] == name and predicate(event):
                return event["data"]
            self.assertNotEqual(event["event"], "error", event)
        self.fail(f"missing {name}")

    def peers(self):
        router = RouterProcess()
        self.addCleanup(router.close)
        patch = mock.patch.dict(os.environ, self.environment, clear=True)
        patch.start()
        self.addCleanup(patch.stop)
        context = aos.Context(config=router.config, domain_id=95)
        self.addCleanup(context.shutdown)
        self.environment.update(AOS_ZENOH_CONFIG=str(router.config), AOS_DOMAIN_ID="95")
        node = aos.Node("bag_cli_provider", context=context)
        self.addCleanup(node.destroy_node)
        interface = aos.Interface.load(ROOT / "interfaces/String.json")
        return context, node, interface

    def small_recording(self):
        context, node, interface = self.peers()
        path = self.directory / f"signal-source-{uuid.uuid4().hex}"
        publisher = node.create_publisher(interface, "/bag/signal")
        recorder = aos.BagRecorder(context, path, topics=["/bag/signal"])
        self.addCleanup(recorder.close)
        self.wait_for(lambda: len(recorder.status["active_channels"]) == 1)
        publisher.publish({"data": "retained"})
        self.wait_for(lambda: recorder.status["counters"]["written_total"] == 1)
        recorder.close()
        return path

    def test_sigint_and_stdin_eof_interrupt_start_subscriber_wait(self):
        path = self.small_recording()
        for control in ("signal", "eof"):
            with self.subTest(control=control):
                process, events, trace = self.start("play", {
                    "directory": str(path), "namespace": f"/unmatched_{control}",
                    "subscriber_wait_ms": 30000,
                    "control_input": "stdin", "status_interval": 0.02,
                })
                # This event is emitted before Start waits for subscribers.
                self.event(events, "playback_ready")
                time.sleep(0.1)
                started = time.monotonic()
                if control == "signal":
                    interrupt_process(process, signal.SIGINT)
                else:
                    process.stdin.close()
                summary = self.event(events, "summary", timeout=2)
                self.assertLess(time.monotonic() - started, 2)
                self.assertTrue(summary["resources_closed"], summary)
                self.assertIsNone(summary["cleanup_error"])
                self.assertEqual(summary["status"]["published_total"], 0)
                self.assertEqual(process.wait(2), 130 if control == "signal" else 0)
                errors = [item for item in trace if item["event"] == "error"]
                if control == "eof":
                    self.assertEqual(errors, [])
                else:
                    self.assertTrue(
                        all(item["data"]["code"] == "INTERRUPTED" for item in errors), errors,
                    )

    def test_sdk_sigint_interrupts_start_and_wait_without_stopping_the_handle(self):
        path = self.small_recording()
        program = r'''
import json, signal, sys, time
from pathlib import Path
import aos_sdk as aos
config, source, operation, output = sys.argv[1:]
context = aos.Context(config=config, domain_id=95)
if operation == "recorder_wait":
    handle = aos.BagRecorder(context, output, all_topics=True)
else:
    handle = aos.BagPlayer(context, source, namespace="/signal",
                           subscriber_wait_ms=30000)
def interrupted(signum, frame):
    raise KeyboardInterrupt("bag signal preserved")
signal.signal(signal.SIGBREAK if hasattr(signal, "SIGBREAK") else signal.SIGINT, interrupted)
try:
    print("waiting", flush=True)
    started = time.monotonic()
    try:
        if operation == "player_start":
            handle.start()
        else:
            handle.wait(30)
        raise AssertionError("wait completed before SIGINT")
    except KeyboardInterrupt as error:
        assert str(error) == "bag signal preserved"
        assert time.monotonic() - started < 2
    assert context.ok()
    assert not handle.status["resources_closed"]
    if operation != "recorder_wait":
        assert handle.status["state"] == "ready", handle.status
        interface = aos.Interface.load(aos.inspect_bag(source)["channels"][0]["interface"])
        node = aos.Node("after_interrupt", context=context)
        subscription = node.create_subscription(interface, "/signal/bag/signal")
        handle.start()
        assert subscription.recv(2) == {"data":"retained"}
        assert handle.wait(2)["status"]["published_total"] == 1
        node.destroy_node()
    report = handle.close()
    assert report["status"]["resources_closed"], report
    assert report["status"]["cleanup_error"] is None, report
    print(json.dumps({"ok":True,"operation":operation}), flush=True)
finally:
    handle.close()
    context.shutdown()
'''
        for operation in ("recorder_wait", "player_start", "player_wait"):
            with self.subTest(operation=operation):
                process = subprocess.Popen(
                    [sys.executable, "-I", "-u", "-c", program,
                     self.environment["AOS_ZENOH_CONFIG"], str(path), operation,
                     str(self.directory / operation)],
                    env=self.environment, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                    text=True, **console_process_options(),
                )
                try:
                    self.assertEqual(read_line(process.stdout, 5).strip(), "waiting")
                    time.sleep(0.1)
                    interrupt_process(process, signal.SIGINT)
                    stdout, stderr = process.communicate(timeout=3)
                    self.assertEqual(process.returncode, 0, stdout + stderr)
                    self.assertEqual(json.loads(stdout), {"ok": True, "operation": operation})
                finally:
                    if process.poll() is None:
                        process.kill()
                    process.wait(5)
                    process.stdout.close()
                    process.stderr.close()

    def test_record_sigterm_info_and_interactive_playback_with_real_consumer(self):
        context, node, interface = self.peers()
        publisher = node.create_publisher(interface, "/bag/live")
        path = self.directory / "recorded"
        recording, events, _ = self.start("record", {
            "directory": str(path), "topics": ["/bag/live"], "follow": True,
            "status_interval": 0.02,
        })
        self.event(events, "recording_status", lambda item: item["data"]["active_channel_count"] == 1)
        for index in range(5):
            publisher.publish({"data": str(index)})
            time.sleep(0.05)
        self.event(events, "recording_status",
                   lambda item: item["data"]["counters"]["written_total"] == 5)
        interrupt_process(recording, signal.SIGTERM)
        summary = self.event(events, "summary")
        self.assertTrue(summary["resources_closed"])
        self.assertEqual(summary["status"]["stop_reason"], "interrupted")
        self.assertEqual(recording.wait(5), 130 if os.name == "nt" else 143)
        self.assertTrue(aos.inspect_bag(path)["manifest"]["clean_close"])
        offline_environment = self.environment | {"AOS_ZENOH_CONFIG": "/missing/config"}
        info = subprocess.run(
            [str(self.binary), "bag", "info", "--request", "-"],
            input=json.dumps({"directory": str(path)}), capture_output=True,
            text=True, env=offline_environment, timeout=10,
        )
        self.assertEqual(info.returncode, 0, info.stdout + info.stderr)
        self.assertEqual(json.loads(info.stdout)["data"]["messages"], 5)
        consumer = aos.Node("bag_cli_consumer", context=context)
        self.addCleanup(consumer.destroy_node)
        subscription = consumer.create_subscription(interface, "/replay/bag/live")
        player, events, _ = self.start("play", {
            "directory": str(path), "namespace": "/replay", "start_paused": True,
            "control_input": "stdin", "subscriber_wait_ms": 1000, "status_interval": 0.02,
        })
        self.event(events, "playback_status", lambda item: item["data"]["state"] == "paused")
        for command in (
            {"id": "seek1", "command": "seek", "offset_ns": 0},
            {"id": "rate1", "command": "rate", "rate": 2},
            {"id": "resume1", "command": "resume"},
        ):
            player.stdin.write(json.dumps(command) + "\n")
            player.stdin.flush()
            reply = self.event(events, "control_result", lambda item: item["data"]["id"] == command["id"])
            self.assertTrue(reply["ok"], reply)
        self.assertEqual([subscription.recv(3)["data"] for _ in range(5)],
                         ["0", "1", "2", "3", "4"])
        summary = self.event(events, "summary")
        self.assertEqual(summary["status"]["published_total"], 5)
        self.assertEqual(summary["status"]["state"], "finished")
        self.assertTrue(summary["resources_closed"])
        self.assertEqual(player.wait(5), 0)

    def test_closed_output_stops_recording_and_commits_the_manifest(self):
        self.peers()
        path = self.directory / "broken-pipe"
        process, _, _ = self.start("record", {
            "directory": str(path), "all_topics": True, "follow": True,
            "status_interval": 0.02,
        }, read_output=False)
        self.assertEqual(json.loads(read_line(process.stdout, 5))["event"], "recording_started")
        process.stdout.close()
        self.assertEqual(process.wait(5), 1, process.stderr.read())
        self.assertTrue(self.clean_manifest(path))
        self.assertEqual(aos.inspect_bag(path)["messages"], 0)

    def test_native_full_pipe_interrupt_cleans_before_output_resumes(self):
        self.peers()
        for resume in (True, False):
            path = self.directory / f"native-full-output-{resume}"
            process, _, _ = self.start("record", {
                "directory": str(path), "all_topics": True, "follow": True,
                "status_interval": 0.02,
            }, read_output=False)
            self.wait_for(lambda: (path / "manifest.json").exists())
            # No reader: periodic status fills the native anonymous pipe.
            time.sleep(3)
            interrupt_process(process)
            self.wait_for(lambda: self.clean_manifest(path), timeout=1.5)
            self.assertIsNone(process.poll(), "terminal output should still await its blocked sink")
            if resume:
                output = process.stdout.read()
                records = [json.loads(line) for line in output.splitlines()]
                summary = next(item["data"] for item in records if item["event"] == "summary")
                self.assertTrue(summary["resources_closed"])
                self.assertEqual(summary["status"]["stop_reason"], "interrupted")
                self.assertEqual(process.wait(5), 130)
            else:
                # The initiating interrupt remains primary if its final report
                # cannot be delivered within the shared terminal deadline.
                self.assertEqual(process.wait(5), 130)
            self.assertTrue(aos.inspect_bag(path)["manifest"]["clean_close"])

    @unittest.skipUnless(os.name == "posix", "Unix socket descriptor backpressure contract")
    def test_full_output_does_not_delay_signal_cleanup(self):
        self.peers()
        path = self.directory / "full-output"
        receive, send = socket.socketpair()
        self.addCleanup(receive.close)
        self.addCleanup(send.close)
        send.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 8192)
        send.setblocking(False)
        prefix = bytearray()
        while True:
            try:
                size = send.send(b"x" * 4096)
                prefix.extend(b"x" * size)
            except BlockingIOError:
                break
        self.assertGreater(len(prefix), 0)
        send.setblocking(True)
        process, _, _ = self.start("record", {
            "directory": str(path), "all_topics": True, "follow": True,
            "status_interval": 0.02,
        }, read_output=False, stdout=send)
        send.close()
        self.wait_for(lambda: (path / "manifest.json").exists())
        # The already-full kernel socket keeps even recording_started blocked.
        # Constructor completion installs the signal handlers before the loop.
        time.sleep(0.1)
        interrupt_process(process, signal.SIGTERM)
        self.wait_for(lambda: self.clean_manifest(path), timeout=1.5)
        self.assertIsNone(process.poll(), "the final report should still await its blocked sink")
        receive.settimeout(5)
        data = bytearray()
        while chunk := receive.recv(65536):
            data.extend(chunk)
        self.assertEqual(data[:len(prefix)], prefix)
        records = [json.loads(line) for line in data[len(prefix):].splitlines()]
        summary = next(item["data"] for item in records if item["event"] == "summary")
        self.assertTrue(summary["resources_closed"])
        self.assertEqual(summary["status"]["stop_reason"], "interrupted")
        self.assertEqual(process.wait(5), 130 if os.name == "nt" else 143)
        self.assertTrue(aos.inspect_bag(path)["manifest"]["clean_close"])

    def test_invalid_controls_preserve_paused_state_and_eof_closes_without_playing(self):
        context, node, interface = self.peers()
        path = self.directory / "controls-source"
        publisher = node.create_publisher(interface, "/bag/controls")
        recorder = aos.BagRecorder(context, path, topics=["/bag/controls"])
        self.addCleanup(recorder.close)
        self.wait_for(lambda: len(recorder.status["active_channels"]) == 1)
        publisher.publish({"data": "retained"})
        self.wait_for(lambda: recorder.status["counters"]["written_total"] == 1)
        recorder.close()
        consumer = node.create_subscription(interface, "/replay/bag/controls")
        process, events, trace = self.start("play", {
            "directory": str(path), "namespace": "/replay", "start_paused": True,
            "control_input": "stdin", "status_interval": 0.02,
        })
        self.event(events, "playback_status", lambda item: item["data"]["state"] == "paused")
        for line in (
            '{"id":"bad","command":"seek","offset_ns":3.0}',
            '{"id":"bad","command":"pause","command":"resume"}',
            '{"id":"bad","command":"rate","rate":0}',
        ):
            process.stdin.write(line + "\n")
            process.stdin.flush()
            reply = self.event(events, "control_result")
            self.assertFalse(reply["ok"], reply)
            self.assertIsNone(reply["id"])
            self.assertEqual(reply["error"]["kind"], "invalid_argument")
            self.assertEqual(reply["status"]["state"], "paused")
            self.assertEqual(reply["status"]["epoch"], 0)
            self.assertEqual(reply["status"]["published_total"], 0)
        process.stdin.write('{"id":"alive","command":"status"}\n')
        process.stdin.flush()
        self.assertTrue(self.event(events, "control_result")["ok"])
        process.stdin.close()
        summary = self.event(events, "summary")
        self.assertTrue(summary["resources_closed"])
        self.assertEqual(summary["status"]["state"], "stopped")
        self.assertEqual(summary["status"]["published_total"], 0)
        self.assertEqual(process.wait(5), 0)
        with self.assertRaises(aos.WaitTimeoutError):
            consumer.recv(0.05)
        self.assertFalse(any(item["event"] == "error" for item in trace))

    def test_offline_corruption_and_recovery_do_not_require_network(self):
        context, node, interface = self.peers()
        path = self.directory / "offline-source"
        publisher = node.create_publisher(interface, "/bag/offline")
        recorder = aos.BagRecorder(context, path, topics=["/bag/offline"])
        self.addCleanup(recorder.close)
        self.wait_for(lambda: len(recorder.status["active_channels"]) == 1)
        publisher.publish({"data": "offline"})
        self.wait_for(lambda: recorder.status["counters"]["written_total"] == 1)
        recorder.close()
        original = next(path.glob("*.mcap")).read_bytes()
        partial = self.directory / "source.mcap.partial"
        partial.write_bytes(original[:-128])
        recovered = self.directory / "offline-recovered"
        environment = self.environment | {"AOS_ZENOH_CONFIG": "/missing/network"}
        result = subprocess.run(
            [str(self.binary), "bag", "recover", "--request", "-"],
            input=json.dumps({"directory": str(partial), "destination": str(recovered)}),
            capture_output=True, text=True, timeout=10, env=environment,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertTrue(json.loads(result.stdout)["data"]["info"]["manifest"]["incomplete"])
        self.assertEqual(partial.read_bytes(), original[:-128])
        for allow, expected in ((False, 2), (True, 0)):
            result = subprocess.run(
                [str(self.binary), "bag", "info", "--request", "-"],
                input=json.dumps({"directory": str(recovered), "allow_incomplete": allow}),
                capture_output=True, text=True, timeout=10, env=environment,
            )
            self.assertEqual(result.returncode, expected, result.stdout + result.stderr)
            if allow:
                self.assertEqual(json.loads(result.stdout)["data"]["messages"], 1)
            else:
                self.assertEqual(json.loads(result.stdout)["error"]["code"], "BAG_INCOMPLETE")
        shard = next(path.glob("*.mcap"))
        shard.write_bytes(original[:-1])
        result = subprocess.run(
            [str(self.binary), "bag", "info", "--request", "-"],
            input=json.dumps({"directory": str(path), "allow_incomplete": True}),
            capture_output=True, text=True, timeout=10, env=environment,
        )
        self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
        self.assertEqual(json.loads(result.stdout)["error"]["code"], "BAG_CORRUPT")

    def test_sigkill_recovers_flushed_chunks_without_modifying_source(self):
        _, node, interface = self.peers()
        publisher = node.create_publisher(interface, "/bag/crash")
        source = self.directory / "crashed"
        process, events, _ = self.start("record", {
            "directory": str(source), "topics": ["/bag/crash"], "follow": True,
            "status_interval": 0.02, "storage": {"chunk_interval_ms": 50},
        })
        self.event(events, "recording_status", lambda item: item["data"]["active_channel_count"] == 1)

        def has_flushed_message_pairs():
            partials = list(source.glob("*.partial"))
            if not partials:
                return False
            data = partials[0].read_bytes()
            # The descriptor chunk is flushed before the subscription exists.
            # A complete chunk alone therefore says nothing about our messages.
            # This fixture writes uncompressed MCAP chunks. Count both channels'
            # sequence 0..4 only inside whole chunks with a valid nonzero CRC.
            sequences = {}
            offset = 8
            while offset + 9 <= len(data):
                end = offset + 9 + int.from_bytes(data[offset + 1:offset + 9], "little")
                if end > len(data):
                    break
                if data[offset] == 6:
                    chunk = data[offset + 9:end]
                    if len(chunk) < 40:
                        return False
                    uncompressed = int.from_bytes(chunk[16:24], "little")
                    crc = int.from_bytes(chunk[24:28], "little")
                    compression_length = int.from_bytes(chunk[28:32], "little")
                    self.assertEqual(compression_length, 0, "fixture must use uncompressed chunks")
                    size = int.from_bytes(chunk[32:40], "little")
                    records = chunk[40:]
                    if (len(records) != size or size != uncompressed or not crc
                            or zlib.crc32(records) != crc):
                        return False
                    cursor = 0
                    while cursor + 9 <= len(records):
                        record_end = cursor + 9 + int.from_bytes(
                            records[cursor + 1:cursor + 9], "little",
                        )
                        if record_end > len(records):
                            return False
                        if records[cursor] == 5:
                            message = records[cursor + 9:record_end]
                            if len(message) < 22:
                                return False
                            channel = int.from_bytes(message[:2], "little")
                            sequence = int.from_bytes(message[2:6], "little")
                            sequences.setdefault(channel, []).append(sequence)
                        cursor = record_end
                    if cursor != len(records):
                        return False
                offset = end
            return len(sequences) == 2 and all(
                sorted(values) == list(range(5)) for values in sequences.values()
            )

        self.assertFalse(has_flushed_message_pairs(), "descriptors must not satisfy message readiness")
        for index in range(5):
            publisher.publish({"data": str(index)})
        self.event(events, "recording_status",
                   lambda item: item["data"]["counters"]["written_total"] == 5)
        self.wait_for(has_flushed_message_pairs)
        process.kill()
        self.assertEqual(process.wait(5), 1 if os.name == "nt" else -signal.SIGKILL)
        before = {path.name: hashlib.sha256(path.read_bytes()).hexdigest()
                  for path in source.iterdir()}
        manifest = json.loads((source / "manifest.json").read_text())
        self.assertFalse(manifest["clean_close"])
        destination = self.directory / "recovered-crash"
        result = subprocess.run(
            [str(self.binary), "bag", "recover", "--request", "-"],
            input=json.dumps({"directory": str(source), "destination": str(destination)}),
            capture_output=True, text=True, timeout=10,
            env=self.environment | {"AOS_ZENOH_CONFIG": "/missing/network",
                                    "AOS_PROCESS_CONFIG": "/missing/bootstrap"},
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        report = json.loads(result.stdout)["data"]
        self.assertEqual(report["info"]["messages"], 5)
        self.assertTrue(report["info"]["manifest"]["incomplete"])
        self.assertTrue(all(item["verified_unchanged"] for item in report["sources"]))
        self.assertEqual(before, {path.name: hashlib.sha256(path.read_bytes()).hexdigest()
                                  for path in source.iterdir()})
        self.assertEqual(aos.inspect_bag(destination, allow_incomplete=True)["messages"], 5)

    def test_cli_process_bootstrap_selects_network_and_owned_node(self):
        context, node, interface = self.peers()
        publisher = node.create_publisher(interface, "/bag/bootstrap")
        bootstrap = self.directory / "bootstrap.json"
        bootstrap.write_text(json.dumps({
            "schema_version": 1, "launch_id": str(uuid.uuid4()),
            "process_id": "capture", "generation": 2,
            "context": {"launch": {"domain_id": 95,
                                   "zenoh_config": self.environment["AOS_ZENOH_CONFIG"]}},
            "node": {"launch": {"name": "deployed_capture", "namespace": "/launch_bag"}},
        }))
        self.environment.update(
            AOS_PROCESS_CONFIG=str(bootstrap), AOS_DOMAIN_ID="17",
            AOS_ZENOH_CONFIG="/missing/overridden-network",
        )
        path = self.directory / "bootstrap-recording"
        process, events, _ = self.start("record", {
            "directory": str(path), "topics": ["/bag/bootstrap"], "follow": True,
            "status_interval": 0.02,
        })
        self.event(events, "recording_status", lambda item: item["data"]["active_channel_count"] == 1)
        self.wait_for(lambda: any(
            item["node_name"] == "deployed_capture" and item["namespace"] == "/launch_bag"
            for item in context.graph()
        ))
        publisher.publish({"data": "injected"})
        self.event(events, "recording_status",
                   lambda item: item["data"]["counters"]["written_total"] == 1)
        interrupt_process(process, signal.SIGTERM)
        self.assertTrue(self.event(events, "summary")["resources_closed"])
        self.assertEqual(process.wait(5), 130 if os.name == "nt" else 143)
        self.assertEqual(aos.inspect_bag(path)["messages"], 1)

    def test_schema_is_available_without_network(self):
        for verb in ("record", "info", "recover", "play"):
            result = subprocess.run([str(self.binary), "schema", "bag", verb],
                                    capture_output=True, text=True, timeout=5)
            self.assertEqual(result.returncode, 0, result.stderr)
            schema = json.loads(result.stdout)["data"]
            self.assertEqual(schema["command"], f"aos bag {verb}")
            self.assertFalse(schema["request_schema"]["additionalProperties"])

    def test_live_invalid_inputs_precede_network_and_creation(self):
        for verb, request in (
            ("record", {"directory": str(self.directory / "no-write"), "all_topics": True,
                        "queue_messages": 3.0}),
            ("record", {"directory": str(self.directory / "no-write"), "all_topics": True,
                        "follow": True, "duration": 1}),
            ("play", {"directory": "/missing/bag", "namespace": "/x", "start_paused": True}),
        ):
            process = subprocess.run(
                [str(self.binary), "bag", verb, "--request", "-"],
                input=json.dumps(request), capture_output=True, text=True, timeout=5,
                env=self.environment | {"AOS_ZENOH_CONFIG": "/missing/network"},
            )
            self.assertEqual(process.returncode, 2, process.stdout + process.stderr)
            self.assertEqual(json.loads(process.stdout)["data"]["code"], "INVALID_ARGUMENT")
        self.assertFalse((self.directory / "no-write").exists())


if __name__ == "__main__":
    unittest.main()
