"""Stage 1: installed-wheel Python peers and the native Rust CLI via zenohd.

Required: AOS_ZENOHD (1.10.1), AOS_CLI (the built native executable).
Run with the fresh wheel environment's Python, never a source-package import:

    python -I -m unittest discover -s <repo>/python/tests -p test_stage1_cli.py -v

AOS_STAGE1_RECORD optionally saves the actual CLI outputs and child evidence.
"""

import json
import os
from pathlib import Path
import queue
import re
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import uuid

from test_process_echo import free_port, read_events, stop_process

ROOT = Path(__file__).resolve().parents[2]
SERVICE_TYPE = "aos_examples/srv/AddTwoInts"

# This is executed by -I -c: the repository supplies interface data only.
# It cannot add python/aos_sdk to the child's import path.
WORKER = r"""
import json, os, pathlib, site, sys, threading
import aos_sdk as aos
from aos_sdk import _native
role, root, config, namespace, durability = sys.argv[1:]
root = pathlib.Path(root)
native_path = pathlib.Path(_native.__file__).resolve()
assert sys.flags.isolated
assert any(native_path.is_relative_to(pathlib.Path(p).resolve()) for p in site.getsitepackages()), str(native_path)
output_lock = threading.Lock()
def emit(event, **data):
    with output_lock:
        print(json.dumps(dict(event=event, pid=os.getpid(), ppid=os.getppid(), **data)), flush=True)
def commands():
    for line in sys.stdin:
        yield json.loads(line)
context = aos.Context(config=config, domain_id=61)
node = aos.Node(role, namespace, context, display_name="Stage 1 provider", summary="Independent process acceptance", tags=["acceptance"])
identity = dict(native_path=str(native_path), isolated=bool(sys.flags.isolated), node=node.fully_qualified_name)
release = threading.Event()
executor = spinner = None
calls = []
try:
    if role == "provider":
        interface = aos.Interface.load(root / "interfaces/AddTwoInts.json")
        def add(request):
            calls.append(dict(service="add", values=request))
            return {"sum": request["a"] + request["b"]}
        def slow(request):
            calls.append(dict(service="slow", values=request))
            emit("slow_entered", values=request)
            assert release.wait(20), "test did not release the provider callback"
            emit("slow_completed", values=request)
            return {"sum": request["a"] + request["b"]}
        node.create_service(interface, "add", add, summary="Add two integers", side_effects="read_only", contract_version="1")
        node.create_service(interface, "slow", slow, callback_group=aos.ReentrantCallbackGroup())
        executor = aos.MultiThreadedExecutor(2, context=context)
        executor.add_node(node)
        spinner = threading.Thread(target=executor.spin, name="acceptance-executor")
        spinner.start()
        emit("ready", **identity)
        for command in commands():
            if command["op"] == "release":
                release.set()
            elif command["op"] == "stop":
                break
            else:
                raise AssertionError(command)
    else:
        interface = aos.Interface.load(root / "interfaces/String.json")
        qos = aos.QoSProfile(depth=1, durability=(aos.DurabilityPolicy.TRANSIENT_LOCAL if durability == "transient_local" else aos.DurabilityPolicy.VOLATILE))
        if role == "publisher":
            publisher = node.create_publisher(interface, "chatter", qos)
            emit("ready", **identity)
            for command in commands():
                if command["op"] == "stop":
                    break
                assert command["op"] == "publish", command
                if durability == "volatile":
                    publisher.wait_for_subscribers(5)
                publisher.publish(command["value"])
                emit("published", value=command["value"])
        else:
            assert role == "subscriber", role
            subscription = node.create_subscription(interface, "chatter", qos_profile=qos)
            emit("ready", **identity)
            emit("message", value=subscription.recv(10))
finally:
    release.set()
    if executor is not None:
        assert executor.shutdown(timeout_sec=5), "executor did not finish"
        spinner.join(5)
        assert not spinner.is_alive(), "executor thread stayed alive"
    node.destroy_node()
    context.shutdown()
emit("done", context_closed=not context.ok(), calls=calls, **identity)
"""


class RouterProcess:
    """Own only this test's router; reuse the T01 process helpers."""

    def __init__(self):
        self.temp = tempfile.TemporaryDirectory(prefix="aos-stage1-")
        self.directory = Path(self.temp.name)
        self.log = (self.directory / "router.log").open("w+")
        self.process = None
        port = free_port()
        endpoint = f"tcp/127.0.0.1:{port}"
        self.config = self.directory / "session.json5"
        text = (ROOT / "config/zenoh-session.json5").read_text()
        assert text.count("tcp/localhost:7447") == 1
        self.config.write_text(text.replace("tcp/localhost:7447", endpoint))
        try:
            self.process = subprocess.Popen(
                [os.environ["AOS_ZENOHD"], "--config", str(ROOT / "config/zenoh-router.json5"),
                 f'--cfg=listen/endpoints:["{endpoint}"]'],
                stdout=self.log, stderr=subprocess.STDOUT,
            )
            deadline = time.monotonic() + 10
            while True:
                if self.process.poll() is not None:
                    raise AssertionError(f"zenohd exited: {self.process.returncode}")
                try:
                    socket.create_connection(("127.0.0.1", port), 0.1).close()
                    break
                except OSError:
                    if time.monotonic() >= deadline:
                        raise AssertionError(f"zenohd did not listen at {endpoint}")
                    threading.Event().wait(0.02)
        except BaseException:
            self.close()
            raise

    def close(self):
        forced = False
        if self.process is not None and self.process.poll() is None:
            if os.name == "posix":
                self.process.send_signal(signal.SIGINT)
            else:
                self.process.terminate()
            try:
                self.process.wait(5)
            except subprocess.TimeoutExpired:
                forced = stop_process(self.process)
        self.log.close()
        self.temp.cleanup()
        if forced:
            raise AssertionError("router needed forced cleanup")


class WorkerProcess:
    def __init__(self, router, role, namespace, durability="volatile"):
        self.log = tempfile.TemporaryFile(mode="w+")
        self.lines = []
        self.messages = queue.Queue()
        self.process = subprocess.Popen(
            [sys.executable, "-I", "-c", WORKER, role, str(ROOT), str(router.config), namespace, durability],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=self.log, text=True,
        )
        self.reader = threading.Thread(target=read_events, args=(self.process.stdout, self.lines, self.messages), daemon=True)
        self.reader.start()

    def event(self, name, timeout=12):
        deadline = time.monotonic() + timeout
        while True:
            try:
                event = self.messages.get(timeout=max(0, deadline - time.monotonic()))
            except queue.Empty:
                self.log.seek(0)
                raise AssertionError(f"child {self.process.pid} did not emit {name}: {self.lines!r} {self.log.read()}") from None
            if event["event"] == name:
                return event
            if event["event"] in ("eof", "reader_error"):
                self.log.seek(0)
                raise AssertionError(f"child ended before {name}: {event!r} {self.log.read()}")

    def send(self, op, **data):
        self.process.stdin.write(json.dumps(dict(op=op, **data)) + "\n")
        self.process.stdin.flush()

    def finish(self, stop=False):
        if stop:
            self.send("stop")
        done = self.event("done")
        self.process.wait(8)
        self.reader.join(2)
        self.log.seek(0)
        assert self.process.returncode == 0, self.log.read()
        assert not self.reader.is_alive()
        assert done["context_closed"] and done["isolated"], done
        return done

    def close(self):
        forced = stop_process(self.process)
        self.reader.join(2)
        self.process.stdin.close()
        self.log.close()
        if forced:
            raise AssertionError("Python child needed forced cleanup")


class Stage1CliTest(unittest.TestCase):
    records = []

    @classmethod
    def setUpClass(cls):
        for name in ("AOS_ZENOHD", "AOS_CLI"):
            if not os.environ.get(name) or not Path(os.environ[name]).is_file():
                raise AssertionError(f"{name} must name an existing executable; this acceptance test cannot skip")
        version = subprocess.run([os.environ["AOS_ZENOHD"], "--version"], capture_output=True, text=True, check=True, timeout=5).stdout
        if not re.search(r"\bzenohd v?1\.10\.1\b", version):
            raise AssertionError(f"expected zenohd 1.10.1, got {version!r}")

    @classmethod
    def tearDownClass(cls):
        record = {"status": "passed" if all(item["status"] == "passed" for item in cls.records) else "failed", "tests": cls.records}
        destination = os.environ.get("AOS_STAGE1_RECORD")
        if destination:
            Path(destination).write_text(json.dumps(record, indent=2) + "\n")

    def setUp(self):
        self.record = {"test": self.id(), "status": "failed", "cli": [], "children": []}
        self.records.append(self.record)
        self.router = RouterProcess()
        self.addCleanup(self.cleanup_process, self.router)
        self.namespace = f"/stage1_{uuid.uuid4().hex}"

    def cleanup_process(self, process):
        try:
            process.close()
        except BaseException:
            self.record["status"] = "failed"
            raise

    def worker(self, role, durability="volatile"):
        worker = WorkerProcess(self.router, role, self.namespace, durability)
        self.addCleanup(self.cleanup_process, worker)
        ready = worker.event("ready")
        from process_support import assert_child_identity
        assert_child_identity(self, ready, worker.process)
        self.assertNotEqual(ready["pid"], os.getpid())
        self.assertTrue(ready["isolated"])
        self.record["children"].append(ready)
        return worker

    def cli(self, group, verb, request=None, expected_exit=0):
        environment = dict(os.environ, AOS_ZENOH_CONFIG=str(self.router.config), AOS_DOMAIN_ID="61")
        # Type lookup must contact the independently installed provider.
        environment.pop("AOS_INTERFACE_PATH", None)
        command = [os.environ["AOS_CLI"], group, verb, "--request", "-"]
        result = subprocess.run(command, input=json.dumps(request or {}), capture_output=True, text=True, env=environment, timeout=20)
        self.record["cli"].append(dict(command=command, request=request or {}, exit=result.returncode, stdout=result.stdout, stderr=result.stderr))
        self.assertEqual(result.returncode, expected_exit, result.stdout + result.stderr)
        lines = result.stdout.splitlines()
        self.assertEqual(len(lines), 1, result.stdout)
        response = json.loads(lines[0])
        self.assertEqual(response["ok"], expected_exit == 0, response)
        return response

    def test_cli_discovers_and_calls_independent_python_provider(self):
        provider = self.worker("provider")
        add_name, slow_name = self.namespace + "/add", self.namespace + "/slow"
        deadline = time.monotonic() + 8
        while True:
            services = self.cli("service", "list")["data"]["services"]
            names = {item["name"] for item in services}
            if {add_name, slow_name}.issubset(names):
                break
            self.assertLess(time.monotonic(), deadline, "provider services were not discovered")
        node = self.cli("node", "info", {"node_name": self.namespace + "/provider"})["data"]
        self.assertEqual(node["fully_qualified_name"], self.namespace + "/provider")
        self.assertEqual(node["display_name"], "Stage 1 provider")
        self.assertIn(add_name, [entry["name"] for entry in node["endpoints"]])
        definition = self.cli("interface", "show", {"type": SERVICE_TYPE})["data"]
        self.assertEqual(definition, json.loads((ROOT / "interfaces/AddTwoInts.json").read_text()))
        request = {"service_name": add_name, "service_type": SERVICE_TYPE, "values": {"a": 17, "b": 25}, "timeout": 2}
        self.assertEqual(self.cli("service", "call", request)["data"]["response"], {"sum": 42})
        invalid = dict(request, values={"a": "17", "b": 25})
        self.assertEqual(self.cli("service", "call", invalid, expected_exit=2)["error"]["code"], "INVALID_ARGUMENT")
        slow = dict(request, service_name=slow_name, timeout=0.2)
        self.assertEqual(self.cli("service", "call", slow, expected_exit=4)["error"]["code"], "TIMEOUT")
        self.assertEqual(provider.event("slow_entered")["values"], request["values"])
        provider.send("release")
        provider.event("slow_completed")
        done = provider.finish(stop=True)
        self.assertEqual(done["calls"], [{"service": "add", "values": request["values"]}, {"service": "slow", "values": request["values"]}])
        self.record["children"].append(done)
        deadline = time.monotonic() + 8
        while True:
            nodes = self.cli("node", "list")["data"]["nodes"]
            services = self.cli("service", "list")["data"]["services"]
            if not any(item["name"].startswith(self.namespace + "/") for item in nodes + services):
                break
            self.assertLess(time.monotonic(), deadline, "provider graph declarations survived normal exit")
        self.record["status"] = "passed"

    def test_two_python_processes_exchange_one_typed_message(self):
        subscriber = self.worker("subscriber")
        publisher = self.worker("publisher")
        self.assertNotEqual(subscriber.process.pid, publisher.process.pid)
        value = {"data": "one typed publication"}
        publisher.send("publish", value=value)
        self.assertEqual(publisher.event("published")["value"], value)
        self.assertEqual(subscriber.event("message")["value"], value)
        self.record["children"] += [subscriber.finish(), publisher.finish(stop=True)]
        self.record["status"] = "passed"

    def test_late_python_subscriber_reads_live_publisher_history(self):
        publisher = self.worker("publisher", "transient_local")
        value = {"data": "published before the subscriber process existed"}
        publisher.send("publish", value=value)
        self.assertEqual(publisher.event("published")["value"], value)
        self.assertIsNone(publisher.process.poll())
        # Start the subscriber only after publication was acknowledged locally.
        subscriber = self.worker("subscriber", "transient_local")
        self.assertNotEqual(subscriber.process.pid, publisher.process.pid)
        self.assertEqual(subscriber.event("message")["value"], value)
        self.assertIsNone(publisher.process.poll(), "history publisher exited too early")
        self.record["children"] += [subscriber.finish(), publisher.finish(stop=True)]
        self.record["status"] = "passed"


if __name__ == "__main__":
    unittest.main()
