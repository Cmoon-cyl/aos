"""Real-router typed SDK tests, using the installed wheel.

Set AOS_ZENOHD to the fixed zenohd 1.10.1 binary, as for stage-0 tests.
"""

import json
import pathlib
import queue
import subprocess
import sys
import threading
import time
import unittest

import aos_sdk as aos
import test_zenoh_session as router_env

ROOT = pathlib.Path(__file__).resolve().parents[2]


def setUpModule():
    router_env.setUpModule()


def tearDownModule():
    router_env.tearDownModule()


class TypedSdkTest(unittest.TestCase):
    def setUp(self):
        self.context = router_env.context(domain_id=55)
        self.provider = aos.Node(
            "provider", "/test", self.context,
            display_name="Example", summary="Test node", tags=["test"],
        )
        self.caller = aos.Node("caller", "/test", self.context)
        self.interface = aos.Interface.load(ROOT / "interfaces/AddTwoInts.json")
        self.message = aos.Interface.load(ROOT / "interfaces/String.json")
        self.executor = aos.MultiThreadedExecutor(4, context=self.context)
        self.executor.add_node(self.provider)
        self.executor.add_node(self.caller)
        self.spinner = threading.Thread(target=self.executor.spin, name="test-spin")
        self.spinner.start()

    def tearDown(self):
        self.context.shutdown()
        self.executor.shutdown(timeout_sec=5)
        self.spinner.join(5)
        self.assertFalse(self.spinner.is_alive())
        for node in (self.caller, self.provider):
            try:
                node.destroy_node()
            except aos.InvalidStateError:
                pass

    def test_readiness_wait_times_out_then_observes_provider_without_work(self):
        client = self.caller.create_client(self.interface, "readiness")
        action_interface = aos.Interface.load(ROOT / "interfaces/action/Fibonacci.json")
        action_client = aos.ActionClient(self.caller, action_interface, "readiness")
        self.assertFalse(client.wait_for_service(timeout_sec=0.1))
        self.assertFalse(action_client.wait_for_server(timeout_sec=0.1))
        work = []
        providers = []

        def provide():
            providers.append(self.provider.create_service(
                self.interface, "readiness",
                lambda request: work.append(request) or {"sum": request["a"] + request["b"]},
            ))
            providers.append(aos.ActionServer(
                self.provider, action_interface, "readiness",
                lambda goal: work.append(goal.request) or {"sequence": []},
            ))

        # Provider creation needs the GIL while the caller blocks in Rust.
        starter = threading.Timer(0.05, provide)
        starter.start()
        try:
            self.assertTrue(client.wait_for_service(timeout_sec=3))
            self.assertTrue(action_client.wait_for_server(timeout_sec=3))
        finally:
            starter.join(3)
        self.assertFalse(starter.is_alive())
        self.assertEqual(len(providers), 2)
        self.assertEqual(work, [])

    def test_names_metadata_and_service_future(self):
        self.provider.create_service(
            self.interface, "add", lambda req: {"sum": req["a"] + req["b"]},
            summary="Add integers", side_effects="read_only", contract_version="1",
        )
        client = self.caller.create_client(self.interface, "add")
        self.assertEqual(self.provider.resolve_name("~/status"), "/test/provider/status")
        card = self.provider.get_node_card()
        self.assertEqual(card["summary"], "Test node")
        self.assertEqual(card["display_name"], "Example")
        self.assertIn("test", card["tags"])
        endpoint = next(e for e in card["endpoints"] if e["name"] == "/test/add")
        self.assertEqual(endpoint["side_effects"], "read_only")
        self.assertEqual(endpoint["contract_version"], "1")
        future = client.call_async({"a": 2, "b": 3})
        self.assertEqual(future.result(5), {"sum": 5})
        done = threading.Event()
        thread_names = []
        future.add_done_callback(
            lambda _: (thread_names.append(threading.current_thread().name), done.set())
        )
        self.assertTrue(done.wait(5))
        self.assertTrue(thread_names[0].startswith("aos-callback"))
        # Client description does not claim read-only when omitted.
        entry = next(
            e for e in self.caller.get_node_card()["endpoints"] if e["name"] == "/test/add"
        )
        self.assertIsNone(entry["side_effects"])

    def test_service_exception_does_not_stop_provider(self):
        calls = []

        def handle(request):
            calls.append(request)
            if request["a"] == 0:
                raise RuntimeError("business secret must not cross the wire")
            return {"sum": request["a"] + request["b"]}

        self.provider.create_service(self.interface, "exceptions", handle)
        client = self.caller.create_client(self.interface, "exceptions")
        with self.assertRaises(aos.ServiceError) as error:
            client.call_async({"a": 0, "b": 1}).result(5)
        self.assertNotIn("business secret", str(error.exception))
        self.assertIn("HANDLER_ERROR", str(error.exception))
        self.assertEqual(client.call_async({"a": 3, "b": 4}).result(5), {"sum": 7})
        with self.assertRaises(aos.InvalidArgumentError):
            client.call_async({"a": True, "b": 1}).result(5)
        self.assertEqual(len(calls), 2)

    def test_reentrant_requests_finish_independently(self):
        started = threading.Event()
        release = threading.Event()

        def handle(request):
            if request["a"] == 1:
                started.set()
                self.assertTrue(release.wait(5))
            return {"sum": request["a"] + request["b"]}

        self.provider.create_service(
            self.interface, "parallel", handle,
            callback_group=aos.ReentrantCallbackGroup(),
        )
        client = self.caller.create_client(self.interface, "parallel")
        first = client.call_async({"a": 1, "b": 1})
        self.assertTrue(started.wait(5))
        try:
            self.assertEqual(
                client.call_async({"a": 2, "b": 2}).result(3), {"sum": 4}
            )
            self.assertFalse(first.done())
        finally:
            release.set()
        self.assertEqual(first.result(5), {"sum": 2})

    def test_default_group_prevents_overlapping_callbacks(self):
        started = threading.Event()
        release = threading.Event()
        observed = []

        def handle(request):
            observed.append(request["a"])
            if request["a"] == 1:
                started.set()
                release.wait(5)
            return {"sum": request["a"]}

        self.provider.create_service(self.interface, "serial", handle)
        client = self.caller.create_client(self.interface, "serial")
        first = client.call_async({"a": 1, "b": 0})
        self.assertTrue(started.wait(5))
        second = client.call_async({"a": 2, "b": 0})
        time.sleep(0.15)
        self.assertEqual(observed, [1])
        release.set()
        self.assertEqual(first.result(5), {"sum": 1})
        self.assertEqual(second.result(5), {"sum": 2})

    def test_topic_callback_and_shared_validation(self):
        received = []
        ready = threading.Event()
        self.caller.create_subscription(
            self.message, "chatter",
            lambda message: (received.append(message), ready.set()),
        )
        publisher = self.provider.create_publisher(self.message, "chatter")
        with self.assertRaises(aos.InvalidArgumentError):
            publisher.publish({"data": 42})
        deadline = time.monotonic() + 5
        while not ready.is_set() and time.monotonic() < deadline:
            publisher.publish({"data": "hello"})
            ready.wait(0.05)
        self.assertEqual(received[0], {"data": "hello"})

    def test_destroy_client_completes_pending_future(self):
        # A known Service keeps a request open until its user callback returns.
        entered = threading.Event()
        release = threading.Event()

        def handle(request):
            entered.set()
            release.wait(5)
            return {"sum": 1}

        self.provider.create_service(self.interface, "waiting", handle)
        client = self.caller.create_client(self.interface, "waiting")
        future = client.call_async({"a": 1, "b": 0}, timeout=30)
        self.assertTrue(entered.wait(5))
        try:
            client.destroy()
            with self.assertRaises(aos.InvalidStateError):
                future.result(3)
        finally:
            release.set()

    def test_service_timeout_does_not_retry_or_cancel_callback(self):
        entered = threading.Event()
        release = threading.Event()
        finished = threading.Event()
        calls = []

        def handle(request):
            calls.append(request)
            entered.set()
            release.wait(5)
            finished.set()
            return {"sum": request["a"] + request["b"]}

        self.provider.create_service(self.interface, "timeout", handle)
        client = self.caller.create_client(self.interface, "timeout")
        future = client.call_async({"a": 1, "b": 2}, timeout=0.15)
        self.assertTrue(entered.wait(5))
        try:
            with self.assertRaises(aos.WaitTimeoutError):
                future.result(3)
            self.assertFalse(finished.is_set())
            self.assertEqual(len(calls), 1)
        finally:
            release.set()
        self.assertTrue(finished.wait(3))


class ProcessExamplesTest(unittest.TestCase):
    def test_service_examples_in_independent_python_processes(self):
        config = str(router_env.session_config)
        server = subprocess.Popen(
            [sys.executable, "-I", str(ROOT / "examples/add_two_ints_server.py"),
             "--config", config, "--stop-on-stdin"],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        try:
            lines = queue.Queue()
            threading.Thread(
                target=lambda: lines.put(server.stdout.readline()), daemon=True
            ).start()
            ready = lines.get(timeout=10)
            self.assertTrue(json.loads(ready)["ready"])
            client = subprocess.run(
                [sys.executable, "-I", str(ROOT / "examples/add_two_ints_client.py"),
                 "17", "25", "--config", config],
                capture_output=True, text=True, timeout=15,
            )
            self.assertEqual(client.returncode, 0, client.stderr)
            self.assertEqual(json.loads(client.stdout), {"sum": 42})
        finally:
            try:
                stdout, stderr = server.communicate(input="stop\n", timeout=10)
            except subprocess.TimeoutExpired:
                server.kill()
                server.communicate()
                raise
        self.assertEqual(server.returncode, 0, stderr)


if __name__ == "__main__":
    unittest.main()
