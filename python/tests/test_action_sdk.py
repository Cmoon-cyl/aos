"""Installed-wheel Action checks over real local Zenoh sessions."""

import json
import pathlib
import queue
import subprocess
import sys
import threading
import time
import unittest
import uuid

import aos_sdk as aos
import test_zenoh_session as router_env

ROOT = pathlib.Path(__file__).resolve().parents[2]


def setUpModule():
    router_env.setUpModule()


def tearDownModule():
    router_env.tearDownModule()


class ActionSdkTest(unittest.TestCase):
    def setUp(self):
        self.context = router_env.context(domain_id=56)
        self.provider = aos.Node("action_provider", "/test", self.context)
        self.caller = aos.Node("action_caller", "/test", self.context)
        self.interface = aos.Interface.load(ROOT / "interfaces/action/Fibonacci.json")
        self.executor = aos.MultiThreadedExecutor(4, context=self.context)
        self.executor.add_node(self.provider)
        self.executor.add_node(self.caller)
        self.spinner = threading.Thread(target=self.executor.spin, name="action-test-spin")
        self.spinner.start()
        self.release = threading.Event()

    def tearDown(self):
        self.release.set()
        self.context.shutdown()
        self.executor.shutdown(timeout_sec=5)
        self.spinner.join(5)
        self.assertFalse(self.spinner.is_alive())
        for node in (self.caller, self.provider):
            try:
                node.destroy_node()
            except aos.InvalidStateError:
                pass

    def make(self, execute, name="fibonacci", **kwargs):
        server = aos.ActionServer(self.provider, self.interface, name, execute, **kwargs)
        client = aos.ActionClient(self.caller, self.interface, name)
        return server, client

    def test_success_feedback_and_repeatable_result(self):
        received = threading.Event()
        messages = []

        def execute(goal):
            goal.publish_feedback({"partial_sequence": [0, 1]})
            self.assertTrue(received.wait(5))
            goal.succeed()
            return {"sequence": [0, 1, 1]}

        _, client = self.make(execute, summary="Example action", side_effects="read_only")
        goal = client.send_goal_async(
            {"order": 3},
            lambda message: (messages.append(message), received.set()),
        ).result(5)
        self.assertTrue(goal.accepted)
        self.assertRegex(goal.goal_id, r"^[0-9a-f]{32}$")
        self.assertGreater(goal.stamp["sec"], 0)
        # No application deadline: the transport uses rmw_zenoh's GetResult policy.
        result = goal.get_result_async().result(5)
        self.assertEqual(result, {"status": 4, "result": {"sequence": [0, 1, 1]}})
        self.assertEqual(goal.get_result_async(timeout=2).result(3), result)
        self.assertEqual(messages[0]["goal_id"], goal.goal_id)
        self.assertEqual(messages[0]["feedback"], {"partial_sequence": [0, 1]})

    def test_reject_and_duplicate_uuid_before_goal_callback(self):
        admissions = []

        def admit(request):
            admissions.append(request["order"])
            return aos.GoalResponse.REJECT if request["order"] == 0 else aos.GoalResponse.ACCEPT

        def execute(goal):
            self.release.wait(5)
            goal.succeed()
            return {"sequence": []}

        _, client = self.make(execute, goal_callback=admit)
        self.assertFalse(client.send_goal_async({"order": 0}).result(5).accepted)
        goal_id = uuid.uuid4().hex
        goal = client.send_goal_async({"order": 2}, goal_id=goal_id).result(5)
        self.assertTrue(goal.accepted)
        self.assertFalse(client.send_goal_async({"order": 2}, goal_id=goal_id).result(5).accepted)
        self.assertEqual(admissions, [0, 2])
        self.release.set()
        self.assertEqual(goal.get_result_async(timeout=5).result(5)["status"], 4)

    def test_cancel_remains_responsive_with_pending_result(self):
        executing = threading.Event()

        def execute(goal):
            executing.set()
            deadline = time.monotonic() + 5
            while not goal.is_cancel_requested and time.monotonic() < deadline:
                time.sleep(0.01)
            self.assertTrue(goal.is_cancel_requested)
            goal.canceled()
            return {"sequence": [0]}

        # The default MutuallyExclusive group does not include execute tasks
        # or result waiters, leaving the cancel callback runnable.
        _, client = self.make(execute, cancel_callback=lambda _: aos.CancelResponse.ACCEPT)
        goal = client.send_goal_async({"order": 30}).result(5)
        self.assertTrue(executing.wait(5))
        result = goal.get_result_async(timeout=5)
        self.assertFalse(result.done())
        response = goal.cancel_goal_async().result(3)
        self.assertIn(goal.goal_id, [g["goal_id"] for g in response["goals_canceling"]])
        self.assertEqual(result.result(5), {"status": 5, "result": {"sequence": [0]}})

    def test_default_cancel_rejects_without_stopping_execution(self):
        executing = threading.Event()

        def execute(goal):
            executing.set()
            self.release.wait(5)
            goal.succeed()
            return {"sequence": [0, 1]}

        _, client = self.make(execute)
        goal = client.send_goal_async({"order": 2}).result(5)
        self.assertTrue(executing.wait(5))
        result = goal.get_result_async(timeout=5)
        response = goal.cancel_goal_async().result(3)
        self.assertEqual(response["return_code"], 0)
        self.assertEqual(response["goals_canceling"], [])
        self.assertFalse(result.done())
        self.release.set()
        self.assertEqual(result.result(5)["status"], aos.GoalStatus.SUCCEEDED)

    def test_cancel_before_delayed_execute(self):
        accepted = queue.Queue()

        def execute(goal):
            self.assertTrue(goal.is_cancel_requested)
            goal.canceled()
            return {"sequence": []}

        _, client = self.make(
            execute, handle_accepted_callback=accepted.put,
            cancel_callback=lambda _: aos.CancelResponse.ACCEPT,
        )
        goal = client.send_goal_async({"order": 3}).result(5)
        server_goal = accepted.get(timeout=5)
        response = goal.cancel_goal_async().result(3)
        self.assertEqual(response["goals_canceling"][0]["goal_id"], goal.goal_id)
        server_goal.execute()
        self.assertEqual(goal.get_result_async(timeout=5).result(5)["status"], aos.GoalStatus.CANCELED)

    def test_exception_missing_terminal_and_invalid_result(self):
        def execute(goal):
            if goal.request["order"] == 1:
                raise RuntimeError("private callback failure")
            if goal.request["order"] == 2:
                return {"sequence": [0, 1]}  # No terminal state: abort.
            goal.succeed()
            return {"sequence": "invalid"}

        _, client = self.make(execute)
        first = client.send_goal_async({"order": 1}).result(5)
        self.assertEqual(first.get_result_async(timeout=5).result(5), {
            "status": 6, "result": {"sequence": []},
        })
        second = client.send_goal_async({"order": 2}).result(5)
        self.assertEqual(second.get_result_async(timeout=5).result(5)["status"], 6)
        third = client.send_goal_async({"order": 3}).result(5)
        with self.assertRaises(aos.AOSError) as error:
            third.get_result_async(timeout=5).result(5)
        self.assertNotIsInstance(error.exception, aos.WaitTimeoutError)
        with self.assertRaises(aos.AOSError):
            third.get_result_async(timeout=2).result(3)

    def test_result_timeout_and_expiration(self):
        def execute(goal):
            self.release.wait(5)
            goal.succeed()
            return {"sequence": [0]}

        _, client = self.make(execute, result_timeout=0.1)
        goal = client.send_goal_async({"order": 1}).result(5)
        with self.assertRaises(aos.WaitTimeoutError):
            goal.get_result_async(timeout=0.05).result(2)
        self.release.set()
        self.assertEqual(goal.get_result_async(timeout=2).result(3)["status"], 4)
        time.sleep(0.3)
        self.assertEqual(client.get_result_async(goal.goal_id, timeout=2).result(3), {
            "status": 0, "result": {"sequence": []},
        })

    def test_destroy_client_wakes_pending_result_wait(self):
        executing = threading.Event()

        def execute(goal):
            executing.set()
            self.release.wait(5)
            goal.succeed()
            return {"sequence": []}

        _, client = self.make(execute)
        goal = client.send_goal_async({"order": 2}).result(5)
        self.assertTrue(executing.wait(5))
        result = goal.get_result_async()
        time.sleep(0.05)
        client.destroy()
        with self.assertRaises(aos.InvalidStateError):
            result.result(3)
        self.release.set()
        observer = aos.ActionClient(self.caller, self.interface, "fibonacci")
        self.assertEqual(
            observer.get_result_async(goal.goal_id, timeout=5).result(5)["status"], 4,
        )


class ActionProcessExampleTest(unittest.TestCase):
    def test_independent_process_success_and_cancel(self):
        config = str(router_env.session_config)
        server = subprocess.Popen(
            [sys.executable, "-I", str(ROOT / "examples/fibonacci_server.py"),
             "--config", config, "--delay", "0.03", "--stop-on-stdin"],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        records = queue.Queue()

        def read_lines():
            for line in server.stdout:
                records.put(json.loads(line))

        threading.Thread(target=read_lines, daemon=True).start()
        try:
            self.assertEqual(records.get(timeout=10)["event"], "ready")
            for order, extra, status in ((6, [], 4), (30, ["--cancel-after", "0.1"], 5)):
                child = subprocess.run(
                    [sys.executable, "-I", str(ROOT / "examples/fibonacci_client.py"),
                     str(order), "--config", config, *extra],
                    capture_output=True, text=True, timeout=15,
                )
                self.assertEqual(child.returncode, 0, child.stderr)
                messages = [json.loads(line) for line in child.stdout.splitlines()]
                goal_response = next(m for m in messages if m["event"] == "goal_response")
                self.assertTrue(goal_response["accepted"])
                result = next(m for m in messages if m["event"] == "result")
                self.assertEqual(result["status"], status)
                self.assertTrue(any(m["event"] == "feedback" for m in messages))
                execution = records.get(timeout=3)
                completion = records.get(timeout=3)
                self.assertEqual(execution["goal_id"], goal_response["goal_id"])
                self.assertEqual(completion["goal_id"], goal_response["goal_id"])
                self.assertEqual(completion["status"], status)
                self.assertEqual(completion["sequence"], result["result"]["sequence"])
        finally:
            try:
                _, stderr = server.communicate(input="stop\n", timeout=10)
            except subprocess.TimeoutExpired:
                server.kill()
                server.communicate()
                raise
        self.assertEqual(server.returncode, 0, stderr)


if __name__ == "__main__":
    unittest.main()
