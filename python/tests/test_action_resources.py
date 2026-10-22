"""Installed SDK checks for provider-owned pending-result admission."""

from pathlib import Path
import queue
import tempfile
import threading
import time
import unittest

import aos_sdk as aos
from test_platform_resources import PEER


ROOT = Path(__file__).resolve().parents[2]


class ActionResourcesTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="aos-action-resources-")
        config = Path(self.directory.name) / "peer.json5"
        config.write_text(PEER, encoding="utf-8")
        self.context = aos.Context(config=str(config), domain_id=198)
        self.node = aos.Node("action_resources", "/resources", self.context)
        self.interface = aos.Interface.load(ROOT / "interfaces/action/Fibonacci.json")
        self.release = threading.Event()
        self.executor = aos.MultiThreadedExecutor(4, context=self.context)
        self.executor.add_node(self.node)
        self.spinner = threading.Thread(target=self.executor.spin)
        self.spinner.start()

    def tearDown(self):
        self.release.set()
        if self.context.ok():
            self.context.shutdown()
        self.executor.shutdown(timeout_sec=3)
        self.spinner.join(3)
        self.assertFalse(self.spinner.is_alive())
        self.directory.cleanup()

    def wait_for_limit(self, futures):
        deadline = time.monotonic() + 2
        while not any(future.done() for future in futures):
            self.assertLess(time.monotonic(), deadline, "provider did not answer excess GetResult")
            time.sleep(0.005)
        completed = [future for future in futures if future.done()]
        pending = [future for future in futures if not future.done()]
        self.assertEqual(len(completed), 1)
        self.assertEqual(len(pending), 1)
        with self.assertRaises(aos.AOSError) as raised:
            completed[0].result()
        self.assertNotIsInstance(raised.exception, aos.WaitTimeoutError)
        self.assertIn("ACTION_RESULT_WAITER_LIMIT", str(raised.exception))
        return completed[0], pending[0]

    def test_waiter_limit_rejects_invalid_and_extreme_public_values(self):
        values = [0, -1, 65537, 2**100, True, False, None, 1.0, float("inf"), float("nan")]
        for value in values:
            with self.subTest(value=value), self.assertRaises(aos.InvalidArgumentError):
                aos.ActionServer(
                    self.node, self.interface, "invalid",
                    lambda goal: {"sequence": []}, max_result_waiters=value,
                )
        for value in [1, 65536]:
            server = aos.ActionServer(
                self.node, self.interface, f"valid_{value}",
                lambda goal: {"sequence": []}, max_result_waiters=value,
            )
            server.destroy()

    def test_excess_result_request_is_remote_error_and_does_not_cancel_execution(self):
        executing = queue.Queue()
        canceled = []

        def execute(goal):
            executing.put(goal)
            self.assertTrue(self.release.wait(4))
            self.assertFalse(goal.is_cancel_requested)
            goal.succeed()
            return {"sequence": [0, 1]}

        server = aos.ActionServer(
            self.node, self.interface, "limit", execute, max_result_waiters=1,
            cancel_callback=lambda goal: (canceled.append(goal.goal_id), aos.CancelResponse.REJECT)[1],
        )
        client = aos.ActionClient(self.node, self.interface, "limit")
        goal = client.send_goal_async({"order": 2}).result(2)
        provider_goal = executing.get(timeout=2)
        rejected, pending = self.wait_for_limit([
            goal.get_result_async(timeout=4),
            goal.get_result_async(timeout=4),
        ])
        self.assertEqual(provider_goal.status, aos.GoalStatus.EXECUTING)
        self.assertTrue(provider_goal.is_active)
        self.assertEqual(canceled, [])
        self.release.set()
        self.assertEqual(pending.result(2), {"status": 4, "result": {"sequence": [0, 1]}})
        # The rejected Future stays rejected; the SDK never retries it.
        with self.assertRaises(aos.AOSError) as raised:
            rejected.result()
        self.assertIn("ACTION_RESULT_WAITER_LIMIT", str(raised.exception))
        self.assertEqual(
            goal.get_result_async(timeout=2).result(2),
            {"status": 4, "result": {"sequence": [0, 1]}},
        )
        server.destroy()
        client.destroy()

    def test_client_close_does_not_free_a_provider_waiter_or_cancel_the_goal(self):
        accepted = queue.Queue()
        server = aos.ActionServer(
            self.node, self.interface, "departed",
            lambda goal: {"sequence": []},
            handle_accepted_callback=accepted.put, max_result_waiters=1,
        )
        client = aos.ActionClient(self.node, self.interface, "departed")
        goal = client.send_goal_async({"order": 1}).result(2)
        provider_goal = accepted.get(timeout=2)
        _, pending = self.wait_for_limit([
            goal.get_result_async(timeout=4),
            goal.get_result_async(timeout=4),
        ])
        client.destroy()
        with self.assertRaises(aos.InvalidStateError):
            pending.result(2)
        observer = aos.ActionClient(self.node, self.interface, "departed")
        with self.assertRaises(aos.AOSError) as raised:
            observer.get_result_async(goal.goal_id, timeout=2).result(2)
        self.assertNotIsInstance(raised.exception, aos.WaitTimeoutError)
        self.assertIn("ACTION_RESULT_WAITER_LIMIT", str(raised.exception))
        self.assertEqual(provider_goal.status, aos.GoalStatus.ACCEPTED)
        self.assertFalse(provider_goal.is_cancel_requested)
        server.destroy()
        observer.destroy()


if __name__ == "__main__":
    unittest.main()
