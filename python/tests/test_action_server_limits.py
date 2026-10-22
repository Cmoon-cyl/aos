"""Public resource limits through an isolated installed wheel and real peer."""

from collections import UserDict
import json
from pathlib import Path
import queue
import tempfile
import threading
import time
import unittest

import aos_sdk as aos
from test_platform_resources import PEER


LIMITS = {
    "max_goals": 4,
    "max_goal_bytes": 8,
    "max_result_bytes": 8,
    "max_result_cache_bytes": 8,
}


class ActionServerLimitsTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="aos-action-limits-")
        root = Path(self.directory.name)
        config = root / "peer.json5"
        config.write_text(PEER, encoding="utf-8")
        interface = root / "action.json"
        interface.write_text(json.dumps({
            "name": "test/action/SmallBytes", "kind": "action",
            "goal": {"type": "string"},
            "result": {"type": "string", "default": ""},
            "feedback": {"type": "object"},
        }), encoding="utf-8")
        self.interface = aos.Interface.load(interface)
        self.context = aos.Context(config=str(config), domain_id=197)
        self.node = aos.Node("action_limits", "/limits", self.context)
        self.release = threading.Event()
        self.executor = aos.MultiThreadedExecutor(4, context=self.context)
        self.executor.add_node(self.node)
        self.errors = []

        def spin():
            try:
                self.executor.spin()
            except BaseException as error:
                self.errors.append(error)

        self.spinner = threading.Thread(target=spin)
        self.spinner.start()

    def tearDown(self):
        self.release.set()
        if self.context.ok():
            self.context.shutdown()
        self.assertTrue(self.executor.shutdown(timeout_sec=3))
        self.spinner.join(3)
        self.assertFalse(self.spinner.is_alive())
        self.assertEqual(self.errors, [])
        self.directory.cleanup()

    def make(self, name, execute, **kwargs):
        kwargs.setdefault("resource_limits", LIMITS)
        server = aos.ActionServer(self.node, self.interface, name, execute, **kwargs)
        client = aos.ActionClient(self.node, self.interface, name)
        return server, client

    def remote_error(self, future, code):
        with self.assertRaises(aos.AOSError) as raised:
            future.result(2)
        self.assertNotIsInstance(raised.exception, aos.WaitTimeoutError)
        self.assertIn(code, str(raised.exception))
        return str(raised.exception)

    def test_public_mapping_rejects_invalid_fields_types_and_extremes(self):
        cases = [True, False, [], "", 1, {"unknown": 1}]
        maxima = {
            "max_goals": 65536,
            "max_goal_bytes": 8 << 20,
            "max_result_bytes": 8 << 20,
            "max_result_cache_bytes": 1 << 30,
        }
        for key, maximum in maxima.items():
            for value in [
                0, -1, maximum + 1, 2**100, True, False, None,
                1.0, float("nan"), float("inf"), "1",
            ]:
                cases.append({key: value})
        cases.append({"max_result_bytes": 2, "max_result_cache_bytes": 1})
        for value in cases:
            with self.subTest(value=value), self.assertRaises(aos.InvalidArgumentError):
                aos.ActionServer(
                    self.node, self.interface, "invalid", lambda goal: "",
                    resource_limits=value,
                )
        for index, value in enumerate([
            None, {}, UserDict(LIMITS),
            dict.fromkeys(LIMITS, 1), maxima,
            {"max_goals": 1},
        ]):
            server = aos.ActionServer(
                self.node, self.interface, f"valid_{index}", lambda goal: "",
                resource_limits=value,
            )
            server.destroy()

    def test_goal_size_and_capacity_reject_before_callbacks_and_ttl_reuses_slot(self):
        accepted = queue.Queue()
        admissions = []
        executions = []

        def execute(goal):
            executions.append(goal.goal_id)
            goal.succeed()
            return ""

        server, client = self.make(
            "admission", execute,
            goal_callback=lambda request: (admissions.append(request), aos.GoalResponse.ACCEPT)[1],
            handle_accepted_callback=accepted.put, result_timeout=0.15,
            resource_limits={**LIMITS, "max_goals": 1},
        )
        self.assertFalse(client.send_goal_async("汉\nxy").result(2).accepted)
        self.assertEqual(admissions, [])
        self.assertEqual(executions, [])
        first = client.send_goal_async("汉\nx").result(2)
        provider = accepted.get(timeout=2)
        self.assertTrue(first.accepted)
        self.assertFalse(client.send_goal_async("").result(2).accepted)
        self.assertEqual(admissions, ["汉\nx"])
        self.assertEqual(executions, [])
        self.assertEqual(provider.status, aos.GoalStatus.ACCEPTED)
        provider.execute()
        self.assertEqual(first.get_result_async(timeout=2).result(2)["status"], 4)
        time.sleep(0.25)
        next_goal = client.send_goal_async("").result(2)
        self.assertTrue(next_goal.accepted)
        next_provider = accepted.get(timeout=2)
        next_provider.execute()
        self.assertEqual(next_goal.get_result_async(timeout=2).result(2)["result"], "")
        self.assertEqual(len(executions), 2)
        server.destroy()
        client.destroy()

    def test_oversized_result_answers_pending_and_later_get_result_without_cancel(self):
        executing = queue.Queue()
        cancels = []

        def execute(goal):
            executing.put(goal)
            self.assertTrue(self.release.wait(3))
            goal.succeed()
            return "1234567"

        server, client = self.make(
            "oversized", execute,
            cancel_callback=lambda goal: (cancels.append(goal.goal_id), aos.CancelResponse.REJECT)[1],
        )
        goal = client.send_goal_async("").result(2)
        provider = executing.get(timeout=2)
        pending = goal.get_result_async(timeout=3)
        self.release.set()
        error = self.remote_error(pending, "ACTION_RESULT_TOO_LARGE")
        self.assertEqual(provider.status, aos.GoalStatus.SUCCEEDED)
        self.assertFalse(provider.is_cancel_requested)
        self.assertEqual(cancels, [])
        self.assertEqual(error, self.remote_error(
            goal.get_result_async(timeout=2), "ACTION_RESULT_TOO_LARGE"
        ))
        self.assertTrue(self.spinner.is_alive())
        server.destroy()
        client.destroy()

    def test_cache_full_keeps_old_results_and_releases_expired_bytes(self):
        executions = []

        def execute(goal):
            executions.append(goal)
            goal.succeed()
            return "ab"

        server, client = self.make("cache", execute, result_timeout=0.5)
        first = client.send_goal_async("").result(2)
        self.assertEqual(first.get_result_async(timeout=2).result(2)["result"], "ab")
        second = client.send_goal_async("").result(2)
        self.assertEqual(second.get_result_async(timeout=2).result(2)["result"], "ab")
        third = client.send_goal_async("").result(2)
        self.remote_error(third.get_result_async(timeout=2), "ACTION_RESULT_CACHE_LIMIT")
        self.assertEqual(executions[2].status, aos.GoalStatus.SUCCEEDED)
        for goal in (first, second):
            self.assertEqual(goal.get_result_async(timeout=2).result(2)["result"], "ab")
        self.remote_error(third.get_result_async(timeout=2), "ACTION_RESULT_CACHE_LIMIT")
        time.sleep(0.65)
        next_goal = client.send_goal_async("").result(2)
        self.assertTrue(next_goal.accepted)
        self.assertEqual(next_goal.get_result_async(timeout=2).result(2)["result"], "ab")
        self.assertEqual(len(executions), 4)
        server.destroy()
        client.destroy()

    def test_non_json_result_error_is_bounded_without_replacing_explicit_success(self):
        handles = queue.Queue()

        def execute(goal):
            handles.put(goal)
            goal.succeed()
            return object()

        server, client = self.make("non_json", execute)
        goal = client.send_goal_async("").result(2)
        self.remote_error(goal.get_result_async(timeout=2), "ACTION_RESULT_TOO_LARGE")
        self.assertEqual(handles.get(timeout=2).status, aos.GoalStatus.SUCCEEDED)
        server.destroy()
        client.destroy()

    def test_resource_error_releases_managed_lease_and_deactivation_drains(self):
        node = aos.LifecycleNode("managed_limits", "/limits", context=self.context)
        self.executor.add_node(node)
        for transition in ("configure", "activate"):
            self.assertTrue(node.change_state_async(transition).result(2)["successful"])
        handles = queue.Queue()

        def execute(goal):
            handles.put(goal)
            goal.succeed()
            return "1234567"

        server = aos.ActionServer(
            node, self.interface, "managed", execute, resource_limits=LIMITS,
        )
        client = aos.ActionClient(self.node, self.interface, "managed")
        goal = client.send_goal_async("").result(2)
        self.remote_error(goal.get_result_async(timeout=2), "ACTION_RESULT_TOO_LARGE")
        report = node.change_state_async("deactivate", drain_timeout=1).result(2)
        self.assertTrue(report["successful"])
        self.assertEqual(node.lifecycle_snapshot["in_flight"], [])
        self.assertEqual(handles.get(timeout=2).status, aos.GoalStatus.SUCCEEDED)
        self.assertEqual(node.get_current_state().label, "inactive")
        server.destroy()
        client.destroy()
        node.destroy_node()


if __name__ == "__main__":
    unittest.main()
