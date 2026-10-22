"""Feedback ownership checks against an installed SDK, plus real peer traffic."""

import gc
from pathlib import Path
import queue
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import weakref

import aos_sdk as aos
import aos_sdk.action as action
from test_platform_resources import PEER


ROOT = Path(__file__).resolve().parents[2]
GOAL_ID = "1" * 32


def eventually(test, condition):
    deadline = time.monotonic() + 2
    while not condition():
        gc.collect()
        test.assertLess(time.monotonic(), deadline, "ownership was not released")
        time.sleep(0.005)


class Callback:
    def __init__(self, messages, error=None):
        self.messages = messages
        self.error = error

    def __call__(self, message):
        self.messages.append(message)
        if self.error is not None:
            raise self.error


class Lease:
    def __init__(self, goal_id=GOAL_ID):
        self.message = {"goal_id": goal_id, "feedback": {"partial_sequence": [1]}}
        self.finished = 0

    def is_valid(self):
        return True

    def finish(self):
        self.finished += 1


class NativeClient:
    def __init__(self):
        self.accepted = True
        self.send_error = None
        self.destroy_error = None
        self.entered = threading.Event()
        self.release = threading.Event()
        self.release.set()
        self.calls = queue.Queue()
        self.result = {"status": 4, "result": {"sequence": [1]}}
        self.result_error = None
        self.result_entered = threading.Event()
        self.result_release = threading.Event()
        self.result_release.set()

    def send_goal(self, goal, *, goal_id, timeout):
        self.entered.set()
        if not self.release.wait(3):
            raise AssertionError("test did not release send")
        if self.send_error:
            raise self.send_error
        return SimpleNamespace(
            accepted=self.accepted, goal_id=goal_id, status=1,
            stamp={"sec": 1, "nanosec": 0},
            get_result=lambda timeout: self.get_result(goal_id, timeout),
        )

    def get_result(self, goal_id, timeout):
        self.result_entered.set()
        if not self.result_release.wait(3):
            raise AssertionError("test did not release result")
        if self.result_error:
            raise self.result_error
        return self.result

    def recv_status(self, timeout):
        raise aos.WaitTimeoutError("no status")

    def take_feedback(self, timeout):
        try:
            return self.calls.get_nowait()
        except queue.Empty:
            raise aos.WaitTimeoutError("no feedback") from None

    def destroy(self):
        if self.destroy_error:
            raise self.destroy_error


class FakeNode:
    _native = None
    default_callback_group = aos.ReentrantCallbackGroup()

    def _track(self, *_):
        pass

    def _wake_executor(self):
        pass

    def _schedule(self, callback, group):
        callback()

    def _forget(self, *_):
        pass


class FeedbackOwnershipTest(unittest.TestCase):
    def setUp(self):
        self.native = NativeClient()
        with patch.object(action._native, "_ActionClient", return_value=self.native):
            self.client = aos.ActionClient(FakeNode(), None, "feedback")
        self.messages = []

    def tearDown(self):
        self.native.release.set()
        self.native.result_release.set()
        self.native.destroy_error = None
        self.client.destroy()

    def send(self, callback=None):
        return self.client.send_goal_async(
            {"order": 1}, callback, goal_id=GOAL_ID,
        ).result(2)

    def invoke(self):
        lease = Lease()
        self.native.calls.put(lease)
        self.client._take_callback()()
        self.assertEqual(lease.finished, 1)

    def test_last_accepted_owner_releases_callback_without_get_result(self):
        callback = Callback(self.messages)
        reference = weakref.ref(callback)
        first = self.send(callback)
        second = self.send()
        del callback, first
        gc.collect()
        self.assertIsNotNone(reference())
        self.invoke()
        self.assertEqual(len(self.messages), 1)
        del second
        eventually(self, lambda: reference() is None)
        self.assertEqual(len(self.client._feedback_callbacks), 0)

    def test_rejected_same_uuid_send_does_not_remove_live_owner_callback(self):
        accepted = self.send()
        callback = Callback(self.messages)
        reference = weakref.ref(callback)
        self.native.accepted = False
        rejected = self.send(callback)
        self.assertFalse(rejected.accepted)
        del callback
        gc.collect()
        self.invoke()
        self.assertEqual(len(self.messages), 1)
        self.assertIsNotNone(reference())
        del accepted
        eventually(self, lambda: reference() is None)
        self.assertEqual(len(self.client._feedback_callbacks), 0)

    def test_early_feedback_survives_until_send_response_then_owner_drop(self):
        self.native.release.clear()
        callback = Callback(self.messages)
        reference = weakref.ref(callback)
        future = self.client.send_goal_async(
            {"order": 1}, callback, goal_id=GOAL_ID,
        )
        del callback
        self.assertTrue(self.native.entered.wait(1))
        self.invoke()
        self.assertEqual(len(self.messages), 1)
        self.native.release.set()
        handle = future.result(2)
        del future, handle
        eventually(self, lambda: reference() is None)

    def test_send_failure_rejection_and_canceled_future_release_their_owners(self):
        for mode in ("error", "rejected", "canceled"):
            with self.subTest(mode=mode):
                self.native.accepted = mode != "rejected"
                self.native.send_error = aos.AOSError("send failed") if mode == "error" else None
                self.native.entered.clear()
                self.native.release.clear()
                callback = Callback(self.messages)
                reference = weakref.ref(callback)
                future = self.client.send_goal_async(
                    {"order": 1}, callback, goal_id=GOAL_ID,
                )
                del callback
                self.assertTrue(self.native.entered.wait(1))
                if mode == "canceled":
                    self.assertTrue(future.cancel())
                self.native.release.set()
                if mode == "error":
                    with self.assertRaises(aos.AOSError):
                        future.result(2)
                elif mode == "rejected":
                    self.assertFalse(future.result(2).accepted)
                # Keep the Future, including any exception traceback, alive.
                eventually(self, lambda: reference() is None)
                self.assertEqual(len(self.client._feedback_callbacks), 0)

    def test_timeout_and_remote_error_keep_live_callback_success_releases_it(self):
        callback = Callback(self.messages)
        reference = weakref.ref(callback)
        handle = self.send(callback)
        del callback
        for error in (aos.WaitTimeoutError("deadline"), aos.AOSError("remote")):
            self.native.result_error = error
            with self.assertRaises(type(error)):
                handle.get_result_async(timeout=0.01).result(2)
            self.invoke()
        self.assertEqual(len(self.messages), 2)
        self.native.result_error = None
        self.assertEqual(handle.get_result_async().result(2)["status"], 4)
        eventually(self, lambda: reference() is None)
        self.assertEqual(len(self.client._feedback_callbacks), 0)

    def test_saved_failed_result_future_does_not_keep_a_dropped_callback_owner(self):
        callback = Callback(self.messages)
        reference = weakref.ref(callback)
        handle = self.send(callback)
        self.native.result_error = aos.WaitTimeoutError("deadline")
        future = handle.get_result_async(timeout=0.01)
        with self.assertRaises(aos.WaitTimeoutError):
            future.result(2)
        del callback, handle
        # The caller may retain the Future and its traceback for diagnostics.
        eventually(self, lambda: reference() is None)
        self.assertIsInstance(future.exception(), aos.WaitTimeoutError)

    def test_failed_send_does_not_remove_another_inflight_send_owner(self):
        first_release, second_release = threading.Event(), threading.Event()
        second_entered = threading.Event()

        def send(goal, *, goal_id, timeout):
            if goal["order"] == 1:
                if not first_release.wait(3):
                    raise AssertionError("first send was not released")
                raise aos.AOSError("first send failed")
            second_entered.set()
            if not second_release.wait(3):
                raise AssertionError("second send was not released")
            return SimpleNamespace(accepted=True, goal_id=goal_id)

        callback = Callback(self.messages)
        reference = weakref.ref(callback)
        try:
            with patch.object(self.native, "send_goal", side_effect=send):
                first = self.client.send_goal_async(
                    {"order": 1}, callback, goal_id=GOAL_ID,
                )
                second = self.client.send_goal_async({"order": 2}, goal_id=GOAL_ID)
                del callback
                self.assertTrue(second_entered.wait(1))
                first_release.set()
                with self.assertRaises(aos.AOSError):
                    first.result(2)
                self.invoke()
                self.assertEqual(len(self.messages), 1)
                second_release.set()
                handle = second.result(2)
            del second, handle
            eventually(self, lambda: reference() is None)
        finally:
            first_release.set()
            second_release.set()

    def test_callback_cycle_is_collectible_after_last_application_owner(self):
        owners = []
        callback = lambda message, owners=owners: None
        reference = weakref.ref(callback)
        handle = self.send(callback)
        owners.append(handle)
        del callback, handle, owners
        eventually(self, lambda: reference() is None)
        self.assertEqual(len(self.client._feedback_callbacks), 0)

    def test_old_result_completion_does_not_remove_new_uuid_registration(self):
        first = self.send(Callback(self.messages))
        self.native.result_release.clear()

        def delayed_result(timeout):
            self.native.result_entered.set()
            if not self.native.result_release.wait(3):
                raise AssertionError("test did not release old result")
            return self.native.result

        first._native.get_result = delayed_result
        old_result = first.get_result_async()
        self.assertTrue(self.native.result_entered.wait(1))
        # Complete another observation first, removing the old registration.
        with patch.object(self.native, "get_result", return_value=self.native.result):
            self.client.get_result_async(GOAL_ID).result(2)
        new_messages = []
        replacement = self.send(Callback(new_messages))
        self.native.result_release.set()
        old_result.result(2)
        self.invoke()
        self.assertEqual(len(new_messages), 1)
        self.assertTrue(replacement.accepted)

    def test_callback_exception_interrupt_and_cancel_finish_the_native_lease(self):
        for index, error in enumerate((RuntimeError("callback"), KeyboardInterrupt("callback"), None)):
            with self.subTest(error=type(error).__name__):
                goal_id = str(index + 2) * 32
                handle = self.client.send_goal_async(
                    {"order": 1}, Callback(self.messages, error), goal_id=goal_id,
                ).result(2)
                lease = Lease(goal_id)
                self.native.calls.put(lease)
                invocation = self.client._take_callback()
                if error is None:
                    invocation._cancel()
                else:
                    with self.assertRaises(type(error)):
                        invocation()
                self.assertEqual(lease.finished, 1)
                del handle, invocation

    def test_destroy_releases_callbacks_even_when_native_is_already_closed(self):
        callback = Callback(self.messages)
        reference = weakref.ref(callback)
        handle = self.send(callback)
        del callback
        self.native.destroy_error = aos.InvalidStateError("already closed")
        with self.assertRaises(aos.InvalidStateError):
            self.client.destroy()
        eventually(self, lambda: reference() is None)
        self.assertEqual(len(self.client._feedback_callbacks), 0)
        self.assertTrue(handle.accepted)


class FeedbackPeerTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="aos-feedback-owner-")
        config = Path(self.directory.name) / "peer.json5"
        config.write_text(PEER, encoding="utf-8")
        self.context = aos.Context(config=str(config), domain_id=199)
        self.provider = aos.Node("feedback_provider", "/ownership", self.context)
        self.caller = aos.Node("feedback_caller", "/ownership", self.context)
        self.interface = aos.Interface.load(ROOT / "interfaces/action/Fibonacci.json")
        self.accepted = queue.Queue()
        self.server = aos.ActionServer(
            self.provider, self.interface, "feedback",
            lambda goal: {"sequence": []}, handle_accepted_callback=self.accepted.put,
        )
        self.client = aos.ActionClient(self.caller, self.interface, "feedback")
        self.executor = aos.MultiThreadedExecutor(2, context=self.context)
        self.executor.add_node(self.provider)
        self.spinner = threading.Thread(target=self.executor.spin)
        self.spinner.start()
        self.messages = []

    def tearDown(self):
        if self.context.ok():
            self.context.shutdown()
        self.executor.shutdown(timeout_sec=3)
        self.spinner.join(3)
        self.assertFalse(self.spinner.is_alive())
        self.directory.cleanup()

    def receive_feedback(self, provider_goal):
        before = len(self.messages)
        provider_goal.publish_feedback({"partial_sequence": [1]})

        def received():
            try:
                self.client._take_callback()()
            except aos.WaitTimeoutError:
                pass
            return len(self.messages) > before

        eventually(self, received)

    def test_result_timeout_keeps_feedback_and_does_not_cancel_provider(self):
        callback = Callback(self.messages)
        reference = weakref.ref(callback)
        handle = self.client.send_goal_async({"order": 1}, callback).result(2)
        del callback
        provider_goal = self.accepted.get(timeout=2)
        with self.assertRaises(aos.WaitTimeoutError):
            handle.get_result_async(timeout=0.02).result(2)
        self.assertEqual(provider_goal.status, aos.GoalStatus.ACCEPTED)
        self.assertFalse(provider_goal.is_cancel_requested)
        self.receive_feedback(provider_goal)
        self.assertIsNotNone(reference())
        del handle
        eventually(self, lambda: reference() is None)
        self.assertEqual(len(self.client._feedback_callbacks), 0)
        self.assertEqual(provider_goal.status, aos.GoalStatus.ACCEPTED)

    def test_duplicate_rejection_preserves_feedback_on_prior_handle(self):
        handle = self.client.send_goal_async({"order": 1}, goal_id=GOAL_ID).result(2)
        provider_goal = self.accepted.get(timeout=2)
        callback = Callback(self.messages)
        reference = weakref.ref(callback)
        rejected = self.client.send_goal_async(
            {"order": 1}, callback, goal_id=GOAL_ID,
        ).result(2)
        self.assertFalse(rejected.accepted)
        del callback
        self.receive_feedback(provider_goal)
        del handle
        eventually(self, lambda: reference() is None)


if __name__ == "__main__":
    unittest.main()
