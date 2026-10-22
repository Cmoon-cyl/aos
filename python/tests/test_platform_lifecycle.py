"""Lifecycle gates and Executor ownership through an installed native wheel."""

import contextlib
import io
import json
from queue import Queue
import subprocess
import sys
import threading
import time
import unittest
from unittest import mock

import aos_sdk as aos

from test_platform_foundation import PlatformTestCase, ROOT


class RecordingNode(aos.LifecycleNode):
    def __init__(self, *args, **kwargs):
        self.calls = []
        self.outcomes = {}
        self.handlers = {}
        super().__init__(*args, **kwargs)

    def record(self, operation, previous):
        self.calls.append((
            operation, previous.label, self.get_current_state().label,
            threading.current_thread().name,
        ))
        if operation in self.handlers:
            return self.handlers[operation](previous)
        return self.outcomes.get(operation, aos.TransitionCallbackReturn.SUCCESS)

    def on_configure(self, previous):
        return self.record("configure", previous)

    def on_activate(self, previous):
        return self.record("activate", previous)

    def on_deactivate(self, previous):
        return self.record("deactivate", previous)

    def on_cleanup(self, previous):
        return self.record("cleanup", previous)

    def on_shutdown(self, previous):
        return self.record("shutdown", previous)

    def on_error(self, previous):
        return self.record("error", previous)


class LifecycleTest(PlatformTestCase):
    def managed(self, name="managed", **kwargs):
        node = RecordingNode(name, context=self.context, **kwargs)

        def close():
            try:
                node.destroy_node()
            except aos.InvalidStateError:
                pass

        self.addCleanup(close)
        return node

    def single(self, *nodes):
        executor = aos.SingleThreadedExecutor(context=self.context)
        for node in nodes:
            executor.add_node(node)
        self.addCleanup(executor.shutdown)
        return executor

    def multi(self, *nodes, threads=1):
        executor = aos.MultiThreadedExecutor(threads, context=self.context)
        for node in nodes:
            executor.add_node(node)
        errors = []

        def spin():
            try:
                executor.spin()
            except Exception as error:
                errors.append(error)

        spinner = threading.Thread(target=spin, name="lifecycle-test-spin")
        spinner.start()

        def stop():
            self.assertTrue(executor.shutdown(timeout_sec=3))
            spinner.join(3)
            self.assertFalse(spinner.is_alive())
            self.assertEqual(errors, [])

        self.addCleanup(stop)
        return executor

    def change(self, node, transition, executor=None, **kwargs):
        future = node.change_state_async(transition, **kwargs)
        if executor is not None:
            executor.spin_until_future_complete(future, 3)
        return future.result(3)

    def active(self, node, executor=None):
        self.assertTrue(self.change(node, "configure", executor)["successful"])
        self.assertTrue(self.change(node, "activate", executor)["successful"])

    def lifecycle_client(self, caller, provider, kind):
        client = caller.create_client(
            aos.lifecycle_service_interface(kind),
            f"{provider.fully_qualified_name}/{kind}",
            qos_profile="lifecycle_services",
        )
        self.assertTrue(client.wait_for_service(2))
        return client

    def parameter_client(self, caller, provider, kind):
        client = caller.create_client(
            aos.parameter_service_interface(kind),
            f"{provider.fully_qualified_name}/{kind}", qos_profile="parameters",
        )
        self.assertTrue(client.wait_for_service(2))
        return client

    def test_oversized_drain_timeout_fails_before_transition_or_callback(self):
        node = self.managed()
        executor = self.single(node)
        before = node.get_current_state()
        # Exceed Duration's u64 seconds on every platform. Instant's range is
        # platform dependent; 1e19 seconds is representable on Windows.
        values = [float(2**64)]
        if sys.platform != "win32":
            values.append(1e19)  # Retain the narrower Instant-overflow check.
        for value in values:
            with self.subTest(drain_timeout=value):
                future = node.change_state_async("configure", drain_timeout=value)
                executor.spin_until_future_complete(future, 3)
                with self.assertRaises(aos.InvalidArgumentError):
                    future.result(3)
                self.assertEqual(node.get_current_state(), before)
                self.assertEqual(node.calls, [])
        self.assertTrue(self.change(node, "configure", executor)["successful"])

    def test_schema_integer_decode_error_replies_and_keeps_executor_registered(self):
        node = self.managed()
        caller = self.node("caller")
        client = self.lifecycle_client(caller, node, "change_state")
        self.multi(node)
        for request in (
            {"transition": {"id": 3.0, "label": "activate"}},
            {"transition": {"id": 1, "label": "configure"}, "drain_timeout_ms": 5.0},
        ):
            reply = client.call(request, timeout=2)
            self.assertFalse(reply["success"])
            self.assertEqual(reply["error"]["code"], "VALIDATION")
            self.assertIsNone(reply["report"])
            self.assertEqual(node.get_current_state().label, "unconfigured")
            self.assertEqual(node.calls, [])
        reply = client.call({"transition": {"id": 1, "label": "configure"}}, timeout=2)
        self.assertTrue(reply["success"])
        self.assertEqual(node.get_current_state().label, "inactive")

    def test_parameter_allowed_states_cover_startup_set_delete_load_and_remote_describe(self):
        node = self.managed(parameter_overrides={"limited": 7})
        descriptor = aos.ParameterDescriptor(
            description="Only adjust while inactive", dynamic_typing=True,
            allowed_states=("inactive",),
        )
        self.assertEqual(node.declare_parameter("limited", 1, descriptor).value, 7)
        node.declare_parameter("ordinary", 1)
        self.assertEqual(node.describe_parameter("limited").allowed_states, ("inactive",))
        path = self.directory / "restricted-parameters.json"
        node.dump_parameters(path)
        observer = self.node("parameter_observer")
        events = observer.create_subscription(
            aos.parameter_event_interface(), "/parameter_events",
            qos_profile="parameter_events",
        )
        before = node.get_parameter_snapshot()
        for parameters in (
            [aos.Parameter("limited", value=9)],
            [aos.Parameter("limited", value=None)],
            [aos.Parameter("ordinary", value=2), aos.Parameter("limited", value=9)],
        ):
            result = node.set_parameters_atomically(parameters)
            self.assertFalse(result.successful)
            self.assertEqual(result.code, "state_not_allowed")
        self.assertEqual(node.load_parameters(path).code, "state_not_allowed")
        self.assertEqual(node.get_parameter_snapshot(), before)
        with self.assertRaises(aos.WaitTimeoutError):
            events.recv(0.05)

        self.multi(node)
        describe = self.parameter_client(observer, node, "describe_parameters")
        remote = describe.call({"names": ["limited"]}, 2)
        self.assertEqual(remote["value"][0]["descriptor"]["allowed_states"], ["inactive"])
        self.change(node, "configure")
        changed = node.set_parameters_atomically([aos.Parameter("limited", value=8)])
        self.assertTrue(changed.successful)
        self.assertEqual(node.get_parameter("limited").value, 8)
        self.assertTrue(node.load_parameters(path).successful)
        self.assertEqual(node.get_parameter("limited").value, 7)
        self.change(node, "activate")
        remote_set = self.parameter_client(observer, node, "set_parameters_atomically")
        rejected = remote_set.call({
            "parameters": [aos.Parameter("limited", value=9).to_dict()],
        }, 2)["result"]
        self.assertFalse(rejected["successful"])
        self.assertEqual(rejected["code"], "state_not_allowed")
        self.change(node, "deactivate")
        self.assertTrue(node.set_parameters_atomically([
            aos.Parameter("limited", value=None),
        ]).successful)
        self.assertFalse(node.has_parameter("limited"))

    def test_parameter_allowed_states_require_managed_primary_states(self):
        plain = self.node("plain_parameters")
        with self.assertRaises(aos.ParameterError) as unavailable:
            plain.declare_parameter("restricted", 1, aos.ParameterDescriptor(
                allowed_states=("inactive",),
            ))
        self.assertEqual(unavailable.exception.code, "unavailable")
        self.assertFalse(plain.has_parameter("restricted"))
        managed = self.managed()
        for states in (("inactive", "inactive"), ("unknown",), ("configuring",), ("typo",), (1,)):
            with self.subTest(states=states), self.assertRaises(aos.ParameterError) as invalid:
                managed.declare_parameter("invalid", 1, aos.ParameterDescriptor(
                    allowed_states=states,
                ))
            self.assertEqual(invalid.exception.code, "invalid_argument")
        self.assertFalse(managed.has_parameter("invalid"))

    def test_parameter_validator_commit_rechecks_epoch_even_after_returning_to_same_state(self):
        node = self.managed()
        node.declare_parameter("limited", 1, aos.ParameterDescriptor(
            allowed_states=("active",),
        ))
        self.multi(node)
        self.active(node)
        observer = self.node("conflict_observer")
        events = observer.create_subscription(
            aos.parameter_event_interface(), "/parameter_events",
            qos_profile="parameter_events",
        )
        client = self.parameter_client(observer, node, "set_parameters_atomically")
        entered, release = threading.Event(), threading.Event()
        validator_threads = []

        def validate(candidate):
            validator_threads.append(threading.current_thread().name)
            entered.set()
            if not release.wait(3):
                raise RuntimeError("validator test barrier timed out")
            return aos.SetParametersResult(True)

        node.add_on_set_parameters_callback(validate)
        before = node.get_parameter_snapshot()
        pending = client.call_async({
            "parameters": [aos.Parameter("limited", value=2).to_dict()],
        }, timeout=4)
        try:
            self.assertTrue(entered.wait(2))
            self.assertTrue(self.change(node, "deactivate", drain_timeout=0.1)["successful"])
            self.assertTrue(self.change(node, "activate")["successful"])
        finally:
            release.set()
        result = pending.result(5)["result"]
        self.assertFalse(result["successful"])
        self.assertEqual(result["code"], "conflict")
        self.assertEqual(node.get_current_state().label, "active")
        self.assertEqual(node.get_parameter_snapshot(), before)
        self.assertEqual(len(validator_threads), 1)
        self.assertTrue(validator_threads[0].startswith("aos-control"))
        with self.assertRaises(aos.WaitTimeoutError):
            events.recv(0.05)

    def test_local_callbacks_wait_for_executor_and_core_owns_transition_table(self):
        node = self.managed()
        executor = self.single(node)
        self.assertEqual(node.get_current_state(), aos.LifecycleState(1, "unconfigured"))
        self.assertFalse(node.lifecycle_snapshot["gate_open"])
        future = node.trigger_configure_async()
        self.assertFalse(future.done())
        self.assertEqual(node.calls, [])
        self.assertEqual(node.get_current_state().label, "unconfigured")
        executor.spin_until_future_complete(future, 2)
        report = future.result(1)
        self.assertEqual(report["final_state"]["label"], "inactive")
        self.assertEqual(node.calls[0][:3], ("configure", "unconfigured", "configuring"))
        self.assertEqual(node.calls[0][3], threading.current_thread().name)
        self.assertEqual(len(node.get_available_states()), 11)
        self.assertEqual(len(node.get_transition_graph()), 25)
        self.assertEqual({edge["id"] for edge in node.get_available_transitions()}, {2, 3, 6})
        for operation, expected in (
            ("activate", "active"), ("deactivate", "inactive"),
            ("cleanup", "unconfigured"), ("shutdown", "finalized"),
        ):
            report = self.change(node, operation, executor)
            self.assertTrue(report["successful"])
            self.assertEqual(node.get_current_state().label, expected)
        events = node.take_transition_events()
        self.assertEqual(len(events), 10)
        self.assertEqual([event["sequence"] for event in events], list(range(1, 11)))
        self.assertEqual(node.take_transition_events(), [])
        self.assertEqual(node.last_transition_report, report)
        self.assertTrue(self.context.ok())
        self.assertEqual(node.get_parameter("use_sim_time").value, False)
        for bad in (10, True, -1, 256, "transition_success"):
            with self.subTest(bad=bad), self.assertRaises(aos.InvalidArgumentError):
                self.change(node, bad, executor)

    def test_failure_error_and_exception_return_through_core_result_edges(self):
        for outcome, error_outcome, expected in (
            (aos.TransitionCallbackReturn.FAILURE, None, "unconfigured"),
            (aos.TransitionCallbackReturn.ERROR, aos.TransitionCallbackReturn.SUCCESS, "unconfigured"),
            (aos.TransitionCallbackReturn.ERROR, aos.TransitionCallbackReturn.FAILURE, "finalized"),
            (aos.TransitionCallbackReturn.ERROR, aos.TransitionCallbackReturn.ERROR, "finalized"),
        ):
            with self.subTest(outcome=outcome, error_outcome=error_outcome):
                node = self.managed(f"result_{int(outcome)}_{error_outcome}")
                node.outcomes["configure"] = outcome
                if error_outcome is not None:
                    node.outcomes["error"] = error_outcome
                executor = self.single(node)
                report = self.change(node, "configure", executor)
                self.assertFalse(report["successful"])
                self.assertEqual(report["final_state"]["label"], expected)
                if error_outcome is not None:
                    self.assertEqual(node.calls[1][:3], (
                        "error", "unconfigured", "errorprocessing",
                    ))
        node = self.managed("exception")
        node.handlers["configure"] = lambda state: (_ for _ in ()).throw(
            RuntimeError("private-lifecycle-payload")
        )
        local_stderr = io.StringIO()
        with contextlib.redirect_stderr(local_stderr):
            report = self.change(node, "configure", self.single(node))
        self.assertEqual(report["callback_return"], "error")
        self.assertEqual(report["error_callback_return"], "success")
        self.assertNotIn("private-lifecycle-payload", str(report))
        self.assertIn("private-lifecycle-payload", local_stderr.getvalue())

    def test_five_real_services_queries_without_executor_and_change_on_executor(self):
        node, caller = self.managed(), self.node("caller")
        clients = {
            kind: self.lifecycle_client(caller, node, kind)
            for kind in (
                "change_state", "get_state", "get_available_states",
                "get_available_transitions", "get_transition_graph",
            )
        }
        self.assertEqual(clients["get_state"].call({}, 1)["current_state"]["label"], "unconfigured")
        self.assertEqual(len(clients["get_available_states"].call({}, 1)["available_states"]), 11)
        self.assertEqual(len(clients["get_transition_graph"].call({}, 1)["available_transitions"]), 25)
        self.assertEqual(
            {edge["id"] for edge in clients["get_available_transitions"].call({}, 1)["available_transitions"]},
            {1, 5},
        )
        future = clients["change_state"].call_async({
            "transition": {"id": 0, "label": "configure"},
        }, timeout=3)
        self.assertEqual(node.calls, [])
        self.multi(node)
        reply = future.result(4)
        self.assertTrue(reply["success"])
        self.assertEqual(reply["report"], node.last_transition_report)
        self.assertTrue(node.calls[0][3].startswith("aos-control"))
        invalid = clients["change_state"].call({
            "transition": {"id": 3, "label": "ignored for nonzero ID"},
            "drain_timeout_ms": 0,
        }, 2)
        self.assertTrue(invalid["success"])
        unavailable = clients["change_state"].call({
            "transition": {"id": 1, "label": "configure"},
        }, 2)
        self.assertFalse(unavailable["success"])
        self.assertEqual(unavailable["error"]["code"], "INVALID_ARGUMENT")
        endpoints = node.get_node_card()["endpoints"]
        for kind in clients:
            entries = [entry for entry in endpoints if entry["name"] == f"/managed/{kind}"]
            self.assertEqual(len(entries), 1)
            self.assertEqual(entries[0]["qos"]["preset"], "lifecycle_services")

    def test_transition_event_topic_and_service_opt_out(self):
        caller = self.node("observer")
        events = caller.create_subscription(
            aos.lifecycle_event_interface(), "/managed/transition_event",
            qos_profile="lifecycle_events",
        )
        node = self.managed(start_lifecycle_services=False)
        self.change(node, "configure", self.single(node))
        first, second = events.recv(2), events.recv(2)
        self.assertEqual(first["transition"]["goal_state"]["label"], "configuring")
        self.assertEqual(second["transition"]["goal_state"]["label"], "inactive")
        endpoints = node.get_node_card()["endpoints"]
        names = {entry["name"] for entry in endpoints}
        self.assertNotIn("/managed/get_state", names)
        self.assertIn("/managed/get_parameters", names)
        event = next(entry for entry in endpoints if entry["name"] == "/managed/transition_event")
        self.assertEqual(event["qos"]["preset"], "lifecycle_events")

    def test_publisher_timer_rate_and_subscription_gate_without_inactive_backlog(self):
        node, outside = self.managed(), self.node("outside")
        interface = aos.Interface.load(ROOT / "interfaces/String.json")
        publisher = node.create_publisher(interface, "managed_out")
        external = outside.create_publisher(interface, "managed_in")
        received = []
        subscription = node.create_subscription(interface, "managed_in", received.append, 2)
        external.wait_for_subscribers(2)
        clock = aos.Clock("ros", context=self.context)
        clock.set_use_sim_time(True)
        clock.update_ros_time(0)
        ticks = []
        timer = node.create_timer(1, lambda: ticks.append("tick"), clock=clock)
        rate = node.create_rate(1, clock=clock)
        executor = self.single(node)
        with self.assertRaises(aos.InactiveError):
            publisher.publish({"data": "blocked"})
        with self.assertRaises(aos.InactiveError):
            rate.sleep()
        for value in ("old1", "old2", "old3"):
            external.publish({"data": value})
        self.wait_for(lambda: subscription.queue_stats["received_total"] == 3)
        clock.update_ros_time(10_000_000_000)
        self.assertFalse(timer.is_ready())
        self.active(node, executor)
        executor.spin_once(0)
        self.assertEqual(received, [])
        self.assertEqual(ticks, [])
        self.assertEqual(subscription.lifecycle_skipped_total, 2)
        self.assertEqual(subscription.queue_stats["dropped_total"], 1)
        publisher.publish({"data": "allowed"})
        external.publish({"data": "fresh"})
        self.wait_for(lambda: subscription.queue_stats["queued"] == 1)
        executor.spin_once(0)
        self.assertEqual(received, [{"data": "fresh"}])
        clock.update_ros_time(11_000_000_000)
        executor.spin_once(0)
        self.assertEqual(ticks, ["tick"])
        self.change(node, "deactivate", executor)
        for value in ("inactive1", "inactive2"):
            external.publish({"data": value})
        self.wait_for(lambda: subscription.queue_stats["received_total"] == 6)
        clock.update_ros_time(20_000_000_000)
        self.assertFalse(timer.is_ready())
        with self.assertRaises(aos.InactiveError):
            publisher.publish({"data": "blocked again"})
        self.assertTrue(self.change(node, "activate", executor)["successful"])
        executor.spin_once(0)
        # Frozen C01 behavior.md: inactive work is not replayed on activation.
        self.assertEqual(received, [{"data": "fresh"}])
        self.assertEqual(ticks, ["tick"])
        self.assertEqual(subscription.lifecycle_skipped_total, 4)
        clock.update_ros_time(21_000_000_000)
        executor.spin_once(0)
        self.assertEqual(ticks, ["tick", "tick"])

    def test_managed_subscription_lease_tracks_queued_work_and_failed_submission(self):
        node, outside = self.managed(), self.node("outside")
        interface = aos.Interface.load(ROOT / "interfaces/String.json")
        received = []
        subscription = node.create_subscription(interface, "input", received.append)
        publisher = outside.create_publisher(interface, "input")
        publisher.wait_for_subscribers(2)
        executor = self.single(node)
        self.active(node, executor)
        publisher.publish({"data": "queued"})
        self.wait_for(lambda: subscription.queue_stats["queued"] == 1)
        entry = next(entry for entry in node._callbacks if getattr(entry, "subscription", None) is subscription)
        queued = entry._take_callback()
        report = self.change(node, "deactivate", executor, drain_timeout=0)
        self.assertFalse(report["successful"])
        self.assertTrue(report["drain_timed_out"])
        self.assertEqual([item["kind"] for item in report["remaining"]], ["subscription"])
        queued()
        # Frozen C01 behavior.md: already enqueued callbacks count toward drain;
        # failed drain must not claim that the admitted work was cancelled.
        self.assertEqual(received, [{"data": "queued"}])
        self.assertEqual(node.lifecycle_snapshot["in_flight"], [])
        executor.remove_node(node)
        multi = aos.MultiThreadedExecutor(1, context=self.context)
        multi.add_node(node)
        self.addCleanup(multi.shutdown)
        publisher.publish({"data": "submit failure"})
        self.wait_for(lambda: subscription.queue_stats["queued"] == 1)
        with mock.patch.object(multi._pool, "submit", side_effect=RuntimeError("submit failed")):
            with self.assertRaisesRegex(RuntimeError, "submit failed"):
                multi.spin_once(0)
        self.assertEqual(node.lifecycle_snapshot["in_flight"], [])
        multi.remove_node(node)
        executor.add_node(node)
        publisher.publish({"data": "closed before execution"})
        self.wait_for(lambda: subscription.queue_stats["queued"] == 1)
        queued = entry._take_callback()
        node.destroy_subscription(subscription)
        queued()
        self.assertEqual(received, [{"data": "queued"}])
        self.assertEqual(node.lifecycle_snapshot["in_flight"], [])
        self.assertTrue(self.change(node, "deactivate", executor)["successful"])

    def test_timer_admission_survives_failed_drain_but_inactive_periods_do_not_replay(self):
        node = self.managed()
        clock = aos.Clock("ros", context=self.context)
        clock.set_use_sim_time(True)
        clock.update_ros_time(0)
        called = []
        timer = node.create_timer(1, lambda: called.append("timer"), clock=clock)
        executor = self.single(node)
        self.active(node, executor)
        clock.update_ros_time(1_000_000_000)
        queued = timer._take_callback()
        report = self.change(node, "deactivate", executor, drain_timeout=0)
        self.assertTrue(report["drain_timed_out"])
        self.assertEqual(report["final_state"]["label"], "active")
        self.assertEqual([item["kind"] for item in report["remaining"]], ["timer"])
        queued()
        self.assertEqual(called, ["timer"])
        self.assertEqual(node.lifecycle_snapshot["in_flight"], [])
        self.assertTrue(self.change(node, "deactivate", executor)["successful"])
        clock.update_ros_time(100_000_000_000)
        self.assertFalse(timer.is_ready())
        self.assertTrue(self.change(node, "activate", executor)["successful"])
        executor.spin_once(0)
        self.assertEqual(called, ["timer"])
        clock.update_ros_time(101_000_000_000)
        executor.spin_once(0)
        self.assertEqual(called, ["timer", "timer"])

    def test_shutdown_after_callback_take_releases_lease_without_submitting(self):
        node, caller = self.managed(), self.node("caller")
        interface = aos.Interface.load(ROOT / "interfaces/AddTwoInts.json")
        called = []
        service = node.create_service(
            interface, "shutdown_before_submit",
            lambda request: called.append(request) or {"sum": 3},
        )
        client = caller.create_client(interface, "shutdown_before_submit")
        self.assertTrue(client.wait_for_service(2))
        single = self.single(node)
        self.active(node, single)
        single.remove_node(node)
        multi = aos.MultiThreadedExecutor(1, context=self.context)
        multi.add_node(node)
        self.addCleanup(multi.shutdown)
        pending = client.call_async({"a": 1, "b": 2}, timeout=2)
        self.wait_for(lambda: service.queue_stats["queued"] == 1)
        taken, release = threading.Event(), threading.Event()
        take = node._take_ready_callback
        errors = []

        def gated_take(**options):
            work = take(**options)
            if work is not None:
                taken.set()
                if not release.wait(3):
                    raise RuntimeError("test callback gate was not released")
            return work

        def spin():
            try:
                multi.spin_once(0)
            except BaseException as error:
                errors.append(error)

        with mock.patch.object(node, "_take_ready_callback", side_effect=gated_take):
            spinner = threading.Thread(target=spin)
            spinner.start()
            try:
                self.assertTrue(taken.wait(2))
                self.assertTrue(multi.shutdown(timeout_sec=1))
            finally:
                release.set()
                spinner.join(3)
        self.assertFalse(spinner.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(called, [])
        self.assertEqual(node.lifecycle_snapshot["in_flight"], [])
        self.assertTrue(service.callback_group.beginning_execution())
        service.callback_group.ending_execution()
        with self.assertRaises(aos.NoResponseError):
            pending.result(3)

    def test_service_submission_failure_releases_admission_even_with_retained_traceback(self):
        node, caller = self.managed(), self.node("caller")
        interface = aos.Interface.load(ROOT / "interfaces/AddTwoInts.json")
        called = []
        service = node.create_service(
            interface, "queued_service", lambda request: called.append(request) or {"sum": 3},
        )
        client = caller.create_client(interface, "queued_service")
        self.assertTrue(client.wait_for_service(2))
        single = self.single(node)
        self.active(node, single)
        single.remove_node(node)
        multi = aos.MultiThreadedExecutor(1, context=self.context)
        multi.add_node(node)
        self.addCleanup(multi.shutdown)
        pending = client.call_async({"a": 1, "b": 2}, timeout=2)
        self.wait_for(lambda: service.queue_stats["queued"] == 1)
        retained_error = None
        with mock.patch.object(multi._pool, "submit", side_effect=RuntimeError("submit failed")):
            try:
                multi.spin_once(0)
            except RuntimeError as error:
                retained_error = error
        self.assertIsNotNone(retained_error)
        self.assertIsNotNone(retained_error.__traceback__)
        self.assertEqual(called, [])
        self.assertEqual(node.lifecycle_snapshot["in_flight"], [])
        with self.assertRaises(aos.NoResponseError):
            pending.result(3)

    def test_action_submission_failure_finishes_with_native_default_abort(self):
        node, caller = self.managed(), self.node("caller")
        interface = aos.Interface.load(ROOT / "interfaces/action/Fibonacci.json")
        accepted, called = [], []
        aos.ActionServer(
            node, interface, "queued_action",
            lambda goal: called.append(goal) or {"sequence": [0]},
            handle_accepted_callback=accepted.append,
        )
        client = aos.ActionClient(caller, interface, "queued_action")
        self.assertTrue(client.wait_for_server(2))
        single = self.single(node)
        self.active(node, single)
        sent = client.send_goal_async({"order": 1}, timeout=2)
        single.spin_until_future_complete(sent, 2)
        goal = sent.result(1)
        self.assertTrue(goal.accepted)
        accepted[0].execute()
        single.remove_node(node)
        multi = aos.MultiThreadedExecutor(1, context=self.context)
        multi.add_node(node)
        self.addCleanup(multi.shutdown)
        result = goal.get_result_async(timeout=2)
        with mock.patch.object(multi._pool, "submit", side_effect=RuntimeError("submit failed")):
            with self.assertRaisesRegex(RuntimeError, "submit failed"):
                multi.spin_once(0)
        self.assertEqual(called, [])
        self.assertEqual(node.lifecycle_snapshot["in_flight"], [])
        self.assertEqual(result.result(3), {"status": 6, "result": {"sequence": []}})

    def test_partial_submission_does_not_start_service_or_release_group_twice(self):
        node, caller = self.managed(), self.node("caller")
        interface = aos.Interface.load(ROOT / "interfaces/AddTwoInts.json")
        called = []
        service = node.create_service(
            interface, "partial_submit", lambda request: called.append(request) or {"sum": 3},
        )
        client = caller.create_client(interface, "partial_submit")
        self.assertTrue(client.wait_for_service(2))
        single = self.single(node)
        self.active(node, single)
        single.remove_node(node)
        multi = aos.MultiThreadedExecutor(1, context=self.context)
        multi.add_node(node)
        self.addCleanup(multi.shutdown)
        pending = client.call_async({"a": 1, "b": 2}, timeout=2)
        self.wait_for(lambda: service.queue_stats["queued"] == 1)
        started = threading.Event()
        threads, errors = [], []

        def partial_submit(run):
            def queued_worker():
                started.set()
                try:
                    run()
                except BaseException as error:
                    errors.append(error)

            worker = threading.Thread(target=queued_worker)
            threads.append(worker)
            worker.start()
            self.assertTrue(started.wait(1))
            raise RuntimeError("thread startup failed after enqueue")

        with mock.patch.object(multi._pool, "submit", side_effect=partial_submit):
            with self.assertRaisesRegex(RuntimeError, "after enqueue"):
                multi.spin_once(0)
        threads[0].join(2)
        self.assertFalse(threads[0].is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(called, [])
        self.assertEqual(node.lifecycle_snapshot["in_flight"], [])
        self.assertTrue(service.callback_group.beginning_execution())
        service.callback_group.ending_execution()
        with self.assertRaises(aos.NoResponseError):
            pending.result(3)

    def test_running_action_invocation_cannot_be_withdrawn_or_finished_twice(self):
        node, caller = self.managed(), self.node("caller")
        interface = aos.Interface.load(ROOT / "interfaces/action/Fibonacci.json")
        entered, release = threading.Event(), threading.Event()
        accepted, called = [], []

        def execute(goal):
            called.append(True)
            entered.set()
            if not release.wait(3):
                raise RuntimeError("test execute barrier timed out")
            goal.succeed()
            return {"sequence": [0, 1]}

        aos.ActionServer(
            node, interface, "running_action", execute,
            handle_accepted_callback=accepted.append,
        )
        client = aos.ActionClient(caller, interface, "running_action")
        self.assertTrue(client.wait_for_server(2))
        executor = self.single(node)
        self.active(node, executor)
        sent = client.send_goal_async({"order": 2}, timeout=2)
        executor.spin_until_future_complete(sent, 2)
        goal = sent.result(1)
        accepted[0].execute()
        invocation, group = node._take_ready_callback()
        errors = []

        def run():
            try:
                invocation()
            except BaseException as error:
                errors.append(error)
            finally:
                group.ending_execution()

        worker = threading.Thread(target=run)
        worker.start()
        result = goal.get_result_async(timeout=3)
        try:
            self.assertTrue(entered.wait(2))
            invocation._cancel()
            invocation._cancel()
            self.assertFalse(result.done())
            self.assertEqual(len(node.lifecycle_snapshot["in_flight"]), 1)
        finally:
            release.set()
            worker.join(3)
        self.assertFalse(worker.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(result.result(4), {"status": 4, "result": {"sequence": [0, 1]}})
        invocation()
        invocation._cancel()
        self.assertEqual(called, [True])
        self.assertEqual(node.lifecycle_snapshot["in_flight"], [])

    def test_inactive_service_error_is_typed_and_ordinary_business_error_is_not(self):
        node, caller = self.managed(), self.node("caller")
        interface = aos.Interface.load(ROOT / "interfaces/AddTwoInts.json")
        seen = []
        node.create_service(interface, "business", lambda request: (
            seen.append(request) or {"sum": request["a"] + request["b"]}
        ))
        client = caller.create_client(interface, "business")
        self.assertTrue(client.wait_for_service(2))
        # Native inactive rejection does not need a Python executor.
        with self.assertRaises(aos.InactiveError):
            client.call({"a": 1, "b": 2}, 1)
        self.assertEqual(seen, [])
        executor = self.multi(node)
        self.active(node)
        self.assertEqual(client.call({"a": 2, "b": 3}, 2), {"sum": 5})
        self.assertTrue(self.change(node, "deactivate")["successful"])
        with self.assertRaises(aos.InactiveError):
            client.call({"a": 3, "b": 4}, 1)
        self.assertEqual(len(seen), 1)
        ordinary = self.node("ordinary")
        raw = ordinary._native.create_service(interface, "application_error")
        error_client = caller.create_client(interface, "application_error")
        self.assertTrue(error_client.wait_for_service(2))
        pending = error_client.call_async({"a": 0, "b": 0}, 2)
        raw.recv(1).reply_error("SERVICE_INACTIVE")
        with self.assertRaises(aos.ServiceError) as error:
            pending.result(3)
        self.assertNotIsInstance(error.exception, aos.InactiveError)

    def test_full_business_pool_keeps_state_and_parameters_available_during_drain(self):
        node, caller = self.managed(), self.node("caller")
        node.declare_parameter("label", "ready")
        interface = aos.Interface.load(ROOT / "interfaces/AddTwoInts.json")
        entered, release = threading.Event(), threading.Event()
        transition_entered, transition_release = threading.Event(), threading.Event()

        def service(request):
            entered.set()
            release.wait(5)
            return {"sum": 42}

        def deactivate(state):
            transition_entered.set()
            transition_release.wait(5)
            return aos.TransitionCallbackReturn.SUCCESS

        node.handlers["deactivate"] = deactivate
        node.create_service(interface, "busy", service)
        busy = caller.create_client(interface, "busy")
        self.assertTrue(busy.wait_for_service(2))
        state = self.lifecycle_client(caller, node, "get_state")
        change = self.lifecycle_client(caller, node, "change_state")
        parameters = caller.create_client(
            aos.parameter_service_interface("get_parameters"), "/managed/get_parameters",
            qos_profile="parameters",
        )
        self.assertTrue(parameters.wait_for_service(2))
        self.multi(node, threads=1)
        self.addCleanup(release.set)
        self.addCleanup(transition_release.set)
        self.active(node)
        pending = busy.call_async({"a": 1, "b": 2}, 5)
        self.assertTrue(entered.wait(2))
        transition = change.call_async({
            "transition": {"id": 4, "label": "deactivate"}, "drain_timeout_ms": 30,
        }, 3)
        self.assertTrue(transition_entered.wait(2))
        self.assertEqual(state.call({}, 1)["current_state"]["label"], "deactivating")
        self.assertEqual(parameters.call({"names": ["label"]}, 1)["value"][0]["value"], "ready")
        transition_release.set()
        report = transition.result(3)["report"]
        self.assertFalse(report["successful"])
        self.assertTrue(report["drain_timed_out"])
        self.assertEqual(report["final_state"]["label"], "active")
        self.assertEqual([item["kind"] for item in report["remaining"]], ["service"])
        self.assertFalse(pending.done())
        release.set()
        self.assertEqual(pending.result(3), {"sum": 42})
        node.handlers.pop("deactivate")
        self.assertTrue(self.change(node, "deactivate")["successful"])

    def test_action_cancel_runs_with_full_business_pool_and_drain_does_not_invent_result(self):
        node, caller = self.managed(), self.node("caller")
        interface = aos.Interface.load(ROOT / "interfaces/action/Fibonacci.json")
        entered, release = threading.Event(), threading.Event()
        seen_cancel = []

        def execute(goal):
            entered.set()
            release.wait(5)
            goal.succeed()
            return {"sequence": [0, 1]}

        def cancel(goal):
            seen_cancel.append(threading.current_thread().name)
            return aos.CancelResponse.ACCEPT

        aos.ActionServer(node, interface, "job", execute, cancel_callback=cancel)
        client = aos.ActionClient(caller, interface, "job")
        self.assertTrue(client.wait_for_server(2))
        # The native inactive goal worker rejects without calling Python.
        self.assertFalse(client.send_goal_async({"order": 2}, timeout=1).result(2).accepted)
        self.multi(node, caller, threads=1)
        self.addCleanup(release.set)
        self.active(node)
        goal = client.send_goal_async({"order": 2}, timeout=2).result(3)
        self.assertTrue(goal.accepted)
        self.assertTrue(entered.wait(2))
        transition = node.trigger_deactivate_async(drain_timeout=0.5)
        self.wait_for(lambda: node.get_current_state().label == "deactivating")
        canceled = goal.cancel_goal_async(timeout=1).result(2)
        self.assertEqual(len(canceled["goals_canceling"]), 1)
        self.assertTrue(seen_cancel[0].startswith("aos-control"))
        report = transition.result(2)
        self.assertTrue(report["drain_timed_out"])
        self.assertEqual([item["kind"] for item in report["remaining"]], ["action"])
        result = goal.get_result_async(timeout=3)
        self.assertFalse(result.done())
        release.set()
        # Accepting cancellation does not force CANCELED. The provider chooses.
        self.assertEqual(result.result(4), {"status": 4, "result": {"sequence": [0, 1]}})
        self.assertTrue(self.change(node, "deactivate")["successful"])
        self.assertFalse(client.send_goal_async({"order": 2}, timeout=1).result(2).accepted)
        self.assertEqual(client.get_result_async(goal.goal_id, timeout=1).result(2)["status"], 4)

    def test_ready_cancel_precedes_new_parameters_and_goals_across_nodes(self):
        parameters, provider = self.managed("parameters"), self.managed("actions")
        caller = self.node("caller")
        parameters.declare_parameter("value", 0)
        order, accepted = [], []
        parameters.add_on_set_parameters_callback(lambda values: (
            order.append("parameter") or aos.SetParametersResult(True)
        ))
        interface = aos.Interface.load(ROOT / "interfaces/action/Fibonacci.json")
        aos.ActionServer(
            provider, interface, "priority", lambda goal: {"sequence": []},
            goal_callback=lambda goal: (order.append("goal") or aos.GoalResponse.ACCEPT),
            cancel_callback=lambda goal: (order.append("cancel") or aos.CancelResponse.ACCEPT),
            handle_accepted_callback=accepted.append,
        )
        client = aos.ActionClient(caller, interface, "priority")
        self.assertTrue(client.wait_for_server(2))
        parameter_client = caller.create_client(
            aos.parameter_service_interface("set_parameters_atomically"),
            "/parameters/set_parameters_atomically", qos_profile="parameters",
        )
        self.assertTrue(parameter_client.wait_for_service(2))
        # The parameter Node is deliberately first in Executor iteration order.
        executor = self.single(parameters, provider)
        self.active(parameters, executor)
        self.active(provider, executor)
        first = client.send_goal_async({"order": 1}, timeout=2)
        executor.spin_until_future_complete(first, 2)
        goal = first.result(1)
        order.clear()
        parameter_set = parameter_client.call_async({
            "parameters": [aos.Parameter("value", value=1).to_dict()],
        }, 3)
        new_goal = client.send_goal_async({"order": 2}, timeout=3)
        cancel = goal.cancel_goal_async(timeout=3)
        # The Executor stays stopped while three independent native request
        # paths enqueue their work. No user callback can run in this interval.
        time.sleep(0.05)
        self.assertEqual(order, [])
        executor.spin_once(0)
        self.assertEqual(len(cancel.result(2)["goals_canceling"]), 1)
        self.assertEqual(order, ["cancel"])
        self.assertFalse(parameter_set.done())
        self.assertFalse(new_goal.done())
        executor.spin_until_future_complete(parameter_set, 2)
        executor.spin_until_future_complete(new_goal, 2)
        self.assertTrue(parameter_set.result(1)["result"]["successful"])
        self.assertTrue(new_goal.result(1).accepted)

    def test_control_shutdown_timeout_does_not_claim_callback_was_stopped(self):
        node = self.managed()
        entered, release = threading.Event(), threading.Event()

        def configure(state):
            entered.set()
            release.wait(5)
            return aos.TransitionCallbackReturn.SUCCESS

        node.handlers["configure"] = configure
        executor = self.multi(node)
        self.addCleanup(release.set)
        future = node.trigger_configure_async()
        self.assertTrue(entered.wait(2))
        self.assertFalse(executor.shutdown(timeout_sec=0.01))
        self.assertFalse(future.done())
        self.assertEqual(node.get_current_state().label, "configuring")
        release.set()
        self.assertEqual(future.result(2)["final_state"]["label"], "inactive")
        self.assertTrue(executor.shutdown(timeout_sec=2))

    def test_managed_action_client_feedback_is_business_but_result_stays_available(self):
        caller, provider = self.managed("managed_caller"), self.node("provider")
        interface = aos.Interface.load(ROOT / "interfaces/action/Fibonacci.json")
        commands = Queue()
        sent = threading.Event()
        received = []

        def execute(goal):
            while True:
                command = commands.get(timeout=5)
                if command is None:
                    goal.succeed()
                    return {"sequence": [0, 1]}
                goal.publish_feedback({"partial_sequence": command})
                sent.set()

        aos.ActionServer(provider, interface, "feedback_job", execute)
        client = aos.ActionClient(caller, interface, "feedback_job")
        self.assertTrue(client.wait_for_server(2))
        with self.assertRaises(aos.InactiveError):
            client.send_goal_async({"order": 2}, timeout=1).result(2)
        self.multi(provider, caller, threads=3)
        self.addCleanup(lambda: commands.put(None))
        self.active(caller)
        goal = client.send_goal_async(
            {"order": 2}, feedback_callback=received.append, timeout=2,
        ).result(3)
        commands.put([0])
        self.wait_for(lambda: len(received) == 1)
        self.assertTrue(self.change(caller, "deactivate")["successful"])
        sent.clear()
        commands.put([0, 99])
        self.assertTrue(sent.wait(2))
        # The native publish completed. Give the local transport callback time
        # to enqueue this sample before the activation discard barrier.
        time.sleep(0.03)
        self.assertEqual(len(received), 1)
        self.assertTrue(self.change(caller, "activate")["successful"])
        sent.clear()
        commands.put([0, 1])
        self.wait_for(lambda: len(received) == 2)
        self.assertEqual(
            [message["feedback"]["partial_sequence"] for message in received],
            [[0], [0, 1]],
        )
        self.assertTrue(self.change(caller, "deactivate")["successful"])
        result = goal.get_result_async(timeout=3)
        commands.put(None)
        self.assertEqual(result.result(4)["status"], 4)

    def test_closing_node_finishes_unscheduled_transition_observation(self):
        node = self.managed()
        future = node.trigger_configure_async()
        node.destroy_node()
        with self.assertRaises(aos.InvalidStateError):
            future.result(1)
        self.assertEqual(node.calls, [])

    def test_installed_example_runs_outside_source_directory(self):
        result = subprocess.run(
            [sys.executable, "-I", str(ROOT / "examples/lifecycle_node.py")],
            cwd=self.directory, capture_output=True, text=True, timeout=20,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        value = json.loads(result.stdout)
        self.assertTrue(value["inactive_publish_rejected"])
        self.assertEqual(value["received"], [{"data": "managed work"}])
        self.assertEqual(value["observed_state"]["current_state"]["label"], "inactive")
        self.assertEqual(
            [entry["final_state"]["label"] for entry in value["transitions"]],
            ["inactive", "active", "inactive", "finalized"],
        )


if __name__ == "__main__":
    unittest.main()
