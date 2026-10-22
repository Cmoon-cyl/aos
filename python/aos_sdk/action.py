"""Action callbacks following the fixed ROS 2 Jazzy rclpy execution model.

The native provider owns goal state and retained results. This module owns
Python callbacks and schedules them on the Node's Executor.
"""

from concurrent.futures import InvalidStateError as FutureStateError
from collections.abc import Mapping
from enum import Enum, IntEnum
import inspect
import json
import logging
import threading
import uuid
import weakref

from . import _native
from .callback_groups import ReentrantCallbackGroup, _ControlCallbackGroup
from .exceptions import AOSError, InvalidArgumentError, InvalidStateError, WaitTimeoutError
from .executors import Future
from .node import _SubscriptionInvocation, _check_callback
from .qos import native_action_qos

_LOG = logging.getLogger("aos_sdk")
_ZERO_UUID = "0" * 32


class GoalStatus(IntEnum):
    UNKNOWN = 0
    ACCEPTED = 1
    EXECUTING = 2
    CANCELING = 3
    SUCCEEDED = 4
    CANCELED = 5
    ABORTED = 6


class GoalResponse(Enum):
    REJECT = 1
    ACCEPT = 2


class CancelResponse(Enum):
    REJECT = 1
    ACCEPT = 2


def default_goal_callback(request):
    return GoalResponse.ACCEPT


def default_cancel_callback(goal_handle):
    return CancelResponse.REJECT


def default_handle_accepted_callback(goal_handle):
    goal_handle.execute()


def _snapshot(value):
    try:
        return json.loads(json.dumps(value, allow_nan=False, ensure_ascii=False))
    except (TypeError, ValueError, UnicodeError) as error:
        raise InvalidArgumentError(f"invalid JSON value: {error}") from error


def _call_async(node, group, operation):
    future = Future(lambda callback: node._schedule(callback, group))

    def wait():
        try:
            result = operation()
        except Exception as error:
            try:
                future.set_exception(error)
            except FutureStateError:
                pass
        else:
            try:
                future.set_result(result)
            except FutureStateError:
                pass
        finally:
            node._wake_executor()

    threading.Thread(target=wait, name="aos-action-wait", daemon=True).start()
    return future


class ServerGoalHandle:
    """A Python view of a provider-owned Rust goal.

    The execute callback should select a terminal state using ``succeed()``,
    ``abort()``, or ``canceled()``, then return its JSON result.
    """

    def __init__(self, server, native):
        self._server = server
        self._native = native

    @property
    def goal_id(self):
        return self._native.goal_id

    @property
    def request(self):
        return self._native.request

    @property
    def status(self):
        return self._native.status

    @property
    def is_active(self):
        return self._native.is_active

    @property
    def is_cancel_requested(self):
        return self._native.is_cancel_requested

    def execute(self, execute_callback=None):
        callback = execute_callback or self._server.execute_callback
        _check_callback(callback)
        # Rust rejects repeated execution, and preserves CANCELING when the
        # goal was cancelled before the accepted callback scheduled it.
        self._native.execute()
        self._server._node._schedule(
            _ActionExecutionInvocation(self._server, self, callback),
            self._server._execution_group,
        )

    def publish_feedback(self, feedback):
        self._native.publish_feedback(feedback)

    def succeed(self):
        self._native.succeed()

    def abort(self):
        self._native.abort()

    def canceled(self):
        self._native.canceled()


class _ActionExecutionInvocation:
    def __init__(self, server, goal_handle, callback):
        self._server = server
        self._goal_handle = goal_handle
        self._callback = callback
        self._lock = threading.Lock()
        # Dispatch ownership only; the Rust goal owns its lifecycle state.
        self._state = "pending"

    def __call__(self):
        with self._lock:
            if self._state != "pending":
                return
            self._state = "running"
            goal_handle = self._goal_handle
        try:
            self._server._execute_goal(goal_handle, self._callback)
        finally:
            with self._lock:
                self._state = "settled"
                self._goal_handle = None

    def _cancel(self):
        with self._lock:
            if self._state != "pending":
                return
            self._state = "settled"
            goal_handle = self._goal_handle
            self._goal_handle = None
        try:
            # Use the same native failure path as an execute callback
            # exception. Core owns the ABORTED transition and result.
            goal_handle._native.finish_default()
        except AOSError:
            _LOG.warning("Action default result could not be completed after submission failed")


class ActionServer:
    """Provide an Action using Executor-dispatched synchronous callbacks.

    ``result_timeout`` controls retention after completion, not execution time.
    It defaults to the Jazzy Python value of 900 seconds.
    ``max_result_waiters`` bounds retained pending GetResult requests across
    all goals on this server (default 1024, range 1..65536).
    ``resource_limits`` is an optional mapping controlling retained goals and
    results: max_goals (1024), max_goal_bytes (1 MiB), max_result_bytes (1 MiB),
    and max_result_cache_bytes (64 MiB). Byte limits count serialized business
    data, not process memory. Result overflow preserves the terminal status
    and makes GetResult return a resource error; it does not retry execution.
    Five independent QoS profiles configure the three services, feedback Topic,
    and status Topic. Their defaults are services_default/default/action_status.
    On a LifecycleNode, pending cancel decisions have priority in the reserved
    control worker. A currently running parameter/goal/cancel callback or a
    shared mutually exclusive CallbackGroup cannot be preempted.
    """

    def __init__(
        self, node, interface, action_name, execute_callback, *,
        callback_group=None, goal_callback=None, cancel_callback=None,
        handle_accepted_callback=None, result_timeout=900.0,
        max_result_waiters=1024, resource_limits=None,
        goal_service_qos_profile=None, result_service_qos_profile=None,
        cancel_service_qos_profile=None, feedback_qos_profile=None, status_qos_profile=None,
        summary="", side_effects=None, contract_version=None,
    ):
        if (isinstance(max_result_waiters, bool)
                or not isinstance(max_result_waiters, int)
                or not 1 <= max_result_waiters <= 65536):
            raise InvalidArgumentError(
                "max_result_waiters must be an integer in 1..65536"
            )
        self._node = node
        self.callback_group = callback_group or getattr(
            node, "_action_callback_group", node.default_callback_group
        )
        if callback_group is not None and node._managed_lifecycle:
            self.callback_group = _ControlCallbackGroup("control", callback_group)
        # Jazzy schedules execution with Executor.create_task independently of
        # the Action waitable's group, so long execution does not hold that lock.
        self._execution_group = ReentrantCallbackGroup()
        self.execute_callback = execute_callback
        self.goal_callback = goal_callback or default_goal_callback
        self.cancel_callback = cancel_callback or default_cancel_callback
        self.handle_accepted_callback = (
            handle_accepted_callback or default_handle_accepted_callback
        )
        for callback in (
            self.execute_callback, self.goal_callback, self.cancel_callback,
            self.handle_accepted_callback,
        ):
            _check_callback(callback)
        self._native = _native._ActionServer(
            node._native, interface, action_name, result_timeout=result_timeout,
            max_result_waiters=max_result_waiters,
            resource_limits=(
                dict(resource_limits) if isinstance(resource_limits, Mapping) else resource_limits
            ),
            qos_profiles=native_action_qos(
                goal_service_qos_profile, result_service_qos_profile,
                cancel_service_qos_profile, feedback_qos_profile, status_qos_profile,
            ),
            summary=summary, side_effects=side_effects,
            contract_version=contract_version,
        )
        node._track(self, self)

    def _take_callback(self):
        try:
            return self._take_priority_callback()
        except WaitTimeoutError:
            request = self._native.take_goal_request(0.0)
            return lambda: self._accept_goal(request)

    def _take_priority_callback(self):
        request = self._native.take_cancel_request(0.0)
        return lambda: self._cancel_goals(request)

    def _accept_goal(self, request):
        try:
            decision = self.goal_callback(request.request)
        except Exception:
            _LOG.exception("Action goal callback failed; rejecting goal")
            decision = GoalResponse.REJECT
        accepted = isinstance(decision, GoalResponse) and decision == GoalResponse.ACCEPT
        try:
            native = request.decide(accepted)
        except AOSError:
            _LOG.exception("Action goal response could not be delivered")
            return
        if native is not None:
            handle = ServerGoalHandle(self, native)
            try:
                self.handle_accepted_callback(handle)
            except Exception:
                _LOG.exception("Action accepted callback failed")
                # This is local callback cleanup. Start permits the existing
                # EXECUTE -> ABORT transition without adding a state edge.
                try:
                    if handle.status == GoalStatus.ACCEPTED:
                        native.execute()
                    native.finish_default()
                except AOSError:
                    _LOG.warning("Failed to finish goal after accepted callback failure")

    def _cancel_goals(self, request):
        accepted = []
        for native in request.candidates():
            handle = ServerGoalHandle(self, native)
            try:
                if self.cancel_callback(handle) == CancelResponse.ACCEPT:
                    accepted.append(handle.goal_id)
            except Exception:
                _LOG.exception("Action cancel callback failed; rejecting candidate")
        try:
            request.respond(accepted)
        except AOSError:
            _LOG.warning("Action cancel response could not be delivered")

    def _execute_goal(self, goal_handle, callback):
        try:
            result = callback(goal_handle)
            if inspect.isawaitable(result):
                if inspect.iscoroutine(result):
                    result.close()
                raise TypeError("Action execute callback must return JSON synchronously")
        except Exception:
            _LOG.exception("Action execute callback failed")
            try:
                # As in fixed Jazzy: active goals abort; an already terminal
                # state is retained. Both cases use the interface's default.
                goal_handle._native.finish_default()
            except AOSError:
                _LOG.warning("Action default result could not be completed")
            return
        try:
            goal_handle._native.finish(result)
        except AOSError:
            # Rust caches invalid-result failure for result waiters. Do not
            # replace that error with a default success result.
            _LOG.exception("Action result could not be completed")

    def destroy(self):
        self._native.destroy()
        self._node._forget(self)

    def __enter__(self):
        return self

    def __exit__(self, *_):
        try:
            self.destroy()
        except InvalidStateError:
            pass


class _FeedbackRegistration:
    """Callback ownership only; native/provider code owns all goal state."""

    __slots__ = ("callback", "__weakref__")

    def __init__(self, callback):
        self.callback = callback


class ClientGoalHandle:
    """An accepted or rejected goal response; the provider remains authoritative."""

    def __init__(self, client, native, feedback_registration=None):
        self._client = client
        self._native = native
        self._feedback_registration = feedback_registration

    @property
    def goal_id(self):
        return self._native.goal_id

    @property
    def accepted(self):
        return self._native.accepted

    @property
    def stamp(self):
        return self._native.stamp

    @property
    def status(self):
        return self._native.status

    def get_result_async(self, *, timeout=None):
        """Wait until completion by default; a timeout never cancels the goal."""
        client, native, goal_id = self._client, self._native, self.goal_id
        reference = (
            weakref.ref(self._feedback_registration)
            if self._feedback_registration is not None else None
        )
        return _call_async(
            client._node, client.callback_group,
            lambda: client._observe_result(
                goal_id, reference, lambda: native.get_result(timeout)
            ),
        )

    def cancel_goal_async(self, *, timeout=10.0):
        """Request cancellation; inspect goals_canceling, then await the result."""
        client, native = self._client, self._native
        return _call_async(
            client._node, client.callback_group,
            lambda: native.cancel_goal(timeout),
        )


class ActionClient:
    """Send goals and observe results/status through the same native Rust core.

    Five independent profiles configure service reply queues and feedback/status
    subscriptions. Defaults are services_default/default/action_status.
    Feedback callbacks are retained by in-flight sends and accepted goal handles,
    then released on the last owner, a successful result, or client destruction.
    """

    def __init__(
        self, node, interface, action_name, *, callback_group=None,
        goal_service_qos_profile=None, result_service_qos_profile=None,
        cancel_service_qos_profile=None, feedback_qos_profile=None, status_qos_profile=None,
        summary="", side_effects=None, contract_version=None,
    ):
        self._node = node
        self.callback_group = callback_group or node.default_callback_group
        self._native = _native._ActionClient(
            node._native, interface, action_name, summary=summary,
            qos_profiles=native_action_qos(
                goal_service_qos_profile, result_service_qos_profile,
                cancel_service_qos_profile, feedback_qos_profile, status_qos_profile,
            ),
            side_effects=side_effects, contract_version=contract_version,
        )
        self._feedback_callbacks = {}
        # Weakref callbacks may run synchronously when a temporary owner drops.
        self._callback_lock = threading.RLock()
        self._feedback_closed = False
        node._track(self, self)

    def _register_feedback(self, goal_id, callback):
        with self._callback_lock:
            if self._feedback_closed:
                return None
            reference = self._feedback_callbacks.get(goal_id)
            registration = reference() if reference is not None else None
            if registration is None:
                registration = _FeedbackRegistration(callback)
                client_reference = weakref.ref(self)

                def discard(reference):
                    client = client_reference()
                    if client is not None:
                        with client._callback_lock:
                            if client._feedback_callbacks.get(goal_id) is reference:
                                client._feedback_callbacks.pop(goal_id)

                self._feedback_callbacks[goal_id] = weakref.ref(registration, discard)
            elif registration.callback is None and callback is not None:
                registration.callback = callback
            return registration

    def wait_for_server(self, timeout_sec: float | None = None) -> bool:
        """Wait for the matching server; return False on timeout.

        None waits until ready or closed. This does not submit a goal.
        """
        return self._native.wait_for_server(timeout_sec)

    def send_goal_async(
        self, goal, feedback_callback=None, *, goal_id=None, timeout=10.0,
    ):
        """Send once; resolve to ClientGoalHandle, including rejected responses."""
        goal = _snapshot(goal)
        goal_id = uuid.uuid4().hex if goal_id is None else goal_id
        if not isinstance(goal_id, str):
            raise InvalidArgumentError("goal_id must be a string")
        if feedback_callback is not None:
            _check_callback(feedback_callback)
        # Register even without a callback: another send for this UUID can add
        # one while an earlier accepted handle still owns the registration.
        registration = self._register_feedback(goal_id, feedback_callback)

        def send():
            nonlocal registration
            try:
                native = self._native.send_goal(goal, goal_id=goal_id, timeout=timeout)
                return ClientGoalHandle(
                    self, native, registration if native.accepted else None,
                )
            finally:
                # Exception tracebacks retained by a Future must not keep the
                # in-flight owner alive after that send has settled.
                registration = None

        return _call_async(self._node, self.callback_group, send)

    def get_result_async(self, goal_id, *, timeout=None):
        """Query a saved UUID, including UNKNOWN for expired/missing results."""
        with self._callback_lock:
            reference = self._feedback_callbacks.get(goal_id)
        return _call_async(
            self._node, self.callback_group,
            lambda: self._observe_result(
                goal_id, reference, lambda: self._native.get_result(goal_id, timeout)
            ),
        )

    def _observe_result(self, goal_id, reference, get_result):
        result = get_result()
        with self._callback_lock:
            registration = reference() if reference is not None else None
            if registration is not None:
                # Keep user callback destruction outside the lock. A later
                # registration for the same UUID belongs to its new owners.
                callback = registration.callback
                registration.callback = None
                current = self._feedback_callbacks.get(goal_id)
                if current is not None and current() is registration:
                    self._feedback_callbacks.pop(goal_id)
        if registration is not None:
            del callback
        return result

    def cancel_goals_async(self, *, goal_id=_ZERO_UUID, stamp=None, timeout=10.0):
        """Use the standard UUID/time selectors; zero UUID/time selects all."""
        request = _snapshot({
            "goal_info": {
                "goal_id": goal_id,
                "stamp": {"sec": 0, "nanosec": 0} if stamp is None else stamp,
            }
        })
        return _call_async(
            self._node, self.callback_group,
            lambda: self._native.cancel(request, timeout),
        )

    def _take_callback(self):
        saw_status = False
        try:
            # Rust updates its client observation cache; Python stores no
            # independent goal state machine.
            self._native.recv_status(0.0)
            saw_status = True
        except WaitTimeoutError:
            pass
        try:
            call = self._native.take_feedback(0.0)
        except WaitTimeoutError:
            if saw_status:
                return lambda: None
            raise
        with self._callback_lock:
            reference = self._feedback_callbacks.get(call.message["goal_id"])
            registration = reference() if reference is not None else None
            callback = registration.callback if registration is not None else None
        return _SubscriptionInvocation(call, callback or (lambda message: None))

    def destroy(self):
        with self._callback_lock:
            self._feedback_closed = True
        try:
            self._native.destroy()
        finally:
            with self._callback_lock:
                registrations = [reference() for reference in self._feedback_callbacks.values()]
                self._feedback_callbacks.clear()
                callbacks = []
                for registration in registrations:
                    if registration is not None:
                        callbacks.append(registration.callback)
                        registration.callback = None
            # Already taken invocations may finish; their native lease is
            # released by _SubscriptionInvocation's existing finally path.
            del callbacks, registrations
            self._node._forget(self)

    def __enter__(self):
        return self

    def __exit__(self, *_):
        try:
            self.destroy()
        except InvalidStateError:
            pass
