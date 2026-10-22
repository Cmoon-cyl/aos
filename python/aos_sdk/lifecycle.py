"""Executor callbacks for the native lifecycle state machine."""

from __future__ import annotations

from concurrent.futures import InvalidStateError as FutureStateError
from dataclasses import dataclass
from enum import IntEnum
import inspect
import logging

from .callback_groups import _ControlCallbackGroup
from .exceptions import AOSError, InvalidArgumentError, InvalidStateError
from .executors import Future
from .node import Node

_LOG = logging.getLogger("aos_sdk")


@dataclass(frozen=True)
class LifecycleState:
    """A state description returned by the Rust core."""

    id: int
    label: str


class TransitionCallbackReturn(IntEnum):
    SUCCESS = 97
    FAILURE = 98
    ERROR = 99


class LifecycleTransition(IntEnum):
    CONFIGURE = 1
    CLEANUP = 2
    ACTIVATE = 3
    DEACTIVATE = 4
    UNCONFIGURED_SHUTDOWN = 5
    INACTIVE_SHUTDOWN = 6
    ACTIVE_SHUTDOWN = 7


# This selects a Python callback only. Rust selects every transition, callback
# state, result edge, admission gate, drain deadline and final state.
_CALLBACKS = {
    "configuring": "on_configure",
    "cleaningup": "on_cleanup",
    "activating": "on_activate",
    "deactivating": "on_deactivate",
    "shuttingdown": "on_shutdown",
    "errorprocessing": "on_error",
}


class LifecycleNode(Node):
    """A managed Node whose lifecycle callbacks execute in an Executor.

    Override the ``on_*`` methods and return ``TransitionCallbackReturn``.
    Use ``change_state_async`` or a ``trigger_*_async`` method, then spin the
    Executor to run callbacks. Queries read the shared native state directly.

    ``drain_timeout`` bounds waiting for admitted business work after a
    deactivate/shutdown callback returns. It cannot interrupt a synchronous
    Python callback. A finalized lifecycle state does not destroy the Node or
    its process; call ``destroy_node()`` for resource cleanup.

    Work already admitted to the Executor counts toward drain and may finish.
    A failed deactivate returns to active without cancelling that work.
    Activation discards subscription backlog and missed Timer periods from
    the inactive interval; it does not replay them.

    A MultiThreadedExecutor reserves one transition worker and one shared
    parameter/Action control worker for all its LifecycleNodes.
    SingleThreadedExecutor runs callbacks serially on its spin thread.
    """

    _managed_lifecycle = True

    def __init__(
        self, name: str, namespace: str = "/", context=None, *,
        start_lifecycle_services=True, **kwargs,
    ):
        if not isinstance(start_lifecycle_services, bool):
            raise InvalidArgumentError("start_lifecycle_services must be a bool")
        self._lifecycle_callback_group = _ControlCallbackGroup("lifecycle")
        self._parameter_callback_group = _ControlCallbackGroup("control")
        self._action_callback_group = _ControlCallbackGroup("control")
        self._lifecycle_pending = set()
        super().__init__(name, namespace, context, **kwargs)
        try:
            self._lifecycle = self._native.lifecycle()
            if start_lifecycle_services:
                services = _LifecycleServices(self)
                self._track(services, services)
        except BaseException:
            try:
                self.destroy_node()
            except AOSError:
                _LOG.warning("Node cleanup failed after lifecycle setup error")
            raise

    @property
    def lifecycle_snapshot(self):
        return self._lifecycle.snapshot()

    def get_current_state(self) -> LifecycleState:
        return LifecycleState(**self._lifecycle.current_state())

    def get_available_states(self) -> list[LifecycleState]:
        return [LifecycleState(**state) for state in self._lifecycle.available_states()]

    def get_available_transitions(self):
        return self._lifecycle.available_transitions()

    def get_transition_graph(self):
        return self._lifecycle.transition_graph()

    @property
    def last_transition_report(self):
        return self._lifecycle.last_transition_report()

    def take_transition_events(self):
        return self._lifecycle.take_transition_events()

    def _invoke_lifecycle_callback(self, state, previous):
        callback = getattr(self, _CALLBACKS[state["label"]])
        result = callback(LifecycleState(**previous))
        if inspect.isawaitable(result):
            if inspect.iscoroutine(result):
                result.close()
            raise TypeError("Lifecycle callback must be synchronous")
        return result

    def change_state_async(self, transition, *, drain_timeout=None) -> Future:
        """Queue a transition by public ID or label and return its report Future.

        The Rust core validates the transition when the Executor executes it.
        Cancelling the Future only cancels observation, not the queued transition.
        A wait timeout also leaves the transition and its callbacks running.
        """
        future = Future(
            lambda callback: self._schedule(callback, self._lifecycle_callback_group)
        )
        invocation = _LocalLifecycleInvocation(self, transition, drain_timeout, future)
        with self._lock:
            if self._closed or not self.context.ok():
                raise InvalidStateError("Node is closed")
            self._lifecycle_pending.add(invocation)
            self._schedule(invocation, self._lifecycle_callback_group)
        return future

    def trigger_configure_async(self, *, drain_timeout=None) -> Future:
        return self.change_state_async("configure", drain_timeout=drain_timeout)

    def trigger_cleanup_async(self, *, drain_timeout=None) -> Future:
        return self.change_state_async("cleanup", drain_timeout=drain_timeout)

    def trigger_activate_async(self, *, drain_timeout=None) -> Future:
        return self.change_state_async("activate", drain_timeout=drain_timeout)

    def trigger_deactivate_async(self, *, drain_timeout=None) -> Future:
        return self.change_state_async("deactivate", drain_timeout=drain_timeout)

    def trigger_shutdown_async(self, *, drain_timeout=None) -> Future:
        return self.change_state_async("shutdown", drain_timeout=drain_timeout)

    def on_configure(self, state: LifecycleState) -> TransitionCallbackReturn:
        return TransitionCallbackReturn.SUCCESS

    def on_cleanup(self, state: LifecycleState) -> TransitionCallbackReturn:
        return TransitionCallbackReturn.SUCCESS

    def on_activate(self, state: LifecycleState) -> TransitionCallbackReturn:
        return TransitionCallbackReturn.SUCCESS

    def on_deactivate(self, state: LifecycleState) -> TransitionCallbackReturn:
        return TransitionCallbackReturn.SUCCESS

    def on_shutdown(self, state: LifecycleState) -> TransitionCallbackReturn:
        return TransitionCallbackReturn.SUCCESS

    def on_error(self, state: LifecycleState) -> TransitionCallbackReturn:
        return TransitionCallbackReturn.SUCCESS

    def destroy_node(self):
        try:
            super().destroy_node()
        finally:
            with self._lock:
                pending = list(self._lifecycle_pending)
                self._lifecycle_pending.clear()
            for invocation in pending:
                invocation.close()


class _LocalLifecycleInvocation:
    def __init__(self, node, transition, drain_timeout, future):
        self._node = node
        self._transition = transition
        self._drain_timeout = drain_timeout
        self._future = future

    def __call__(self):
        try:
            result = self._node._lifecycle.execute(
                self._transition, self._node._invoke_lifecycle_callback,
                drain_timeout=self._drain_timeout,
            )
        except Exception as error:
            self._complete(error=error)
        else:
            self._complete(result=result)
        finally:
            with self._node._lock:
                self._node._lifecycle_pending.discard(self)
            self._node._wake_executor()

    def _complete(self, *, result=None, error=None):
        try:
            if error is not None:
                self._future.set_exception(error)
            else:
                self._future.set_result(result)
        except FutureStateError:
            pass

    def _cancel(self):
        self._complete(error=InvalidStateError("Executor could not submit lifecycle callback"))
        with self._node._lock:
            self._node._lifecycle_pending.discard(self)

    def close(self):
        self._complete(error=InvalidStateError(
            "Node closed; running lifecycle callbacks were not forcibly stopped"
        ))


class _LifecycleServices:
    def __init__(self, node):
        self._node = node
        self._native = node._native.enable_lifecycle_services()
        self.callback_group = node._lifecycle_callback_group

    def _take_callback(self):
        request = self._native.recv(0.0)

        def invoke():
            try:
                request.execute(self._node._invoke_lifecycle_callback)
            except AOSError:
                _LOG.warning("Lifecycle service response could not be delivered")

        return invoke

    def destroy(self):
        self._native.destroy()
        self._node._forget(self)
