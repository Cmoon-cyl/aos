"""ROS-style execution of Python callbacks outside Zenoh callback threads."""

from __future__ import annotations

import asyncio
import os
from concurrent.futures import Future as ConcurrentFuture, ThreadPoolExecutor
from queue import Empty, SimpleQueue
import threading
import time
from typing import TYPE_CHECKING
import weakref

from .exceptions import InvalidArgumentError, InvalidStateError

if TYPE_CHECKING:
    from .node import Node


class Future(ConcurrentFuture):
    """A call result whose user completion callbacks run in an Executor.

    ``result(timeout)`` may wait directly; the native wait releases the GIL.
    Use ``executor.spin_until_future_complete(future)`` when the same thread
    also serves callbacks. Awaiting is an optional asyncio adapter and requires
    a spinning executor for completion notifications. Cancelling this future
    only stops local observation, never work at the remote provider.
    """

    def __init__(self, schedule):
        super().__init__()
        self._schedule = schedule

    def result(self, timeout=None):
        if os.name != "nt":
            return super().result(timeout)
        # A Windows Condition's indefinite kernel wait does not wake for a
        # Python console signal. Re-enter Python at bounded intervals without
        # changing the caller's deadline or cancelling the remote operation.
        deadline = None if timeout is None else time.monotonic() + max(0, timeout)
        while True:
            remaining = None if deadline is None else max(0, deadline - time.monotonic())
            try:
                return super().result(0.05 if remaining is None else min(0.05, remaining))
            except TimeoutError:
                if self.done():
                    return super().result(0)  # Preserve a provider TimeoutError.
                if deadline is not None and time.monotonic() >= deadline:
                    raise

    def add_done_callback(self, fn):
        super().add_done_callback(lambda future: self._schedule(lambda: fn(future)))

    def __await__(self):
        return asyncio.wrap_future(self).__await__()


class Executor:
    """Poll ready Rust entities and schedule their Python callbacks.

    ``flush_logs=True`` explicitly drains captured native records to stderr on
    the thread calling spin, never a callback worker or transport thread.
    Context capture is enabled separately with ``context.enable_log_capture``.
    I/O may block; failures are counted in each Context's telemetry snapshot
    without stopping business callbacks. Shutdown performs no implicit I/O.
    """

    def __init__(self, *, context=None, flush_logs=False) -> None:
        if not isinstance(flush_logs, bool):
            raise InvalidArgumentError("flush_logs must be a bool")
        self.context = context
        self._flush_logs_on_spin = flush_logs
        self._nodes: list[Node] = []
        self._nodes_lock = threading.Lock()
        self._spin_lock = threading.Lock()
        self._shutdown = threading.Event()
        self._wake = threading.Event()
        self._errors = SimpleQueue()

    def add_node(self, node: Node) -> bool:
        with self._nodes_lock:
            if node in self._nodes:
                return False
            current = node.executor
            if current is not None and current is not self:
                raise InvalidStateError("node already belongs to another executor")
            if self._shutdown.is_set():
                raise InvalidStateError("executor is shut down")
            node._executor_ref = weakref.ref(self)
            self._nodes.append(node)
        self.wake()
        return True

    def remove_node(self, node: Node) -> None:
        with self._nodes_lock:
            if node in self._nodes:
                self._nodes.remove(node)
                node._executor_ref = None
        self.wake()

    def get_nodes(self) -> list[Node]:
        with self._nodes_lock:
            return list(self._nodes)

    def wake(self) -> None:
        self._wake.set()

    def _log_contexts(self):
        candidates = ([self.context] if self.context is not None else []) + [
            node.context for node in self.get_nodes()
        ]
        seen = set()
        for context in candidates:
            if id(context) not in seen:
                seen.add(id(context))
                yield context

    def flush_logs_to_stderr(self) -> int:
        """Explicitly drain associated Contexts; OSError reports an I/O failure."""
        return sum(context.flush_logs_to_stderr() for context in self._log_contexts())

    def _maybe_flush_logs(self):
        if self._flush_logs_on_spin:
            for context in self._log_contexts():
                try:
                    context.flush_logs_to_stderr()
                except OSError:
                    # Rust already counted the failed output and lost records.
                    # Avoid recursive logging to the same failing destination.
                    pass

    def _running(self) -> bool:
        return not self._shutdown.is_set() and (
            self.context is None or self.context.ok()
        )

    def _dispatch(self, callback, group) -> None:
        try:
            callback()
        finally:
            group.ending_execution()

    def _can_dispatch(self, group=None) -> bool:
        return True

    def spin_once(self, timeout_sec: float | None = None) -> None:
        """Execute or schedule at most one ready callback.

        Only one thread may call spin methods on an executor at a time.
        """
        if timeout_sec is not None and (
            not isinstance(timeout_sec, (int, float))
            or not 0 <= timeout_sec < float("inf")
        ):
            raise InvalidArgumentError("timeout_sec must be finite and non-negative")
        if not self._spin_lock.acquire(blocking=False):
            raise InvalidStateError("executor is already spinning")
        try:
            deadline = None if timeout_sec is None else time.monotonic() + timeout_sec
            while self._running():
                self._maybe_flush_logs()
                try:
                    error = self._errors.get_nowait()
                except Empty:
                    pass
                else:
                    raise error
                nodes = self.get_nodes()
                # Scan all LifecycleNodes for pending cancel decisions before
                # taking new parameter requests or goals from any Node.
                for priority_only in (True, False):
                    for node in nodes:
                        work = node._take_ready_callback(
                            can_dispatch=self._can_dispatch, priority_only=priority_only,
                        )
                        if work is not None:
                            self._dispatch(*work)
                            return
                if deadline is not None and time.monotonic() >= deadline:
                    return
                wait = 0.01
                if deadline is not None:
                    wait = min(wait, max(0, deadline - time.monotonic()))
                self._wake.wait(wait)
                self._wake.clear()
        finally:
            try:
                self._maybe_flush_logs()
            finally:
                self._spin_lock.release()

    def spin(self) -> None:
        while self._running():
            self.spin_once()

    def spin_until_future_complete(
        self, future: Future, timeout_sec: float | None = None
    ) -> None:
        deadline = None if timeout_sec is None else time.monotonic() + timeout_sec
        while self._running() and not future.done():
            remaining = None if deadline is None else deadline - time.monotonic()
            if remaining is not None and remaining <= 0:
                return
            self.spin_once(0.1 if remaining is None else min(remaining, 0.1))

    def shutdown(self, timeout_sec: float | None = None) -> bool:
        """Stop dispatching; running user callbacks are never forcibly stopped."""
        self._shutdown.set()
        self.wake()
        for node in self.get_nodes():
            self.remove_node(node)
        return True

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.shutdown()


class SingleThreadedExecutor(Executor):
    """Run callbacks on the thread calling spin."""


class MultiThreadedExecutor(Executor):
    """Run callbacks in a bounded pool, respecting their CallbackGroup.

    LifecycleNode also has two bounded control slots: one lifecycle transition
    and one parameter/Action control callback per Executor. They remain available when all
    business workers are occupied. Ordinary Nodes keep their existing pool.
    """

    def __init__(self, num_threads: int = 2, *, context=None, flush_logs=False) -> None:
        if isinstance(num_threads, bool) or not isinstance(num_threads, int) or num_threads < 1:
            raise InvalidArgumentError("num_threads must be a positive integer")
        super().__init__(context=context, flush_logs=flush_logs)
        self._pool = ThreadPoolExecutor(
            max_workers=num_threads, thread_name_prefix="aos-callback"
        )
        # ThreadPoolExecutor starts no threads until work is submitted. Creating
        # this pool up front makes shutdown close both admission paths together.
        self._control_pool = ThreadPoolExecutor(
            max_workers=2, thread_name_prefix="aos-control"
        )
        self._active = set()
        self._active_control = {"lifecycle": set(), "control": set()}
        self._active_lock = threading.Lock()
        self._submitting = 0
        self._num_threads = num_threads

    def _can_dispatch(self, group=None) -> bool:
        with self._active_lock:
            kind = getattr(group, "_control_kind", None)
            if kind in self._active_control:
                return len(self._active_control[kind]) < 1
            return len(self._active) < self._num_threads

    def _dispatch(self, callback, group) -> None:
        kind = getattr(group, "_control_kind", None)
        control = kind in self._active_control
        pool = self._control_pool if control else self._pool
        active = self._active_control[kind] if control else self._active
        submit_decided = threading.Event()
        admitted = False

        def run():
            # ThreadPoolExecutor can enqueue before thread creation raises.
            # User work starts only after submit returned and active tracks it.
            submit_decided.wait()
            if not admitted:
                return
            try:
                callback()
            except Exception as error:
                self._errors.put(error)
            finally:
                group.ending_execution()
                self.wake()

        with self._active_lock:
            stopping = self._shutdown.is_set()
            if not stopping:
                self._submitting += 1
        if stopping:
            # Shutdown may finish after spin took this callback but before
            # dispatch registered it. Release its lease without reopening work.
            try:
                cancel = getattr(callback, "_cancel", None)
                if cancel is not None:
                    cancel()
            finally:
                group.ending_execution()
            return
        try:
            try:
                pending = pool.submit(run)
            except BaseException:
                try:
                    cancel = getattr(callback, "_cancel", None)
                    if cancel is not None:
                        cancel()
                finally:
                    group.ending_execution()
                raise
            with self._active_lock:
                active.add(pending)

            def finished(done):
                with self._active_lock:
                    active.discard(done)
                self.wake()

            pending.add_done_callback(finished)
            admitted = True
        finally:
            submit_decided.set()
            with self._active_lock:
                self._submitting -= 1
            self.wake()

    def shutdown(self, timeout_sec: float | None = None) -> bool:
        super().shutdown(timeout_sec)
        deadline = None if timeout_sec is None else time.monotonic() + timeout_sec
        while True:
            with self._active_lock:
                pending = bool(
                    self._submitting or self._active or any(self._active_control.values())
                )
            if not pending:
                self._pool.shutdown(wait=True)
                self._control_pool.shutdown(wait=True)
                return True
            if deadline is not None and time.monotonic() >= deadline:
                self._pool.shutdown(wait=False)
                self._control_pool.shutdown(wait=False)
                return False
            self._wake.wait(0.01)
            self._wake.clear()
