"""Python callback ownership and small facades around native Rust handles."""

from __future__ import annotations

from concurrent.futures import InvalidStateError as FutureStateError
import inspect
import json
import logging
from queue import Empty, SimpleQueue
import threading

from . import _native
from .callback_groups import MutuallyExclusiveCallbackGroup
from .exceptions import (
    AOSError, InactiveError, InvalidArgumentError, InvalidStateError, WaitTimeoutError,
)
from .executors import Future
from .qos import native_qos
from .parameter import Parameter, ParameterNodeMixin, parameter_overrides_wire
from .time import Clock, Rate, Timer, native_clock, timer_callback_takes_info

_LOG = logging.getLogger("aos_sdk")


def _check_callback(callback):
    if not callable(callback) or inspect.iscoroutinefunction(callback):
        raise InvalidArgumentError(
            "a callback must be a synchronous callable; use call_async for requests"
        )


class Client:
    """A typed Service client. Calls are never automatically retried."""

    def __init__(self, node, native, callback_group):
        self._node = node
        self._native = native
        self.callback_group = callback_group

    @property
    def queue_stats(self):
        """Local reply queue depth/evictions, or None when unavailable."""
        return self._native.queue_stats

    def wait_for_service(self, timeout_sec: float | None = None) -> bool:
        """Wait for a matching provider; return False on timeout.

        None waits until ready or closed. This sends no business request.
        """
        return self._native.wait_for_service(timeout_sec)

    def call(self, request, timeout: float = 10.0):
        """Wait for one response, releasing the GIL inside native code.

        Do not call this from a callback that must also serve the request on
        the same single-threaded executor.
        """
        return self._native.call(request, timeout)

    def call_async(self, request, timeout: float = 10.0) -> Future:
        """Return immediately; ``future.result(timeout)`` waits for the reply."""
        # Freeze the call's value before the caller can mutate its dict/list.
        # This is JSON conversion only; the Rust interface validates the data.
        try:
            request = json.loads(json.dumps(request, allow_nan=False, ensure_ascii=False))
        except (TypeError, ValueError, UnicodeError) as error:
            raise InvalidArgumentError(f"invalid JSON request: {error}") from error
        future = Future(
            lambda callback: self._node._schedule(callback, self.callback_group)
        )

        def wait():
            try:
                result = self._native.call(request, timeout)
            except Exception as error:
                try:
                    future.set_exception(error)
                except FutureStateError:
                    pass  # Local observation was cancelled while Rust waited.
            else:
                try:
                    future.set_result(result)
                except FutureStateError:
                    pass
            finally:
                self._node._wake_executor()

        threading.Thread(target=wait, name="aos-service-wait", daemon=True).start()
        return future

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


class Service:
    """A provider callback; each received request owns its response handle."""

    def __init__(self, node, native, callback, callback_group):
        self._node = node
        self._native = native
        self.callback = callback
        self.callback_group = callback_group

    @property
    def queue_stats(self):
        """Local pending-request queue depth/evictions, or None when unavailable."""
        return self._native.queue_stats

    def _take_callback(self):
        return _ServiceInvocation(self._native.recv(0.0), self.callback)

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


class _ServiceInvocation:
    def __init__(self, request, callback):
        self._request = request
        self._callback = callback

    def _reject(self):
        try:
            self._request.reply_error("HANDLER_ERROR")
        except AOSError:
            _LOG.warning("Service error response could not be delivered")

    def __call__(self):
        if self._request is None:
            return
        try:
            try:
                response = self._callback(self._request.request)
                if inspect.isawaitable(response):
                    if inspect.iscoroutine(response):
                        response.close()
                    raise TypeError("Service callback must return a JSON response")
            except Exception:
                # Keep detailed diagnostics local. The wire never contains the
                # exception message or traceback of arbitrary business code.
                _LOG.exception("Service callback failed")
                self._reject()
                return
            try:
                self._request.reply(response)
            except InvalidArgumentError:
                _LOG.exception("Service callback returned an invalid response")
                self._reject()
            except AOSError:
                # The response may have been submitted already; never retry it.
                _LOG.warning("Service response could not be delivered")
        finally:
            self._request = None

    def _cancel(self):
        # Drop the owned native request even if a submission exception retains
        # this invocation in its traceback. Rust releases its admission lease.
        self._request = None


class _SubscriptionCallback:
    def __init__(self, subscription, callback, group):
        self.subscription = subscription
        self.callback = callback
        self.callback_group = group

    def _take_callback(self):
        call = self.subscription.take_message(0.0)
        return _SubscriptionInvocation(call, self.callback)


class _SubscriptionInvocation:
    def __init__(self, call, callback):
        self._call = call
        self._callback = callback

    def __call__(self):
        try:
            if self._call.is_valid():
                result = self._callback(self._call.message)
                if inspect.isawaitable(result):
                    if inspect.iscoroutine(result):
                        result.close()
                    raise TypeError("Subscription callback must be synchronous")
        finally:
            self._call.finish()

    def _cancel(self):
        self._call.finish()


class Node(ParameterNodeMixin):
    """A named Rust Node with Python callback references.

    Explicitly pass a Context when several Nodes should share a session.
    When omitted, the Node opens and owns a Context with default settings.
    Name/namespace, config files, environment, launch_config and cli_overrides
    are resolved by the Rust core. ``remaps`` supplies program-level rules;
    ``effective_config`` and ``resolve_name_details()`` explain the outcome.
    Standard Context startup automatically reads ``AOS_PROCESS_CONFIG`` once.
    All Nodes in that Context use its retained deployment input, selected by
    their declared name/namespace before any rename. A NodeCard's optional
    ``process`` field is self-reported launch correlation, not authentication.
    Parameter overrides take effect when each parameter is declared. The six
    parameter services run through the Executor by default; set
    ``start_parameter_services=False`` to keep only local operations and events.
    """

    _managed_lifecycle = False

    def __init__(
        self, name: str, namespace: str = "/", context=None, *,
        display_name: str | None = None, summary: str = "", tags=(),
        config_files=(), remaps=(), use_sim_time=None,
        launch_config=None, cli_overrides=None,
        parameter_overrides=None, start_parameter_services=True,
    ):
        if not isinstance(start_parameter_services, bool):
            raise InvalidArgumentError("start_parameter_services must be a bool")
        overrides = parameter_overrides_wire(parameter_overrides)
        self.context = context if context is not None else _native.Context()
        self._owns_context = context is None
        try:
            self._native = _native._Node(
                self.context, name, namespace,
                display_name=display_name, summary=summary, tags=list(tags),
                config_files=config_files, remaps=remaps,
                use_sim_time=use_sim_time,
                launch_config=launch_config, cli_overrides=cli_overrides,
                parameter_overrides=overrides,
                lifecycle=self._managed_lifecycle,
            )
        except BaseException:
            if self._owns_context:
                try:
                    self.context.shutdown()
                except AOSError:
                    _LOG.warning("Context cleanup failed after Node creation error")
            raise
        self.default_callback_group = MutuallyExclusiveCallbackGroup()
        self._executor_ref = None
        self._resources = []
        self._callbacks = []
        self._lock = threading.Lock()
        self._ready = SimpleQueue()
        self._pending_ready = []
        self._closed = False
        self._clock = Clock._from_native(self._native.clock(), owner=self)
        try:
            self._initialize_parameters(start_parameter_services)
        except BaseException:
            try:
                self.destroy_node()
            except AOSError:
                _LOG.warning("Node cleanup failed after parameter setup error")
            raise

    @property
    def executor(self):
        return None if self._executor_ref is None else self._executor_ref()

    @property
    def name(self):
        return self._native.name

    @property
    def namespace(self):
        return self._native.namespace

    @property
    def fully_qualified_name(self):
        return self._native.fully_qualified_name

    @property
    def effective_config(self):
        """Return the core's effective values and their configuration sources."""
        return self._native.effective_config()

    @property
    def clock(self):
        return self._clock

    def get_clock(self):
        return self.clock

    def set_use_sim_time(self, enabled: bool):
        """Validate the shared parameter, then switch the native ROS clock."""
        if not isinstance(enabled, bool):
            raise InvalidArgumentError("use_sim_time must be a bool")
        transaction = self._parameter_store.prepare_set(
            [Parameter("use_sim_time", value=enabled).to_dict()],
        )
        self._commit_parameter_transaction(transaction)
        self._wake_executor()

    def get_name(self):
        return self.name

    def get_namespace(self):
        return self.namespace

    def get_fully_qualified_name(self):
        return self.fully_qualified_name

    def resolve_name(self, name, *, kind="any"):
        return self._native.resolve_name(name, kind=kind)

    def resolve_name_details(self, name, *, kind="any"):
        """Return the original name, final endpoint and applied rule source."""
        return self._native.resolve_name_details(name, kind=kind)

    def get_node_card(self):
        return self._native.node_card()

    def _track(self, resource, callback=None):
        with self._lock:
            closed = self._closed
            if not closed:
                self._resources.append(resource)
                if callback is not None:
                    self._callbacks.append(callback)
        if closed:
            try:
                resource.destroy()
            except InvalidStateError:
                pass
            raise InvalidStateError("node is destroyed")
        self._wake_executor()
        return resource

    def _forget(self, resource):
        with self._lock:
            if resource in self._resources:
                self._resources.remove(resource)
            self._callbacks[:] = [
                entry for entry in self._callbacks
                if entry is not resource
                and getattr(entry, "subscription", None) is not resource
            ]

    def create_publisher(
        self, interface, topic: str, qos_profile=None, *,
        summary="", side_effects=None, contract_version=None,
    ):
        return self._track(self._native.create_publisher(
            interface, topic, native_qos(qos_profile), summary=summary,
            side_effects=side_effects, contract_version=contract_version,
        ))

    def create_subscription(
        self, interface, topic: str, callback=None, qos_profile=None, *,
        callback_group=None, summary="", side_effects=None, contract_version=None,
    ):
        if callback is not None:
            _check_callback(callback)
        native = self._native.create_subscription(
            interface, topic, native_qos(qos_profile), summary=summary,
            side_effects=side_effects, contract_version=contract_version,
        )
        entry = None if callback is None else _SubscriptionCallback(
            native, callback, callback_group or self.default_callback_group
        )
        return self._track(native, entry)

    def create_client(
        self, interface, service: str, *, callback_group=None, qos_profile=None,
        summary="", side_effects=None, contract_version=None,
    ):
        native = self._native.create_client(
            interface, service, qos=native_qos(qos_profile),
            summary=summary, side_effects=side_effects,
            contract_version=contract_version,
        )
        return self._track(Client(
            self, native, callback_group or self.default_callback_group
        ))

    def create_service(
        self, interface, service: str, callback, *, callback_group=None, qos_profile=None,
        summary="", side_effects=None, contract_version=None,
    ):
        _check_callback(callback)
        native = self._native.create_service(
            interface, service, qos=native_qos(qos_profile),
            summary=summary, side_effects=side_effects,
            contract_version=contract_version,
        )
        entity = Service(
            self, native, callback, callback_group or self.default_callback_group
        )
        return self._track(entity, entity)

    def destroy_publisher(self, publisher):
        publisher.destroy()
        self._forget(publisher)

    def destroy_subscription(self, subscription):
        subscription.destroy()
        self._forget(subscription)

    def destroy_client(self, client):
        client.destroy()

    def destroy_service(self, service):
        service.destroy()

    def create_timer(
        self, timer_period_sec, callback, *, callback_group=None, clock=None, autostart=True,
    ):
        _check_callback(callback)
        takes_info = timer_callback_takes_info(callback)
        if not isinstance(autostart, bool):
            raise InvalidArgumentError("autostart must be a bool")
        native = self._native.create_timer(
            timer_period_sec, clock=native_clock(clock), autostart=autostart,
        )
        timer = Timer(
            self, native, callback, callback_group or self.default_callback_group,
            takes_info, self.clock if clock is None else clock,
        )
        return self._track(timer, timer)

    def create_rate(self, frequency, *, clock=None):
        native = self._native.create_rate(frequency, clock=native_clock(clock))
        return self._track(Rate(self, native, self.clock if clock is None else clock))

    def destroy_timer(self, timer):
        timer.destroy()

    def destroy_rate(self, rate):
        rate.destroy()

    def _wake_executor(self):
        executor = self.executor
        if executor is not None:
            executor.wake()

    def _schedule(self, callback, group):
        self._ready.put((callback, group))
        self._wake_executor()

    def _take_ready_callback(self, *, can_dispatch=None, priority_only=False):
        if self._closed or not self.context.ok():
            return None
        if not priority_only:
            while True:
                try:
                    self._pending_ready.append(self._ready.get_nowait())
                except Empty:
                    break
            for index, (callback, group) in enumerate(self._pending_ready):
                if can_dispatch is not None and not can_dispatch(group):
                    continue
                if group.beginning_execution():
                    del self._pending_ready[index]
                    return callback, group
        with self._lock:
            entries = list(self._callbacks)
        for entry in entries:
            group = entry.callback_group
            take = entry._take_callback
            if priority_only:
                take = getattr(entry, "_take_priority_callback", None)
                if take is None or getattr(group, "_control_kind", None) != "control":
                    continue
            if can_dispatch is not None and not can_dispatch(group):
                continue
            if not group.beginning_execution():
                continue
            try:
                callback = take()
            except WaitTimeoutError:
                group.ending_execution()
                continue
            except InactiveError:
                # The managed Service still exists; its core rejection worker
                # replies to new requests while business admission is closed.
                group.ending_execution()
                continue
            except InvalidStateError:
                group.ending_execution()
                self._forget(getattr(entry, "subscription", entry))
                continue
            except InvalidArgumentError:
                group.ending_execution()
                # Rust has already rejected a malformed Service request.
                # One bad sender must not stop other provider callbacks.
                _LOG.warning("Rejected incoming payload that failed its interface")
                continue
            except BaseException:
                group.ending_execution()
                raise
            with self._lock:
                if entry in self._callbacks:
                    self._callbacks.remove(entry)
                    self._callbacks.append(entry)
            return callback, group
        return None

    def destroy_node(self):
        try:
            self._native.destroy_node()
        finally:
            self._closed = True
            executor = self.executor
            if executor is not None:
                executor.remove_node(self)
            with self._lock:
                self._callbacks.clear()
                self._resources.clear()
            if self._owns_context and self.context.ok():
                self.context.shutdown()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        try:
            self.destroy_node()
        except InvalidStateError:
            pass
