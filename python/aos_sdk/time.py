"""Python views of the shared Rust clocks, timers and rates."""

from dataclasses import dataclass
from enum import Enum
import inspect
import weakref

from . import _native
from .exceptions import InvalidArgumentError, InvalidStateError, WaitTimeoutError


class ClockType(str, Enum):
    STEADY = "steady"
    SYSTEM = "system"
    ROS = "ros"


@dataclass(frozen=True)
class TimePoint:
    """A Rust clock sample; zero and uninitialized remain distinguishable."""

    nanoseconds: int
    clock_type: ClockType
    initialized: bool
    use_sim_time: bool

    def seconds_nanoseconds(self):
        return divmod(self.nanoseconds, 1_000_000_000)


@dataclass(frozen=True)
class TimerInfo:
    """Expected and actual take times, in the Timer clock's nanoseconds."""

    expected_call_time: int
    actual_call_time: int


class Clock:
    """A shared native clock, independent of Python scheduling.

    Standalone clocks default to steady. Pass ``context`` to close the clock
    on Context shutdown, or use ``node.clock`` for the Node's default ROS clock.
    ROS clocks use system time until simulation is enabled.
    """

    def __init__(self, clock_type=ClockType.STEADY, *, context=None):
        if not isinstance(clock_type, str):
            raise InvalidArgumentError("clock_type must be steady, system or ros")
        self._native = _native._Clock(clock_type, context=context)
        self._owner_ref = None

    @classmethod
    def _from_native(cls, native, owner=None):
        clock = cls.__new__(cls)
        clock._native = native
        clock._owner_ref = None if owner is None else weakref.ref(owner)
        return clock

    @property
    def clock_type(self):
        return self.now().clock_type

    def now(self) -> TimePoint:
        value = self._native.now()
        value["clock_type"] = ClockType(value["clock_type"])
        return TimePoint(**value)

    def set_use_sim_time(self, enabled: bool):
        # Enabling a Node clock must also create that Node's /clock subscription.
        owner = None if self._owner_ref is None else self._owner_ref()
        if owner is not None:
            owner.set_use_sim_time(enabled)
        else:
            self._native.set_use_sim_time(enabled)

    def update_ros_time(self, nanoseconds: int):
        """Supply one non-negative ROS time sample to the native clock.

        Nodes normally receive samples through ``clock_interface()`` on /clock.
        Direct samples are useful for explicit simulation and replay control.
        """
        self._native.update_ros_time(nanoseconds)

    def take_jump_events(self):
        """Drain native diagnostics, with consecutive forward samples coalesced.

        Each forward record spans its first before to its latest after.
        Initialization, backward jumps and clock source changes split records.
        Sequence numbers count records; gaps expose buffer eviction.
        """
        return self._native.take_jump_events()

    @property
    def forward_update_count(self) -> int:
        """Count positive ROS time samples while simulation is enabled."""
        return self._native.forward_update_count

    def destroy(self):
        self._native.destroy()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        try:
            self.destroy()
        except InvalidStateError:
            pass


def native_clock(clock):
    if clock is None:
        return None
    if not isinstance(clock, Clock):
        raise InvalidArgumentError("clock must be a Clock or None")
    return clock._native


def timer_callback_takes_info(callback):
    try:
        signature = inspect.signature(callback)
    except (TypeError, ValueError):
        return False
    try:
        signature.bind()
    except TypeError:
        try:
            signature.bind(None)
        except TypeError as error:
            raise InvalidArgumentError(
                "Timer callback must accept no arguments or one TimerInfo"
            ) from error
        return True
    return False


class _TimerCallback:
    """Own one native lease across executor submission and user invocation."""

    def __init__(self, lease, callback, takes_info):
        self._lease = lease
        self._callback = callback
        self._takes_info = takes_info

    def __call__(self):
        try:
            if not self._lease.is_valid():
                return
            result = (
                self._callback(TimerInfo(**self._lease.info))
                if self._takes_info else self._callback()
            )
            if inspect.isawaitable(result):
                if inspect.iscoroutine(result):
                    result.close()
                raise InvalidArgumentError("Timer callback must be synchronous")
        finally:
            self._lease.finish()

    def _cancel(self):
        self._lease.finish()


class Timer:
    """An Executor callback driven by a native Timer.

    Create with ``node.create_timer``. A callback accepts either no arguments or
    one TimerInfo. Missed periods coalesce; even a ReentrantCallbackGroup cannot
    run two callbacks from this Timer at once.
    """

    def __init__(self, node, native, callback, callback_group, takes_info, clock):
        self._node = node
        self._native = native
        self.callback = callback
        self.callback_group = callback_group
        self._takes_info = takes_info
        self.clock = clock

    @property
    def timer_period_ns(self):
        return self._native.timer_period_ns

    def _take_callback(self):
        lease = self._native.take_ready()
        if lease is None:
            raise WaitTimeoutError("timer is not ready")
        return _TimerCallback(lease, self.callback, self._takes_info)

    def is_ready(self):
        return self._native.is_ready()

    def is_canceled(self):
        return self._native.is_canceled()

    def cancel(self):
        self._native.cancel()
        self._node._wake_executor()

    def reset(self):
        self._native.reset()
        self._node._wake_executor()

    def time_until_next_call(self):
        """Return nanoseconds until ready, or None when canceled/uninitialized."""
        return self._native.time_until_next_call()

    def destroy(self):
        self._native.destroy()
        self._node._forget(self)
        self._node._wake_executor()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        try:
            self.destroy()
        except InvalidStateError:
            pass


class Rate:
    """A native periodic wait, created with ``node.create_rate``.

    ``sleep`` releases the GIL and needs no Executor. A paused/uninitialized
    simulated clock does not advance it; Node/Context shutdown wakes the wait
    with InvalidStateError. This is a scheduling target, not a real-time promise.
    """

    def __init__(self, node, native, clock):
        self._node = node
        self._native = native
        self.clock = clock

    def sleep(self):
        self._native.sleep()

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
