"""Callback admission, independent of the communication receive threads."""

from threading import Lock


class CallbackGroup:
    """An executor asks permission before taking and running a callback."""

    def beginning_execution(self) -> bool:
        raise NotImplementedError

    def ending_execution(self) -> None:
        raise NotImplementedError


class MutuallyExclusiveCallbackGroup(CallbackGroup):
    """At most one callback in this group runs at a time."""

    def __init__(self) -> None:
        self._lock = Lock()

    def beginning_execution(self) -> bool:
        return self._lock.acquire(blocking=False)

    def ending_execution(self) -> None:
        self._lock.release()


class ReentrantCallbackGroup(CallbackGroup):
    """Different callbacks, or repeated invocations, may overlap."""

    def beginning_execution(self) -> bool:
        return True

    def ending_execution(self) -> None:
        pass


class _ControlCallbackGroup(MutuallyExclusiveCallbackGroup):
    """LifecycleNode control work, with a fixed per-Executor capacity."""

    def __init__(self, kind, delegate=None):
        super().__init__()
        self._control_kind = kind
        self._delegate = delegate

    def beginning_execution(self):
        if self._delegate is not None:
            return self._delegate.beginning_execution()
        return super().beginning_execution()

    def ending_execution(self):
        if self._delegate is not None:
            return self._delegate.ending_execution()
        return super().ending_execution()
