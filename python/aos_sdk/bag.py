"""MCAP recording and observation-only playback, owned by the Rust runtime.

``stop()`` requests closure; ``wait(timeout)`` returns the worker's final report.
Confirm resource closure with resources_closed=True and cleanup_error=None.
The returned report can still contain a failed state and an error. A timeout
never claims that a control barrier or closure has completed. ``close()`` waits
and raises for a failed report. The caller retains ownership of its Context.
"""

from collections.abc import Mapping
from os import fspath

from . import _native
from .exceptions import BagError, InvalidArgumentError
from .qos import native_qos


def _qos(value):
    resolved = _native._resolve_qos(native_qos(value))
    return {key: resolved[key] for key in ("depth", "reliability", "durability")}


def _options(options):
    options = dict(options)
    if "qos" in options:
        if options["qos"] is None:
            del options["qos"]
        else:
            options["qos"] = _qos(options["qos"])
    for field in ("topic_qos", "channel_qos"):
        if field in options:
            if not isinstance(options[field], Mapping):
                raise InvalidArgumentError(f"{field} must be a mapping")
            options[field] = {name: _qos(value) for name, value in options[field].items()}
    return options


def _checked(report):
    status = report["status"]
    error = status.get("error") or status.get("cleanup_error")
    if error is not None or status["state"] == "failed" or not status["resources_closed"]:
        error = error or {
            "kind": "io",
            "message": "bag runtime or resource cleanup failed",
            "file_offset": None,
        }
        exception = BagError(error["message"])
        exception.kind = error["kind"]
        exception.file_offset = error.get("file_offset")
        exception.report = report
        raise exception
    return report


def inspect_bag(directory, *, allow_incomplete=False, limits=None):
    """Fully validate all shards offline and return Interface/QoS/time evidence.

    ``allow_incomplete`` admits an explicit incomplete manifest; it never
    bypasses SHA, CRC, Interface, or payload validation.
    """
    options = {"allow_incomplete": allow_incomplete}
    if limits is not None:
        options["limits"] = limits
    return _native._inspect_bag(fspath(directory), options)


def recover_bag(source, destination, *, storage=None):
    """Read validated chunks into a new directory, preserving the source.

    Recovery output always remains marked incomplete. Its report identifies
    source errors, retained messages, and unpaired records. Output failures
    raise BagError; a partial destination is retained for inspection.
    """
    options = {} if storage is None else {"storage": storage}
    return _native._recover_bag(fspath(source), fspath(destination), options)


class _BagHandle:
    @property
    def status(self):
        """An owned snapshot; observing it does not stop or change the runtime."""
        return self._handle.status

    def stop(self):
        """Request closure without waiting for workers or changing the Context."""
        self._handle.stop()

    def wait(self, timeout=5.0):
        """Wait for the worker report; inspect state/error and cleanup fields."""
        return self._handle.wait(timeout)

    def close(self, timeout=5.0):
        """Request closure, wait, and raise BagError if the final report failed."""
        self.stop()
        return _checked(self.wait(timeout))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        try:
            self.close()
        except Exception as cleanup_error:
            if exc_value is None:
                raise
            # Keep the caller's failure primary while retaining the cleanup
            # exception as structured evidence for an enclosing handler.
            exc_value.bag_cleanup_error = cleanup_error
            exc_value.add_note(f"Bag cleanup also failed: {cleanup_error}")
        return False


class BagRecorder(_BagHandle):
    """Start recording selected Topics or ``all_topics=True`` to a new directory.

    Discovery is asynchronous: a constructed handle does not promise that any
    channel is active. Inspect ``status["active_channels"]`` and ``unresolved``.
    Additional keyword options are validated Rust RecordOptions fields, such
    as queue_messages, queue_bytes, storage, node, exclude, and topic_qos.
    """

    def __init__(self, context, directory, *, topics=(), all_topics=False, **options):
        options = _options(options)
        options.update(directory=fspath(directory), topics=list(topics), all_topics=all_topics)
        self._handle = _native._BagRecorder(context, options)


class BagPlayer(_BagHandle):
    """Validate a bag and prepare a Ready player; call ``start()`` explicitly.

    Choose a non-root replay namespace or explicitly set original_names=True.
    pause/seek succeed only after the native dispatch barrier. Already submitted
    transport messages cannot be recalled. seek uses exact integer nanoseconds
    since the recording anchor. All blocking native operations release the GIL.
    """

    def __init__(self, context, directory, *, namespace=None, original_names=False, **options):
        options = _options(options)
        options.update(namespace=namespace, original_names=original_names)
        self._handle = _native._BagPlayer(context, fspath(directory), options)

    def start(self):
        return self._handle.start()

    def pause(self):
        return self._handle.pause()

    def resume(self):
        return self._handle.resume()

    def seek(self, offset_ns):
        return self._handle.seek(offset_ns)

    def set_rate(self, rate):
        return self._handle.set_rate(rate)


__all__ = ["BagRecorder", "BagPlayer", "inspect_bag", "recover_bag"]
