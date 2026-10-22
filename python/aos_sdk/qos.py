"""Supported QoS presets and explicit overrides, resolved by the Rust core."""

from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass, field
from enum import Enum

from . import _native


class ReliabilityPolicy(str, Enum):
    RELIABLE = "reliable"
    BEST_EFFORT = "best_effort"


class DurabilityPolicy(str, Enum):
    VOLATILE = "volatile"
    TRANSIENT_LOCAL = "transient_local"


class HistoryPolicy(str, Enum):
    KEEP_LAST = "keep_last"


_UNSET = object()


@dataclass(frozen=True, init=False)
class QoSProfile:
    """A native-validated preset with optional policy overrides.

    ``QoSProfile(preset="sensor_data", depth=7)`` keeps the preset's
    best-effort/volatile policies and records depth as an explicit override.
    When ``preset`` is omitted, each entity supplies its own default preset:
    a depth-only Action status profile retains transient-local durability.
    Standalone policy properties preview the ordinary Topic default; inspect
    the entity's NodeCard for the final policies and sources.
    Unknown presets, policies and fields fail instead of being ignored.
    Topic/Service/Action entrypoints also accept a preset string, depth integer,
    mapping, or None for their documented default.
    """

    depth: int
    reliability: ReliabilityPolicy
    durability: DurabilityPolicy
    history: HistoryPolicy
    preset: str | None
    _request: tuple = field(repr=False, compare=False)
    _effective: dict = field(repr=False, compare=False)

    def __init__(
        self, depth=_UNSET, reliability=_UNSET, durability=_UNSET, history=_UNSET,
        *, preset=None, **unsupported,
    ):
        request = dict(unsupported)
        if preset is not None:
            request["preset"] = preset
        for key, value in (
            ("depth", depth), ("reliability", reliability),
            ("durability", durability), ("history", history),
        ):
            if value is not _UNSET:
                request[key] = value
        effective = _native._resolve_qos(request)
        object.__setattr__(self, "depth", effective["depth"])
        object.__setattr__(self, "reliability", ReliabilityPolicy(effective["reliability"]))
        object.__setattr__(self, "durability", DurabilityPolicy(effective["durability"]))
        object.__setattr__(self, "history", HistoryPolicy(effective["history"]))
        object.__setattr__(self, "preset", preset)
        object.__setattr__(self, "_request", tuple(request.items()))
        object.__setattr__(self, "_effective", effective)

    @property
    def effective_config(self):
        """Preview policies with the ordinary Topic default when no preset is set."""
        return deepcopy(self._effective)

    def _native_options(self):
        # Preserve the request, rather than treating preset-derived values as
        # explicit overrides when an entity resolves its own QoS.
        return dict(self._request)


def native_qos(value):
    if isinstance(value, QoSProfile):
        return value._native_options()
    if isinstance(value, Mapping):
        return dict(value)
    return value


def native_action_qos(goal_service, result_service, cancel_service, feedback, status):
    return {
        name: native_qos(value) for name, value in (
            ("goal_service", goal_service), ("result_service", result_service),
            ("cancel_service", cancel_service), ("feedback", feedback), ("status", status),
        )
    }
