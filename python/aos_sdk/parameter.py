"""Typed parameter values and callbacks; the Rust Node owns every committed value."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from enum import Enum
import copy
import inspect
import logging
from collections.abc import Mapping

from . import _native
from .exceptions import AOSError, InvalidArgumentError, ParameterError

_LOG = logging.getLogger("aos_sdk")


class ParameterType(str, Enum):
    NOT_SET = "not_set"
    BOOL = "bool"
    INTEGER = "integer"
    DOUBLE = "double"
    STRING = "string"
    BYTE_ARRAY = "byte_array"
    BOOL_ARRAY = "bool_array"
    INTEGER_ARRAY = "integer_array"
    DOUBLE_ARRAY = "double_array"
    STRING_ARRAY = "string_array"


@dataclass(frozen=True, init=False)
class Parameter:
    """A copied, core-validated value. Empty arrays require an explicit type."""

    name: str
    _wire: dict
    Type = ParameterType

    def __init__(self, name: str, type_=None, value=None):
        if not isinstance(name, str):
            raise InvalidArgumentError("parameter name must be a string")
        if isinstance(value, (bytes, bytearray)):
            value = list(value)
            if type_ is None:
                type_ = ParameterType.BYTE_ARRAY
        kind = type_.value if isinstance(type_, ParameterType) else type_
        wire = _native._parameter_value(value, kind)
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "_wire", wire)

    @property
    def value(self):
        return copy.deepcopy(self._wire.get("value"))

    @property
    def type_(self):
        return ParameterType(self._wire["type"])

    def get_parameter_value(self):
        """Return canonical JSON with an explicit ``type`` and optional ``value``."""
        return copy.deepcopy(self._wire)

    def to_dict(self):
        return {"name": self.name, "value": self.get_parameter_value()}

    @classmethod
    def _from_wire(cls, wire):
        return cls(wire["name"], value=wire["value"])


@dataclass(frozen=True)
class IntegerRange:
    from_value: int
    to_value: int
    step: int = 0


@dataclass(frozen=True)
class FloatingPointRange:
    from_value: float
    to_value: float
    step: float = 0.0


@dataclass(frozen=True)
class ParameterDescriptor:
    """Native constraints; allowed_states applies to later set/delete/load.

    The empty tuple is unrestricted. Lifecycle state labels are validated by
    Rust, including the Node kind and any state change during a validator.
    Declaration and startup overrides still establish the initial value.
    """

    parameter_type: ParameterType | str | None = None
    description: str = ""
    additional_constraints: str = ""
    read_only: bool = False
    dynamic_typing: bool = False
    integer_range: IntegerRange | None = None
    floating_point_range: FloatingPointRange | None = None
    allowed_states: tuple[str, ...] = ()

    def to_dict(self):
        # Enums derive from str; JSON serialization retains their wire value.
        return asdict(self)

    @classmethod
    def _from_wire(cls, wire):
        value = dict(wire)
        if value["parameter_type"] is not None:
            value["parameter_type"] = ParameterType(value["parameter_type"])
        if value["integer_range"] is not None:
            value["integer_range"] = IntegerRange(**value["integer_range"])
        if value["floating_point_range"] is not None:
            value["floating_point_range"] = FloatingPointRange(**value["floating_point_range"])
        value["allowed_states"] = tuple(value.get("allowed_states", ()))
        return cls(**value)


@dataclass(frozen=True)
class SetParametersResult:
    successful: bool
    reason: str = ""
    code: str | None = None
    version: int | None = None
    transaction_id: str | None = None
    event_published: bool = False
    event_publish_error: str | None = None

    @classmethod
    def _committed(cls, commit):
        return cls(
            successful=True, version=commit["version"],
            transaction_id=commit["transaction_id"],
            event_published=commit["event_published"],
            event_publish_error=commit["event_publish_error"],
        )

    @classmethod
    def _failed(cls, error):
        return cls(successful=False, code=error.code, reason=error.reason)


@dataclass(frozen=True)
class ListParametersResult:
    names: list[str]
    prefixes: list[str]


def _validation_error(reason):
    error = ParameterError(f"validation: {reason}")
    error.code = "validation"
    error.reason = reason
    return error


def _descriptor_wire(descriptor):
    if descriptor is None:
        return {}
    if isinstance(descriptor, ParameterDescriptor):
        return descriptor.to_dict()
    if isinstance(descriptor, Mapping):
        return dict(descriptor)
    raise InvalidArgumentError("descriptor must be a ParameterDescriptor or mapping")


def _parameters_wire(parameters):
    values = list(parameters)
    if not all(isinstance(value, Parameter) for value in values):
        raise InvalidArgumentError("parameters must contain Parameter objects")
    return [value.to_dict() for value in values]


def parameter_overrides_wire(overrides):
    if overrides is None:
        return None
    if isinstance(overrides, Mapping):
        return dict(overrides)
    parameters = _parameters_wire(overrides)
    result = {}
    for parameter in parameters:
        name = parameter["name"]
        if name in result:
            raise InvalidArgumentError(f"duplicate parameter override {name!r}")
        result[name] = parameter["value"]
    return result


class _ParameterServices:
    def __init__(self, node, native):
        self._node = node
        self._native = native
        self.callback_group = getattr(
            node, "_parameter_callback_group", node.default_callback_group,
        )

    def _take_callback(self):
        request = self._native.recv(0.0)

        def invoke():
            try:
                request.execute(self._node._validate_parameter_candidate)
            except AOSError:
                _LOG.warning("Parameter service response could not be delivered")

        return invoke

    def destroy(self):
        self._native.destroy()
        self._node._forget(self)


class ParameterNodeMixin:
    """Executor ownership only; reads and mutations always go through the core.

    Transactions expire after five steady seconds unless timeout_sec is given.
    Parameter events remain enabled when the six parameter services are disabled.
    """

    def _initialize_parameters(self, start_parameter_services):
        self._parameter_store = self._native.parameters()
        self._parameter_validators = []
        self._native.enable_parameter_events()
        if start_parameter_services:
            services = _ParameterServices(self, self._native.enable_parameter_services())
            self._track(services, services)

    def add_on_set_parameters_callback(self, callback):
        """Add a synchronous validator; the newest validator runs first.

        Return SetParametersResult. Validators must not perform business side
        effects: a later validator or optimistic commit can still reject a set.
        """
        if not callable(callback) or inspect.iscoroutinefunction(callback):
            raise InvalidArgumentError("a parameter validator must be a synchronous callable")
        with self._lock:
            self._parameter_validators.insert(0, callback)

    def remove_on_set_parameters_callback(self, callback):
        with self._lock:
            self._parameter_validators.remove(callback)

    def _validate_parameter_candidate(self, candidate):
        with self._lock:
            validators = list(self._parameter_validators)
        for callback in validators:
            try:
                # Each callback receives a fresh view. Editing a list or
                # returned array cannot alter the core transaction candidate.
                result = callback([Parameter._from_wire(value) for value in candidate])
                if inspect.isawaitable(result):
                    if inspect.iscoroutine(result):
                        result.close()
                    raise TypeError("parameter validators must be synchronous")
                if not isinstance(result, SetParametersResult):
                    raise TypeError("parameter validators must return SetParametersResult")
                if not isinstance(result.successful, bool) or not isinstance(result.reason, str):
                    raise TypeError("validator result requires a bool and a string reason")
            except Exception:
                _LOG.exception("Parameter validator failed")
                return {"successful": False, "reason": "parameter validator raised an exception"}
            if not result.successful:
                return {"successful": False, "reason": result.reason}
        return {"successful": True, "reason": ""}

    def _commit_parameter_transaction(self, transaction):
        try:
            result = self._validate_parameter_candidate(transaction.candidate)
            if not result["successful"]:
                raise _validation_error(result["reason"])
            return transaction.commit()
        finally:
            transaction.abort()

    def declare_parameter(
        self, name, value=None, descriptor=None, *, ignore_override=False, timeout_sec=None,
    ):
        """Declare a value after applying and validating its startup override."""
        if not isinstance(ignore_override, bool):
            raise InvalidArgumentError("ignore_override must be a bool")
        if isinstance(value, ParameterType):
            descriptor = _descriptor_wire(descriptor)
            descriptor["parameter_type"] = value.value
            value = None
        parameter = Parameter(name, value=value)
        transaction = self._parameter_store.prepare_declare(
            name, parameter.get_parameter_value(), _descriptor_wire(descriptor),
            ignore_override=ignore_override, timeout_sec=timeout_sec,
        )
        candidate = transaction.candidate[0]
        self._commit_parameter_transaction(transaction)
        return Parameter._from_wire(candidate)

    def get_parameter(self, name):
        return self.get_parameters([name])[0]

    def get_parameters(self, names):
        names = list(names)
        return [
            Parameter(name, value=value)
            for name, value in zip(names, self._parameter_store.get(names))
        ]

    def get_parameter_types(self, names):
        return [
            ParameterType(kind) for kind in self._parameter_store.get_types(list(names))
        ]

    def has_parameter(self, name):
        try:
            self._parameter_store.describe([name])
        except ParameterError as error:
            if error.code == "not_declared":
                return False
            raise
        return True

    def describe_parameter(self, name):
        return self.describe_parameters([name])[0]

    def describe_parameters(self, names):
        return [
            ParameterDescriptor._from_wire(record["descriptor"])
            for record in self._parameter_store.describe(list(names))
        ]

    def list_parameters(self, prefixes=(), depth=0):
        return ListParametersResult(**self._parameter_store.list(list(prefixes), depth))

    def set_parameters_atomically(self, parameters, *, timeout_sec=None):
        """Validate once and commit all values, or return one structured failure."""
        wire = _parameters_wire(parameters)
        try:
            transaction = self._parameter_store.prepare_set(wire, timeout_sec=timeout_sec)
            return SetParametersResult._committed(self._commit_parameter_transaction(transaction))
        except ParameterError as error:
            return SetParametersResult._failed(error)

    def set_parameters(self, parameters, *, timeout_sec=None):
        """Apply each parameter independently, matching ROS non-atomic semantics."""
        return [
            self.set_parameters_atomically([parameter], timeout_sec=timeout_sec)
            for parameter in parameters
        ]

    def get_parameter_snapshot(self):
        """Include values, declaration defaults, descriptors, versions and sources."""
        return self._parameter_store.snapshot()

    def dump_parameters(self, path):
        """Atomically replace a snapshot file through the shared Rust writer.

        New Unix files are private (0600); replacements preserve permissions.
        File data is synced before replacement. Unsupported directory sync
        permits success with weaker crash durability. Other failures after
        replacement explicitly say that the destination was already replaced.
        Normal parameter updates do not write files.
        """
        self._parameter_store.dump(path)

    def load_parameters(self, path):
        """Validate and atomically apply a snapshot to already declared parameters."""
        try:
            transaction = self._parameter_store.prepare_load(path)
            return SetParametersResult._committed(self._commit_parameter_transaction(transaction))
        except ParameterError as error:
            return SetParametersResult._failed(error)
