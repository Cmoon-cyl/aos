"""Exception classes raised by the AOS SDK.

The classes are defined by the native extension so the Rust binding can raise
them directly. Action outcomes such as rejected, ABORTED, CANCELED, and
UNKNOWN are protocol fields on the result, not exceptions.
"""

from ._native import (
    AOSError,
    BagError,
    CommunicationError,
    InactiveError,
    InvalidArgumentError,
    ResourceLimitError,
    InvalidStateError,
    NotFoundError,
    NoResponseError,
    QueueOverflowError,
    ParameterError,
    ServiceError,
    WaitTimeoutError,
)

__all__ = [
    "AOSError",
    "BagError",
    "CommunicationError",
    "InactiveError",
    "InvalidArgumentError",
    "ResourceLimitError",
    "InvalidStateError",
    "NotFoundError",
    "NoResponseError",
    "QueueOverflowError",
    "ParameterError",
    "ServiceError",
    "WaitTimeoutError",
]
