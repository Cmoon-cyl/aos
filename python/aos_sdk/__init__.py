"""AOS SDK package; the native extension runs inside this Python process."""

from ._native import (
    Context, Interface, Publisher, Subscription, __version__, clock_interface,
    parameter_event_interface, parameter_service_interface,
    lifecycle_event_interface, lifecycle_service_interface,
    compile_security_policy, client_security_config, router_security_config,
)
from .exceptions import (
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
from .callback_groups import (
    CallbackGroup, MutuallyExclusiveCallbackGroup, ReentrantCallbackGroup,
)
from .executors import Executor, Future, MultiThreadedExecutor, SingleThreadedExecutor
from .node import Client, Node, Service
from .lifecycle import (
    LifecycleNode, LifecycleState, LifecycleTransition, TransitionCallbackReturn,
)
from .parameter import (
    FloatingPointRange, IntegerRange, ListParametersResult, Parameter,
    ParameterDescriptor, ParameterType, SetParametersResult,
)
from .packages import (
    PackageIndex, get_package_prefix, get_package_share_directory,
    get_package_resource,
)
from .qos import DurabilityPolicy, HistoryPolicy, QoSProfile, ReliabilityPolicy
from .time import Clock, ClockType, Rate, TimePoint, Timer, TimerInfo
from .action import (
    ActionClient, ActionServer, CancelResponse, ClientGoalHandle,
    GoalResponse, GoalStatus, ServerGoalHandle,
)
from .bag import BagPlayer, BagRecorder, inspect_bag, recover_bag


def spin(node, executor=None):
    """Run a Node until its Context or Executor shuts down."""
    own = executor is None
    executor = executor or SingleThreadedExecutor(context=node.context)
    executor.add_node(node)
    try:
        executor.spin()
    finally:
        if own:
            executor.shutdown()


def doctor_report(*, config=None, domain_id=None, security_profile=None, timeout=5.0):
    """Return bounded SDK diagnostics; see :mod:`aos_sdk.doctor` for scope."""
    # Keep `python -m aos_sdk.doctor` free of runpy's pre-import warning.
    from .doctor import doctor_report as report
    return report(config=config, domain_id=domain_id, security_profile=security_profile, timeout=timeout)


__all__ = [
    "__version__",
    "Context",
    "compile_security_policy",
    "client_security_config",
    "router_security_config",
    "Publisher",
    "Subscription",
    "Interface",
    "Node",
    "LifecycleNode",
    "LifecycleState",
    "LifecycleTransition",
    "TransitionCallbackReturn",
    "lifecycle_event_interface",
    "lifecycle_service_interface",
    "Client",
    "Service",
    "Clock",
    "ClockType",
    "TimePoint",
    "Timer",
    "TimerInfo",
    "Rate",
    "clock_interface",
    "parameter_event_interface",
    "parameter_service_interface",
    "Parameter",
    "ParameterType",
    "ParameterDescriptor",
    "IntegerRange",
    "FloatingPointRange",
    "ListParametersResult",
    "SetParametersResult",
    "PackageIndex",
    "get_package_prefix",
    "get_package_share_directory",
    "get_package_resource",
    "Executor",
    "SingleThreadedExecutor",
    "MultiThreadedExecutor",
    "CallbackGroup",
    "MutuallyExclusiveCallbackGroup",
    "ReentrantCallbackGroup",
    "Future",
    "QoSProfile",
    "HistoryPolicy",
    "ReliabilityPolicy",
    "DurabilityPolicy",
    "spin",
    "doctor_report",
    "ActionClient",
    "ActionServer",
    "ServerGoalHandle",
    "ClientGoalHandle",
    "GoalResponse",
    "CancelResponse",
    "GoalStatus",
    "AOSError",
    "BagError",
    "BagPlayer",
    "BagRecorder",
    "inspect_bag",
    "recover_bag",
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
