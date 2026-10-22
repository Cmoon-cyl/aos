# Action source adoption and runtime contract

AOS keeps ROS 2's provider-owned goal state and five endpoints. Python owns
business callbacks; Rust owns accepted goals, transitions, deferred result
requests, cached results and expiration. `ActionServer::take_goal_request` and
`take_cancel_request` hand owned work to the language executor. The result
service has its own Rust worker and stores request handles while a goal runs,
so waiting for a result cannot consume the cancellation execution slot.

## Fixed upstream sources

| AOS file/behavior | Fixed source | Adoption and adaptations |
| --- | --- | --- |
| `state.rs`: eight legal transitions, invalid edges return errors | [rcl goal_state_machine.c](https://github.com/ros2/rcl/blob/18d96c9c2b98e616324ae9a2c7701dd0b0886ff0/rcl_action/src/rcl_action/goal_state_machine.c), [goal_handle.c](https://github.com/ros2/rcl/blob/18d96c9c2b98e616324ae9a2c7701dd0b0886ff0/rcl_action/src/rcl_action/goal_handle.c) | Transition table translated into a Rust match. UNKNOWN is never assigned by a failed transition. Original Apache-2.0 header retained. |
| `server.rs`: UUID/time cancellation selection | [rcl action_server.c, lines 816–905](https://github.com/ros2/rcl/blob/18d96c9c2b98e616324ae9a2c7701dd0b0886ff0/rcl_action/src/rcl_action/action_server.c#L816) | Both selection branches translated, including union selection and the exact-ID CANCELING case. Rust mutexes/owned handles replace C arrays and allocation. Original Apache-2.0 header retained. |
| `server.rs`: acceptance, callback order, cancellation replies, result completion | [rclpy action/server.py](https://github.com/ros2/rclpy/blob/4e607c09d624debc39a4ae5405680c06d87fac35/rclpy/rclpy/action/server.py#L289) | Provider ordering and callback decisions migrated to Rust handle operations and owned service requests; user callbacks remain in the Python executor. Result waiter replies run outside state locks. Original Apache-2.0 header retained. |
| `messages.rs`: GoalInfo, UUID/time, cancel codes and five wrappers | [rcl_interfaces action_msgs](https://github.com/ros2/rcl_interfaces/tree/7aa3caf43377ea6ad615bc1040832e2c7566bfbe/action_msgs) | Contract adaptation into generated JSON Schema, not a verbatim source translation. UUID is 32 lowercase hex characters; time preserves signed seconds and nanoseconds. |
| Retention defaults | [rcl action_server.c](https://github.com/ros2/rcl/blob/18d96c9c2b98e616324ae9a2c7701dd0b0886ff0/rcl_action/src/rcl_action/action_server.c#L303), [rclpy ActionServer](https://github.com/ros2/rclpy/blob/4e607c09d624debc39a4ae5405680c06d87fac35/rclpy/rclpy/action/server.py#L200) | Rust options default to rcl's 10 seconds; Python explicitly passes rclpy's 900 seconds. This is result retention after a terminal state, not a request wait deadline. |

No Action state resides in transport, CLI or Python. Graph contains one public
ActionServer/ActionClient entry with the original action name, type and hash;
the five component endpoints retain the `/_action/` names. NodeCard metadata
and type-description registration include the original interface.

## Execution and result rules

The SDK's default goal callback accepts, its accepted callback calls execute,
and its default cancel callback rejects. Core rejects a previously accepted
UUID before callback dispatch and checks again when accepting after the
callback. A rejected goal never enters the state map.

`ServerGoalHandle::execute` performs ACCEPTED → EXECUTING. For CANCELING it
keeps the state and lets the language executor schedule the callback, so the
callback can observe cancellation and do cleanup. `succeed`, `abort` and
`canceled` perform only the legal state transition. `finish` supplies the
returned business result and wakes all result waiters. If the callback returns
without a terminal transition, finish aborts an executing/canceling goal.

Callback exceptions use `finish(None)`, which supplies the interface's
validated default result. As in the fixed rclpy source, a callback that already
set a terminal state before throwing keeps that state; it cannot take an
illegal terminal → ABORTED edge. An invalid or non-JSON returned result instead
uses a cached result error: pending and subsequent GetResult calls receive an
explicit provider error, without a fabricated successful business result.

Cancel callback rejection removes candidates from `goals_canceling` but does
not rewrite rcl's return code. ERROR_NONE with an empty list therefore does not
mean a goal accepted cancellation. CANCELING remains active; it may later
succeed, abort, or finish canceled. Expired/unknown IDs return UNKNOWN with the
interface default result. Result timeout alone never cancels provider work.

`ActionClient::get_result_wait` accepts an optional application deadline. The
transport must implement its wait capability; the default trait reports
Unsupported. An adapter may follow a documented middleware protocol maximum,
as fixed rmw_zenoh does specifically for Action GetResult. This is distinct
from an application wait deadline and must be disclosed by the adapter. No
query is retried and no Action endpoint is added by core. Explicit finite
application waits remain available.

## Necessary validation

`cargo test -p aos-core --offline` includes six Action-focused tests:

- the exact eight-entry Jazzy transition table;
- wrapper identity and embedded local schema references;
- goal acceptance, feedback, successful result, repeated result and expiry;
- a pending result while cancellation is accepted or rejected;
- all four UUID/time selection predicates and cancellation before execute;
- duplicate UUID rejection, execution failure default, invalid-result error cache.

These isolate core behavior with an in-memory adapter. Native Python executor,
Zenoh transient-local late joining, process boundaries and CLI behavior are
verified by the integrated tests, not claimed from these unit tests.
