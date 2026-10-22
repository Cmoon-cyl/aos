Start the fixed Zenoh router as described in `config/README.md`. From the
repository root, run the installed SDK in separate terminals:

```sh
python -m examples.fibonacci_server
python -m examples.fibonacci_client 8
python -m examples.fibonacci_client 30 --cancel-after 0.2
```

Pass `--config /path/to/session.json5` to both programs when using an explicit
router configuration. The caller prints the goal response, feedback, optional
cancel response, and final result as JSONL. The provider independently prints
its execution and completion observations.

`ActionServer(node, interface, name, execute_callback, ...)` defaults to
accepting goals, executing accepted goals, and rejecting cancellation. Supply
`goal_callback`, `handle_accepted_callback`, or `cancel_callback` to change
those choices. Goal/cancel callbacks return `GoalResponse` / `CancelResponse`.
The accepted callback receives a `ServerGoalHandle`; it may defer execution by
keeping the handle and calling `execute()` later.

An execute callback reads `goal.request`, publishes JSON feedback, explicitly
calls `succeed()`, `abort()`, or `canceled()`, then returns the result JSON.
It checks `is_cancel_requested` at safe points. Cancel acceptance moves the
goal into CANCELING; it does not forcibly stop Python or prove CANCELED.
Inspect `goals_canceling` to determine whether the requested UUID was accepted,
then read the final result's `status`.

User callbacks run through the Node's Executor. As in the fixed Jazzy source,
execution is a separate task and does not hold the Action control callback
group. Synchronous long callbacks still occupy worker threads: use a
`MultiThreadedExecutor` with spare workers for responsive cancellation. This
example uses four workers and a `ReentrantCallbackGroup`. GetResult waits run
inside Rust and do not occupy Python callback workers or callback groups.

`send_goal_async()` returns a Future resolving to `ClientGoalHandle`, including
rejected goal responses. Its `get_result_async(timeout=None)` waits without a
local application deadline by default; explicitly passing seconds only limits
this observation. As in fixed
[`rmw_zenoh`](https://github.com/ros2/rmw_zenoh/blob/dda52d6d9bc863b4b6c86a0b4fd1c6788f4a651e/rmw_zenoh_cpp/src/detail/rmw_client_data.cpp#L135),
Action GetResult uses the protocol's maximum `u64` millisecond timeout for
this case. That wire timeout is finite; it is not an infinite Zenoh query.
`cancel_goal_async(timeout=10)` sends an independent cancellation request.
Neither a result timeout nor local Future cancellation cancels the remote
goal. Destroying the client or Context wakes its pending native waits.

Provider result retention defaults to 900 seconds, matching Jazzy rclpy; set
the server's `result_timeout` to change it. Retention starts after completion
and is separate from a caller's waiting timeout. Unknown or expired goal IDs
return status UNKNOWN (0) and the interface's top-level result default.
Results do not survive provider restart.

If an execute callback returns while the goal is active, it becomes ABORTED.
An exception uses the interface result default and aborts only an active goal,
as in the fixed Jazzy source. If user code already selected a terminal state
before raising, that state is retained; terminal states are never rewritten.
An invalid result value is reported as an error to result waiters, rather than
being replaced with a successful default result.

The callback ordering follows the fixed
[Jazzy ActionServer](https://github.com/ros2/rclpy/blob/4e607c09d624debc39a4ae5405680c06d87fac35/rclpy/rclpy/action/server.py).
This is a Rust/JSON implementation of those semantics; it does not import
`rclpy` or a ROS C extension.
