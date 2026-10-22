# AOS core: typed nodes and communication

`Context`, `Publisher`, and `Subscription` are owned, `Send + Sync + 'static`
Rust handles suitable for PyO3 storage. The crate owns JSON Schema validation and interface hashing. It does not
depend on Zenoh or a Python runtime.

The adapter opens a process-local session and implements `transport::Transport`,
`PublisherBackend`, and `SubscriptionBackend`. SDK/CLI assembly passes that
session to `Context::new`. A session belongs to one Context ownership tree;
clone the Context to share it within a process. Network configuration, domain
isolation and middleware key construction remain in the adapter.

## Typed Node API

`Node::new(context, name, namespace, NodeOptions)` creates a logical node and
its private `get_node_card` and `get_type_description` services. Relative,
absolute and `~/private` names expand in core. Node names and namespaces use
ASCII identifier segments; `{node}`, `{ns}` and `{namespace}` substitutions are
supported. Graph entries use a unique node ID in addition to its user-facing
name. `Context::graph`, `get_node_card` and `get_type_description` let a client
inspect a running provider without a preinstalled business interface.

`Interface::from_json` and `Interface::load` accept closed definitions:

```json
{
  "name": "example/srv/Add",
  "kind": "srv",
  "request": {
    "type": "object",
    "properties": {"a": {"type": "integer"}, "b": {"type": "integer"}},
    "required": ["a", "b"],
    "additionalProperties": false
  },
  "response": {
    "type": "object",
    "properties": {"sum": {"type": "integer"}},
    "required": ["sum"],
    "additionalProperties": false
  }
}
```

`msg` definitions have `message`; `srv` definitions have `request`/`response`;
`action` definitions have `goal`/`result`/`feedback`. Validation uses JSON Schema
2020-12. References must be local fragments; package/remote references must be
resolved before loading. An action's result schema must have a valid top-level
`default`. Action execution uses the same validated interface and generated wrappers. The interface hash is lower-case SHA-256 of RFC 8785 canonical
JSON. Interface integer literals must be exactly representable by IEEE-754 so
canonicalization cannot silently merge distinct schema bounds. This restriction
does not truncate business payload integers.

Node's `create_publisher`/`create_subscription` return typed JSON handles with
`publish`/`recv_timeout`; both sides validate. QoS supports keep-last with a
positive `depth`, reliable/best-effort and volatile/transient-local. History
storage and delivery are the adapter's responsibility. A publisher can wait for
matching subscriptions using `wait_for_subscribers` before its first publish.

`publish_serialized` and `recv_serialized_timeout` also validate the complete
message schema while preserving the original JSON bytes. Receive returns
`Sample<Vec<u8>>` with the adapter's original receive/source metadata. Publisher
lifecycle leases still apply; managed subscriptions must use `take_message`
and cannot use the serialized receive method to bypass callback draining.

For recorders and replay tools, `create_action_observation_publisher` and
`create_action_observation_subscription` accept a complete Action Interface,
the Action base name, and `ActionObservationKind::Feedback` or `Status`.
Core derives the message type and remaps the base before adding its fixed
suffix. These methods create only Topic handles and never submit, retrieve,
cancel, or update a provider goal.

`create_client` returns a synchronous `call(request, timeout)` handle. Native
bindings must release the GIL and adapt it to their Future/Executor. Closing
the client or node cancels the local wait and releases transport query state;
it does not cancel provider work and never retries a request. `create_service`
returns `Service`, whose `recv_timeout` yields an owned `ServiceRequest` with
`request`, `reply` and `reply_error`. Core validates request and response data.
An invalid incoming request gets an error reply before executor dispatch;
executors should catch that validation error and continue serving. A request
settles once. User callbacks run only after receive returns to the executor.

`NodeOptions` stores display name, summary and tags. `EndpointOptions` stores
summary, optional contract version and optional side effects (`read_only`,
`write`, `external_effect`). Omitted side effects remain null. NodeCard is
computed from current live resources and includes their interface definitions.
The two built-in description services run on Rust worker threads so they do not
wait for business callbacks. These threads never execute application code.

Nodes own entity lifetimes. Destroying a Node or dropping its last clone closes
its entities, removes graph declarations and invalidates surviving handles.
Closing/dropping an entity removes only that entity; Context remains usable.
The process-local Context shutdown remains the final session close boundary.

## Action API

`ActionServer::new` combines three services and two topics and registers the
original action in Graph/NodeCard. Its `take_goal_request` and
`take_cancel_request` return owned requests for the language executor; accepted
`ServerGoalHandle`s expose execute, feedback, terminal state and finish methods.
The provider's Rust state is authoritative. GetResult request handles wait in
Rust without occupying business or cancellation workers.

`ActionClient` sends goals, reads results and cancels using the same five
endpoints. `ClientGoalHandle` contains the UUID, acceptance/stamp and observed
status; it does not own another state machine. Client close interrupts its
local waits and does not cancel remote business execution. Retention defaults
are Rust 10 seconds and Python 900 seconds, following their respective fixed
Jazzy source layers. See [the Action source and behavior record](src/action/README.md)
for exact adoption, callback order, exception behavior and validation scope.

## Byte-level echo example

This compiles against the core API. The caller supplies the real adapter; it
is not a standalone network example.

```no_run
use aos_core::{Context, Result, transport::Transport};
use std::time::Duration;

// Provider process: construct its own adapter/session before calling this.
fn echo_once(transport: impl Transport) -> Result<()> {
    let context = Context::new(transport);
    let request = context.create_subscription("/echo/request")?;
    let response = context.create_publisher("/echo/response")?;
    let result = (|| {
        let bytes = request.recv_timeout(Duration::from_secs(5))?;
        response.publish(&bytes)
    })();
    // Attempt cleanup even if receive/publish failed.
    let cleanup = context.shutdown();
    result.and(cleanup)
}

// Client process: create its reply subscription before sending.
fn call_echo(transport: impl Transport, bytes: &[u8]) -> Result<Vec<u8>> {
    let context = Context::new(transport);
    let response = context.create_subscription("/echo/response")?;
    let request = context.create_publisher("/echo/request")?;
    let result = (|| {
        request.publish(bytes)?;
        response.recv_timeout(Duration::from_secs(5))
    })();
    let cleanup = context.shutdown();
    match (result, cleanup) {
        (Err(error), _) | (Ok(_), Err(error)) => Err(error),
        (Ok(bytes), Ok(())) => Ok(bytes),
    }
}
```

I's cross-process harness must establish provider/subscriber readiness before
the client publishes: P0 delivery is volatile. These two fixed topic names are
for a single echo exchange, without concurrent-client correlation. Bytes are
opaque, including empty/non-UTF-8 data. These raw Context methods remain available for the P0 compatibility probe;
applications use Node methods for JSON typesupport, Graph, QoS and Service.
P0 accepts only absolute topic names whose slash-separated segments start with
an ASCII letter or underscore and contain ASCII letters, digits or underscores.
The adapter maps these AOS names to keys; callers do not supply middleware keys.

## Ownership, shutdown and races

| Operation | Contract |
| --- | --- |
| Drop a Context clone | Surviving clones/entities keep the session alive. |
| Drop the last owner | Attempt session close; emit cleanup failures to stderr. |
| `Context::shutdown()` | Atomically stop new work, then close the backend outside locks. Invalidate all entities and wake all pending receives. |
| Entity `close()` / Drop | Release only that entity; a subscription close wakes its receivers. Other entities remain usable. |
| Repeated/concurrent close or shutdown | Return `Error::Closed`. Only the winning caller receives the cleanup result. `Closed` means closing has begun, not that another caller has finished cleanup. |
| Backend cleanup failure | Return the error to the explicit caller; the handle stays closed and is never reopened or retried. Backend RAII still owns resource release. |
| Declaration races with shutdown | The backend must reject or invalidate the declaration; core checks again and drops any losing handle. |
| Publish races with close | An operation already in flight may have submitted its bytes. No retry and no rollback/delivery acknowledgement is implied. |
| Receive races with close | Backend wakes the wait; core rejects data when it observes closure before returning. A completed call can naturally precede concurrent shutdown. |

`Error::InvalidArgument`, `Validation`, `Unsupported`, `Remote`, `Timeout`,
`Closed`, and `Transport` remain distinct.
Transport diagnostics are retained. A zero receive timeout is a nonblocking
poll. Receiving does not invoke user code; application dispatch happens after
the call returns, outside shared locks. The Python binding must release the GIL during blocking
receive/shutdown/close and invoke Python callbacks through the executor, not
from middleware callbacks.

The transport adapter must enforce the concurrency/cleanup requirements documented in
`src/transport.rs`, including waking **all** blocked receivers when an entity
or session closes. Core atomics prevent new calls after observed closure, but
cannot interrupt an arbitrary backend by themselves. Adapter-specific bounded
close and cancellation tests are required for implementations of this trait. Drop
is a fallback; callers needing a cleanup result must explicitly close/shutdown.
The illustrative echo functions prefer the operation error if both operation
and cleanup fail; applications can record both results independently.

## Upstream relationship

Context ownership reference:
[Hiroz context.rs at d9f979a](https://github.com/ZettaScaleLabs/hiroz/blob/d9f979a0404311de12f6884bafaa9b20600eb490/crates/hiroz/src/context.rs).
The inspected `ZContext` owns an `Arc<Session>`, clones session ownership into
nodes and explicitly closes that session on `shutdown` (lines 659–746 in the
fixed source). This implementation follows that ownership responsibility.
It is original AOS code; no Hiroz source or test body was copied or translated.
Unlike Hiroz's concrete Zenoh field, AOS injects a core-defined transport to
preserve the required dependency direction. The byte-level example creates its two topic handles directly. Typed applications
create Nodes and entities on the same Context transport boundary. Core adds explicit terminal
closure checks and typed errors for the binding boundary.

Name expansion independently implements the specified ROS 2 Jazzy behavior
from [`rcl/expand_topic_name.c` at 18d96c9c2b98e616324ae9a2c7701dd0b0886ff0](https://github.com/ros2/rcl/blob/18d96c9c2b98e616324ae9a2c7701dd0b0886ff0/rcl/src/rcl/expand_topic_name.c).
This original Rust implementation supports built-in substitutions and reports
AOS errors; it does not expose rcl's custom substitution-map parameter. No
upstream naming source or test body was copied. Action source translations and
retained license headers are recorded separately above. NodeCard, dynamic JSON Schema and the
transport-neutral boundary are AOS adaptations.

## Verification boundary

`cargo test -p aos-core --offline` covers the existing 11 byte lifecycle tests
and core typed tests for names, canonical interface identity, schema validation,
action default validation, Topic delivery, NodeCard/type-description services,
Service success/error, and entity destruction. A test-only in-memory transport
isolates these core responsibilities; these tests do not prove Zenoh QoS,
network discovery, independent-process communication, or Python callbacks.
Those paths have separate adapter and SDK integration tests.

## Platform configuration and QoS

`resolve_node_config(name, namespace, options, &NodeConfigRequest)` is the
shared SDK/CLI resolver. `Node::new_with_config` invokes it;
`Node::new_resolved(context, resolved)` uses a previously validated snapshot
without rereading files. `Node::new` retains its old signature and defaults.
The request contains `program`, ordered JSON `config_files`, explicit
`environment`, `launch`, and `cli`; absent values do not erase lower layers.
The resolver never reads the process environment implicitly.

Node layers accept `name`, `namespace`, `display_name`, `summary`, `tags`,
`use_sim_time`, `parameters`, `remaps`, and `nodes`. A `nodes` key selects by the **declared**
name or declared fully qualified name, before deployment renaming. Node-scoped
rules win over generic rules in the same source; higher sources win first.
Remaps accept `{node?, kind?, from, to}`, with `kind` equal to
`topic|service|action|any`, and apply at most once. Action names are remapped
before their five channel names are generated. Direct channel remaps fail.
Configuration JSON rejects duplicate fields, unknown keys and wrong types.

`resolve_context_config(&ContextConfigRequest)` uses the same layer order for
`domain_id` and `zenoh_config`. It runs **before** an adapter opens. Context
files and Node files are separate, and either resolver rejects the other's
fields. Node environment keys are `AOS_NODE_NAME`, `AOS_NAMESPACE`, and
`AOS_USE_SIM_TIME`; Context keys are `AOS_DOMAIN_ID` and `AOS_ZENOH_CONFIG`.
`effective_value()`/`Node::effective_config()` expose value/source pairs;
the Zenoh path is exposed, never its contents or credentials.

`resolve_qos(&Value, default_preset, service)` accepts a depth integer, preset
name, strict override object, or null. `ResolvedQoS` preserves the expanded
profile, preset and each field's source. Node entity constructors pass only
the expanded profile to the transport and preserve provenance in NodeCard.
Graph carries the effective profile; legacy/missing Graph QoS remains unknown.
`qos_compatibility` is directional, explicitly `diagnostic_only`, and does not
claim the transport blocked traffic. Service reliability/durability are limited
to reliable/volatile; unsupported values fail before declaration.

`create_client_with_qos` and `create_service_with_qos` preserve default wrappers.
A legacy transport must reject a non-default service/client profile unless it
implements the new boundary. A persistent ClientBackend owns the completed
reply queue; a timeout never retries execution. Subscription, Service and
Client `queue_stats()` expose adapter observations when supported.
ActionServer/ActionClient `new_with_qos` append an `ActionQos` argument to their
old constructor parameters. Its five `ResolvedQoS` fields retain independent
presets and sources. Existing `new` constructors use fixed Jazzy defaults.

## Clock, Timer and Rate

All clock state and timer scheduling are in Rust. `Node::clock()` is a shared
ROS clock that normally reads system time. `set_use_sim_time(true)` installs an
independent Node clock worker on typed `/clock` (subject to topic remapping).
The shared `clock_interface()` accepts
`{"clock":{"sec":0,"nanosec":0}}`, including a received zero sample. Before
the first sample, `now().initialized` is false and business timers do not
advance; Service/Action control waits continue using steady time.
Deployments must provide one time source for each resolved clock topic. The
ROS-shaped sample has no authenticated publisher identity; this worker neither
elects a source nor claims to enforce exclusive ownership across publishers.

`Node::create_timer(period, clock, autostart)` and
`Node::create_rate(frequency, clock)` default to the Node clock. Standalone
`Clock::new`, `Timer::new`, and `Rate::new` also exist. Context-created clocks
and Node-created timers/rates register Rust shutdown hooks, so Node destruction
or Context shutdown wakes blocked Rate sleepers. A supplied external clock is
still owned by its caller; destroying one Node closes its timers, not that
external clock. Rate does not require an Executor to receive clock updates.

Executors call `Timer::take_ready()` and hold the returned `TimerCall` through
dispatch. They check `is_valid()` immediately before user code and finish/drop
the lease in every path, including rejected pool submission and exceptions.
At most one lease exists per Timer, even in a reentrant callback group.
Cancel/reset/close, source switches and backward jumps invalidate a queued
lease. An in-progress callback is not forcibly interrupted. Forward progress
wakes waiters without consuming the next timer period. After a slow callback
finishes, one overdue callback may run; taking that lease merges earlier
missed periods. Invalid leases cannot consume a new epoch's timer periods.
Repeated equal clock samples do not advance a timer. Consecutive forward
diagnostics are coalesced into one before/after interval, while
`forward_update_count()` counts individual advances. Initialization, backward
jumps and source switches separate these intervals.

The transition arithmetic is translated from fixed Jazzy
[`rcl/timer.c` at 18d96c9c2b98e616324ae9a2c7701dd0b0886ff0](https://github.com/ros2/rcl/blob/18d96c9c2b98e616324ae9a2c7701dd0b0886ff0/rcl/src/rcl/timer.c#L65-L125).
Lines 87–111 save `credit = old_next - old_now`, then use
`new_last = new_now - credit; new_next = new_last + period`.
The implementation follows this actual formula: its comment's description as
preserving the remaining duration is stronger than the arithmetic proves.
The source-change test records this distinction explicitly; the fixed
[`test_timer.cpp` tests](https://github.com/ros2/rcl/blob/18d96c9c2b98e616324ae9a2c7701dd0b0886ff0/rcl/test/rcl/test_timer.cpp#L649-L777)
check the switch and backward-jump behavior. The next-call advance and
missed-cycle coalescing follow timer.c lines 283–319. AOS replaces the upstream
zero-is-uninitialized sentinel with an explicit flag and adds executor leases.
These adaptations are covered by deterministic simulated-time tests and
in-memory Node clock/close tests; they do not claim middleware timing accuracy.

## Node parameters

`Node::parameters()` returns a shared Rust `ParameterStore`. The store owns
declarations, current values, defaults, descriptors, startup sources and one
monotonic version. SDKs retain handles and validator callbacks, not copies of
parameter state. Node destruction and Context shutdown close the store and
invalidate prepared transactions even if a store handle survives.

The scalar types are `bool`, signed 64-bit `integer`, finite `double`, and
`string`; arrays are `byte_array`, `bool_array`, `integer_array`, `double_array`,
and `string_array`. The canonical JSON form is
`{"type":"integer","value":3}`; `not_set` is `{"type":"not_set"}`.
`ParameterValue::from_json` also infers raw scalar and homogeneous nonempty
array values. Empty arrays need an explicit type. `from_json_with_type` is
the shared SDK/CLI parser for a raw value plus an optional type hint.

`declare(name, default_value, descriptor, ignore_override)` rejects an existing
name. Names must be nonempty; they do not use Topic-name grammar. A static
declaration infers its type from the default, or specifies `parameter_type`
with `NotSet` to remain uninitialized. Reading an uninitialized static value
fails. A `NotSet` startup override cannot replace a concrete static default;
declaration rejects it with `wrong_type` and leaves the store unchanged.
An explicitly uninitialized static declaration remains supported, and
`ignore_override=true` uses the application's default. Dynamic declarations allow later type changes; setting dynamic
`NotSet` deletes the declaration. Runtime set operations on undeclared or read-only
parameters fail, including same-value writes to read-only parameters.

Descriptors expose `description`, `additional_constraints`, `read_only`,
`dynamic_typing`, a type and at most one integer or floating-point range.
Inclusive endpoints are valid even when the step does not divide the range.
Zero step means continuous, reversed endpoints are normalized and a negative
floating step uses its magnitude. Float comparisons follow Jazzy rclpy's
relative tolerance `1e-6`, zero absolute tolerance and ties-to-even rounding.
`additional_constraints` is descriptive text. Nonempty lifecycle
`allowed_states` requires a LifecycleNode and constrains all later set/load
candidates, including unchanged load values and dynamic deletion. Declaration
initialization is not restricted. Preparation checks the allowed state and
commit rechecks the lifecycle epoch, including transitions back to the same
state. See [the lifecycle contract](../../docs/platform/lifecycle-core.md).

`prepare_declare` and `prepare_set` return a single-use
`ParameterTransaction`. A caller reads `candidate()`, runs its application
validator in its own Executor/caller thread, then consumes `commit()`. No
store lock is held during application validation. Abort/drop changes nothing.
Commit checks the owner, the monotonic deadline and `base_version()` again;
any intervening commit causes `conflict` with no automatic retry.
The default five-second lease is an **AOS adaptation**, configurable with the
`*_with_timeout(Duration)` variants. Empty set batches and duplicate names
are rejected; an empty load is a validated no-op. Type, range, validator, expiry and conflict failures change no
parameter and produce no event. Direct `declare`/`set_atomically` are Rust
conveniences for callers without application validators.

The `parameters` object merges each parameter by layer, retaining its
winning source. Overrides apply when an application declares that name; they
never auto-declare application parameters. For example:

```json
{
  "parameters": {
    "max_jobs": 4,
    "labels": {"type":"string_array","value":[]},
    "use_sim_time": true
  }
}
```

Every Node declares the mutable static boolean `use_sim_time` at version 1.
The old top-level config field and `parameters.use_sim_time` are one setting;
specifying both in the same layer fails. Runtime parameter commits,
`Node::set_use_sim_time` and the Node's `Clock::set_use_sim_time` all use this
same store. Enabling it creates the Node's clock subscription before committing
the value. SDK convenience setters should enter their own parameter API to
run application validators before the shared commit. Standalone clocks retain
their independent clock API.

`snapshot()` captures records and their store version under one lock.
`ParameterSnapshot::write/read` are explicit file operations; there is no
automatic persistence. Writes use `write_private_atomic`: a private temporary
file in the destination directory, file sync, rename, then directory sync.
New Unix files have mode 0600 and replacements preserve existing permission
bits; symlinks and read-only/non-regular destinations are rejected. Unsupported
directory sync permits success with weaker crash durability; the shared helper
reports `directory_synced=false`. Other post-rename errors explicitly say the
destination was already replaced. This is not a guarantee against power loss.

`snapshot.startup_config()` produces a reusable configuration, including
explicit `NotSet` values subject to the declaration rule above.
`prepare_load(&snapshot)` applies values atomically against target declarations.
Unchanged read-only values are accepted, changed read-only values fail, and
`NotSet` asserts an existing uninitialized declaration rather than deleting it.
Every candidate still participates in validators and `allowed_states` checks.
All-no-op loads preserve values, records and versions and publish no event.
Load never restores descriptors or auto-declares missing names. See
[parameter snapshot loading](../../docs/platform/parameter-load.md).

`Node::enable_parameter_services()` creates six ordinary private services:
`describe_parameters`, `get_parameter_types`, `get_parameters`,
`list_parameters`, `set_parameters`, and `set_parameters_atomically`. Rust
core enables them explicitly so legacy adapters can still create basic Nodes;
language SDKs may enable them by default. `ParameterServer::recv_timeout`
returns a request without running application code. The caller invokes
`request.execute(validator)` inside its Executor callback. There is no hidden
parameter callback worker. Closing the server permanently disables this
Node's service set; local parameter access remains available until Node close.
Already-running caller work is not forcibly interrupted.
`Node::enable_parameter_events()` independently enables its event publisher;
SDKs call it even with `start_parameter_services=False`, following Jazzy.
Enabling services also enables events. Closing only the service set leaves
the Node's event publisher active.

`ParameterServiceKind::interface()` supplies the shared schemas, and
`ParameterClient` supplies typed Rust/CLI calls. Query responses contain
`version`, `value`, and `error`; description values are full `ParameterRecord`s,
including default/source metadata. A structured query error is an AOS
extension. Set results contain `successful`, stable `code`, `reason`,
`version`, `transaction_id`, and event-publication status. Non-atomic
`set_parameters` processes each value in order, like Jazzy; atomic set
validates and commits the complete batch once.

The six services use the fixed Jazzy `parameters` profile; `/parameter_events`
uses `parameter_events`. Both are reliable, volatile, keep-last depth 1000.
An enabled event publisher publishes the committed `ParameterEvent` only after the
state write, in commit order. Events carry Node identity, transaction ID,
version, time and new/changed/deleted values. Following Jazzy, initializing an
unset value is `new`; deleted values carry `NotSet`. A rejected update has no
event. A publish failure does not roll back committed values: the successful
commit reports `event_published=false` and `event_publish_error`. Without an
enabled event publisher, the commit still returns its event but publishes nothing.
Volatile events are not a historical snapshot or a guaranteed catch-up stream.

Behavior is grounded in fixed Jazzy
[`rclpy/node.py`](https://github.com/ros2/rclpy/blob/4e607c09d624debc39a4ae5405680c06d87fac35/rclpy/rclpy/node.py),
[`parameter.py`](https://github.com/ros2/rclpy/blob/4e607c09d624debc39a4ae5405680c06d87fac35/rclpy/rclpy/parameter.py),
and [`parameter_service.py`](https://github.com/ros2/rclpy/blob/4e607c09d624debc39a4ae5405680c06d87fac35/rclpy/rclpy/parameter_service.py).
Core tests cover declarations, ranges, atomicity, optimistic concurrency,
shutdown, startup precedence, shared clock state, all six wire schemas,
Executor dispatch and event failure semantics. The in-memory service tests
do not claim real middleware queue depth, durability or cross-process proof.
