# Upstream sources and adoption

The fixed upstream revisions are in `source-revisions.json`;
`source-map.json` records
what the current code actually uses. A reference alone is not a completed port.

| Source | Current use | Selected license |
| --- | --- | --- |
| Zenoh and zenoh-ext 1.10.1 | Fixed dependencies; `zenoh` is vendored with the local liveliness history ordering patch | Apache-2.0 |
| Hiroz d9f979a0404311de12f6884bafaa9b20600eb490 | Adapted data/liveliness key layouts, QoS configuration and Service query/reply configuration | Apache-2.0 |
| Hiroz Context/PyO3 layout | Ownership and build references; no copied Context or binding bodies | Apache-2.0 |
| rmw_zenoh fixed config | Reference for AOS-authored loopback peer/router configuration | Apache-2.0 |
| ROS 2 rcl/rclpy Jazzy | Translated Action state/cancel/result/callback behavior; naming and executor references | Apache-2.0 |
| rmw_zenoh fixed client | Ported GetResult maximum-millisecond query timeout policy | Apache-2.0 |

Hiroz's workspace declares Apache-2.0 OR MIT. The adapted portions select
Apache-2.0; its exact root LICENSE is retained in `licenses/hiroz-LICENSE`.
There is no root NOTICE file at the fixed commit. Modified target files carry
adaptation notices. Zenoh's license and NOTICE are also retained in `licenses/`.
The exact fixed-commit Zenoh license and NOTICE are included in its vendor
directory. See [the history ordering patch](zenoh-history-gate.md) for archive
hashes, source comparison, callback/cleanup semantics and regression gates.

`aos-core` has no Zenoh or Python dependency. Node ownership, JSON Interface
validation/hash, metadata and endpoint lifetimes live in the common Rust core.
Python bindings use PyO3 0.30.1 with the Python 3.11 ABI floor and release the
GIL around blocking native calls. Python business callbacks run in executors.

The Hiroz adaptations preserve domain/topic/type/hash separation, reliable
Block versus best-effort Drop congestion control, and transient-local
Advanced publisher caches and late-join history. AOS adds an `aos/` namespace,
JSON identities/payloads, explicit closure, bounded local queues, and protocol
error handling. Service calls send once; timing out stops the local wait and
does not cancel or retry remote business work. See each source-map entry for
fixed paths and line ranges. ROS CDR interoperability is not implemented.

## Configuration decisions

Zenoh `unstable` and `zenoh-ext/unstable` enable Advanced pub/sub. TCP is the
configured development transport. The checked-in peer configuration uses
loopback, gossip discovery, timestamps for cached history and background
connection/reconnection to the local Router. The Router listens on `[::]:7447`
and connects to remote Routers only through explicit endpoints. These topology
defaults follow `ros2/rmw_zenoh@dda52d6d9bc863b4b6c86a0b4fd1c6788f4a651e`;
the earlier 3000 ms fail-fast development default has been replaced. TLS peer
and Router federation profiles are described in [topology.md](../docs/platform/topology.md).

Transitive dependency licenses remain with their distributions; this file is
an implementation provenance record, not a complete dependency audit.

## Action behavior migrated from ROS 2

The common Rust ActionServer retains the original five endpoints and eight
legal transitions. It owns accepted goals, cancellation selection, feedback,
terminal result waiters and cache expiry. The Python facade schedules business
callbacks and does not own a second state machine. Native callback and state
translations retain OSRF copyright/license headers; complete fixed-source
licenses are in `licenses/`. Details are in
[`crates/aos-core/src/action/README.md`](https://github.com/Cmoon-cyl/aos/blob/v0.2.0/crates/aos-core/src/action/README.md).

The fixed rmw_zenoh implementation already handles long GetResult waits by
using the maximum u64 millisecond query timeout for that endpoint only. AOS
ports this policy directly. `timeout=None` means no application deadline;
the middleware has the same finite protocol bound as upstream. This does not
add a reply topic or repeated GetResult queries. CLI waits remain explicitly
finite, and local timeout/close does not call CancelGoal. Result retention is
a separate setting: core defaults to rcl's 10 seconds; Python defaults to
rclpy's 900 seconds, measured from terminal completion.
