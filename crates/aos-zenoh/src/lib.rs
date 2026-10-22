//! Zenoh implementations of aos-core communication abstractions.
//! Context and entity ownership stay in each application's process.
//!
//! [`ZenohTransport`] opens one Zenoh session and implements the aos-core P0
//! transport traits: typed Topics, QoS, liveliness discovery and Service queries.
//! Application callbacks and Action state remain in aos-core and its caller.
//!
//! Native Zenoh callbacks must return without synchronously waiting for a
//! declaration-backed result on any Session, including joining a thread
//! doing liveliness get/recv or waiting for a matching transition. The routing
//! FIFO may be held by this callback, an outer callback, or another callback
//! waiting on it. Such waits can time out and leave cached history ghosts. These
//! adapters only update owned state/queues; application code runs afterward.
//!
//! `SubscriptionBackend::recv_sample_timeout` returns the original payload
//! size and local steady/system times captured at the entry to the Advanced
//! subscriber delivery callback, before copying and the adapter's KEEP_LAST
//! queue. The same sample carries this metadata through queueing and typed
//! JSON decoding. Old `recv_timeout` consumers still receive only the payload.
//!
//! The scope is `adapter_delivery`, including history/recovery delivery after
//! Advanced pub/sub ordering. These times measure neither network ingress nor
//! a historical sample's original publication. Payload size excludes protocol
//! framing. Source evidence preserves optional publisher identity/sequence and
//! the raw Zenoh HLC value/clock ID separately. Zenoh 1.10.1 routers can add or
//! replace HLC timestamps, so their origin remains `router_or_publisher` and
//! `comparable` is false; presence alone cannot establish cross-machine delay.
//!
//! Bounded telemetry shares one handle with `Context`. Capture is opt-in and
//! performs no output in middleware callbacks. The caller drains JSONL to
//! stderr explicitly. `zenoh.*` records use the wire key as `endpoint`; core
//! records can add their own logical endpoint and Node identity. Service query
//! IDs travel in a small versioned attachment and successful replies echo them.
//! Missing, invalid, and mismatched attachments remain diagnostic and never
//! change business results. Error replies have no attachment support in Zenoh
//! 1.10.1 and use the original local query context. All IDs are unauthenticated.

// A workspace/sdist/package that loses the local Cargo patch must fail to
// compile instead of silently restoring the registry history ordering race.
const _: () = assert!(zenoh::Session::AOS_LIVELINESS_HISTORY_PATCH_VERSION == 2);

mod byte_budget;
mod context;
mod graph;
mod key;
mod pubsub;
mod queue;
mod reply_queue;
pub mod security;
mod service;
mod session;
mod telemetry;

pub use context::{
    open_context, open_resolved_secure_context, open_secure_context, DEFAULT_SESSION_CONFIG,
};
pub use session::ZenohTransport;
