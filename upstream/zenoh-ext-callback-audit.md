# Zenoh-ext 1.10.1 callback and locking behavior

The audited detection and recovery callbacks do not synchronously wait for a
Declare-backed result. They update their state, release the state mutex, then
issue ordinary `Session::get(...).callback(...).wait()` queries. That builder
registers a handler and sends a Request; it does not wait for query completion.
With the default `Locality::Any`, the same call can synchronously invoke a local
Queryable and its reply callback before returning. Safety depends on the state
mutex having already been released. No audited internal callback explicitly
declares an entity while holding its state lock; final State destruction can
undeclare its detection token as described below.

This is a source audit of the fixed dependency used by AOS, not a claim that
arbitrary user handlers are safe. AdvancedSubscriber calls its delivery and
miss handlers while holding its own state mutex. AOS's delivery handler only
copies a sample, records bounded telemetry and enqueues it; application code
runs outside the middleware callback. A custom handler that synchronously
waits on a Declare result, re-enters AdvancedSubscriber state, or panics can
still deadlock or poison that upstream state.

## Source identity and feature use

The Cargo registry source was compared byte for byte with the local published
`zenoh-ext-1.10.1.crate`. Its archive SHA-256 is
`d1a5c51c0c04ed54c16da3b66b72f622a5a081c7bb9ed91a54a008135a4d40ce`;
`.cargo_vcs_info.json` identifies repository commit
`1211779c3647f5a96713dade452c546a07823580`, path `zenoh-ext`.

| Fixed file | SHA-256 | Lines |
| --- | --- | ---: |
| `advanced_subscriber.rs` | `58bfefe6eaa7047b9c703422ab9f25dc993710383b2f72fbb1425da54a33480d` | 1953 |
| `advanced_publisher.rs` | `61b424c8fd2f8ccf160e1d808a5fe633751f2dca6dab51263b03469781af5b37` | 842 |
| `advanced_cache.rs` | `b97f04b2bb24ca102c04a1a84437d3c8863c7a3fbce6564a83b2c2c078d0d52b` | 392 |

All line references below refer to those fixed files. AOS uses the
unmodified zenoh-ext crate; its routing changes are in vendored Zenoh.

In `crates/aos-zenoh/src/pubsub.rs`, TransientLocal publishers enable
publisher detection and a bounded cache at lines 75–80; reliable publishers
also enable sporadic heartbeat at lines 81–84. TransientLocal subscribers
enable history, late publisher detection and subscriber detection at lines
237–245; reliable subscribers add heartbeat recovery at lines 246–248.
The audit includes these paths, periodic recovery as an additional check,
and the cache invoked by AdvancedPublisher.

## Callback and lock inventory

| Path | State/lock behavior | Protocol work and waiting |
| --- | --- | --- |
| `advanced_subscriber.rs:733–789`, periodic recovery | Takes `statesref` at 734; updates query count and copies the query arguments; explicitly drops the guard at 760. | At 766–788 registers a reply callback and sends an ordinary query. No receive or join. The periodic task awaits only its timer before entering this function. |
| `advanced_subscriber.rs:892–955`, sample/gap callback | Takes state at 898 and calls `handle_sample`. For recovery, copies range and increments pending count; drops the lock at 928. | At 933–951 sends a Request with a reply callback. No entity declaration or blocking result receive. |
| `advanced_subscriber.rs:981–1029`, initial history | Constructor prepares an InitialRepliesHandler; holds no state lock while sending the query. | Ordinary query with callback; no synchronous reply receive. |
| `advanced_subscriber.rs:1032–1272`, publisher liveliness callback | Delete only marks the matching source not alive and returns. Put branches update timestamped, sequenced or global query state. Locks are explicitly dropped at 1095, 1178 and 1232. | Each Put branch sends an ordinary query with a reply callback at 1101–1120, 1184–1202 or 1237–1255. The liveliness subscriber is declared by the constructor at 1266–1272, outside the callback and without its state lock. |
| `advanced_subscriber.rs:1280–1381`, heartbeat callback | Parses the heartbeat, locks at 1319, updates source/query state and releases the lock at 1353. Early returns release the guard normally. | At 1359–1376 issues an ordinary Request with callback. No entity declaration or reply receive. |
| Reply callbacks at 768, 935, 1007, 1103, 1186, 1239 and 1361 | Lock the state and call `handle_sample`; that helper orders/buffers data and calls the supplied delivery/miss handlers. | No get/declare/wait inside these reply handlers themselves. The supplied AOS handler only enqueues owned data. |
| `advanced_subscriber.rs:1577–1659`, reply-handler destructors | Take the state lock, decrement pending counts and flush buffered samples; the initial handler may spawn a timer task. | The destructor bodies do not receive queries or join threads. Handler invocation still occurs under the ext state lock. Releasing their retained State Arc can reach the token destructor in the row below. |
| `advanced_subscriber.rs:960–967`, subscriber drop callback | Takes state, removes the delivery callback and clears miss handlers. | The callback body does not explicitly declare an entity or query. Final State release can undeclare its token; arbitrary custom handler destructors are not certified by this audit. |
| `advanced_subscriber.rs:452, 1401–1406`, final State destruction | State owns an optional `LivelinessToken`. Its last `Arc<Mutex<State>>` may be released by a reply-handler destructor or a collected callback clone. | Token Drop undeclares it, so ext can create a nested routing producer during FIFO dispatch. A busy lane enqueues without waiting; this path therefore does not create a Declare wait cycle. It is subject to the same unwinding and poisoned-state limits as other destructor declarations. |
| `advanced_subscriber.rs:1386–1406`, subscriber detection | Constructor first declares the token at 1401–1405; only afterward stores it under the state mutex at 1406. | No state lock is held during declare. `detect_publishers` at 1466–1471 only returns a public builder; callers still owe the native callback contract. |
| `advanced_publisher.rs:371–505`, publisher/cache/detection construction | No native sample or liveliness callback is installed by this constructor. It declares the publisher, cache queryable and detection token without an ext state mutex. | The heartbeat publisher is declared before spawning its task; the task only awaits a timer and publishes Push data at 458–488. Public detection/matching-listener builders do not install an internal blocking callback. |
| `advanced_cache.rs:248–355`, cache queryable callback | Holds the cache read lock while selecting and replying with samples. | `reply_sample(...).wait()` at 298 and 336 sends a Response, not a Declare-backed query result wait. This callback neither declares entities nor receives query replies. Slow transport or custom re-entry can still hold that read lock for longer; R3 makes no bounded latency claim. |

The decisive builder implementation is the vendored
`api/builders/query.rs:405–437`: `SessionGetBuilder::wait` converts the handler,
calls `session.query(...)`, and returns the handler's receiver. In
`api/session.rs:2830–2858`, that query sends `RequestBody::Query` through
`send_request`. At `api/session.rs:2860–2891`, default `Locality::Any` also
synchronously calls matching local Queryables before returning. A local
AdvancedCache may execute `reply_sample().wait()` there, synchronously reaching
the registered reply callback and reacquiring `statesref`. The get sites in
the inventory explicitly release that mutex first. There is no blocking
receive for query completion or a Declare-backed result; the lack of such a
receive alone would not have proved safety if those locks remained held.

`advanced_subscriber.rs:605–710` and the flush helpers at 1503–1568 do invoke
the caller-supplied handlers while the ext caller holds state. This is why
the AOS queue-only callback boundary is material to compatibility. It does
not generalize to arbitrary handlers supplied directly to zenoh-ext.

## Scope of the conclusion

For the fixed AOS callbacks and zenoh-ext paths above, the new entity FIFO
does not introduce a same-lane Declare wait cycle. This conclusion combines
the ext audit with the seven-site AOS callback inventory in
[the routing contract](zenoh-routing-order.md#aos-callback-audit).
It does not promise arbitrary callback re-entry, recovery after a poisoned
ext mutex, synchronous declaration-to-data ordering, or bounded queues when
a callback never returns.

Mac regression evidence accompanies the R3 record. Linux execution of the
new vendor remains a separate pending gate; this source audit cannot replace
it.
