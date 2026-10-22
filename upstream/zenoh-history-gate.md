# Zenoh 1.10.1 liveliness history ordering patch

The AOS graph watch uses `history(true)` so that a watch can discover tokens
that predate its creation and keep receiving subsequent changes. In the
published Zenoh 1.10.1 crate, `declare_liveliness_subscriber_inner` takes a
snapshot of `remote_tokens` while holding session state, registers the
subscriber, releases state, and spawns an asynchronous loop that sends the
snapshot as Put samples. A concurrent `UndeclareToken` can send Delete before
that old Put is replayed. A last-event-wins consumer then retains a departed
entity until another event or watch replacement changes its state.

The session gate orders the initial cached snapshot before later events for
that subscriber. Patch version 2 also orders routing-generated declarations
with the [routing outbox](zenoh-routing-order.md). It does not make discovery a cross-session synchronization barrier:
a short readiness call may return true from the last state known locally while
removal is still in flight.

## Fixed sources and upstream check

The vendored source is the published `zenoh` 1.10.1 crate, not an arbitrary
checkout of the upstream workspace:

| Item | Value |
| --- | --- |
| Repository | <https://github.com/eclipse-zenoh/zenoh> |
| Release commit | `1211779c3647f5a96713dade452c546a07823580` |
| Crate archive SHA-256 | `453ba5d28a1197653aae4bb024fd74a9d5c7051d5a19a77f3a43d83f40c22584` |
| Changed upstream files | 13 paths pinned in `zenoh-1.10.1-vendor-manifest.json` (three API and ten routing files) |
| New helpers | `zenoh/src/api/liveliness_history.rs`, `zenoh/src/net/routing/dispatcher/declare_outbox.rs` |
| Added distribution files | `zenoh/AOS-PATCH.md`, `zenoh/LICENSE`, `zenoh/NOTICE.md` |
| Selected license | Apache-2.0 |

The archive's `.cargo_vcs_info.json` identifies the release commit.
`vendor/zenoh/LICENSE` and `NOTICE.md` are exact copies from the repository root
at that commit. The new helper has an Apache-2.0 header; changed upstream files
retain their original headers and carry AOS modification notices.

A read-only GitHub check on 2026-10-21 found no release tag newer than 1.10.1.
The current main commit was
[`9b150e9924b5f4d120fc71ef6d34ef6b3ec0bc66`](https://github.com/eclipse-zenoh/zenoh/blob/9b150e9924b5f4d120fc71ef6d34ef6b3ec0bc66/zenoh/src/api/session.rs).
Its complete `declare_liveliness_subscriber_inner` function was byte-identical
to the published 1.10.1 function: both SHA-256 values were
`a3eec5e5cc318fd6169ce0648c7340b3b6988c33b0a5340bcfb4db8baf881e24`.
There was no corresponding upstream implementation to transplant at that
snapshot. This is a local patch, not a claim that upstream has accepted it.

The full original/final file hashes are in
[`zenoh-1.10.1-vendor-manifest.json`](zenoh-1.10.1-vendor-manifest.json).
All 13 modified and all five added files, including the modification notice
and upstream license/notice copies, are retained as a complete unified diff in
[`patches/zenoh-1.10.1-liveliness-history.patch`](patches/zenoh-1.10.1-liveliness-history.patch).

## Ordering and ownership

While holding the existing session write lock, the declaration captures the
history snapshot and installs a gated callback in `SubscriberState` and its
resource lists. The original callback, including its callback-drop notifier,
is the inner callback. A nonempty cached snapshot creates one replay owner.
An empty snapshot and `history(false)` retain the existing direct path.

1. Incoming events queue while the initial snapshot is replayed.
2. The replay owner invokes every historical callback, then pops and invokes
   queued events in FIFO order.
3. It switches to live delivery in the same locked operation that observes an
   empty queue. A new event cannot fall between draining and opening.
4. Every user callback runs outside both the session lock and the gate lock.
   Normal live callback concurrency after replay remains unchanged.

The queue has no event-dropping capacity limit. Its normal lifetime is the
initial replay; it is not a permanent graph cache. No token tombstone is
introduced, so Delete followed by Put for the same key remains valid.

An explicit subscriber undeclare marks its gate closed under its mutex. Session close
closes its gates in the same state-lock critical section that removes its
primitives, before dropping the subscriber/resource maps. Queued events are
released, and a running callback may finish. Each replay owner can additionally
deliver at most one historical sample that already passed its closed check, or
one pending sample already dequeued, before close acquired the mutex. Callback
invocation is deliberately outside the mutex. This does not strengthen the
upstream live path's handling of callbacks already collected for delivery.
`SubscriberState::drop` also closes the gate as an ownership fallback.

The replay owner has two levels of cleanup:

- A callback panic discards the incomplete replay and queued events, then
  restores live delivery. Callback panic already invalidates the consumer's
  uninterrupted event history; the patch does not claim recovery of lost
  samples. Later live samples are still delivered.
- A replay future dropped without panic drains queued events outside the
  locks. If it is dropped before its first poll, the cached history snapshot
  is discarded: that snapshot belongs to the future, not the replay guard.
  Only already-queued live events are drained before the gate opens. Tokens
  represented only by the discarded history are not recovered until another
  declaration supplies a Put. If the drain itself panics, a recovery guard
  releases the remainder and restores live delivery. Explicitly closed gates
  remain closed. In the verified Session close path, gates close before
  `task_controller.terminate_all_async()` cancels replay work; cancellation by
  another runtime owner is not claimed to preserve the history snapshot.

These paths prevent a cancelled or panicking replay from retaining a queue
that accumulates events forever. They do not preempt user callbacks that block
forever. Zenoh's existing session close waits for running replay work to finish.
`Session::is_closed()` reads `primitives.is_none()` under the same session state
lock. The live `execute_subscriber_callbacks` path also explicitly releases
that lock before invoking its collected callbacks.

## Regression gates

`crates/aos-zenoh/tests/liveliness_history.rs` compiles the exact vendored helper
for its unit tests and exercises real Zenoh sessions over loopback TCP. Its
network cases run in separate test subprocesses. Each child receives a default
Net runtime of eight workers only when the caller has not set `ZENOH_RUNTIME`;
the multithreaded parent never mutates process environment variables.
History callback barriers intentionally block runtime workers.

The unit tests use event/barrier synchronization to check initial Put before
queued Delete and same-key Put, live passthrough, reentry, FIFO while draining,
panic recovery, drop before first poll, panic during drop cleanup, explicit
close, payload release and owner release.

All 12 history tests (seven helper tests and five network tests), all 15
readiness tests, and the transport tests are ordinary `aos-zenoh` integration
targets and run under `cargo test --workspace`. The helper is included by path
from the integration target, so these checks do not depend on Cargo running a
dependency's `cfg(test)` tests. The rest of Zenoh's upstream test suite is not
included in this claim.

The network ordering test primes a token, blocks B's cached Put, undeclares the
token, and waits for a later same-session network marker before releasing the
Put. It includes a same-key `history(false)` witness declared after B. That
witness alone is not the ordering proof: the suffix-based Delete callback
selection uses a HashMap rather than insertion order. The later TCP marker is
the additional ingress barrier. The original registry crate delivers
`[Delete, Put]`; the patched crate delivers `[Put, Delete]`.

Other network tests declare and undeclare a subscriber from inside both
historical and live callbacks, close a subscriber/session during blocked
history, and stop/restart the only router while retaining the original provider
session, observer session and token handle. The reconnect sequence is
Put → Delete → Put → explicit token undeclare → Delete.
The close/undeclare tests also wait for a later same-session marker before
closing, so their Delete is already queued behind the blocked history callback.

The AOS readiness tests assert absence directly on the snapshot that first
satisfies the live-marker predicate. They also recreate a watch after removal.
There is no second absence wait that could hide Delete arriving after its
ordered marker. An unrelated session's
absent snapshot is not used as an immediate false oracle for the caller.

Run from the workspace root:

```sh
python3 scripts/check_zenoh_vendor.py --archive /path/to/zenoh-1.10.1.crate
cargo test -p aos-zenoh --test liveliness_history --test readiness --locked
cargo test -p aos-zenoh --test transport --locked
```

The original-registry comparison uses the same ordering test with the local
Cargo patch removed and the original registry lock entry restored in an
isolated checkout. An expected assertion failure is evidence for that
regression, not a passing acceptance run. Native platform and packaged-wheel
acceptance remain separate delivery gates.

The verifier requires the pinned original archive. If `--archive` is omitted,
it checks `AOS_ZENOH_CRATE_ARCHIVE`, then the local Cargo archive cache; it does
not fetch source itself. It pins the upstream identity and license bytes,
requires exactly the declared 13 modified/five added paths, applies the
complete patch to the original crate in a temporary directory, and compares
every resulting file with the checked-in tree. Updating a manifest hash alone
cannot authorize another edited path or a missing patch hunk.

`scripts/native_acceptance.py`, which the native CI workflow invokes, runs this
verification before toolchain setup or any native build. It retains the
original archive and the verification result with the candidate evidence.
Set `AOS_ZENOH_CRATE_ARCHIVE` to reuse local bytes; otherwise that harness
downloads the fixed crate from `static.crates.io` and checks its pinned digest.
The source-gate mutation tests run alongside the existing evidence-harness tests.

Ordinary Cargo builds do not run Python. Instead, `aos-zenoh` must resolve the
hidden `zenoh::Session::AOS_LIVELINESS_HISTORY_PATCH_VERSION` constant at compile
time and require version 2. Session re-exports the routing-core definition in
`dispatcher/tables.rs`, so retaining only session.rs cannot preserve this
marker when that routing core is removed. Exact source replay remains required
to detect arbitrary partial edits. A build that loses the workspace patch, including
an incomplete source distribution, fails rather than silently selecting the
unpatched registry crate. `vendor/zenoh` is explicitly excluded from workspace
members, and Git marks vendor/patch files `-text` to preserve bytes when
`core.autocrlf` is enabled.

The Python source distribution uses Maturin's Git generator so that the complete
workspace manifest and lock file stay together. The Cargo generator removes
workspace members unrelated to the Python crate, which invalidates this
workspace's lock file under `--locked`. The sdist includes all 168 vendor files,
the embedded session configuration, and the source verifier, manifest and
complete patch. Distribution checks compare every vendored byte and build a
wheel from an extracted sdist with `--offline --locked`; checking only the
checkout is insufficient.

Wheel metadata declares `vendor/zenoh/LICENSE`, `vendor/zenoh/NOTICE.md` and
`vendor/zenoh/AOS-PATCH.md` as license files. They are copied into the wheel's
`.dist-info/licenses` directory so the native binary carries the upstream
license, upstream notice and local modification notice. This does not declare
a license for unrelated AOS source files.

The expected-failure original-registry comparison remains a separate diagnostic,
not part of the patched workspace's passing suite. Its isolated diagnostic
checkout must remove the compile-time guard deliberately as well as the Cargo
patch; no production feature or environment variable bypasses that guard.
