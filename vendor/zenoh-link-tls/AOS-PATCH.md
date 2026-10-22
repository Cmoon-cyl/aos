# AOS private CA roots, synchronized TLS I/O and certificate expiration

Upstream: zenoh-link-tls 1.10.1, Zenoh commit 1211779c3647f5a96713dade452c546a07823580.
Original crate SHA256: cce65fd71b4631821a85058542e28faac0151af6c53cd61f2dfafc7e16c4d6c4.

An explicit client CA replaces the built-in Web PKI roots. With no explicit CA, the upstream root set is unchanged. Server-side client authentication is unchanged. The public AOS_PRIVATE_ROOTS_PATCH_VERSION constant makes an absent workspace patch fail to build in aos-zenoh. LICENSE and NOTICE.md are copied from the fixed upstream commit; AOS selects Apache-2.0.

The change is restricted to `src/utils.rs` (explicit root-store construction and its tests), `src/unicast.rs` (TLS I/O synchronization and its tests), and `src/lib.rs` (build guards). The complete original crate is preserved; the manifest and replayable patch include the license, notice and this modification notice. `scripts/check_zenoh_tls_vendor.py` verifies the pinned archive, exact edit set, and full patch replay. The historical patch filename includes both fixes.

Two unit tests check the root-selection helper. Three further tests call the real `TlsClientConfig::new` constructor with one explicit PEM CA, an explicitly empty CA, and no CA. A `#[cfg(test)]` field observes the length of the same `RootCertStore` consumed by the rustls builder; it adds no field to production builds. The tests require counts of one, zero, and the upstream public-root count respectively. The embedded fixture is a public self-signed test certificate; its private key is not retained and no network connection is required. Restoring the pinned upstream constructor's public-roots-plus-extend block makes the explicit-CA constructor assertions fail while the helper tests still pass.

`tests/security_runtime.rs` exercises the resulting transport. An empty-CA client failing against a private-CA router is only a fail-closed observation: the same rejection can occur when public roots are present. That handshake result alone does not prove that public-root fallback is absent. The constructor tests establish the root-store wiring; the source verifier establishes provenance and complete patch replay. Neither substitutes for the transport's runtime acceptance.

## TLS I/O synchronization

Upstream 1.10.1 stores a complete `TlsStream` in `UnsafeCell`, with independent read and write mutexes, and creates a mutable reference to the same stream in both directions. The mutexes do not exclude a read from a write. Both operations mutate rustls connection state; the unsafe Send/Sync implementations therefore do not make this access sound. The same upstream defect is reported in [Zenoh issue #2557](https://github.com/eclipse-zenoh/zenoh/issues/2557).

AOS replaces the cell and manual Send/Sync implementations with `tokio::io::split`. Its halves share one mutex around each synchronous poll, released before Pending is returned. Per-direction async mutexes retain ordering across complete read/write operations. A pending read cannot hold the shared TLS lock while waiting for peer input.

Both `write` and `write_all` flush the TLS writer before releasing that operation's mutex. tokio-rustls can accept plaintext while retaining encrypted output; without this flush an idle link can leave its final record buffered until a later write or close. The traffic regression requires both receivers to finish before either link is closed, so shutdown cannot hide buffered application data.

Explicit close and certificate expiration flush and shut down the TLS write half, including close_notify, instead of directly shutting down the underlying TCP stream through an aliased mutable reference. Dropping both owned halves closes the TCP socket without entering another runtime. `AOS_TLS_IO_PATCH_VERSION` makes an absent I/O patch fail to build in aos-zenoh independently of the private-CA guard.

Three real TLS regressions cover simultaneous two-way traffic (256 frames of 64 KiB in each direction, every sequence and payload byte checked), writing and graceful closing while a read is pending, and releasing the socket outside a runtime enter guard. They generate temporary loopback certificates with OpenSSL and retain no credentials. The same traffic test against the original UnsafeCell implementation reproduced `handshake not complete` at its write_all path. These tests complement, rather than replace, installed multi-host acceptance.

## Certificate expiration during link construction

Upstream starts `LinkCertExpirationManager` inside `Arc::new_cyclic`. An already-due expiration task can run before that constructor returns; its `Weak` cannot yet upgrade, so the task exits without closing the link. Native diagnostics observed old links closing at server-certificate expiry followed by a new TCP connection whose expiration task started but never closed it. Certificate handshake time has second precision, making a connection in the expiration second a concrete trigger.

AOS registers a fully constructed intermediate expiration target. That target waits for a retained `Notify` permit before upgrading the weak link. The constructor publishes the link Arc and then releases the permit. Dropping the registration also releases it, so initialization unwinding cannot leave the task waiting forever. The target owns only a weak link, preserving the absence of an ownership cycle. Newly admitted monitored links also reject certificate deadlines that have already arrived; the initialization gate still handles expiry during construction.

Two deterministic regressions force the expiration callback to execute while the link is still inside `Arc::new_cyclic`, and force initialization to unwind while that callback is waiting. They require actual expiration after successful initialization and release of the waiting task after failed initialization. Native acceptance explicitly runs the TLS crate's full unit suite in addition to the workspace and live certificate-boundary tests.
