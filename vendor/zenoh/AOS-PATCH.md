# AOS local modification of Zenoh 1.10.1

This directory contains the published `zenoh` 1.10.1 crate, with a local
liveliness history and routing declaration ordering fixes. Other Zenoh crates remain registry dependencies
at version 1.10.1. The workspace selects this directory using
`[patch.crates-io]`; no global Cargo cache is modified.

- Upstream commit: `1211779c3647f5a96713dade452c546a07823580`
- Published crate SHA-256:
  `453ba5d28a1197653aae4bb024fd74a9d5c7051d5a19a77f3a43d83f40c22584`
- Upstream license: EPL-2.0 OR Apache-2.0; AOS selects Apache-2.0.
- `LICENSE` and `NOTICE.md` are copied byte-for-byte from that upstream commit.
  Existing source copyright and license headers are retained.
- Modified API files: `src/api/mod.rs`, `src/api/session.rs`,
  `src/api/subscriber.rs`.
- Modified routing files: `src/net/primitives/demux.rs`,
  `src/net/routing/dispatcher/{face,interests,mod,tables}.rs`,
  `src/net/routing/gateway.rs`, and
  `src/net/routing/hat/router/{mod,pubsub,queries,token}.rs`.
- New implementations: `src/api/liveliness_history.rs` and
  `src/net/routing/dispatcher/declare_outbox.rs`.
- Added distribution files: this `AOS-PATCH.md`, `LICENSE`, and `NOTICE.md`.
- `Session::AOS_LIVELINESS_HISTORY_PATCH_VERSION` is a hidden compile-time
  re-export of the routing-core marker in `dispatcher/tables.rs`, required by
  aos-zenoh. Version 2 covers both ordering fixes; a missing or history-only
  local tree is a build error. Exact source replay, not the version alone,
  detects partial or arbitrary source changes.

The behavior, limitations, source comparison and verification commands are
documented in `../../upstream/zenoh-history-gate.md` and
`../../upstream/zenoh-routing-order.md`. The unified source patch is
`../../upstream/patches/zenoh-1.10.1-liveliness-history.patch`.
`../../upstream/zenoh-1.10.1-vendor-manifest.json` records the original and final
file hashes. Run `python3 scripts/check_zenoh_vendor.py --archive
/path/to/zenoh-1.10.1.crate` from the AOS root to verify the fixed change set
and reproduce all vendor bytes, including these notice/license files, by
applying the complete patch to the original archive. The native acceptance
harness requires this check before building.
