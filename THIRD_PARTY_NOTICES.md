# Third-party sources and notices

AOS-authored code is licensed under Apache-2.0; see `LICENSE`. AOS uses
fixed upstream dependencies and documented adaptations. This notice does not
replace the license of any upstream component.

| Source | Use | Retained record |
| --- | --- | --- |
| Zenoh / zenoh-ext 1.10.1 | Communication, discovery, Advanced pub/sub | `upstream/licenses/zenoh-LICENSE`, `zenoh-NOTICE.md` |
| Vendored Zenoh and zenoh-link-tls | Documented local patches | `vendor/*/LICENSE`, `NOTICE.md`, `AOS-PATCH.md`; platform bundles retain copies under `upstream/licenses/` |
| Hiroz | Adapted key layouts and communication configuration | `upstream/licenses/hiroz-LICENSE` and source adaptation notices |
| ROS 2 rcl / rclpy Jazzy | Adapted Action behavior and implementation references | `upstream/licenses/rcl-LICENSE`, `rclpy-LICENSE` and source notices |
| rmw_zenoh | Configuration and GetResult timeout policy references | `upstream/licenses/rmw_zenoh-LICENSE` and source notices |

Exact revisions, adoption scope and modifications are recorded in
[`upstream/README.md`](upstream/README.md), `upstream/source-revisions.json`
and `upstream/source-map.json`. The selected license for the documented
adaptations is Apache-2.0, as recorded there.

When included, `router/` retains the full pinned upstream Zenoh standalone
distribution. `manifest.json` records its original archive URL and SHA256,
the tested executable digest, and every bundled file digest. Its bundled
dependencies retain their upstream distributions and notices.

This file is a source attribution index, not a claim that every transitive
dependency has received a separate license audit.
