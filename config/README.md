# Local Zenoh router

Use upstream `zenohd` **1.10.1**, matching the Rust dependencies. It routes
communication and discovery; each application owns its Context and business
state. These samples are for trusted development networks. The Router listens on all
interfaces by default; use the [TLS topology guide](../docs/platform/topology.md)
for untrusted networks.

On Apple Silicon macOS, from the repository root:

```sh
mkdir -p .tools/zenoh
curl -fL https://github.com/eclipse-zenoh/zenoh/releases/download/1.10.1/zenoh-1.10.1-aarch64-apple-darwin-standalone.zip \
  -o .tools/zenoh/zenoh.zip
echo 'a27a177014fa81bc1bdb9b824373a2a8bce0e0a33c40e6c189d6fc9414b57b8a  .tools/zenoh/zenoh.zip' | shasum -a 256 -c -
unzip .tools/zenoh/zenoh.zip -d .tools/zenoh
.tools/zenoh/zenohd --version
.tools/zenoh/zenohd --config config/zenoh-router.json5
```

Keep the router in the foreground in its own terminal; Ctrl-C stops it. Port
7447 must be free. Node processes use `zenoh-session.json5`: peer mode, a random
loopback listener, gossip via the router, and no multicast. A missing Router
does not prevent local startup: peers connect and reconnect in the background.
Session open is not network readiness and does not retry business requests.

On Windows, use `config/zenoh-router-windows.json5`. It listens on IPv4
`0.0.0.0:7447`; all other fields match the common Router configuration.
The pinned Zenoh `[::]` listener was verified as IPv6-only on Windows.
The common sample retains the ROS reference listener for macOS and Linux.

The session enables timestamping so cache-only AdvancedPublishers can provide
best-effort transient-local history. Loopback peers use the reference
`greater-zid` autoconnection strategy so only one peer initiates each pair's
connection. Configuration choices and retained Zenoh defaults are documented
in [the upstream record](../upstream/README.md#configuration-decisions).

For other architectures, choose the matching archive from the
[same 1.10.1 release](https://github.com/eclipse-zenoh/zenoh/releases/tag/1.10.1)
and verify its published digest. When an archive is unavailable, build the
fixed source:

```sh
git clone https://github.com/eclipse-zenoh/zenoh .tools/zenoh-source
git -C .tools/zenoh-source checkout --detach 1211779c3647f5a96713dade452c546a07823580
cargo build --manifest-path .tools/zenoh-source/Cargo.toml --locked --release -p zenohd
.tools/zenoh-source/target/release/zenohd --version
.tools/zenoh-source/target/release/zenohd --config config/zenoh-router.json5
```

Platform installation and business evidence is recorded per fixed candidate.
Loopback and AOS domain IDs are not authentication mechanisms.
