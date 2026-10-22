// Exercise the exact vendored FIFO without requiring Zenoh's upstream test
// workspace. The production adapter supplies owned destination primitives.
#[path = "../../../vendor/zenoh/src/net/routing/dispatcher/declare_outbox.rs"]
mod declare_outbox;
