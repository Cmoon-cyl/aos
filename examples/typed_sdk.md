The typed examples use the installed native wheel and a running Zenoh router.
Start the router using `config/README.md`, then run from the repository root:

```sh
python -m examples.typed_listener --count 3
python -m examples.typed_talker
```

Run the listener in one terminal and the talker in another. For a retained
last value, start the talker with `--count 1 --transient-local --hold 20`,
then run the listener with `--count 1 --transient-local` while the talker
remains alive. The publisher's lifetime bounds the retained data.

For a Service, run these in separate terminals:

```sh
python -m examples.add_two_ints_server
python -m examples.add_two_ints_client 2 3
```

Pass `--config /path/to/session.json5` to use an explicit router configuration.
These examples do not start a router or retry Service requests.

`Interface.load()` accepts a JSON file path or a mapping. The Rust interface
implementation validates messages and Service requests/responses. The
`interfaces/` directory contains the exact types used here.

`Node.create_subscription()` accepts a callback, or returns a subscription for
direct `recv(timeout)` when no callback is given. A callback must be synchronous
and return normally; Service callbacks return the response JSON value.
`SingleThreadedExecutor` runs callbacks on its spin thread.
`MultiThreadedExecutor` runs callbacks in its bounded worker pool. A Node's
default `MutuallyExclusiveCallbackGroup` prevents overlap; assign a
`ReentrantCallbackGroup` when callbacks may safely overlap.

`Client.call_async()` returns a Future. `future.result(timeout)` can wait
directly, while `spin_until_future_complete()` also services callbacks on the
same executor. User `add_done_callback()` functions run only when the Node's
executor spins. Asyncio awaiting is an optional adapter and also needs a
spinning executor. A timeout or local Future cancellation does not cancel the
remote Service and never triggers a retry.

Explicitly destroy a Node or leave its context manager before shutting down its
shared Context. Node creation metadata and endpoint `summary`, `side_effects`,
and `contract_version` appear in the Rust NodeCard. Omitted `side_effects` and
`contract_version` remain null; absence does not mean the endpoint is read-only.
