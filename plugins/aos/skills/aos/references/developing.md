# Developing an AOS application

Read current.json under the selected AOS prefix. It gives the installed Python,
native CLI, bundle, and default session configuration. The bundle contains:

- docs/python-sdk.md for SDK examples and resource cleanup;
- interfaces/ for JSON Interface definitions;
- examples/add_two_ints_server.py for a Service provider;
- examples/fibonacci_server.py and fibonacci_client.py for Action feedback/results;
- docs/platform/memory-server.md for persistence and application-level idempotency;
- docs/platform/process-cli.md for package, run and launch resources.

Run Python code with the absolute python path from current.json. Keep provider
and client on the same explicit config and domain. From the bundle directory:

    /path/to/python -m examples.add_two_ints_server --config /path/to/session.json5

Substitute the absolute interpreter and configuration paths from current.json.
The provider stays running while the CLI calls it. For new code, follow the
installed SDK example for Context, Node, Interface, and SingleThreadedExecutor,
and close all resources when the program ends.

Use Topic for publication/subscription, Service for bounded request/response,
and Action for acceptance, feedback, terminal results and cooperative cancellation.
Keep business persistence and idempotency in the application. Provider restart
does not recover old in-memory Action goals.

Validate the complete provider/caller path. Include the observed response,
configuration and execution host in the result. Do not infer cross-machine or
secure-network acceptance from a loopback test.
