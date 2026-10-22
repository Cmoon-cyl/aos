# Calling AOS

Use the absolute cli path in the installation result or current.json. The
examples below call it aos for readability.

    aos --help
    aos --version
    aos env status
    aos env ensure
    aos node list
    aos service list
    aos schema service call

Write a JSON request to a file and call aos service call --request that-file.json.
For an AddTwoInts provider, the request is:

    {"service_name":"/add_two_ints","service_type":"aos_examples/srv/AddTwoInts","values":{"a":17,"b":25},"timeout":5}

The successful response contains data.response.sum: 42. This provider is only
present if an application started it; the installer's isolated provider stops
after its verification. To rerun that verification, use aos env test.

Select configuration using AOS_ZENOH_CONFIG and AOS_DOMAIN_ID, or the documented
request fields. The managed local environment uses an explicit loopback client
configuration and a Router on 127.0.0.1:7447. env ensure never joins other machines.
Read the installed docs/deployment.md for per-machine Router links and mTLS.

Native commands use GROUP VERB --request PATH|- and describe their contract with
schema GROUP VERB. Check actual command schemas instead of inventing business
flags. Streaming commands, including Action send_goal and launch start, produce
JSONL. Keep stderr out of the JSON stream.

A valid protocol response is not necessarily business success:

- Service timeout ends the local wait; do not implicitly retry a write.
- Action acceptance is not completion. Observe feedback and the terminal result.
- A cancellation reply is not the terminal CANCELED result.
- Doctor may return exit code zero while data.status reports a problem.
- Graph discovery is a bounded observation, not a permanent complete inventory.

## Updating

    aos update check
    aos schema update apply
    aos update apply
    aos update rollback

update check reads the public stable release catalog and selects a tagged
aos-agent-vX.Y.Z distribution. Its version is the agent/CLI manager distribution;
each distribution pins an accepted native runtime. --version reports both.

To select a particular available manager version, write
{"version":"0.1.0"} to a file and pass it with --request. Applying the installed
version is a no-op. Downgrading uses rollback to the previous verified installation.
An update verifies the archive, installs to a separate directory, runs a real
Service call, then atomically replaces current.json. Failed candidates do not
replace the working version. Existing applications and owned Router processes
keep their original executable until explicitly restarted.

env stop stops only the Router owned by this installation. It does not kill a
process merely because its PID or port appears in a saved state file.
