#!/usr/bin/env python3
"""Install the MemoryServer business example into a fresh package prefix.

Use a Python environment that already contains the candidate AOS wheel. This
installer downloads nothing, starts no process/session, and never opens the
business data directory. Generated requests select explicit paths and identities.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import sys
import uuid


def regular_file(parser, value, option):
    if not value.is_absolute():
        parser.error(f"{option} must be an absolute path")
    try:
        result = value.resolve(strict=True)
    except OSError:
        parser.error(f"{option} must exist")
    if not result.is_file():
        parser.error(f"{option} must be a regular file")
    return result


def namespace(parser, value, option):
    if not re.fullmatch(r"/(?:[A-Za-z_][A-Za-z0-9_]*)(?:/[A-Za-z_][A-Za-z0-9_]*)*", value):
        parser.error(f"{option} must be an absolute non-root Node namespace")
    return value


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prefix", type=Path, required=True)
    parser.add_argument("--work-dir", type=Path, required=True,
        help="Caller-selected empty or previously owned data directory; not opened by this installer.")
    selection = parser.add_mutually_exclusive_group(required=True)
    selection.add_argument("--config", type=Path, help="Absolute plaintext Zenoh configuration.")
    selection.add_argument("--supervisor-profile", type=Path, help="Absolute native TLS client profile for readiness control.")
    parser.add_argument("--provider-profile", type=Path)
    parser.add_argument("--consumer-profile", type=Path)
    parser.add_argument("--domain-id", type=int)
    parser.add_argument("--namespace", default="/memory")
    parser.add_argument("--client-name", default="memory_supervisor")
    parser.add_argument("--client-namespace", default="/memory_control")
    parser.add_argument("--export-request-telemetry", action="store_true",
        help="Explicitly enable bounded Core Service/Action diagnostics in provider and consumer; requires matching deployment ACLs.")
    args = parser.parse_args(argv)
    if not args.prefix.is_absolute() or not args.work_dir.is_absolute():
        parser.error("prefix and work-dir must be absolute paths")
    prefix = args.prefix.absolute()
    if prefix.exists() or prefix.is_symlink():
        parser.error("prefix already exists; select a new installation directory")
    selected_namespace = namespace(parser, args.namespace, "namespace")
    namespace(parser, args.client_namespace, "client-namespace")
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", args.client_name):
        parser.error("client-name must be a valid Node name")
    security = args.supervisor_profile is not None
    profiles = {}
    if security:
        if args.provider_profile is None or args.consumer_profile is None:
            parser.error("secure installation requires explicit provider-profile and consumer-profile")
        routing = []
        for role in ("supervisor", "provider", "consumer"):
            path = regular_file(parser, getattr(args, role + "_profile"), role + "-profile")
            if path.stat().st_size > 1024 * 1024:
                parser.error(f"{role}-profile exceeds 1 MiB")
            try:
                profile = json.loads(path.read_text(encoding="utf-8"))
                domain, endpoints = profile["domain_id"], profile["router_endpoints"]
                if type(domain) is not int or not 0 <= domain <= 2**32 - 1:
                    raise ValueError("domain")
                if not isinstance(endpoints, list) or not endpoints or any(
                        not isinstance(endpoint, str) or not endpoint.startswith("tls/") for endpoint in endpoints):
                    raise ValueError("endpoints")
            except (KeyError, TypeError, ValueError, OSError):
                parser.error(f"{role}-profile must contain an explicit domain_id and TLS router_endpoints")
            profiles[role] = path
            routing.append((domain, endpoints))
        if any(route != routing[0] for route in routing[1:]):
            parser.error("all three profiles must select the same domain and Router endpoints")
        domain = routing[0][0] if args.domain_id is None else args.domain_id
        if domain != routing[0][0]:
            parser.error("domain-id differs from the explicitly selected security profiles")
        config = None
    else:
        if args.provider_profile is not None or args.consumer_profile is not None:
            parser.error("provider-profile and consumer-profile require supervisor-profile")
        config = regular_file(parser, args.config, "config")
        domain = 0 if args.domain_id is None else args.domain_id
    if not 0 <= domain <= 2**32 - 1:
        parser.error("domain-id must be an unsigned 32-bit integer")

    import aos_sdk as aos
    from aos_sdk import _native
    sources = Path(__file__).resolve().parents[1] / "examples/memory_server"
    for name in ("Put", "Get", "List", "Status", "Export", "RequestTelemetry"):
        # Production schema parsing only. This does not open a Zenoh session.
        aos.Interface.load(sources / "interfaces" / f"{name}.json")
    prefix.mkdir(parents=True)
    committed = False
    try:
        entries = prefix / "lib/aos_memory"
        share = prefix / "share/aos_memory"
        index = prefix / "share/aos/resource_index/packages"
        entries.mkdir(parents=True)
        for name in ("interfaces", "launch", "requests"):
            (share / name).mkdir(parents=True)
        index.mkdir(parents=True)
        installed = []

        def write(path, value):
            path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")
            installed.append(path)

        for name in ("provider.py", "consumer.py", "memory_store.py", "request_observer.py"):
            destination = entries / name
            shutil.copy2(sources / name, destination)
            installed.append(destination)
        if os.name != "nt":
            for role in ("provider", "consumer", "request_observer"):
                entry = entries / role
                entry.write_text("#!/bin/sh\nexec " + shlex.quote(sys.executable) +
                    " -I -u " + shlex.quote(str(entries / f"{role}.py")) + ' "$@"\n', encoding="utf-8")
                entry.chmod(0o755)
                installed.append(entry)
        for name in ("Put", "Get", "List", "Status", "Export", "RequestTelemetry"):
            destination = share / "interfaces" / f"{name}.json"
            shutil.copy2(sources / "interfaces" / f"{name}.json", destination)
            installed.append(destination)
        context = {"domain_id": domain, "zenoh_config": None if config is None else str(config)}
        installation_id = uuid.uuid4().hex
        provider = {
            "id": "provider", "package": "aos_memory", "entry": "provider",
            "args": ["--work-dir", str(args.work_dir), "--namespace", selected_namespace],
            "node": {"nodes": {"server": {"namespace": selected_namespace}}},
            "readiness": {
                "node": f"{selected_namespace}/server", "timeout": 20,
                "endpoints": [
                    {"kind": kind, "name": f"{selected_namespace}/{name}",
                     "interface": {"package": "aos_memory", "resource": name}}
                    for name, kind in (("put", "service"), ("get", "service"), ("list", "service"),
                                       ("export", "action"), ("status", "publisher"))
                ],
                "lifecycle": {"target": "active", "autostart": True},
            },
            "restart": {"policy": "on_failure", "delay": 0.5, "max_restarts": 2},
        }
        consumer = {
            "id": "consumer", "package": "aos_memory", "entry": "consumer",
            "args": ["--namespace", selected_namespace, "demo", "--request-prefix", installation_id],
            "node": {"nodes": {"consumer": {"namespace": selected_namespace}}},
            "depends_on": ["provider"], "restart": {"policy": "never"},
        }
        telemetry = []
        if args.export_request_telemetry:
            for role, process, node_name in (("provider", provider, "server"),
                                              ("consumer", consumer, "consumer")):
                topic = f"{selected_namespace}/{node_name}/request_telemetry"
                # Global consumer arguments must precede its subcommand.
                process["args"][0:0] = ["--telemetry-topic", topic]
                telemetry.append({"role": role, "node": f"{selected_namespace}/{node_name}_request_exporter",
                                  "topic": topic, "resource": "request_telemetry"})
        # Each child deliberately chooses its own credentials. Plaintext does
        # not clear a selected inherited security profile to force a downgrade.
        if security:
            for role, process in (("provider", provider), ("consumer", consumer)):
                process["env"] = {"AOS_SECURITY_PROFILE": str(profiles[role])}
        shutdown = {"sigint_timeout": 8, "sigterm_timeout": 3}
        pair = {"schema_version": 1, "context": context,
                "processes": [provider, consumer], "shutdown": shutdown}
        standalone = {"schema_version": 1, "context": context,
                      "processes": [provider], "shutdown": shutdown}
        write(share / "launch/pair.json", pair)
        write(share / "launch/server.json", standalone)
        controls = {"security_profile": str(profiles["supervisor"]),
            "client_node": {"name": args.client_name, "namespace": args.client_namespace}} if security else {}
        run = {"prefixes": [str(prefix)], **provider, "context": context,
               "shutdown": shutdown, **controls}
        write(share / "requests/run-server.json", run)
        for name in ("pair", "server"):
            write(share / "requests" / f"launch-{name}.json", {
                "prefixes": [str(prefix)], "package": "aos_memory", "resource": name, **controls})
        write(share / "requests/run-consumer.json", {
            "prefixes": [str(prefix)], **{key: value for key, value in consumer.items() if key != "depends_on"},
            "context": context, "shutdown": shutdown, **controls})
        manifest = {
            "schema_version": 1, "name": "aos_memory", "version": aos.__version__,
            "runtime_abi": "aos.process.v1",
            "entries": {role: {"kind": "aos_node", "executable": f"lib/aos_memory/{role}.py" if os.name == "nt" else f"lib/aos_memory/{role}",
                    **({"interpreter": sys.executable, "interpreter_args": ["-I", "-u"]} if os.name == "nt" else {})}
                        for role in ("provider", "consumer", "request_observer")},
            "resources": {
                **{name.lower(): {"kind": "interface", "path": f"share/aos_memory/interfaces/{name}.json"}
                   for name in ("Put", "Get", "List", "Status", "Export")},
                "request_telemetry": {"kind": "interface", "path": "share/aos_memory/interfaces/RequestTelemetry.json"},
                **{name: {"kind": "launch", "path": f"share/aos_memory/launch/{name}.json"}
                   for name in ("pair", "server")},
            },
        }
        pending = index / ".aos_memory.pending"
        pending.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
        pending.replace(index / "aos_memory.json")
        installed.append(index / "aos_memory.json")
        package = aos.PackageIndex([prefix]).package("aos_memory", version=aos.__version__)
        report = {
            "status": "installed", "prefix": str(prefix), "package": package,
            "python": sys.executable, "sdk_version": aos.__version__,
            "native_sha256": hashlib.sha256(Path(_native.__file__).read_bytes()).hexdigest(),
            "installation_id": installation_id, "work_dir": str(args.work_dir),
            "security_selected": security, "credentials_validated": False,
            "request_telemetry_enabled": args.export_request_telemetry,
            "request_telemetry_exporters": telemetry,
            "files": {path.relative_to(prefix).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
                      for path in installed},
        }
        write(share / "installation.json", report)
        committed = True
        print(json.dumps(report))
        return 0
    finally:
        if not committed:
            shutil.rmtree(prefix)


if __name__ == "__main__":
    raise SystemExit(main())
