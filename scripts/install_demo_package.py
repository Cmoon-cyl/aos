#!/usr/bin/env python3
"""Install the AOS launch demo into a new prefix using an installed SDK Python.

The invoking Python must already contain the candidate wheel. No dependency is
downloaded and no entry is executed by the installer. Install into a fresh
directory to preserve any existing package.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shlex
import shutil
import sys


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prefix", type=Path, required=True)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--domain-id", type=int, default=0)
    parser.add_argument("--namespace", default="/demo")
    args = parser.parse_args()
    if not 0 <= args.domain_id <= 2**32 - 1:
        parser.error("domain-id must be an unsigned 32-bit integer")
    if not args.namespace.startswith("/") or args.namespace == "/":
        parser.error("namespace must be an absolute non-root Node namespace")
    args.namespace = args.namespace.rstrip("/")
    config = args.config.resolve(strict=True)
    if not config.is_file():
        parser.error("config must be a regular file")
    prefix = args.prefix.absolute()
    if prefix.exists():
        parser.error("prefix already exists; select a new installation directory")
    import aos_sdk as aos
    from aos_sdk import _native
    # Parse the interface with the installed core without opening a session.
    aos.Interface.load(Path(__file__).resolve().parents[1] / "examples/launch/Label.json")
    sources = Path(__file__).resolve().parents[1] / "examples/launch"
    prefix.mkdir(parents=True)
    committed = False
    try:
        entries = prefix / "lib/aos_demo"
        share = prefix / "share/aos_demo"
        index = prefix / "share/aos/resource_index/packages"
        entries.mkdir(parents=True)
        (share / "interfaces").mkdir(parents=True)
        (share / "launch").mkdir()
        index.mkdir(parents=True)
        installed = []
        for role in ("provider", "consumer"):
            # A short portable shebang avoids platform interpreter-path limits.
            # Shell quoting is performed by shlex, never string interpolation of
            # unquoted paths; the launcher only execs this exact interpreter.
            script = entries / f"{role}.py"
            shutil.copy2(sources / f"{role}.py", script)
            installed.append(script)
            if os.name != "nt":
                entry = entries / role
                entry.write_text(
                    "#!/bin/sh\nexec " + shlex.quote(sys.executable) +
                    " -I -u " + shlex.quote(str(script)) + ' "$@"\n'
                )
                entry.chmod(0o755)
                installed.append(entry)
        for name in ("Label", "Status"):
            dest = share / "interfaces" / f"{name}.json"
            shutil.copy2(sources / f"{name}.json", dest)
            installed.append(dest)
        namespace = args.namespace
        plan = {
            "schema_version": 1,
            "context": {"domain_id": args.domain_id, "zenoh_config": str(config)},
            "processes": [
                {
                    "id": "provider", "package": "aos_demo", "entry": "provider",
                    "node": {"nodes": {"worker": {
                        "namespace": namespace, "parameters": {"threshold": 3},
                    }}},
                    "readiness": {
                        "node": f"{namespace}/worker", "timeout": 15,
                        "endpoints": [
                            {"kind": "publisher", "name": f"{namespace}/status",
                             "interface": {"package": "aos_demo", "resource": "status"}},
                            {"kind": "subscription", "name": f"{namespace}/labels",
                             "interface": {"package": "aos_demo", "resource": "label"}},
                        ],
                        "lifecycle": {"target": "active", "autostart": True},
                    },
                    "restart": {"policy": "on_failure", "delay": 0.5, "max_restarts": 2},
                },
                {
                    "id": "consumer", "package": "aos_demo", "entry": "consumer",
                    "depends_on": ["provider"],
                    "node": {"nodes": {"consumer": {"namespace": namespace}}},
                    "readiness": {"node": f"{namespace}/consumer", "timeout": 15},
                    "restart": {"policy": "never"},
                },
            ],
            "shutdown": {"sigint_timeout": 5, "sigterm_timeout": 5},
        }
        plan_file = share / "launch/pair.json"
        plan_file.write_text(json.dumps(plan, indent=2) + "\n")
        installed.append(plan_file)
        manifest = {
            "schema_version": 1, "name": "aos_demo", "version": aos.__version__,
            "runtime_abi": "aos.process.v1",
            "entries": {
                role: {"kind": "aos_node", "executable": f"lib/aos_demo/{role}.py" if os.name == "nt" else f"lib/aos_demo/{role}",
                    **({"interpreter": sys.executable, "interpreter_args": ["-I", "-u"]} if os.name == "nt" else {})}
                for role in ("provider", "consumer")
            },
            "resources": {
                "label": {"kind": "interface", "path": "share/aos_demo/interfaces/Label.json"},
                "status": {"kind": "interface", "path": "share/aos_demo/interfaces/Status.json"},
                "pair": {"kind": "launch", "path": "share/aos_demo/launch/pair.json"},
            },
        }
        pending = index / ".aos_demo.pending"
        pending.write_text(json.dumps(manifest, indent=2) + "\n")
        pending.replace(index / "aos_demo.json")
        installed.append(index / "aos_demo.json")
        # Use the production Rust index to validate the fully installed package.
        package = aos.PackageIndex([prefix]).package("aos_demo", version=aos.__version__)
        report = {
            "status": "installed", "prefix": str(prefix), "package": package,
            "python": sys.executable, "sdk_version": aos.__version__,
            "native_sha256": hashlib.sha256(Path(_native.__file__).read_bytes()).hexdigest(),
            "files": {path.relative_to(prefix).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
                      for path in installed},
        }
        (share / "installation.json").write_text(json.dumps(report, indent=2) + "\n")
        committed = True
        print(json.dumps(report))
    finally:
        if not committed:
            shutil.rmtree(prefix)


if __name__ == "__main__":
    main()
