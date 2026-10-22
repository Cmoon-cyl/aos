#!/usr/bin/env python3
"""Run the exact DeMux unit test and prove it detects an all-message scope.

The published zenoh crate includes link_weights tests but omits their local
zenoh_test dev-dependency. In an isolated copy only, omit that unrelated test
module. Production source, the tested routing files and both manifests are
unchanged. Cargo uses the crate's own locked test graph, not AOS Cargo.lock.
Run inside the configured Rust toolchain; test dependencies must be cached.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import time


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def tree(root: Path) -> dict[str, str]:
    return {
        p.relative_to(root).as_posix(): digest(p.read_bytes())
        for p in sorted(root.rglob("*")) if p.is_file()
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    root = Path(__file__).resolve().parents[1]
    source = root / "vendor/zenoh"
    original = tree(source)
    result = {"source_sha256": original, "runs": []}
    with tempfile.TemporaryDirectory(prefix="aos-zenoh-demux-unit-") as temporary:
        copied = Path(temporary) / "zenoh"
        shutil.copytree(source, copied)
        support = copied / "src/tests/mod.rs"
        original_support = support.read_text()
        assert original_support.count("mod link_weights;\n") == 1
        support.write_text(original_support.replace(
            "mod link_weights;\n",
            "// Isolated DeMux harness: omitted unrelated missing zenoh_test dependency.\n",
        ))
        altered = tree(copied)
        changed = [name for name in original if original[name] != altered[name]]
        assert changed == ["src/tests/mod.rs"], changed
        result["isolated_test_support_change"] = {
            "path": "src/tests/mod.rs",
            "original_sha256": original["src/tests/mod.rs"],
            "harness_sha256": altered["src/tests/mod.rs"],
            "reason": "published crate omits the unrelated link_weights zenoh_test dependency",
        }
        command = [
            "cargo", "test", "--manifest-path", str(copied / "Cargo.toml"),
            "--offline", "--locked", "--no-default-features",
            "--features", "transport_tcp,unstable,internal", "--lib",
            "push_ingress_does_not_begin_a_declaration_batch", "--", "--nocapture",
        ]
        environment = os.environ.copy()
        environment.setdefault("CARGO_TARGET_DIR", str(Path(temporary) / "target"))
        demux = copied / "src/net/primitives/demux.rs"
        good = demux.read_text()
        marker = "        let _span = tracing::enabled!"
        assert good.count(marker) == 1
        for mutation in (False, True):
            if mutation:
                demux.write_text(good.replace(
                    marker,
                    "        let _drain = self.face.tables.declarations.drain_after_background();\n"
                    + marker,
                ))
            start = time.monotonic()
            run = subprocess.run(command, env=environment, text=True,
                                 stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            name = "routing-demux-push-scope-mutation" if mutation else "routing-demux-push-scope"
            (output / f"{name}.log").write_text(run.stdout)
            record = {
                "mutation": mutation,
                "command": command,
                "exit_code": run.returncode,
                "elapsed_seconds": round(time.monotonic() - start, 3),
                "test_summary": [line for line in run.stdout.splitlines() if "test result:" in line],
                "tested_demux_sha256": digest(demux.read_bytes()),
            }
            result["runs"].append(record)
            print(json.dumps(record), flush=True)
            if mutation:
                assert run.returncode != 0, "all-message scope mutation was not rejected"
                assert "1 failed" in run.stdout and "left: 128" in run.stdout, run.stdout[-8000:]
            else:
                assert run.returncode == 0, run.stdout[-8000:]
                assert "1 passed; 0 failed" in run.stdout, run.stdout[-8000:]
        demux.write_text(good)
        assert tree(copied) == altered
    assert tree(source) == original, "test harness changed production source"
    result["passed"] = True
    (output / "routing-demux-push-scope.json").write_text(json.dumps(result, indent=2) + "\n")


if __name__ == "__main__":
    main()
