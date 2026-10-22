#!/usr/bin/env python3
"""Build reproducible AOS plugin archives from an explicit public file list."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
from pathlib import Path
import tarfile
import zipfile

ROOT = Path(__file__).resolve().parents[1]
FILES = (
    "LICENSE", "README.md", "plugin.json",
    ".codex-plugin/plugin.json", ".claude-plugin/plugin.json",
    "skills/aos/SKILL.md", "skills/aos-setup/SKILL.md", "skills/aos-develop/SKILL.md",
    "skills/aos/references/usage.md", "skills/aos/references/developing.md",
    "skills/aos/references/discovery.md",
    "skills/aos/scripts/aos_manager.py", "skills/aos/scripts/distribution.json",
    "skills/aos/scripts/setup.sh", "skills/aos/scripts/setup.ps1",
)


def package(plugin, output):
    plugin, output = Path(plugin), Path(output)
    manifest = json.loads((plugin/"plugin.json").read_text())
    distribution = json.loads((plugin/"skills/aos/scripts/distribution.json").read_text())
    version = manifest["version"]
    if version != distribution["manager_version"]:
        raise ValueError("Plugin and manager versions differ")
    inventory = []
    for name in FILES:
        path = plugin/name
        if path.is_symlink() or not path.is_file():
            raise ValueError(f"Required regular file is missing: {name}")
        inventory.append((name, path.read_bytes(), 0o755 if name.endswith((".sh", ".py")) else 0o644))
    output.mkdir(parents=True, exist_ok=True)
    tar_path = output/f"aos-agent-{version}.tar.gz"
    with tar_path.open("wb") as raw, gzip.GzipFile(filename="", fileobj=raw, mode="wb", mtime=0) as zipped:
        with tarfile.open(fileobj=zipped, mode="w") as archive:
            for name, content, mode in inventory:
                info = tarfile.TarInfo("aos-agent/"+name)
                info.size, info.mode, info.mtime = len(content), mode, 0
                archive.addfile(info, io.BytesIO(content))
    archives = [tar_path]
    for name, prefix in ((f"aos-agent-{version}.zip", "aos-agent/"), ("aos.plugin", "")):
        path = output/name
        with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            for relative, content, mode in inventory:
                info = zipfile.ZipInfo(prefix+relative)
                info.create_system = 3
                info.external_attr = (0o100000 | mode) << 16
                info.compress_type = zipfile.ZIP_DEFLATED
                archive.writestr(info, content)
        archives.append(path)
    assets = []
    for path in archives:
        sha = hashlib.sha256(path.read_bytes()).hexdigest()
        (output/(path.name+".sha256")).write_text(f"{sha}  {path.name}\n")
        assets.append({"name": path.name, "digest": "sha256:"+sha, "size": path.stat().st_size})
    (output/"SHA256SUMS").write_text("".join(f"{a['digest'][7:]}  {a['name']}\n" for a in assets))
    return {"schema_version": 1, "manager_version": version,
            "runtime_version": distribution["runtime_version"], "assets": assets}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plugin", type=Path, default=ROOT/"plugins/aos")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--catalog", type=Path)
    args = parser.parse_args()
    result = package(args.plugin, args.output)
    if args.catalog:
        args.catalog.parent.mkdir(parents=True, exist_ok=True)
        args.catalog.write_text(json.dumps(result, indent=2)+"\n")
    print(json.dumps(result))


if __name__ == "__main__":
    main()
