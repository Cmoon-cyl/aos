#!/usr/bin/env python3
"""Prepare release metadata without changing SDK code or native binaries."""

from __future__ import annotations

import argparse
import base64
import csv
import hashlib
import io
import json
import os
from pathlib import Path, PurePosixPath
import re
import stat
import tempfile
import tomllib
import zipfile


def normalize_sbom(value):
    """CycloneDX bom-ref values are identifiers, not build-machine paths."""
    if isinstance(value, dict):
        result = {key: normalize_sbom(item) for key, item in value.items()}
        if result.get("name", "").startswith("aos-") and "version" in result:
            result["licenses"] = [{"license": {"id": "Apache-2.0"}}]
        return result
    if isinstance(value, list):
        return [normalize_sbom(item) for item in value]
    if isinstance(value, str):
        return re.sub(r"^path\+file://.*?/(crates|vendor)/", r"aos-source:\1/", value)
    return value


def prepare(wheel: Path, source: Path) -> None:
    config = tomllib.loads((source / "pyproject.toml").read_text(encoding="utf-8"))["project"]
    if config.get("license") != "Apache-2.0":
        raise ValueError("Expected the project's Apache-2.0 license declaration")
    with zipfile.ZipFile(wheel) as original:
        infos = original.infolist()
        names = [item.filename for item in infos]
        if len(names) != len(set(names)):
            raise ValueError("Duplicate wheel member")
        for item in infos:
            path = PurePosixPath(item.filename)
            if (path.is_absolute() or ".." in path.parts or "\\" in item.filename
                    or stat.S_ISLNK(item.external_attr >> 16)):
                raise ValueError("Unsafe wheel member")
        files = {item.filename: original.read(item) for item in infos if not item.is_dir()}
    metadata_names = [name for name in files if name.endswith(".dist-info/METADATA")]
    if len(metadata_names) != 1:
        raise ValueError("Expected one wheel METADATA file")
    metadata_name = metadata_names[0]
    prefix = metadata_name.rsplit("/", 1)[0]
    if not prefix.startswith("aos_sdk-"):
        raise ValueError("Expected an aos-sdk wheel")
    original_metadata = files[metadata_name].decode("utf-8")
    headers = original_metadata.partition("\n\n")[0].splitlines()
    # Maturin emits unfolded license headers. Reject folded input instead of
    # accidentally retaining part of an old license declaration.
    if any(line.startswith((" ", "\t")) for line in headers):
        raise ValueError("Folded wheel metadata headers are not supported")
    headers = [line for line in headers if not line.startswith(("License:", "License-Expression:", "License-File:"))]
    headers.append("License-Expression: Apache-2.0")
    for relative in config["license-files"]:
        path = PurePosixPath(relative)
        if path.is_absolute() or ".." in path.parts:
            raise ValueError("License path must be repository-relative")
        headers.append("License-File: " + relative)
        files[prefix + "/licenses/" + relative] = (source / relative).read_bytes()
    files[metadata_name] = ("\n".join(headers) + "\n\n" + (source / "README.md").read_text(encoding="utf-8")).encode("utf-8")
    for name in list(files):
        if name.startswith(prefix + "/sboms/") and name.endswith(".json"):
            files[name] = (json.dumps(normalize_sbom(json.loads(files[name])), indent=2) + "\n").encode("utf-8")
    record_name = prefix + "/RECORD"
    record = io.StringIO(newline="")
    writer = csv.writer(record, lineterminator="\n")
    for name, data in sorted(files.items()):
        if name == record_name:
            continue
        digest = base64.urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(b"=").decode("ascii")
        writer.writerow([name, "sha256=" + digest, str(len(data))])
    writer.writerow([record_name, "", ""])
    files[record_name] = record.getvalue().encode("utf-8")
    existing = {item.filename: item for item in infos}
    descriptor, temporary = tempfile.mkstemp(prefix=".public-wheel-", suffix=".whl", dir=wheel.parent)
    os.close(descriptor)
    try:
        with zipfile.ZipFile(temporary, "w", zipfile.ZIP_DEFLATED) as target:
            for name, data in sorted(files.items()):
                info = existing.get(name, zipfile.ZipInfo(name))
                info.compress_type = zipfile.ZIP_DEFLATED
                target.writestr(info, data)
        os.replace(temporary, wheel)
    finally:
        Path(temporary).unlink(missing_ok=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wheel", type=Path, required=True)
    parser.add_argument("--source-repo", type=Path, required=True)
    args = parser.parse_args()
    prepare(args.wheel.resolve(), args.source_repo.resolve())
