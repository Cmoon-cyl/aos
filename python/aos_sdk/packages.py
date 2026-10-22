"""Read installed package manifests and resources without importing entries.

Explicit prefixes are used alone. Otherwise the SDK reads AOS_PREFIX_PATH and
its installed interpreter's sys.prefix. A launch child uses only its supervisor's
fixed AOS_PREFIX_PATH. No lookup implicitly searches cwd.
The native Rust PackageIndex applies the same path, manifest and ambiguity
checks used by the CLI. An index snapshots its manifests at construction;
create another index after installing or changing a package.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import sys
from collections.abc import Iterable

from . import _native
from .exceptions import InvalidArgumentError


class PackageIndex:
    def __init__(self, prefixes: Iterable[str | os.PathLike[str]] | None = None):
        if isinstance(prefixes, (str, bytes, os.PathLike)):
            raise InvalidArgumentError("prefixes must be a sequence of directory paths")
        paths = None if prefixes is None else [os.fspath(path) for path in prefixes]
        if paths is not None and any(not isinstance(path, str) for path in paths):
            raise InvalidArgumentError("prefixes must contain text paths")
        self._native = _native._PackageIndex(paths, sys.prefix)

    def packages(self) -> dict[str, list[dict]]:
        """Return all candidates; multiple candidates retain their ambiguity."""
        return json.loads(self._native.packages_json())

    def package(self, name: str, *, version: str | None = None) -> dict:
        """Resolve exactly one package, optionally requiring its exact version."""
        return json.loads(self._native.package_json(name, version))

    def resource(
        self, package: str, name: str, *, package_version: str | None = None,
    ) -> dict:
        """Return kind and canonical absolute path of a declared resource."""
        return json.loads(self._native.resource_json(package, name, package_version))


def get_package_prefix(package: str, *, prefixes=None, version: str | None = None) -> Path:
    return Path(PackageIndex(prefixes).package(package, version=version)["prefix"])


def get_package_share_directory(
    package: str, *, prefixes=None, version: str | None = None,
) -> Path:
    installed = PackageIndex(prefixes).package(package, version=version)
    return Path(installed["prefix"]) / "share" / installed["manifest"]["name"]


def get_package_resource(
    package: str, resource: str, *, prefixes=None, package_version: str | None = None,
) -> Path:
    return Path(PackageIndex(prefixes).resource(
        package, resource, package_version=package_version,
    )["path"])
