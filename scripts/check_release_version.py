from __future__ import annotations

import json
import os
import sys
import tomllib
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    pyproject = tomllib.loads((ROOT / "pyproject.toml").read_text())
    package = json.loads((ROOT / "typescript/package.json").read_text())
    package_lock = json.loads((ROOT / "typescript/package-lock.json").read_text())
    versions = {
        "pyproject.toml": pyproject["project"]["version"],
        "typescript/package.json": package["version"],
        "typescript/package-lock.json": package_lock["version"],
        "typescript/package-lock.json packages['']": package_lock["packages"][""]["version"],
    }
    expected = next(iter(versions.values()))
    for integration in ("mem0", "compaction", "openai", "anthropic", "interaction", "sqlite", "mcp"):
        base = ROOT / "integrations" / integration
        integration_python = tomllib.loads((base / "python/pyproject.toml").read_text())
        integration_package = json.loads((base / "typescript/package.json").read_text())
        integration_lock = json.loads((base / "typescript/package-lock.json").read_text())
        versions.update({
            f"purra-{integration} Python": integration_python["project"]["version"],
            f"purra-{integration} TypeScript": integration_package["version"],
            f"purra-{integration} lock": integration_lock["version"],
            f"purra-{integration} lock root": integration_lock["packages"][""]["version"],
            f"purra-{integration} PurrA peer": integration_package["peerDependencies"]["purra"],
        })
        if f"purra=={expected}" not in integration_python["project"]["dependencies"]:
            print(f"purra-{integration} Python dependency must match the release", file=sys.stderr)
            return 1
    mismatches = {name: version for name, version in versions.items() if version != expected}
    if mismatches:
        print(f"release version mismatch: {versions}", file=sys.stderr)
        return 1

    tag = sys.argv[1] if len(sys.argv) > 1 else None
    if tag is None and os.getenv("GITHUB_REF_TYPE") == "tag":
        tag = os.getenv("GITHUB_REF_NAME")
    if tag is not None and tag != f"v{expected}":
        print(f"tag {tag!r} does not match release version {expected!r}", file=sys.stderr)
        return 1

    print(f"release version {expected} is consistent")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
