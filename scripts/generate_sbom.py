#!/usr/bin/env python3
"""Generate a CycloneDX 1.5 JSON SBOM from installed Python packages and pinned Docker images."""

from __future__ import annotations

import argparse
import importlib.metadata as meta
import json
import re
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


def parse_docker_images(compose_path: Path) -> list[dict[str, str]]:
    images: list[dict[str, str]] = []
    if not compose_path.exists():
        return images
    for line in compose_path.read_text(encoding="utf-8").splitlines():
        m = re.match(r"^\s*image:\s*['\"]?([^'\"\s]+)['\"]?", line)
        if m:
            ref = m.group(1)
            if ":" in ref:
                name, ver = ref.rsplit(":", 1)
            else:
                name, ver = ref, "unpinned"
            images.append({"name": name, "version": ver, "ref": ref})
    return images


def generate_sbom(output_path: Path) -> dict[str, Any]:
    root = Path(__file__).resolve().parent.parent
    components: list[dict[str, Any]] = []

    for dist in sorted(meta.distributions(), key=lambda d: (d.metadata["Name"] or "").lower()):
        name = dist.metadata["Name"]
        version = dist.version
        lic = dist.metadata.get("License-Expression") or dist.metadata.get("License") or "UNKNOWN"
        lic_clean = lic.splitlines()[0][:80] if lic else "UNKNOWN"
        components.append(
            {
                "type": "library",
                "name": name,
                "version": version,
                "purl": f"pkg:pypi/{name.lower()}@{version}",
                "licenses": [{"license": {"name": lic_clean}}],
            }
        )

    for img in parse_docker_images(root / "infra" / "docker" / "docker-compose.yml"):
        components.append(
            {
                "type": "container",
                "name": img["name"],
                "version": img["version"],
                "purl": f"pkg:oci/{img['name']}@{img['version']}",
            }
        )

    sbom: dict[str, Any] = {
        "bomFormat": "CycloneDX",
        "specVersion": "1.5",
        "serialNumber": f"urn:uuid:{uuid.uuid4()}",
        "version": 1,
        "metadata": {
            "timestamp": datetime.now(UTC).isoformat(),
            "component": {
                "type": "application",
                "name": "autonomous-fleet-data-platform",
                "version": "0.1.0",
                "licenses": [{"license": {"id": "Apache-2.0"}}],
            },
        },
        "components": components,
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(sbom, indent=2), encoding="utf-8")
    return sbom


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate CycloneDX 1.5 SBOM.")
    parser.add_argument("--output", type=Path, default=Path("results/security/sbom.cdx.json"))
    args = parser.parse_args()
    sbom = generate_sbom(args.output)
    print(
        f"Generated CycloneDX 1.5 SBOM with {len(sbom['components'])} components at {args.output}"
    )


if __name__ == "__main__":
    main()
