#!/usr/bin/env python3
"""Verify open-source license compliance and disallow floating :latest container image tags."""

from __future__ import annotations

import importlib.metadata as meta
import json
import re
import sys
from pathlib import Path

DISALLOWED_PATTERNS = re.compile(
    r"(proprietary|commercial|confluent community|source-available|bsl|sspl)",
    re.IGNORECASE,
)


def main() -> None:
    root = Path(__file__).resolve().parent.parent
    violations: list[str] = []
    audited_python = 0

    for dist in meta.distributions():
        audited_python += 1
        name = dist.metadata["Name"]
        lic = dist.metadata.get("License-Expression") or dist.metadata.get("License") or ""
        first_line = lic.splitlines()[0] if lic else ""
        if DISALLOWED_PATTERNS.search(first_line):
            violations.append(f"Python package '{name}' has restricted license: {first_line}")

    # Check container manifests for floating :latest tags
    for manifest in (
        root / "infra" / "docker" / "docker-compose.yml",
        root / "infra" / "kubernetes" / "fleet-platform.yaml",
    ):
        if not manifest.exists():
            continue
        for lineno, line in enumerate(manifest.read_text(encoding="utf-8").splitlines(), 1):
            if re.search(r"^\s*image:.*:latest\b", line):
                violations.append(
                    f"{manifest.name}:{lineno} uses prohibited :latest tag: {line.strip()}"
                )

    report = {
        "python_packages_audited": audited_python,
        "violations": violations,
        "status": "PASS" if not violations else "FAIL",
    }
    print(json.dumps(report, indent=2))
    if violations:
        sys.exit(1)


if __name__ == "__main__":
    main()
