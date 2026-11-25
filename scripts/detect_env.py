#!/usr/bin/env python3
"""Detect host platform, CPU/RAM constraints, and available container/JVM runtimes."""

from __future__ import annotations

import json
import os
import platform
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

import psutil


def _cmd_version(cmd: list[str]) -> str | None:
    if not shutil.which(cmd[0]):
        return None
    try:
        out = subprocess.check_output(cmd, stderr=subprocess.STDOUT, text=True, timeout=5)
        return out.strip().splitlines()[0] if out.strip() else "available"
    except Exception:
        return None


def detect_environment() -> dict[str, Any]:
    mem = psutil.virtual_memory()
    disk = shutil.disk_usage(Path.cwd())
    docker_ver = _cmd_version(["docker", "--version"])
    docker_running = False
    if docker_ver:
        try:
            subprocess.check_call(
                ["docker", "info"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=5,
            )
            docker_running = True
        except Exception:
            docker_running = False

    java_ver = _cmd_version(["java", "-version"])
    if java_ver and "Unable to locate a Java Runtime" in java_ver:
        java_ver = None

    arch = platform.machine().lower()
    otel_arch = "arm64" if arch in ("arm64", "aarch64") else "amd64"
    ram_gb = round(mem.total / (1024**3), 2)

    recommended_profile = "docker" if docker_running and ram_gb >= 12.0 else "local_native"
    fallback_reasons: list[str] = []
    if not docker_ver:
        fallback_reasons.append("docker CLI not installed on host")
    elif not docker_running:
        fallback_reasons.append("docker daemon not running")
    if ram_gb < 12.0:
        fallback_reasons.append(
            f"host RAM ({ram_gb} GiB) below 12 GiB recommended for full Docker stack"
        )
    if not java_ver:
        fallback_reasons.append("no host Java runtime available for standalone JVM engines")

    return {
        "os": platform.system(),
        "os_release": platform.release(),
        "architecture": arch,
        "otel_arch_tag": otel_arch,
        "cpu_cores_logical": os.cpu_count() or 1,
        "memory_total_gib": ram_gb,
        "memory_available_gib": round(mem.available / (1024**3), 2),
        "disk_free_gib": round(disk.free / (1024**3), 2),
        "python_version": sys.version.split()[0],
        "uv_version": _cmd_version(["uv", "--version"]),
        "docker_version": docker_ver,
        "docker_daemon_running": docker_running,
        "java_version": java_ver,
        "recommended_execution_profile": recommended_profile,
        "docker_compose_compatible_profiles": (
            ["core", "analytics", "observability", "full"] if ram_gb >= 16.0 else ["core"]
        ),
        "fallback_reasons": fallback_reasons,
        "apple_silicon_notes": (
            "Uses imresamu/postgis:17-3.6.1-alpine3.23 (native arm64) and Rosetta2 amd64 for marquez:0.51.1."
            if arch in ("arm64", "aarch64")
            else "Native linux/amd64 images across all services."
        ),
    }


def main() -> None:
    info = detect_environment()
    print(json.dumps(info, indent=2))


if __name__ == "__main__":
    main()
