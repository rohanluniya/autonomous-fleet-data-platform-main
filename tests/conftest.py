"""Shared pytest fixtures providing isolated temporary warehouses, WALs, and checkpoints."""

from __future__ import annotations

from pathlib import Path

import pytest

from fleet_platform.config import PlatformConfig


@pytest.fixture
def isolated_config(tmp_path: Path) -> PlatformConfig:
    cfg = PlatformConfig(
        env="dev",
        execution_profile="local_native",
        transport_mode="local_wal",
        local_data_dir=tmp_path / "data",
        warehouse_dir=tmp_path / "warehouse",
        wal_dir=tmp_path / "wal",
        checkpoint_dir=tmp_path / "checkpoints",
        lineage_dir=tmp_path / "lineage",
        sqlite_catalog_path=tmp_path / "iceberg_catalog.db",
    )
    cfg.ensure_local_dirs()
    return cfg
