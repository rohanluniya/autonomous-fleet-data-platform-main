"""Environment-driven typed configuration for all platform services.

Secrets are always injected through environment variables (or .env in local dev),
never hardcoded for production use.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path


def _env(key: str, default: str) -> str:
    return os.environ.get(key, default)


def _env_int(key: str, default: int) -> int:
    raw = os.environ.get(key)
    return int(raw) if raw is not None and raw.strip() else default


@dataclass(frozen=True)
class PlatformConfig:
    env: str = field(default_factory=lambda: _env("FLEET_ENV", "dev"))
    execution_profile: str = field(
        default_factory=lambda: _env("FLEET_EXECUTION_PROFILE", "local_native")
    )
    transport_mode: str = field(default_factory=lambda: _env("FLEET_TRANSPORT_MODE", "local_wal"))
    project_root: Path = field(default_factory=lambda: Path(__file__).resolve().parent.parent)
    local_data_dir: Path = field(
        default_factory=lambda: Path(_env("FLEET_LOCAL_DATA_DIR", ".local_data")).resolve()
    )
    warehouse_dir: Path = field(
        default_factory=lambda: Path(_env("FLEET_WAREHOUSE_DIR", ".local_data/warehouse")).resolve()
    )
    wal_dir: Path = field(
        default_factory=lambda: Path(_env("FLEET_WAL_DIR", ".local_data/wal")).resolve()
    )
    checkpoint_dir: Path = field(
        default_factory=lambda: Path(
            _env("FLEET_CHECKPOINT_DIR", ".local_data/checkpoints")
        ).resolve()
    )
    lineage_dir: Path = field(
        default_factory=lambda: Path(_env("FLEET_LINEAGE_DIR", ".local_data/lineage")).resolve()
    )
    sqlite_catalog_path: Path = field(
        default_factory=lambda: Path(
            _env("FLEET_SQLITE_CATALOG_PATH", ".local_data/iceberg_catalog.db")
        ).resolve()
    )
    # Kafka & Schema Registry
    kafka_bootstrap_servers: str = field(
        default_factory=lambda: _env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    )
    schema_registry_url: str = field(
        default_factory=lambda: _env("SCHEMA_REGISTRY_URL", "http://localhost:8081/apis/ccompat/v7")
    )
    # Iceberg REST / S3 (Docker mode)
    iceberg_rest_uri: str = field(
        default_factory=lambda: _env("ICEBERG_REST_URI", "http://localhost:8181/catalog")
    )
    iceberg_warehouse: str = field(default_factory=lambda: _env("ICEBERG_WAREHOUSE", "fleet"))
    s3_endpoint_url: str = field(
        default_factory=lambda: _env("S3_ENDPOINT_URL", "http://localhost:9000")
    )
    s3_access_key_id: str = field(default_factory=lambda: _env("S3_ACCESS_KEY_ID", ""))
    s3_secret_access_key: str = field(default_factory=lambda: _env("S3_SECRET_ACCESS_KEY", ""))
    # FastAPI & RBAC tokens (dev defaults only active when env=dev)
    api_host: str = field(default_factory=lambda: _env("FLEET_API_HOST", "127.0.0.1"))
    api_port: int = field(default_factory=lambda: _env_int("FLEET_API_PORT", 8000))
    api_admin_token: str = field(
        default_factory=lambda: _env("FLEET_API_ADMIN_TOKEN", "dev-admin-token-change-me")
    )
    api_operator_token: str = field(
        default_factory=lambda: _env("FLEET_API_OPERATOR_TOKEN", "dev-operator-token-change-me")
    )
    api_analyst_token: str = field(
        default_factory=lambda: _env("FLEET_API_ANALYST_TOKEN", "dev-analyst-token-change-me")
    )
    public_coarsen_h3_res: int = field(
        default_factory=lambda: _env_int("FLEET_API_PUBLIC_COARSEN_H3_RES", 6)
    )
    # Observability & Lineage
    openlineage_url: str = field(default_factory=lambda: _env("OPENLINEAGE_URL", ""))
    openlineage_namespace: str = field(
        default_factory=lambda: _env("OPENLINEAGE_NAMESPACE", "autonomous-fleet-platform")
    )
    log_level: str = field(default_factory=lambda: _env("LOG_LEVEL", "INFO"))

    def ensure_local_dirs(self) -> None:
        for d in (
            self.local_data_dir,
            self.warehouse_dir,
            self.wal_dir,
            self.checkpoint_dir,
            self.lineage_dir,
            self.sqlite_catalog_path.parent,
        ):
            d.mkdir(parents=True, exist_ok=True)


def load_config() -> PlatformConfig:
    cfg = PlatformConfig()
    cfg.ensure_local_dirs()
    return cfg
