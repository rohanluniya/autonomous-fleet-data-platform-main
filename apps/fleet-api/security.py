"""API authentication, Role-Based Access Control (RBAC), and spatial privacy enforcement.

Roles:
- admin: Full access including exact vehicle coordinates and platform operational diagnostics.
- operator: Operational access including exact vehicle coordinates (audited).
- analyst: Read-only analytical access; exact GPS coordinates are coarsened to H3 resolution 6.
- viewer: Dashboard public/read-only role (default when no bearer token is sent in local dev);
          receives strictly coarsened H3 resolution 6 centroids, never raw GPS traces.
"""

from __future__ import annotations

import hmac
from dataclasses import dataclass
from typing import Any

from fastapi import Header, HTTPException, Request, status

from fleet_platform.config import PlatformConfig, load_config
from fleet_platform.geospatial.spatial import coarsen_location
from fleet_platform.observability import get_logger

_AUDIT_LOG = get_logger("fleet-api", "security")


@dataclass(frozen=True)
class Principal:
    role: str
    can_view_raw_gps: bool
    authenticated: bool


def resolve_principal(
    request: Request,
    authorization: str | None = Header(default=None),
) -> Principal:
    cfg: PlatformConfig = getattr(request.app.state, "config", None) or load_config()

    if not authorization:
        # In local dev mode, unauthenticated dashboard views receive strictly coarsened 'viewer' role.
        # In staging/prod, unauthenticated access is rejected with 401.
        if cfg.env == "dev":
            return Principal(role="viewer", can_view_raw_gps=False, authenticated=False)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing Authorization Bearer token",
        )

    parts = authorization.split(" ", 1)
    if len(parts) != 2 or parts[0].lower() != "bearer":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authorization header must use Bearer scheme",
        )
    token = parts[1].strip()

    if hmac.compare_digest(token, cfg.api_admin_token):
        return Principal(role="admin", can_view_raw_gps=True, authenticated=True)
    if hmac.compare_digest(token, cfg.api_operator_token):
        return Principal(role="operator", can_view_raw_gps=True, authenticated=True)
    if hmac.compare_digest(token, cfg.api_analyst_token):
        return Principal(role="analyst", can_view_raw_gps=False, authenticated=True)

    raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="Invalid API token",
    )


def apply_location_privacy(
    row: dict[str, Any],
    principal: Principal,
    *,
    coarsen_res: int = 6,
    correlation_id: str = "",
) -> dict[str, Any]:
    """Enforce RBAC raw vs. coarsened geospatial output and audit privileged GPS reads."""
    out = dict(row)
    lat = out.get("latitude")
    lon = out.get("longitude")
    if principal.can_view_raw_gps:
        out["location_precision"] = "raw_gps_authorized"
        _AUDIT_LOG.audit(
            "audit.raw_location_read",
            f"Authorized raw GPS read for vehicle={out.get('vehicle_id', 'batch')}",
            correlation_id=correlation_id,
            context={"role": principal.role, "vehicle_id": out.get("vehicle_id")},
        )
        return out

    # Non-privileged roles: strip exact coordinates and serve coarsened H3 centroid only
    if lat is not None and lon is not None:
        coarsened = coarsen_location(float(lat), float(lon), resolution=coarsen_res)
        out["latitude"] = coarsened["coarsened_lat"]
        out["longitude"] = coarsened["coarsened_lon"]
        out["h3_cell_res6"] = coarsened["h3_cell"]
    out["location_precision"] = f"coarsened_h3_res{coarsen_res}"
    return out
