"""Geospatial intelligence: H3 indexing, Shapely geofences, haversine distance,
route deviation checks, GPS validity bounds, and privacy-preserving coarsening.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any

import h3
from shapely.geometry import LineString, Point, Polygon

EARTH_RADIUS_KM = 6371.0088


@dataclass(frozen=True)
class MetroRegion:
    region_id: str
    display_name: str
    center_lat: float
    center_lon: float
    bbox: tuple[float, float, float, float]  # (min_lat, min_lon, max_lat, max_lon)
    geofences: dict[str, Polygon]
    routes: dict[str, list[tuple[float, float]]]


def _rect_poly(min_lat: float, min_lon: float, max_lat: float, max_lon: float) -> Polygon:
    # Shapely uses (x=lon, y=lat) in WGS84 / EPSG:4326
    return Polygon(
        [
            (min_lon, min_lat),
            (max_lon, min_lat),
            (max_lon, max_lat),
            (min_lon, max_lat),
            (min_lon, min_lat),
        ]
    )


METRO_REGIONS: dict[str, MetroRegion] = {
    "sf_bay": MetroRegion(
        region_id="sf_bay",
        display_name="San Francisco Bay",
        center_lat=37.7749,
        center_lon=-122.4194,
        bbox=(37.705, -122.515, 37.812, -122.365),
        geofences={
            "sf_downtown_core": _rect_poly(37.772, -122.425, 37.798, -122.388),
            "sf_mission_corridor": _rect_poly(37.745, -122.432, 37.772, -122.405),
            "sf_depot_dogpatch": _rect_poly(37.752, -122.398, 37.768, -122.380),
        },
        routes={
            "sf_route_market": [
                (37.7620, -122.4350),
                (37.7715, -122.4235),
                (37.7785, -122.4145),
                (37.7870, -122.4035),
                (37.7945, -122.3945),
            ],
            "sf_route_embarcadero": [
                (37.7600, -122.3885),
                (37.7740, -122.3870),
                (37.7865, -122.3895),
                (37.7985, -122.3990),
                (37.8065, -122.4125),
            ],
            "sf_route_mission": [
                (37.7380, -122.4210),
                (37.7510, -122.4185),
                (37.7640, -122.4195),
                (37.7745, -122.4160),
            ],
        },
    ),
    "phoenix_metro": MetroRegion(
        region_id="phoenix_metro",
        display_name="Phoenix Metro",
        center_lat=33.4484,
        center_lon=-112.0740,
        bbox=(33.370, -112.160, 33.540, -111.900),
        geofences={
            "phx_downtown": _rect_poly(33.435, -112.090, 33.468, -112.055),
            "phx_tempe_hub": _rect_poly(33.405, -111.960, 33.440, -111.915),
            "phx_sky_harbor_zone": _rect_poly(33.425, -112.035, 33.450, -111.985),
        },
        routes={
            "phx_route_central": [
                (33.4150, -112.0740),
                (33.4350, -112.0740),
                (33.4550, -112.0740),
                (33.4850, -112.0740),
            ],
            "phx_route_tempe": [
                (33.4480, -112.0700),
                (33.4420, -112.0250),
                (33.4310, -111.9750),
                (33.4220, -111.9350),
            ],
        },
    ),
    "austin_metro": MetroRegion(
        region_id="austin_metro",
        display_name="Austin Metro",
        center_lat=30.2672,
        center_lon=-97.7431,
        bbox=(30.195, -97.835, 30.355, -97.670),
        geofences={
            "atx_downtown": _rect_poly(30.258, -97.760, 30.282, -97.730),
            "atx_east_depot": _rect_poly(30.250, -97.725, 30.275, -97.695),
            "atx_domain_north": _rect_poly(30.315, -97.745, 30.345, -97.710),
        },
        routes={
            "atx_route_congress": [
                (30.2320, -97.7520),
                (30.2540, -97.7475),
                (30.2710, -97.7425),
                (30.2950, -97.7395),
            ],
            "atx_route_lamar": [
                (30.2420, -97.7710),
                (30.2660, -97.7560),
                (30.2920, -97.7440),
                (30.3220, -97.7300),
            ],
        },
    ),
}


def validate_gps(latitude: float, longitude: float) -> bool:
    """Validate WGS84 coordinate bounds and exclude null-island (0, 0) glitches."""
    if not (-90.0 <= latitude <= 90.0 and -180.0 <= longitude <= 180.0):
        return False
    return not (abs(latitude) < 1e-6 and abs(longitude) < 1e-6)


def to_h3_cell(latitude: float, longitude: float, resolution: int = 6) -> str | None:
    if not validate_gps(latitude, longitude):
        return None
    return str(h3.latlng_to_cell(latitude, longitude, resolution))


def h3_cell_center(cell: str) -> tuple[float, float]:
    lat, lon = h3.cell_to_latlng(cell)
    return float(lat), float(lon)


def h3_cell_boundary_geojson(cell: str) -> list[list[float]]:
    """Return closed GeoJSON polygon ring [[lon, lat], ...] for an H3 cell."""
    boundary = h3.cell_to_boundary(cell)
    ring = [[float(lon), float(lat)] for lat, lon in boundary]
    if ring:
        ring.append(ring[0])
    return ring


def coarsen_location(
    latitude: float,
    longitude: float,
    resolution: int = 6,
) -> dict[str, Any]:
    """Coarsen exact GPS coordinates to H3 cell centroid for non-privileged roles."""
    cell = to_h3_cell(latitude, longitude, resolution=resolution)
    if cell is None:
        return {"h3_cell": None, "coarsened_lat": None, "coarsened_lon": None}
    clat, clon = h3_cell_center(cell)
    return {
        "h3_cell": cell,
        "coarsened_lat": round(clat, 4),
        "coarsened_lon": round(clon, 4),
        "h3_resolution": resolution,
    }


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in kilometers between two WGS84 points."""
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2.0) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2.0) ** 2
    return 2.0 * EARTH_RADIUS_KM * math.atan2(math.sqrt(a), math.sqrt(max(0.0, 1.0 - a)))


def match_geofences(region_id: str, latitude: float, longitude: float) -> list[str]:
    """Return sorted list of geofence IDs containing the point."""
    if not validate_gps(latitude, longitude):
        return []
    region = METRO_REGIONS.get(region_id)
    if region is None:
        return []
    pt = Point(longitude, latitude)
    return sorted(name for name, poly in region.geofences.items() if poly.contains(pt))


def route_deviation_meters(
    region_id: str,
    route_id: str,
    latitude: float,
    longitude: float,
) -> float | None:
    """Approximate perpendicular distance in meters from a point to its planned route polyline."""
    region = METRO_REGIONS.get(region_id)
    if region is None or route_id not in region.routes:
        return None
    waypoints = region.routes[route_id]
    line = LineString([(lon, lat) for lat, lon in waypoints])
    pt = Point(longitude, latitude)
    nearest_dist_deg = pt.distance(line)
    # Approximate meters per degree at mid-latitudes (~111,139 m/deg)
    return float(nearest_dist_deg * 111_139.0)


def export_geofences_geojson() -> dict[str, Any]:
    """Export all synthetic metro geofences and planned routes as GeoJSON for self-hosted dashboard rendering."""
    features: list[dict[str, Any]] = []
    for region in METRO_REGIONS.values():
        for fence_name, poly in region.geofences.items():
            coords = [[list(pt) for pt in poly.exterior.coords]]
            features.append(
                {
                    "type": "Feature",
                    "properties": {
                        "feature_type": "geofence",
                        "region": region.region_id,
                        "name": fence_name,
                    },
                    "geometry": {"type": "Polygon", "coordinates": coords},
                }
            )
        for route_name, waypoints in region.routes.items():
            features.append(
                {
                    "type": "Feature",
                    "properties": {
                        "feature_type": "route",
                        "region": region.region_id,
                        "name": route_name,
                    },
                    "geometry": {
                        "type": "LineString",
                        "coordinates": [[lon, lat] for lat, lon in waypoints],
                    },
                }
            )
    return {"type": "FeatureCollection", "features": features}
