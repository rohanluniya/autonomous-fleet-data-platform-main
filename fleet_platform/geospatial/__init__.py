"""Geospatial indexing, geofencing, and privacy-safe coarsening utilities."""

from fleet_platform.geospatial.spatial import (
    METRO_REGIONS,
    MetroRegion,
    coarsen_location,
    export_geofences_geojson,
    h3_cell_boundary_geojson,
    h3_cell_center,
    haversine_km,
    match_geofences,
    route_deviation_meters,
    to_h3_cell,
    validate_gps,
)

__all__ = [
    "METRO_REGIONS",
    "MetroRegion",
    "coarsen_location",
    "export_geofences_geojson",
    "h3_cell_boundary_geojson",
    "h3_cell_center",
    "haversine_km",
    "match_geofences",
    "route_deviation_meters",
    "to_h3_cell",
    "validate_gps",
]
