"""Generate route shape and stop shapefiles from GTFS data.

This script creates shapefiles for CTA train lines, bus routes, and stops
for use in the dashboard map display. Train lines are offset to prevent
overlapping where routes share tracks.
"""

from pathlib import Path

import geopandas as gpd
import gtfs_kit as gk
import pandas as pd
from shapely.geometry import MultiLineString, Point

# Offset distances (in meters) for train lines to prevent overlap
# Positive values offset left, negative values offset right
TRAIN_LINE_OFFSETS = {
    "Blue": -56.0,
    "Brn": -40.0,
    "G": -24.0,
    "Org": -8.0,
    "P": 8.0,
    "Pink": 24.0,
    "Red": 48.0,
    "Y": 56.0,
}


def offset_lines(geom, dist_m: float):
    """Apply a parallel offset to a LineString or MultiLineString.

    Args:
        geom: A Shapely LineString or MultiLineString geometry.
        dist_m: Offset distance in meters. Positive offsets left, negative right.

    Returns:
        The offset geometry, preserving the original type where possible.
    """
    if geom is None or dist_m == 0:
        return geom

    side = "left" if dist_m > 0 else "right"
    d = abs(dist_m)

    def offset_linestring(ls):
        """Offset a single LineString, handling various return types."""
        off = ls.parallel_offset(d, side=side, join_style=2)

        if off.is_empty:
            return []
        if off.geom_type == "LineString":
            return [off]
        if off.geom_type == "MultiLineString":
            return list(off.geoms)
        return []

    if geom.geom_type == "LineString":
        parts = offset_linestring(geom)
        if not parts:
            return geom
        return parts[0] if len(parts) == 1 else MultiLineString(parts)

    if geom.geom_type == "MultiLineString":
        parts = []
        for ls in geom.geoms:
            parts.extend(offset_linestring(ls))
        if not parts:
            return geom
        return MultiLineString(parts)

    return geom


def create_train_shapes(feed, output_dir: Path) -> gpd.GeoDataFrame:
    """Create train route shapes with offsets to prevent overlap.

    Args:
        feed: A gtfs_kit Feed object.
        output_dir: Directory to save the output shapefile.

    Returns:
        GeoDataFrame of train routes.
    """
    print("Processing train routes...")

    routes_gdf = gk.routes.get_routes(feed, as_gdf=True, use_utm=True)

    # Filter to train routes (those without route_short_name)
    trains_gdf = routes_gdf[routes_gdf["route_short_name"].isna()].copy()

    # Dissolve by route_id to merge all shapes for each route
    trains_gdf = (
        trains_gdf.dissolve(
            by="route_id",
            aggfunc={
                "route_long_name": "first",
                "route_color": "first",
                "route_text_color": "first",
            },
        )
        .reset_index()
    )

    # Apply offsets to prevent overlapping
    trains_gdf["geometry"] = trains_gdf.apply(
        lambda r: offset_lines(r.geometry, TRAIN_LINE_OFFSETS.get(r.route_id, 0)),
        axis=1,
    )

    # Convert to WGS84 for web mapping
    trains_gdf = trains_gdf.to_crs("EPSG:4326")

    # Save to shapefile
    output_path = output_dir / "train_shapes"
    output_path.mkdir(parents=True, exist_ok=True)
    trains_gdf.to_file(output_path / "trains.shp")

    print(f"  Saved {len(trains_gdf)} train routes to {output_path / 'trains.shp'}")
    return trains_gdf


def create_bus_shapes(feed, output_dir: Path) -> gpd.GeoDataFrame:
    """Create bus route shapes.

    Args:
        feed: A gtfs_kit Feed object.
        output_dir: Directory to save the output shapefile.

    Returns:
        GeoDataFrame of bus routes.
    """
    print("Processing bus routes...")

    routes_gdf = gk.routes.get_routes(feed, as_gdf=True, use_utm=True)

    # Filter to bus routes (those with route_short_name)
    buses_gdf = routes_gdf[routes_gdf["route_short_name"].notna()].copy()

    # Convert to WGS84 for web mapping
    buses_gdf = buses_gdf.to_crs("EPSG:4326")

    # Save to shapefile
    output_path = output_dir / "bus_shapes"
    output_path.mkdir(parents=True, exist_ok=True)
    buses_gdf.to_file(output_path / "buses.shp")

    print(f"  Saved {len(buses_gdf)} bus routes to {output_path / 'buses.shp'}")
    return buses_gdf


def create_stop_shapes(data_dir: Path, output_dir: Path) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """Create train station and bus stop shapefiles.

    Args:
        data_dir: Directory containing GTFS data.
        output_dir: Directory to save the output shapefiles.

    Returns:
        Tuple of (train_stations_gdf, bus_stops_gdf).
    """
    print("Processing stops...")

    stops_df = pd.read_csv(data_dir / "gtfs" / "stops.txt")

    # Train stations (location_type=1)
    train_stations = stops_df[stops_df["location_type"] == 1].copy()
    train_stations_gdf = gpd.GeoDataFrame(
        train_stations[["stop_id", "stop_name"]],
        geometry=[
            Point(lon, lat)
            for lon, lat in zip(train_stations["stop_lon"], train_stations["stop_lat"])
        ],
        crs="EPSG:4326",
    )

    train_output_path = output_dir / "train_shapes"
    train_output_path.mkdir(parents=True, exist_ok=True)
    train_stations_gdf.to_file(train_output_path / "train_stations.shp")
    print(f"  Saved {len(train_stations_gdf)} train stations")

    # Bus stops (location_type=0 with stop_code)
    bus_stops = stops_df[
        (stops_df["location_type"] == 0) & (stops_df["stop_code"].notna())
    ].copy()
    bus_stops_gdf = gpd.GeoDataFrame(
        bus_stops[["stop_id", "stop_name", "stop_code"]],
        geometry=[
            Point(lon, lat)
            for lon, lat in zip(bus_stops["stop_lon"], bus_stops["stop_lat"])
        ],
        crs="EPSG:4326",
    )

    bus_output_path = output_dir / "bus_shapes"
    bus_output_path.mkdir(parents=True, exist_ok=True)
    bus_stops_gdf.to_file(bus_output_path / "bus_stops.shp")
    print(f"  Saved {len(bus_stops_gdf)} bus stops")

    return train_stations_gdf, bus_stops_gdf


def create_route_shapes(data_dir: Path, output_dir: Path):
    """Main function to generate all route and stop shapefiles.

    Args:
        data_dir: Directory containing GTFS data.
        output_dir: Directory to save output shapefiles.
    """
    print("=" * 60)
    print("Generating route shapes from GTFS data")
    print("=" * 60)

    # Load GTFS feed
    gtfs_path = data_dir / "gtfs" / "google_transit.zip"
    print(f"\nLoading GTFS feed from {gtfs_path}...")
    feed = gk.read_feed(gtfs_path, dist_units="mi")

    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)

    # Generate shapes
    print()
    create_train_shapes(feed, output_dir)
    create_bus_shapes(feed, output_dir)
    create_stop_shapes(data_dir, output_dir)

    print("\nDone!")


def main():
    """Run route shape generation."""
    project_root = Path(__file__).parent.parent
    data_dir = project_root / "data"
    output_dir = project_root / "dashboard" / "precomputed"

    create_route_shapes(data_dir, output_dir)


if __name__ == "__main__":
    main()
