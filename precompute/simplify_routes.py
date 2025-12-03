"""Precompute simplified route shapes for the dashboard.

This script takes the full GTFS shapes data (~54MB) and creates a compact
JSON file (~1-2MB) with simplified route geometries suitable for map display.
"""

import json
import sys
from pathlib import Path

import pandas as pd
from shapely.geometry import LineString

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

# Route colors for trains
ROUTE_COLORS = {
    "Red": "#c60c30",
    "Blue": "#00a1de",
    "G": "#009b3a",
    "Org": "#f9461c",
    "Brn": "#62361b",
    "P": "#522398",
    "Pink": "#e27ea6",
    "Y": "#f9e300",
}

# Train line display names
TRAIN_LINE_DISPLAY = {
    "Red": "Red Line",
    "Blue": "Blue Line",
    "G": "Green Line",
    "Org": "Orange Line",
    "Brn": "Brown Line",
    "P": "Purple Line",
    "Pink": "Pink Line",
    "Y": "Yellow Line",
}


def simplify_coordinates(coords: list, tolerance: float = 0.0001) -> list:
    """Simplify coordinates using Douglas-Peucker algorithm.

    Args:
        coords: List of (lat, lon) tuples
        tolerance: Simplification tolerance in degrees (~0.0001 = ~10m)

    Returns:
        Simplified list of coordinates that preserves the shape
    """
    if len(coords) <= 2:
        return coords

    # Convert to LineString (note: shapely uses lon, lat order)
    line = LineString([(lon, lat) for lat, lon in coords])

    # Simplify using Douglas-Peucker algorithm
    simplified_line = line.simplify(tolerance, preserve_topology=True)

    # Convert back to (lat, lon) order
    simplified = [(lat, lon) for lon, lat in simplified_line.coords]

    return simplified


def get_route_shapes(data_dir: Path, tolerance: float = 0.0001) -> dict:
    """Load and simplify route shapes from GTFS data.

    Args:
        data_dir: Path to data directory
        tolerance: Douglas-Peucker tolerance in degrees (~0.0001 = ~10m accuracy)
    """
    gtfs_dir = data_dir / "gtfs"

    routes = pd.read_csv(gtfs_dir / "routes.txt")
    trips = pd.read_csv(gtfs_dir / "trips.txt")
    shapes = pd.read_csv(gtfs_dir / "shapes.txt")

    print(f"Loaded GTFS data:")
    print(f"  Routes: {len(routes)}")
    print(f"  Trips: {len(trips)}")
    print(f"  Shape points: {len(shapes)}")

    route_shapes = {}
    total_points_original = 0
    total_points_simplified = 0

    for _, route in routes.iterrows():
        route_id = route["route_id"]
        route_type = route["route_type"]

        # Get trips for this route
        route_trips = trips[trips["route_id"] == route_id]
        if len(route_trips) == 0:
            continue

        # Get first non-null shape_id (one representative shape per route)
        shape_ids = route_trips["shape_id"].dropna().unique()
        if len(shape_ids) == 0:
            continue

        shape_id = shape_ids[0]
        shape_points = shapes[shapes["shape_id"] == shape_id].sort_values(
            "shape_pt_sequence"
        )

        if len(shape_points) == 0:
            continue

        # Get coordinates
        coords = list(zip(
            shape_points["shape_pt_lat"].round(5),  # Round to 5 decimal places (~1m precision)
            shape_points["shape_pt_lon"].round(5)
        ))

        total_points_original += len(coords)

        # Simplify bus routes only; keep train lines at full resolution
        if route_type == 1:  # Rail
            # Keep full resolution for train lines
            simplified_coords = [(lat, lon) for lat, lon in coords]
        else:
            # Simplify bus routes using Douglas-Peucker algorithm
            simplified_coords = simplify_coordinates(coords, tolerance)

        coords = simplified_coords
        total_points_simplified += len(coords)

        # Get color
        if route_id in ROUTE_COLORS:
            color = ROUTE_COLORS[route_id]
        elif pd.notna(route.get("route_color")) and route["route_color"]:
            color = f"#{route['route_color']}"
        else:
            color = "#666666"

        # Get display name
        if route_id in TRAIN_LINE_DISPLAY:
            display_name = TRAIN_LINE_DISPLAY[route_id]
        elif pd.notna(route.get("route_short_name")) and route["route_short_name"]:
            display_name = f"Bus {route['route_short_name']}"
        else:
            display_name = route.get("route_long_name", route_id)

        route_shapes[route_id] = {
            "coordinates": [[lat, lon] for lat, lon in coords],  # JSON-friendly format
            "color": color,
            "route_type": int(route_type),  # 1=rail, 3=bus
            "display_name": display_name,
        }

    print(f"\nSimplification results:")
    print(f"  Original points: {total_points_original:,}")
    print(f"  Simplified points: {total_points_simplified:,}")
    print(f"  Reduction: {(1 - total_points_simplified/total_points_original)*100:.1f}%")
    print(f"  Routes processed: {len(route_shapes)}")

    return route_shapes


def get_stops(data_dir: Path) -> dict:
    """Load and categorize stops from GTFS data."""
    gtfs_dir = data_dir / "gtfs"
    stops = pd.read_csv(gtfs_dir / "stops.txt")

    # Train stations (location_type=1)
    train_stations = stops[stops["location_type"] == 1].copy()

    # Bus stops (location_type=0 with stop_code)
    bus_stops = stops[
        (stops["location_type"] == 0) & (stops["stop_code"].notna())
    ].copy()

    print(f"\nStops:")
    print(f"  Train stations: {len(train_stations)}")
    print(f"  Bus stops: {len(bus_stops)}")

    return {
        "train_stations": [
            {
                "name": row["stop_name"],
                "lat": round(row["stop_lat"], 5),
                "lon": round(row["stop_lon"], 5),
            }
            for _, row in train_stations.iterrows()
        ],
        "bus_stops": [
            {
                "name": row["stop_name"],
                "lat": round(row["stop_lat"], 5),
                "lon": round(row["stop_lon"], 5),
            }
            for _, row in bus_stops.iterrows()
        ],
    }


def main():
    """Generate simplified route shapes and stops."""
    # Paths
    project_root = Path(__file__).parent.parent
    data_dir = project_root / "data"
    output_dir = project_root / "dashboard" / "precomputed"

    output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("Generating simplified route shapes")
    print("=" * 60)

    # Generate route shapes (tolerance ~0.00005 = ~5m accuracy for better curves)
    route_shapes = get_route_shapes(data_dir, tolerance=0.00005)

    # Generate stops
    stops = get_stops(data_dir)

    # Combine into single file
    output_data = {
        "routes": route_shapes,
        "stops": stops,
    }

    # Save as JSON
    output_path = output_dir / "routes_simplified.json"
    with open(output_path, "w") as f:
        json.dump(output_data, f, separators=(",", ":"))  # Compact JSON

    file_size = output_path.stat().st_size / (1024 * 1024)
    print(f"\nSaved to: {output_path}")
    print(f"File size: {file_size:.2f} MB")

    print("\nDone!")


if __name__ == "__main__":
    main()
