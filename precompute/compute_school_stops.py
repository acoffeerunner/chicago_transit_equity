"""Compute nearest CTA stops for each CPS school."""

import json
import math
from pathlib import Path

import pandas as pd
from shapely.geometry import Point, shape


def load_community_areas(data_dir: Path) -> list[dict]:
    """Load Chicago community areas from GeoJSON."""
    geojson_path = data_dir / "cps_data" / "chicago-community-areas.geojson"
    if not geojson_path.exists():
        print(f"Warning: Community areas file not found at {geojson_path}")
        return []

    with open(geojson_path) as f:
        data = json.load(f)

    areas = []
    for feature in data["features"]:
        areas.append({
            "name": feature["properties"]["community"].title(),  # Title case
            "geometry": shape(feature["geometry"]),
        })

    print(f"Loaded {len(areas)} community areas")
    return areas


def get_neighborhood(lat: float, lon: float, community_areas: list[dict]) -> str:
    """Find which community area contains the given point."""
    if not community_areas:
        return "Unknown"

    point = Point(lon, lat)  # Note: shapely uses (lon, lat) order

    for area in community_areas:
        if area["geometry"].contains(point):
            return area["name"]

    return "Unknown"


def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate haversine distance between two points in miles."""
    R = 3958.8  # Earth radius in miles
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)

    a = (
        math.sin(delta_phi / 2) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    return R * c


def load_schools(data_dir: Path) -> pd.DataFrame:
    """Load CPS schools from GeoJSON file."""
    geojson_files = list((data_dir / "cps_data").glob("*.geojson"))
    if not geojson_files:
        raise FileNotFoundError("No GeoJSON files found in data/cps_data/")

    geojson_path = geojson_files[0]
    print(f"Loading schools from {geojson_path}")

    with open(geojson_path) as f:
        data = json.load(f)

    schools = []
    for feature in data["features"]:
        props = feature["properties"]
        coords = feature["geometry"]["coordinates"]
        schools.append(
            {
                "school_id": props.get("school_id"),
                "school_name": props.get("short_name"),
                "address": props.get("address"),
                "grade_cat": props.get("grade_cat"),
                "lat": float(props.get("lat", coords[1])),
                "lon": float(props.get("long", coords[0])),
            }
        )

    return pd.DataFrame(schools)


def load_gtfs_stops(data_dir: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Load GTFS stops and separate bus stops from train stations."""
    stops_path = data_dir / "gtfs" / "stops.txt"
    print(f"Loading stops from {stops_path}")

    stops = pd.read_csv(stops_path)

    # Train stations: location_type == 1 (stations)
    train_stations = stops[stops["location_type"] == 1].copy()
    print(f"Found {len(train_stations)} train stations")

    # Bus stops: have stop_code, no parent_station, location_type == 0 or NaN
    bus_stops = stops[
        stops["stop_code"].notna()
        & stops["parent_station"].isna()
        & ((stops["location_type"] == 0) | stops["location_type"].isna())
    ].copy()
    print(f"Found {len(bus_stops)} bus stops")

    return bus_stops, train_stations


def get_routes_for_stop(stop_id: str, stop_times: pd.DataFrame, trips: pd.DataFrame, routes: pd.DataFrame) -> list[str]:
    """Get list of routes that serve a given stop."""
    # Get trip_ids that stop at this stop
    trip_ids = stop_times[stop_times["stop_id"] == stop_id]["trip_id"].unique()
    if len(trip_ids) == 0:
        return []

    # Get route_ids for these trips
    route_ids = trips[trips["trip_id"].isin(trip_ids)]["route_id"].unique()

    # Get route names
    route_names = []
    for route_id in route_ids:
        route_row = routes[routes["route_id"] == route_id]
        if len(route_row) > 0:
            short_name = route_row.iloc[0]["route_short_name"]
            long_name = route_row.iloc[0]["route_long_name"]
            # Use short_name for buses, long_name for trains
            if pd.notna(short_name) and short_name.strip():
                route_names.append(str(short_name))
            else:
                route_names.append(str(long_name))

    return sorted(set(route_names))


def find_nearest_stop(
    school_lat: float,
    school_lon: float,
    stops: pd.DataFrame,
) -> tuple:
    """Find the nearest stop to a school location."""
    min_distance = float("inf")
    nearest_stop_id = None
    nearest_stop_name = None

    for _, stop in stops.iterrows():
        distance = haversine_distance(
            school_lat, school_lon, stop["stop_lat"], stop["stop_lon"]
        )
        if distance < min_distance:
            min_distance = distance
            nearest_stop_id = stop["stop_id"]  # Keep original type (int)
            nearest_stop_name = stop["stop_name"]

    return nearest_stop_id, nearest_stop_name, min_distance


def compute_school_stops(data_dir: Path, output_dir: Path) -> pd.DataFrame:
    """Compute nearest bus stop and train station for each school."""
    # Load data
    schools = load_schools(data_dir)
    bus_stops, train_stations = load_gtfs_stops(data_dir)
    community_areas = load_community_areas(data_dir)

    # Load GTFS files for route lookup
    stops = pd.read_csv(data_dir / "gtfs" / "stops.txt")  # Full stops for parent lookup
    stop_times = pd.read_csv(data_dir / "gtfs" / "stop_times.txt")
    trips = pd.read_csv(data_dir / "gtfs" / "trips.txt")
    routes = pd.read_csv(data_dir / "gtfs" / "routes.txt")

    print(f"Processing {len(schools)} schools...")

    # Build stop-to-routes lookup for efficiency
    print("Building stop-to-routes lookup...")
    stop_trip_map = stop_times.groupby("stop_id")["trip_id"].apply(set).to_dict()
    trip_route_map = trips.set_index("trip_id")["route_id"].to_dict()

    route_info = {}
    for _, row in routes.iterrows():
        route_id = row["route_id"]
        short_name = row["route_short_name"]
        long_name = row["route_long_name"]
        route_type = row["route_type"]
        if pd.notna(short_name) and str(short_name).strip():
            route_info[route_id] = {"name": str(short_name), "type": route_type}
        else:
            route_info[route_id] = {"name": str(long_name), "type": route_type}

    # Build parent-to-children lookup for train stations
    parent_to_children = {}
    for _, stop in stops.iterrows():
        parent = stop.get("parent_station")
        if pd.notna(parent):
            parent_id = int(parent) if not isinstance(parent, int) else parent
            if parent_id not in parent_to_children:
                parent_to_children[parent_id] = []
            parent_to_children[parent_id].append(stop["stop_id"])

    def get_routes_fast(stop_id, is_station=False):
        """Get routes for a stop. For stations, look up via child stops."""
        stop_ids_to_check = [stop_id]

        # For train stations, also check child stops (platforms)
        if is_station and stop_id in parent_to_children:
            stop_ids_to_check.extend(parent_to_children[stop_id])

        route_ids = set()
        for sid in stop_ids_to_check:
            trip_ids = stop_trip_map.get(sid, set())
            for trip_id in trip_ids:
                route_id = trip_route_map.get(trip_id)
                if route_id:
                    route_ids.add(route_id)

        # For train stations, only return rail routes (route_type=1)
        if is_station:
            return [route_info[rid]["name"] for rid in route_ids
                    if rid in route_info and route_info[rid]["type"] == 1]
        else:
            return [route_info[rid]["name"] for rid in route_ids if rid in route_info]

    # Process each school
    results = []
    for idx, school in schools.iterrows():
        if idx % 100 == 0:
            print(f"  Processing school {idx + 1}/{len(schools)}...")

        school_lat = school["lat"]
        school_lon = school["lon"]

        # Find nearest bus stop
        bus_stop_id, bus_stop_name, bus_distance = find_nearest_stop(
            school_lat, school_lon, bus_stops
        )
        bus_routes = get_routes_fast(bus_stop_id) if bus_stop_id else []

        # Find nearest train station
        train_stop_id, train_stop_name, train_distance = find_nearest_stop(
            school_lat, school_lon, train_stations
        )
        train_lines = get_routes_fast(train_stop_id, is_station=True) if train_stop_id else []

        # Get neighborhood
        neighborhood = get_neighborhood(school_lat, school_lon, community_areas)

        results.append(
            {
                "school_id": school["school_id"],
                "school_name": school["school_name"],
                "address": school["address"],
                "neighborhood": neighborhood,
                "lat": school_lat,
                "lon": school_lon,
                "grade_cat": school["grade_cat"],
                "nearest_bus_stop_id": bus_stop_id,
                "nearest_bus_stop_name": bus_stop_name,
                "bus_distance_mi": round(bus_distance, 2),
                "bus_routes": bus_routes,
                "nearest_train_station_id": train_stop_id,
                "nearest_train_station_name": train_stop_name,
                "train_distance_mi": round(train_distance, 2),
                "train_lines": train_lines,
            }
        )

    df = pd.DataFrame(results)

    # Save to parquet
    output_path = output_dir / "school_stops.parquet"
    df.to_parquet(output_path, index=False)
    print(f"Saved {len(df)} school-stop mappings to {output_path}")

    # Print summary statistics
    print("\nSummary Statistics:")
    print(f"  Average distance to nearest bus stop: {df['bus_distance_mi'].mean():.2f} mi")
    print(f"  Average distance to nearest train station: {df['train_distance_mi'].mean():.2f} mi")
    print(f"  Schools within 0.31 mi (500m) of bus stop: {(df['bus_distance_mi'] <= 0.31).sum()}")
    print(f"  Schools within 0.62 mi (1km) of train station: {(df['train_distance_mi'] <= 0.62).sum()}")

    return df


def main():
    """Run the school-stop computation."""
    data_dir = Path(__file__).parent.parent / "data"
    output_dir = Path(__file__).parent.parent / "dashboard" / "precomputed"
    output_dir.mkdir(parents=True, exist_ok=True)

    compute_school_stops(data_dir, output_dir)


if __name__ == "__main__":
    main()
