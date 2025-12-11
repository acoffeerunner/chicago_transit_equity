"""Map display component using Folium integration."""

import json
from typing import Optional

import folium
import streamlit as st
from folium.plugins import MarkerCluster
from streamlit_folium import st_folium
from utils.data_loader import get_data_dir, load_school_stops

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

# Train station colors based on lines served
TRAIN_STATION_COLOR = "#6B7280"  # Gray for generic train stations


@st.cache_data(ttl=3600)
def load_simplified_routes():
    """Load precomputed simplified routes and stops from JSON."""
    data_dir = get_data_dir()
    json_path = data_dir / "routes_simplified.json"

    if not json_path.exists():
        return {"routes": {}, "stops": {"train_stations": [], "bus_stops": []}}

    with open(json_path, "r") as f:
        return json.load(f)


def render_map(
    height: int = 500,
    filter_school_ids: Optional[list] = None,
    map_key: str = "main_map",
    show_bus_stops: bool = False,
    show_train_stations: bool = False,
) -> Optional[str]:
    """
    Render the map and return the selected school ID if any.

    Args:
        height: Map height in pixels
        filter_school_ids: Optional list of school IDs to show. If None, shows all schools.
        map_key: Unique key for the map component. Change to force re-render.
        show_bus_stops: Whether to render bus stops layer (slow with 10k+ stops).
        show_train_stations: Whether to render train stations layer.

    Returns:
        school_id if a school marker was clicked, None otherwise
    """
    # Load data
    schools = load_school_stops()
    route_data = load_simplified_routes()

    # Filter schools if filter_school_ids is provided
    if filter_school_ids is not None and not schools.empty:
        schools = schools[schools["school_id"].isin(filter_school_ids)]

    m = folium.Map(
        location=[41.8781, -87.6298],
        zoom_start=11,
    )

    # Determine the theme (Light or Dark)
    theme = st.context.theme.type
    tiles = "cartodbpositron" if theme == "light" else "cartodbdark_matter"
    folium.TileLayer(tiles=tiles, name="CartoDB", control=False).add_to(m)

    # Add CTA routes to the map
    routes = route_data.get("routes", {})

    if routes:
        # Create feature groups for routes
        train_routes_group = folium.FeatureGroup(name="Train Lines", show=True)
        bus_routes_group = folium.FeatureGroup(name="Bus Routes", show=True)

        for route_id, shape_data in routes.items():
            coords = shape_data["coordinates"]
            if len(coords) < 2:
                continue

            color = shape_data["color"]
            route_type = shape_data["route_type"]
            display_name = shape_data["display_name"]

            line = folium.PolyLine(
                coords,
                color=color,
                weight=4 if route_type == 1 else 2,
                opacity=0.8 if route_type == 1 else 0.5,
                popup=display_name,
                tooltip=display_name,
            )

            if route_type == 1:  # Rail
                line.add_to(train_routes_group)
            else:  # Bus
                line.add_to(bus_routes_group)

        train_routes_group.add_to(m)
        bus_routes_group.add_to(m)

    # Add train stations and bus stops (only when enabled)
    stops = route_data.get("stops", {})
    train_stations = stops.get("train_stations", []) if show_train_stations else []
    bus_stops = stops.get("bus_stops", []) if show_bus_stops else []

    # Train stations layer (shown when enabled)
    if train_stations:
        train_stations_group = folium.FeatureGroup(name="Train Stations", show=True)

        for station in train_stations:
            station_name = station["name"]
            station_color = TRAIN_STATION_COLOR

            # Try to match station to a line color based on name
            for line_id, color in ROUTE_COLORS.items():
                if f"({line_id})" in station_name or line_id in station_name:
                    station_color = color
                    break

            folium.CircleMarker(
                location=[station["lat"], station["lon"]],
                radius=6,
                color=station_color,
                fill=True,
                fill_color=station_color,
                fill_opacity=0.8,
                popup=station_name,
                tooltip=station_name,
            ).add_to(train_stations_group)

        train_stations_group.add_to(m)

    # Bus stops layer (only loaded when show_bus_stops is True)
    if bus_stops:
        bus_stops_group = folium.FeatureGroup(name="Bus Stops", show=True)
        bus_stops_cluster = MarkerCluster(
            name="Bus Stops Cluster",
            show_coverage_on_hover=False,
            options={"maxClusterRadius": 50, "disableClusteringAtZoom": 16},
        )

        for stop in bus_stops:
            stop_name = stop["name"]
            folium.CircleMarker(
                location=[stop["lat"], stop["lon"]],
                radius=4,
                color="#3B82F6",
                fill=True,
                fill_color="#3B82F6",
                fill_opacity=0.6,
                popup=stop_name,
                tooltip=stop_name,
            ).add_to(bus_stops_cluster)

        bus_stops_cluster.add_to(bus_stops_group)
        bus_stops_group.add_to(m)

    # Add school markers - separate groups for Elementary and High Schools
    if not schools.empty:
        elementary_group = folium.FeatureGroup(name="Elementary Schools", show=True)
        high_school_group = folium.FeatureGroup(name="High Schools", show=True)

        for _, school in schools.iterrows():
            # Format data for popup - handle numpy arrays
            bus_routes = school.get("bus_routes", [])
            if isinstance(bus_routes, str):
                try:
                    bus_routes = eval(bus_routes)
                except:
                    bus_routes = []
            if hasattr(bus_routes, "tolist"):
                bus_routes = bus_routes.tolist()
            if not isinstance(bus_routes, list):
                bus_routes = list(bus_routes) if bus_routes is not None else []

            train_lines = school.get("train_lines", [])
            if isinstance(train_lines, str):
                try:
                    train_lines = eval(train_lines)
                except:
                    train_lines = []
            if hasattr(train_lines, "tolist"):
                train_lines = train_lines.tolist()
            if not isinstance(train_lines, list):
                train_lines = list(train_lines) if train_lines is not None else []

            # Create popup HTML
            popup_html = f"""
            <div style="min-width: 250px; font-family: Arial, sans-serif;">
                <h4 style="margin: 0 0 8px 0; color: #1a1a2e;">{school["school_name"]}</h4>
                <p style="margin: 0 0 4px 0; color: #666; font-size: 12px;">
                    <strong>Type:</strong> {"High School" if school["grade_cat"] == "HS" else "Elementary School"}
                </p>
                <p style="margin: 0 0 8px 0; color: #666; font-size: 12px;">
                    <strong>Address:</strong> {school.get("address", "N/A")}
                </p>
                <hr style="margin: 8px 0; border: none; border-top: 1px solid #ddd;">
                <p style="margin: 0 0 4px 0; font-size: 13px;">
                    <strong>Nearest Bus Stop:</strong><br>
                    {school["nearest_bus_stop_name"]}<br>
                    <span style="color: #666; font-size: 11px;">
                        Distance: {school["bus_distance_mi"]:.2f} mi
                    </span><br>
                    <span style="color: #666; font-size: 11px;">
                        Routes: {", ".join(bus_routes[:5]) if len(bus_routes) > 0 else "N/A"}
                        {"..." if len(bus_routes) > 5 else ""}
                    </span>
                </p>
                <p style="margin: 8px 0 0 0; font-size: 13px;">
                    <strong>Nearest Train Station:</strong><br>
                    {school["nearest_train_station_name"]}<br>
                    <span style="color: #666; font-size: 11px;">
                        Distance: {school["train_distance_mi"]:.2f} mi
                    </span><br>
                    <span style="color: #666; font-size: 11px;">
                        Lines: {", ".join(train_lines) if len(train_lines) > 0 else "N/A"}
                    </span>
                </p>
            </div>
            """

            color = "red" if school["grade_cat"] == "HS" else "green"

            marker = folium.Marker(
                location=[school["lat"], school["lon"]],
                popup=folium.Popup(popup_html, max_width=300),
                tooltip=f"{school['school_name']} ({school['grade_cat']})",
                icon=folium.Icon(
                    icon="graduation-cap",
                    prefix="fa",
                    color=color,
                ),
            )

            # Store school_id in marker for click detection
            marker.options["school_id"] = school["school_id"]

            # Add to appropriate group based on school type
            if school["grade_cat"] == "HS":
                marker.add_to(high_school_group)
            else:
                marker.add_to(elementary_group)

        elementary_group.add_to(m)
        high_school_group.add_to(m)

    # Add layer control
    folium.LayerControl().add_to(m)

    # Render with st_folium for interactivity
    map_data = st_folium(
        m,
        width=None,  # Use full width
        height=height,
        returned_objects=["last_object_clicked", "last_object_clicked_popup"],
        key=map_key,
    )

    # Check for clicked marker
    selected_school_id = None

    if map_data and map_data.get("last_object_clicked"):
        clicked = map_data["last_object_clicked"]
        lat = clicked.get("lat")
        lng = clicked.get("lng")

        if lat and lng:
            # Find the school at this location
            for _, school in schools.iterrows():
                if (
                    abs(school["lat"] - lat) < 0.0001
                    and abs(school["lon"] - lng) < 0.0001
                ):
                    selected_school_id = school["school_id"]
                    break

    return selected_school_id
