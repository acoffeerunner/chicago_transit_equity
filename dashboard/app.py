"""CTA Transit Equity Dashboard - Main Streamlit Application."""

import sys
from pathlib import Path

# Add dashboard directory to path for imports when running standalone
# This allows both `streamlit run dashboard/app.py` from parent and
# `streamlit run app.py` from within dashboard directory
dashboard_dir = Path(__file__).parent
if str(dashboard_dir) not in sys.path:
    sys.path.insert(0, str(dashboard_dir))

import streamlit as st

# Page config must be first Streamlit command
st.set_page_config(
    page_title="CPS Transit Equity Dashboard",
    page_icon="train",
    layout="wide",
    initial_sidebar_state="expanded",
)

from components.map_display import render_map
from components.post_previews import render_post_previews
from components.sentiment_charts import render_sentiment_charts
from components.sidebar import render_sidebar
from utils.data_loader import (
    get_all_routes,
    get_routes_for_school,
    get_schools_for_route,
    load_school_stops,
)
from utils.route_mapping import get_route_display_name


def init_session_state():
    """Initialize session state variables."""
    if "selected_school_id" not in st.session_state:
        st.session_state.selected_school_id = None
    if "selected_routes" not in st.session_state:
        st.session_state.selected_routes = []
    if "cleared_school_id" not in st.session_state:
        st.session_state.cleared_school_id = None
    if "map_key_counter" not in st.session_state:
        st.session_state.map_key_counter = 0


def main():
    """Main application entry point."""
    init_session_state()

    # Header
    st.title("Transit Equity Dashboard for Chicago Public Schools")
    st.markdown(
        """
        *Analyzing public transit accessibility and sentiment for Chicago Public Schools*

        **Click on a school marker** to see nearby transit options and sentiment analysis for the nearest routes serving that school.
        """
    )

    # Render sidebar and get filters
    filters = render_sidebar(st.session_state.selected_school_id)

    # Determine school filter for the map based on filters
    filter_school_ids = None
    route_schools_df = None
    schools = load_school_stops()

    # Apply neighborhood filter first
    if filters.get("neighborhood") and not st.session_state.selected_school_id:
        neighborhood_schools = schools[
            schools["neighborhood"] == filters["neighborhood"]
        ]
        filter_school_ids = neighborhood_schools["school_id"].tolist()

    # Then apply route filter if specified
    if filters.get("selected_route") and not st.session_state.selected_school_id:
        # Get schools within 1 mile of the selected route
        route_schools_df = get_schools_for_route(filters["selected_route"])
        if not route_schools_df.empty:
            route_school_ids = route_schools_df["school_id"].tolist()
            # If neighborhood filter is active, intersect with it
            if filter_school_ids is not None:
                filter_school_ids = [
                    sid for sid in route_school_ids if sid in filter_school_ids
                ]
            else:
                filter_school_ids = route_school_ids
        else:
            filter_school_ids = []  # Empty list means no schools to show

    # Main content area
    st.markdown("---")

    # Map section
    st.subheader("Chicago Transit Map")
    if filter_school_ids is not None:
        if filters.get("neighborhood") and filters.get("selected_route"):
            route_display = get_route_display_name(filters["selected_route"])
            st.caption(
                f"Showing {len(filter_school_ids)} schools in {filters['neighborhood']} "
                f"near {route_display}. Click a school to see transit details."
            )
        elif filters.get("neighborhood"):
            st.caption(
                f"Showing {len(filter_school_ids)} schools in {filters['neighborhood']}. "
                "Click a school to see transit details."
            )
        elif filters.get("selected_route"):
            route_display = get_route_display_name(filters["selected_route"])
            st.caption(
                f"Showing {len(filter_school_ids)} schools within 1 mile of {route_display}. "
                "Click a school to see transit details."
            )
    else:
        st.caption(
            "Schools are shown in green (Elementary) and red (High School). "
            "Click a school to see transit details."
        )

    # Stop layer toggles
    show_train_stations = st.checkbox(
        "Show train stations",
        value=False,
        key="show_train_stations",
    )
    show_bus_stops = st.checkbox(
        "Show bus stops",
        value=False,
        key="show_bus_stops",
        help="Loads 10,000+ bus stop markers. (Slower performance)",
    )

    # Render map and get clicked school
    map_key = f"main_map_{st.session_state.map_key_counter}"
    clicked_school_id = render_map(
        height=500,
        filter_school_ids=filter_school_ids,
        map_key=map_key,
        show_bus_stops=show_bus_stops,
        show_train_stations=show_train_stations,
    )

    # Update selected school if clicked (but not if we just cleared this school)
    if clicked_school_id and clicked_school_id != st.session_state.selected_school_id:
        # Don't re-select a school we just cleared
        if clicked_school_id != st.session_state.cleared_school_id:
            st.session_state.selected_school_id = clicked_school_id
            st.session_state.cleared_school_id = None  # Reset the cleared flag
            st.rerun()

    # Reset cleared flag if user clicked on a different school
    if clicked_school_id and clicked_school_id != st.session_state.cleared_school_id:
        st.session_state.cleared_school_id = None

    # Determine which routes to show
    routes_to_show = []

    if st.session_state.selected_school_id:
        # Get routes for selected school
        schools = load_school_stops()
        if not schools.empty:
            school_match = schools[
                schools["school_id"] == st.session_state.selected_school_id
            ]
            if len(school_match) > 0:
                school = school_match.iloc[0]
                routes_to_show = get_routes_for_school(school)

                # Show school info banner
                st.info(
                    f"**Showing data for: {school['school_name']}** "
                    f"({school['grade_cat']}) - "
                    f"{len(routes_to_show)} routes serving this school"
                )

                # Clear selection button
                if st.button("Clear Selection", key="clear_school"):
                    # Store the cleared school ID to prevent immediate re-selection
                    st.session_state.cleared_school_id = (
                        st.session_state.selected_school_id
                    )
                    st.session_state.selected_school_id = None
                    # Increment map key to force re-render and close popups
                    st.session_state.map_key_counter += 1
                    st.rerun()

    elif filters.get("neighborhood"):
        # Neighborhood filter is active (may also have route filter)
        routes_to_show = filters.get("neighborhood_routes", [])
        neighborhood = filters["neighborhood"]
        selected_route = filters.get("selected_route")
        school_count = filters.get("neighborhood_school_count", 0)

        # Build info message based on active filters
        if selected_route:
            # Both neighborhood and specific route
            route_display = get_route_display_name(selected_route)
            if routes_to_show:
                st.info(
                    f"**{route_display}** in **{neighborhood}** - "
                    f"serves {len(filter_school_ids) if filter_school_ids else 0} schools"
                )
            else:
                st.warning(
                    f"**{route_display}** does not serve any schools in **{neighborhood}**"
                )
        else:
            # Just neighborhood (possibly with route type filter)
            route_type = filters.get("route_type", "All")
            type_suffix = ""
            if route_type == "Train only":
                type_suffix = " train"
            elif route_type == "Bus only":
                type_suffix = " bus"

            st.info(
                f"**{neighborhood}** - {len(routes_to_show)}{type_suffix} routes serve "
                f"{school_count} schools in this neighborhood"
            )

            if routes_to_show:
                with st.expander(f"View routes serving {neighborhood}", expanded=False):
                    # Separate train and bus routes
                    train_routes = sorted([r for r in routes_to_show if r.endswith("_line")])
                    bus_routes = sorted([r for r in routes_to_show if r.startswith("bus_")])

                    if train_routes:
                        st.markdown("**Train Lines**")
                        train_names = [get_route_display_name(r) for r in train_routes]
                        st.write(", ".join(train_names))

                    if bus_routes:
                        st.markdown("**Bus Routes**")
                        bus_names = [get_route_display_name(r) for r in bus_routes]
                        if len(bus_names) > 10:
                            cols = st.columns(3)
                            for i, name in enumerate(bus_names):
                                cols[i % 3].write(name)
                        else:
                            st.write(", ".join(bus_names))

    elif filters.get("selected_route"):
        # Single route selected via filter (no neighborhood)
        routes_to_show = [filters["selected_route"]]
        route_display = get_route_display_name(filters["selected_route"])

        if filter_school_ids and route_schools_df is not None:
            st.info(
                f"**{route_display}** serves {len(filter_school_ids)} schools "
                f"(within 1 mile)"
            )

            with st.expander(f"View schools near {route_display}", expanded=False):
                # Sort by distance (train for train routes, bus for bus routes)
                if filters["selected_route"].endswith("_line"):
                    sorted_schools = route_schools_df.sort_values("train_distance_mi")
                    distance_col = "train_distance_mi"
                else:
                    sorted_schools = route_schools_df.sort_values("bus_distance_mi")
                    distance_col = "bus_distance_mi"

                # Display as a table
                cols = st.columns([3, 1, 1])
                cols[0].markdown("**School**")
                cols[1].markdown("**Type**")
                cols[2].markdown("**Distance**")

                for _, school in sorted_schools.iterrows():
                    cols = st.columns([3, 1, 1])
                    cols[0].write(school["school_name"])
                    cols[1].write("HS" if school["grade_cat"] == "HS" else "ES")
                    distance_mi = school[distance_col]
                    cols[2].write(f"{distance_mi:.2f} mi")
        else:
            st.info(f"**{route_display}** - No schools within 1 mile")
    else:
        # Show all routes (or filtered by type)
        all_routes = get_all_routes()
        route_type = filters.get("route_type", "All")

        if route_type == "Train only":
            routes_to_show = [r for r in all_routes if r.endswith("_line")]
        elif route_type == "Bus only":
            routes_to_show = [r for r in all_routes if r.startswith("bus_")]
        else:
            routes_to_show = all_routes

    # Store for other components
    st.session_state.selected_routes = routes_to_show

    st.markdown("---")

    # Analytics section
    if routes_to_show:
        st.subheader("Sentiment Analysis")

        # Show which routes are being analyzed
        if len(routes_to_show) <= 10:
            route_names = [get_route_display_name(r) for r in routes_to_show]
            st.caption(f"Analyzing routes: {', '.join(route_names)}")
        else:
            st.caption(f"Analyzing {len(routes_to_show)} routes")

        # Render sentiment charts
        render_sentiment_charts(routes_to_show, filters)

        st.markdown("---")

        # Post previews
        render_post_previews(routes_to_show, filters)

    else:
        st.info(
            "Select a school on the map or choose a route from the sidebar "
            "to see sentiment analysis."
        )

    # Footer
    st.markdown("---")
    st.markdown(
        """
        <div style="text-align: center; color: #666; font-size: 12px;">
            <p>
                Data sources: CPS School Locations, CTA GTFS Feed,
                Reddit (r/chicago, r, Bluesky #CTA posts
            </p>
            <p>
                Built for transit equity analysis to support
                education advocates and city planners.
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()
