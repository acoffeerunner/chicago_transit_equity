"""Sidebar component with filters and stats."""

from datetime import datetime
from typing import Optional

import streamlit as st
from utils.data_loader import (
    compute_aggregate_stats,
    filter_sentiment_by_routes,
    get_all_routes,
    get_bus_routes,
    get_routes_for_school,
    get_train_routes,
    load_route_sentiment,
    load_school_stops,
)
from utils.route_mapping import SENTIMENT_COLORS, get_route_display_name

# Source icons (Base64 encoded or emoji fallback)
SOURCE_ICONS = {
    "reddit": "https://www.redditstatic.com/desktop2x/img/favicon/android-icon-192x192.png",
    "bluesky": "https://bsky.app/static/favicon-32x32.png",
}


def render_filters() -> dict:
    """Render filter controls and return selected values."""
    st.sidebar.header("Filters")

    # Source filter with icons
    st.sidebar.subheader("Data Source")
    col1, col2 = st.sidebar.columns(2)

    with col1:
        reddit_selected = st.checkbox("Reddit", value=True, key="filter_reddit")
    with col2:
        bluesky_selected = st.checkbox("Bluesky", value=True, key="filter_bluesky")

    sources = []
    if reddit_selected:
        sources.append("reddit")
    if bluesky_selected:
        sources.append("bluesky")

    # Time window filter
    st.sidebar.subheader("Time Window")
    time_window = st.sidebar.radio(
        "Select period:",
        options=["Last 7 days", "Last 30 days", "Last 90 days", "All time"],
        index=3,  # Default to "All time"
        key="filter_time_window",
        horizontal=True,
    )

    # Time of day filter
    st.sidebar.subheader("Time of Day")
    time_of_day_options = ["Morning", "Afternoon", "Evening", "Night"]
    time_of_day = st.sidebar.multiselect(
        "Select periods:",
        options=time_of_day_options,
        default=time_of_day_options,
        key="filter_time_of_day",
    )

    # Route type filter
    st.sidebar.subheader("Route Type")
    route_type = st.sidebar.radio(
        "Filter by:",
        options=["All", "Train only", "Bus only"],
        index=0,
        key="filter_route_type",
        horizontal=True,
    )

    # Get routes based on type
    if route_type == "Train only":
        available_routes = get_train_routes()
    elif route_type == "Bus only":
        available_routes = get_bus_routes()
    else:
        available_routes = get_all_routes()

    # Specific route filter (optional)
    st.sidebar.subheader("Specific Route")
    route_options = ["All routes"] + [
        get_route_display_name(r) for r in available_routes
    ]
    selected_route_display = st.sidebar.selectbox(
        "Select route:",
        options=route_options,
        index=0,
        key="filter_specific_route",
    )

    # Convert display name back to route code
    selected_route = None
    if selected_route_display != "All routes":
        for r in available_routes:
            if get_route_display_name(r) == selected_route_display:
                selected_route = r
                break

    # Neighborhood filter
    st.sidebar.subheader("Neighborhood")
    schools = load_school_stops()
    neighborhoods = (
        sorted(schools["neighborhood"].unique()) if not schools.empty else []
    )
    selected_neighborhood = st.sidebar.selectbox(
        "Select neighborhood:",
        options=["All neighborhoods"] + neighborhoods,
        index=0,
        key="filter_neighborhood",
    )

    return {
        "sources": sources,
        "time_window": time_window,
        "time_of_day": [t.lower() for t in time_of_day],
        "route_type": route_type,
        "selected_route": selected_route,
        "available_routes": available_routes,
        "neighborhood": selected_neighborhood
        if selected_neighborhood != "All neighborhoods"
        else None,
    }


def render_school_stats(school_id: Optional[str] = None):
    """Render statistics for selected school."""
    # st.sidebar.markdown("---")
    # st.sidebar.header("School Info")

    # if not school_id:
    #     st.sidebar.info("Click on a school marker to see details")
    #     return

    # # Get school data
    # schools = load_school_stops()
    # if schools.empty:
    #     st.sidebar.warning("School data not available")
    #     return

    # school_match = schools[schools["school_id"] == school_id]
    # if len(school_match) == 0:
    #     st.sidebar.warning(f"School {school_id} not found")
    #     return

    # school = school_match.iloc[0]

    # # Display school info
    # st.sidebar.subheader(school["school_name"])
    # st.sidebar.caption(
    #     f"{'High School' if school['grade_cat'] == 'HS' else 'Elementary School'}"
    # )
    # st.sidebar.text(school.get("address", ""))

    # # Nearest stops info
    # st.sidebar.markdown("---")
    # st.sidebar.markdown("**Nearest Bus Stop**")

    # bus_routes = school.get("bus_routes", [])
    # if isinstance(bus_routes, str):
    #     try:
    #         bus_routes = eval(bus_routes)
    #     except:
    #         bus_routes = []

    # st.sidebar.markdown(f"*{school['nearest_bus_stop_name']}*")
    # st.sidebar.markdown(
    #     f"Distance: {school['bus_distance_mi']:.2f} mi"
    # )
    # if bus_routes:
    #     st.sidebar.markdown(f"Routes: {', '.join(bus_routes[:5])}")
    #     if len(bus_routes) > 5:
    #         st.sidebar.caption(f"...and {len(bus_routes) - 5} more")

    # st.sidebar.markdown("---")
    # st.sidebar.markdown("**Nearest Train Station**")

    # train_lines = school.get("train_lines", [])
    # if isinstance(train_lines, str):
    #     try:
    #         train_lines = eval(train_lines)
    #     except:
    #         train_lines = []

    # st.sidebar.markdown(f"*{school['nearest_train_station_name']}*")
    # st.sidebar.markdown(f"Distance: {school['train_distance_mi']:.2f} mi")
    # if train_lines:
    #     st.sidebar.markdown(f"Lines: {', '.join(train_lines)}")


def render_route_stats(
    routes: list[str],
    filters: dict,
):
    """Render aggregate stats for selected routes."""
    st.sidebar.markdown("---")
    st.sidebar.header("Sentiment Stats")

    if not routes:
        st.sidebar.info("No routes selected")
        return

    # Load and filter sentiment data
    sentiment_df = load_route_sentiment()
    if sentiment_df.empty:
        st.sidebar.warning("Sentiment data not available")
        return

    filtered = filter_sentiment_by_routes(
        sentiment_df,
        routes=routes,
        sources=filters.get("sources"),
        time_of_day=filters.get("time_of_day"),
    )

    if filtered.empty:
        st.sidebar.warning("No data for selected filters")
        return

    # Compute stats
    stats = compute_aggregate_stats(filtered)

    # Display metrics
    col1, col2 = st.sidebar.columns(2)

    with col1:
        st.metric("Total Posts", f"{stats['total_posts']:,}")

    with col2:
        st.metric("Avg Score", f"{stats['avg_score']:.2f}")

    # Sentiment distribution
    st.sidebar.markdown("**Sentiment Distribution**")

    # Create a simple bar using markdown
    total = stats["positive_count"] + stats["negative_count"] + stats["neutral_count"]
    if total > 0:
        pos_pct = stats["positive_pct"]
        neg_pct = stats["negative_pct"]
        neu_pct = stats["neutral_pct"]

        st.sidebar.markdown(
            f"""
            <div style="display: flex; height: 20px; border-radius: 4px; overflow: hidden;">
                <div style="background-color: {SENTIMENT_COLORS["positive"]}; width: {pos_pct}%;"></div>
                <div style="background-color: {SENTIMENT_COLORS["neutral"]}; width: {neu_pct}%;"></div>
                <div style="background-color: {SENTIMENT_COLORS["negative"]}; width: {neg_pct}%;"></div>
            </div>
            """,
            unsafe_allow_html=True,
        )

        st.sidebar.caption(
            f"Positive: {pos_pct:.1f}% | Neutral: {neu_pct:.1f}% | Negative: {neg_pct:.1f}%"
        )

    # Highlight current time of day
    current_hour = datetime.now().hour
    if 5 <= current_hour < 12:
        current_period = "morning"
    elif 12 <= current_hour < 17:
        current_period = "afternoon"
    elif 17 <= current_hour < 21:
        current_period = "evening"
    else:
        current_period = "night"

    # Get stats for current period (need to query the specific period, not "all")
    current_period_df = sentiment_df[
        (sentiment_df["route"].isin(routes))
        & (sentiment_df["time_of_day"] == current_period)
    ]
    if filters.get("sources"):
        current_period_df = current_period_df[
            current_period_df["source"].isin([s.lower() for s in filters["sources"]])
        ]

    if not current_period_df.empty:
        current_stats = compute_aggregate_stats(current_period_df, current_period)
        st.sidebar.markdown("---")
        st.sidebar.markdown(f"**Current Period ({current_period.capitalize()})**")
        # Display metrics
        col1, col2 = st.sidebar.columns(2)

        with col1:
            st.metric("Total Posts", f"{current_stats['total_posts']:,}")

        with col2:
            st.metric("Avg Score", f"{current_stats['avg_score']:.2f}")
        st.sidebar.markdown(
            f"""
            <div style="display: flex; height: 20px; border-radius: 4px; overflow: hidden;">
                <div style="background-color: {SENTIMENT_COLORS["positive"]}; width: {current_stats["positive_pct"]}%;"></div>
                <div style="background-color: {SENTIMENT_COLORS["neutral"]}; width: {current_stats["neutral_pct"]}%;"></div>
                <div style="background-color: {SENTIMENT_COLORS["negative"]}; width: {current_stats["negative_pct"]}%;"></div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        st.sidebar.caption(
            f"Positive: {current_stats['positive_pct']:.1f}% | "
            f"Neutral: {current_stats['neutral_pct']:.1f}% | "
            f"Negative: {current_stats['negative_pct']:.1f}%"
        )


def render_sidebar(selected_school_id: Optional[str] = None) -> dict:
    """Render the complete sidebar and return filter values."""
    # Render filters
    filters = render_filters()

    # Determine routes based on selection
    routes = []
    schools = load_school_stops()

    if selected_school_id:
        # School is selected - use routes for that school
        render_school_stats(selected_school_id)

        if not schools.empty:
            school_match = schools[schools["school_id"] == selected_school_id]
            if len(school_match) > 0:
                routes = get_routes_for_school(school_match.iloc[0])
                filters["school_routes"] = routes
    else:
        # No school selected - build routes based on all active filters
        neighborhood = filters.get("neighborhood")
        selected_route = filters.get("selected_route")
        route_type = filters.get("route_type", "All")

        if neighborhood:
            # Get routes serving schools in that neighborhood
            neighborhood_schools = schools[schools["neighborhood"] == neighborhood]
            neighborhood_routes = set()
            for _, school in neighborhood_schools.iterrows():
                school_routes = get_routes_for_school(school)
                neighborhood_routes.update(school_routes)
            routes = list(neighborhood_routes)
            filters["neighborhood_school_count"] = len(neighborhood_schools)

            # Apply route type filter (Train only / Bus only)
            if route_type == "Train only":
                routes = [r for r in routes if r.endswith("_line")]
            elif route_type == "Bus only":
                routes = [r for r in routes if r.startswith("bus_")]

            # Apply specific route filter if set
            if selected_route:
                if selected_route in routes:
                    routes = [selected_route]
                else:
                    # Selected route not in neighborhood
                    routes = []

            # Store filtered neighborhood routes
            filters["neighborhood_routes"] = routes

        elif selected_route:
            # Just a specific route selected (no neighborhood)
            routes = [selected_route]

        else:
            # No neighborhood or specific route - use all routes filtered by type
            routes = filters.get("available_routes", get_all_routes())

    # Always show aggregate stats for the selected/filtered routes
    if routes:
        render_route_stats(routes, filters)

    return filters
