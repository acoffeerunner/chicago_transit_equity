"""Data loading utilities for the dashboard."""

from pathlib import Path
from typing import Optional

import pandas as pd
import streamlit as st

from utils.route_mapping import (
    parse_sentiment_routes_from_gtfs_list,
    sentiment_route_to_gtfs,
)


def get_data_dir() -> Path:
    """Get the precomputed data directory path (dashboard/precomputed)."""
    # Relative to this file: dashboard/utils/data_loader.py -> dashboard/precomputed
    data_dir = Path(__file__).parent.parent / "precomputed"
    if data_dir.exists():
        return data_dir
    # Fall back to current working directory's dashboard/precomputed
    return Path("dashboard") / "precomputed"


@st.cache_data(ttl=3600)
def load_school_stops() -> pd.DataFrame:
    """Load precomputed school-stop data."""
    path = get_data_dir() / "school_stops.parquet"
    if not path.exists():
        st.error(f"School stops data not found. Run precomputation scripts first.")
        return pd.DataFrame()

    df = pd.read_parquet(path)

    # Convert list columns - handle numpy arrays, strings, and lists
    for col in ["bus_routes", "train_lines"]:
        if col in df.columns:
            def convert_to_list(x):
                if x is None:
                    return []
                if isinstance(x, str):
                    try:
                        return eval(x)
                    except:
                        return []
                if hasattr(x, 'tolist'):  # numpy array
                    return x.tolist()
                if isinstance(x, list):
                    return x
                return []
            df[col] = df[col].apply(convert_to_list)

    return df


@st.cache_data(ttl=3600)
def load_route_sentiment() -> pd.DataFrame:
    """Load precomputed route sentiment aggregates."""
    path = get_data_dir() / "route_sentiment.parquet"
    if not path.exists():
        st.error(f"Route sentiment data not found. Run precomputation scripts first.")
        return pd.DataFrame()

    return pd.read_parquet(path)


@st.cache_data(ttl=3600)
def load_top_posts() -> pd.DataFrame:
    """Load precomputed top posts."""
    path = get_data_dir() / "top_posts.parquet"
    if not path.exists():
        st.warning("Top posts data not found.")
        return pd.DataFrame()

    return pd.read_parquet(path)


@st.cache_data(ttl=3600)
def load_sentiment_time_series() -> pd.DataFrame:
    """Load precomputed sentiment time series."""
    path = get_data_dir() / "sentiment_time_series.parquet"
    if not path.exists():
        return pd.DataFrame()

    df = pd.read_parquet(path)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
    return df


def get_school_by_id(school_id: str) -> Optional[pd.Series]:
    """Get a school by its ID."""
    schools = load_school_stops()
    if schools.empty:
        return None

    matches = schools[schools["school_id"] == school_id]
    if len(matches) == 0:
        return None

    return matches.iloc[0]


def get_routes_for_school(school: pd.Series) -> list[str]:
    """Get all routes (in sentiment format) that serve a school's nearest stops."""
    routes = []

    # Bus routes
    bus_routes = school.get("bus_routes", [])
    if isinstance(bus_routes, list):
        for route in bus_routes:
            routes.append(f"bus_{route}")

    # Train lines
    train_lines = school.get("train_lines", [])
    if isinstance(train_lines, list):
        routes.extend(parse_sentiment_routes_from_gtfs_list(train_lines))

    return routes


def filter_sentiment_by_routes(
    sentiment_df: pd.DataFrame,
    routes: list[str],
    sources: Optional[list[str]] = None,
    time_of_day: Optional[list[str]] = None,
    time_window: str = "all",
) -> pd.DataFrame:
    """Filter sentiment data by routes and other criteria."""
    if sentiment_df.empty:
        return sentiment_df

    df = sentiment_df.copy()

    # Filter by routes
    if routes:
        df = df[df["route"].isin(routes)]

    # Filter by source
    if sources:
        df = df[df["source"].isin([s.lower() for s in sources])]

    # Filter by time of day
    if time_of_day:
        all_periods = ["morning", "afternoon", "evening", "night"]
        selected_periods = [t.lower() for t in time_of_day]

        # If all periods are selected, use "all" aggregate; otherwise use specific periods
        if set(selected_periods) == set(all_periods):
            df = df[df["time_of_day"] == "all"]
        else:
            df = df[df["time_of_day"].isin(selected_periods)]
    else:
        # No time_of_day filter, use "all" aggregates
        df = df[df["time_of_day"] == "all"]

    return df


def filter_sentiment_by_routes_all_periods(
    sentiment_df: pd.DataFrame,
    routes: list[str],
    sources: Optional[list[str]] = None,
) -> pd.DataFrame:
    """Filter sentiment data by routes and sources, keeping all time periods.

    This is used for the time of day chart which needs individual period data.
    """
    if sentiment_df.empty:
        return sentiment_df

    df = sentiment_df.copy()

    # Filter by routes
    if routes:
        df = df[df["route"].isin(routes)]

    # Filter by source
    if sources:
        df = df[df["source"].isin([s.lower() for s in sources])]

    # Keep only the individual time periods (not "all")
    df = df[df["time_of_day"].isin(["morning", "afternoon", "evening", "night"])]

    return df


def filter_posts_by_routes(
    posts_df: pd.DataFrame,
    routes: list[str],
    sources: Optional[list[str]] = None,
    time_of_day: Optional[list[str]] = None,
) -> pd.DataFrame:
    """Filter posts by routes, sources, and time of day."""
    if posts_df.empty:
        return posts_df

    df = posts_df.copy()

    # Filter by routes
    if routes:
        df = df[df["route"].isin(routes)]

    # Filter by source
    if sources:
        df = df[df["source"].isin([s.lower() for s in sources])]

    # Filter by time of day (based on post timestamp)
    if time_of_day:
        all_periods = ["morning", "afternoon", "evening", "night"]
        selected_periods = [t.lower() for t in time_of_day]

        # Only filter if not all periods are selected
        if set(selected_periods) != set(all_periods):
            # Parse timestamps and determine time of day
            df["parsed_ts"] = pd.to_datetime(df["timestamp"], errors="coerce")

            def get_time_of_day(ts):
                if pd.isna(ts):
                    return None
                hour = ts.hour
                if 5 <= hour < 12:
                    return "morning"
                elif 12 <= hour < 17:
                    return "afternoon"
                elif 17 <= hour < 21:
                    return "evening"
                else:
                    return "night"

            df["post_time_of_day"] = df["parsed_ts"].apply(get_time_of_day)
            df = df[df["post_time_of_day"].isin(selected_periods)]
            df = df.drop(columns=["parsed_ts", "post_time_of_day"])

    return df


def get_all_routes() -> list[str]:
    """Get list of all unique routes from sentiment data."""
    sentiment = load_route_sentiment()
    if sentiment.empty:
        return []

    return sorted(sentiment["route"].unique())


def get_train_routes() -> list[str]:
    """Get list of train routes."""
    all_routes = get_all_routes()
    return [r for r in all_routes if r.endswith("_line")]


def get_bus_routes() -> list[str]:
    """Get list of bus routes."""
    all_routes = get_all_routes()
    return [r for r in all_routes if r.startswith("bus_")]


def get_schools_for_route(route: str, max_distance_miles: float = 1.0) -> pd.DataFrame:
    """Get all schools that have a route serving their nearest stop within a radius.

    Args:
        route: Route in sentiment format (e.g., 'blue_line', 'bus_66')
        max_distance_miles: Maximum distance in miles to consider (default 1 mile)

    Returns:
        DataFrame of schools served by this route within the distance limit
    """
    from utils.route_mapping import sentiment_route_to_gtfs

    schools = load_school_stops()
    if schools.empty:
        return pd.DataFrame()

    # Convert sentiment route to GTFS format for matching
    gtfs_route = sentiment_route_to_gtfs(route)

    # For train lines, also check the display name format
    train_display_names = {
        "Red": "Red Line",
        "Blue": "Blue Line",
        "G": "Green Line",
        "Org": "Orange Line",
        "Brn": "Brown Line",
        "P": "Purple Line",
        "Pink": "Pink Line",
        "Y": "Yellow Line",
    }

    matching_schools = []

    for _, school in schools.iterrows():
        served = False
        distance_mi = None

        # Check bus routes
        if route.startswith("bus_"):
            bus_routes = school.get("bus_routes", [])
            distance_mi = school.get("bus_distance_mi", float("inf"))
            if isinstance(bus_routes, list) and gtfs_route in bus_routes:
                served = True

        # Check train lines
        elif route.endswith("_line"):
            train_lines = school.get("train_lines", [])
            distance_mi = school.get("train_distance_mi", float("inf"))
            if isinstance(train_lines, list):
                # Check both GTFS ID and display name formats
                if gtfs_route in train_lines:
                    served = True
                elif train_display_names.get(gtfs_route) in train_lines:
                    served = True

        # Only include if served AND within distance limit
        if served and distance_mi is not None and distance_mi <= max_distance_miles:
            matching_schools.append(school)

    if not matching_schools:
        return pd.DataFrame()

    return pd.DataFrame(matching_schools)


def compute_aggregate_stats(
    sentiment_df: pd.DataFrame,
    time_of_day_filter: str = "all",
) -> dict:
    """Compute aggregate statistics from filtered sentiment data."""
    if sentiment_df.empty:
        return {
            "total_posts": 0,
            "positive_pct": 0,
            "negative_pct": 0,
            "neutral_pct": 0,
            "avg_score": 0,
            "feedback_count": 0,
        }

    # Use the already-filtered data directly
    # (time_of_day filtering is now done in filter_sentiment_by_routes)
    df = sentiment_df

    if df.empty:
        return {
            "total_posts": 0,
            "positive_pct": 0,
            "negative_pct": 0,
            "neutral_pct": 0,
            "avg_score": 0,
            "feedback_count": 0,
        }

    total_posts = df["total_posts"].sum()
    positive = df["positive_count"].sum()
    negative = df["negative_count"].sum()
    neutral = df["neutral_count"].sum()
    total_feedback = df["total_feedback_count"].sum()

    # Weighted average sentiment score
    if total_posts > 0:
        weighted_score = (df["avg_sentiment_score"] * df["total_posts"]).sum() / total_posts
    else:
        weighted_score = 0

    return {
        "total_posts": int(total_posts),
        "positive_count": int(positive),
        "negative_count": int(negative),
        "neutral_count": int(neutral),
        "positive_pct": round(positive / total_posts * 100, 1) if total_posts > 0 else 0,
        "negative_pct": round(negative / total_posts * 100, 1) if total_posts > 0 else 0,
        "neutral_pct": round(neutral / total_posts * 100, 1) if total_posts > 0 else 0,
        "avg_score": round(weighted_score, 3),
        "feedback_count": int(total_feedback),
    }
