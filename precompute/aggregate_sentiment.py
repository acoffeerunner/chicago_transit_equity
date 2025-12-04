"""Aggregate sentiment data per route and extract top posts."""

from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd


# =============================================================================
# Route Mapping Utilities (will be moved to dashboard.utils.route_mapping)
# =============================================================================

SENTIMENT_TO_GTFS = {
    # Train lines
    "red_line": "Red",
    "blue_line": "Blue",
    "green_line": "G",
    "orange_line": "Org",
    "brown_line": "Brn",
    "purple_line": "P",
    "pink_line": "Pink",
    "yellow_line": "Y",
}


def sentiment_route_to_gtfs(route: str) -> str:
    """Convert a sentiment data route name to GTFS route_id."""
    if route in SENTIMENT_TO_GTFS:
        return SENTIMENT_TO_GTFS[route]
    if route.startswith("bus_"):
        return route[4:]
    return route


def get_route_display_name(route: str) -> str:
    """Get a human-readable display name for a route."""
    if route.endswith("_line"):
        color = route.replace("_line", "")
        return f"{color.capitalize()} Line"
    if route.startswith("bus_"):
        number = route[4:]
        return f"Bus {number}"
    return route


def load_sentiment_data(data_dir: Path) -> pd.DataFrame:
    """Load and combine sentiment data from both sources."""
    bsky_path = data_dir / "processed" / "bsky" / "bsky_transit_feedback_labeled.csv"
    reddit_path = data_dir / "processed" / "reddit" / "reddit_transit_feedback_labeled.csv"

    dfs = []

    # Load Bluesky data
    if bsky_path.exists():
        bsky_df = pd.read_csv(bsky_path)
        bsky_df["source"] = "bluesky"
        dfs.append(bsky_df)
        print(f"Loaded {len(bsky_df)} Bluesky records")
    else:
        print(f"Warning: Bluesky data not found at {bsky_path}")

    # Load Reddit data
    if reddit_path.exists():
        reddit_df = pd.read_csv(reddit_path)
        reddit_df["source"] = "reddit"
        dfs.append(reddit_df)
        print(f"Loaded {len(reddit_df)} Reddit records")
    else:
        print(f"Warning: Reddit data not found at {reddit_path}")

    if not dfs:
        raise FileNotFoundError("No sentiment data files found")

    combined = pd.concat(dfs, ignore_index=True)
    print(f"Combined total: {len(combined)} records")

    return combined


def parse_timestamps(df: pd.DataFrame) -> pd.DataFrame:
    """Parse timestamp column to datetime."""
    df = df.copy()

    def safe_parse(ts):
        if pd.isna(ts):
            return None
        try:
            # Handle various formats
            ts_str = str(ts)
            # Remove timezone info for simplicity
            if "+" in ts_str:
                ts_str = ts_str.split("+")[0]
            if "Z" in ts_str:
                ts_str = ts_str.replace("Z", "")
            return pd.to_datetime(ts_str)
        except Exception:
            return None

    df["parsed_timestamp"] = df["timestamp"].apply(safe_parse)
    return df


def compute_route_sentiment_aggregates(df: pd.DataFrame) -> pd.DataFrame:
    """Compute aggregate sentiment metrics per route, source, and time_of_day."""
    # Filter to valid routes
    df = df[df["route"].notna()].copy()

    # Add GTFS route ID
    df["gtfs_route_id"] = df["route"].apply(sentiment_route_to_gtfs)
    df["route_display_name"] = df["route"].apply(get_route_display_name)

    # Group by route, source, time_of_day
    aggregations = []

    # Overall aggregation per route + source
    for (route, source), group in df.groupby(["route", "source"]):
        total = len(group)
        positive_count = (group["route_sentiment_adjusted"] == "positive").sum()
        negative_count = (group["route_sentiment_adjusted"] == "negative").sum()
        neutral_count = (group["route_sentiment_adjusted"] == "neutral").sum()
        sarcasm_count = (group["is_sarcastic"] == True).sum()

        # Feedback counts (either is_feedback or is_feedback_sem)
        is_feedback_col = group["is_feedback"] == True
        is_feedback_sem_col = group["is_feedback_sem"] == True if "is_feedback_sem" in group.columns else False
        is_feedback_any = is_feedback_col | is_feedback_sem_col
        feedback_posts = (is_feedback_any & (group["record_type"] == "post")).sum()
        feedback_comments = (is_feedback_any & (group["record_type"] == "comment")).sum()

        aggregations.append({
            "route": route,
            "source": source,
            "time_of_day": "all",
            "gtfs_route_id": sentiment_route_to_gtfs(route),
            "route_display_name": get_route_display_name(route),
            "total_posts": total,
            "positive_count": positive_count,
            "negative_count": negative_count,
            "neutral_count": neutral_count,
            "positive_pct": round(positive_count / total * 100, 1) if total > 0 else 0,
            "negative_pct": round(negative_count / total * 100, 1) if total > 0 else 0,
            "neutral_pct": round(neutral_count / total * 100, 1) if total > 0 else 0,
            "avg_sentiment_score": round(group["route_sentiment_score"].mean(), 3) if total > 0 else 0,
            "sarcasm_count": sarcasm_count,
            "sarcasm_rate": round(sarcasm_count / total * 100, 1) if total > 0 else 0,
            "feedback_post_count": int(feedback_posts),
            "feedback_comment_count": int(feedback_comments),
            "total_feedback_count": int(feedback_posts + feedback_comments),
        })

    # Aggregation by time of day
    for (route, source, tod), group in df.groupby(["route", "source", "time_of_day"]):
        if pd.isna(tod) or tod == "unknown":
            continue

        total = len(group)
        positive_count = (group["route_sentiment_adjusted"] == "positive").sum()
        negative_count = (group["route_sentiment_adjusted"] == "negative").sum()
        neutral_count = (group["route_sentiment_adjusted"] == "neutral").sum()
        sarcasm_count = (group["is_sarcastic"] == True).sum()

        # Feedback counts (either is_feedback or is_feedback_sem)
        is_feedback_col = group["is_feedback"] == True
        is_feedback_sem_col = group["is_feedback_sem"] == True if "is_feedback_sem" in group.columns else False
        is_feedback_any = is_feedback_col | is_feedback_sem_col
        feedback_posts = (is_feedback_any & (group["record_type"] == "post")).sum()
        feedback_comments = (is_feedback_any & (group["record_type"] == "comment")).sum()

        aggregations.append({
            "route": route,
            "source": source,
            "time_of_day": tod,
            "gtfs_route_id": sentiment_route_to_gtfs(route),
            "route_display_name": get_route_display_name(route),
            "total_posts": total,
            "positive_count": positive_count,
            "negative_count": negative_count,
            "neutral_count": neutral_count,
            "positive_pct": round(positive_count / total * 100, 1) if total > 0 else 0,
            "negative_pct": round(negative_count / total * 100, 1) if total > 0 else 0,
            "neutral_pct": round(neutral_count / total * 100, 1) if total > 0 else 0,
            "avg_sentiment_score": round(group["route_sentiment_score"].mean(), 3) if total > 0 else 0,
            "sarcasm_count": sarcasm_count,
            "sarcasm_rate": round(sarcasm_count / total * 100, 1) if total > 0 else 0,
            "feedback_post_count": int(feedback_posts),
            "feedback_comment_count": int(feedback_comments),
            "total_feedback_count": int(feedback_posts + feedback_comments),
        })

    return pd.DataFrame(aggregations)


def extract_top_posts(df: pd.DataFrame, n_per_sentiment: int = 5) -> pd.DataFrame:
    """Extract top N posts by confidence score for each sentiment type per route."""
    df = df[df["route"].notna()].copy()

    # Ensure body is not empty
    df = df[df["body"].notna() & (df["body"].str.len() > 0)]

    top_posts = []

    for route in df["route"].unique():
        route_df = df[df["route"] == route]

        for sentiment in ["positive", "negative", "neutral"]:
            sentiment_df = route_df[
                route_df["route_sentiment_adjusted"] == sentiment
            ].copy()

            if len(sentiment_df) == 0:
                continue

            # Sort by confidence score (descending)
            sentiment_df = sentiment_df.sort_values(
                "route_sentiment_score", ascending=False
            ).head(n_per_sentiment)

            for _, row in sentiment_df.iterrows():
                body = str(row["body"])
                # Truncate to 280 chars
                if len(body) > 280:
                    body = body[:277] + "..."

                top_posts.append({
                    "route": route,
                    "gtfs_route_id": sentiment_route_to_gtfs(route),
                    "route_display_name": get_route_display_name(route),
                    "sentiment": sentiment,
                    "score": round(row["route_sentiment_score"], 4),
                    "body": body,
                    "full_body": str(row["body"]),
                    "author": row.get("author", "unknown"),
                    "timestamp": row.get("timestamp"),
                    "source": row.get("source", "unknown"),
                    "record_type": row.get("record_type", "post"),
                    "is_sarcastic": row.get("is_sarcastic", False),
                })

    return pd.DataFrame(top_posts)


def compute_time_series_data(df: pd.DataFrame) -> pd.DataFrame:
    """Compute daily sentiment aggregates for time series charts."""
    df = df[df["route"].notna() & df["parsed_timestamp"].notna()].copy()

    if len(df) == 0:
        return pd.DataFrame()

    df["date"] = df["parsed_timestamp"].dt.date

    time_series = []

    for (route, source, date), group in df.groupby(["route", "source", "date"]):
        total = len(group)
        positive_count = (group["route_sentiment_adjusted"] == "positive").sum()
        negative_count = (group["route_sentiment_adjusted"] == "negative").sum()

        time_series.append({
            "route": route,
            "source": source,
            "date": date,
            "total_posts": total,
            "positive_count": positive_count,
            "negative_count": negative_count,
            "avg_sentiment_score": round(group["route_sentiment_score"].mean(), 3),
            # Net sentiment: (positive - negative) / total
            "net_sentiment": round((positive_count - negative_count) / total, 3) if total > 0 else 0,
        })

    return pd.DataFrame(time_series)


def aggregate_sentiment(data_dir: Path, output_dir: Path):
    """Main function to aggregate sentiment data."""
    print("Loading sentiment data...")
    df = load_sentiment_data(data_dir)

    print("\nParsing timestamps...")
    df = parse_timestamps(df)

    print("\nComputing route sentiment aggregates...")
    route_sentiment = compute_route_sentiment_aggregates(df)
    route_sentiment_path = output_dir / "route_sentiment.parquet"
    route_sentiment.to_parquet(route_sentiment_path, index=False)
    print(f"Saved route sentiment aggregates to {route_sentiment_path}")

    # Print summary
    print(f"\nRoute sentiment summary:")
    print(f"  Unique routes: {route_sentiment['route'].nunique()}")
    print(f"  Total aggregation rows: {len(route_sentiment)}")

    print("\nExtracting top posts...")
    top_posts = extract_top_posts(df, n_per_sentiment=5)
    top_posts_path = output_dir / "top_posts.parquet"
    top_posts.to_parquet(top_posts_path, index=False)
    print(f"Saved {len(top_posts)} top posts to {top_posts_path}")

    print("\nComputing time series data...")
    time_series = compute_time_series_data(df)
    if len(time_series) > 0:
        time_series_path = output_dir / "sentiment_time_series.parquet"
        time_series.to_parquet(time_series_path, index=False)
        print(f"Saved time series data ({len(time_series)} rows) to {time_series_path}")
    else:
        print("No time series data available (missing timestamps)")

    # Print overall statistics
    print("\n" + "=" * 50)
    print("Overall Statistics:")
    print("=" * 50)

    overall = route_sentiment[route_sentiment["time_of_day"] == "all"]
    total_feedback = overall["total_feedback_count"].sum()
    total_posts = overall["total_posts"].sum()

    print(f"Total posts/comments: {total_posts}")
    print(f"Total feedback identified: {total_feedback}")
    print(f"Unique routes with data: {overall['route'].nunique()}")

    # Top routes by feedback volume
    print("\nTop 10 routes by feedback volume:")
    top_routes = (
        overall.groupby("route")["total_feedback_count"]
        .sum()
        .sort_values(ascending=False)
        .head(10)
    )
    for route, count in top_routes.items():
        print(f"  {get_route_display_name(route)}: {count} feedbacks")


def main():
    """Run sentiment aggregation."""
    data_dir = Path(__file__).parent.parent / "data"
    output_dir = Path(__file__).parent.parent / "dashboard" / "precomputed"
    output_dir.mkdir(parents=True, exist_ok=True)

    aggregate_sentiment(data_dir, output_dir)


if __name__ == "__main__":
    main()
