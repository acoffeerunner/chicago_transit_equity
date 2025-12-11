"""Sentiment visualization charts using Plotly."""

from datetime import datetime
from typing import Optional

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

from utils.data_loader import (
    compute_aggregate_stats,
    filter_sentiment_by_routes,
    filter_sentiment_by_routes_all_periods,
    load_route_sentiment,
    load_sentiment_time_series,
)
from utils.route_mapping import (
    SENTIMENT_COLORS,
    get_route_color,
    get_route_display_name,
)


def render_sentiment_donut(
    sentiment_df: pd.DataFrame,
    title: str = "Sentiment Distribution",
) -> go.Figure:
    """Render a donut chart showing sentiment distribution."""
    stats = compute_aggregate_stats(sentiment_df)

    labels = ["Positive", "Neutral", "Negative"]
    values = [
        stats["positive_count"],
        stats["neutral_count"],
        stats["negative_count"],
    ]
    colors = [
        SENTIMENT_COLORS["positive"],
        SENTIMENT_COLORS["neutral"],
        SENTIMENT_COLORS["negative"],
    ]

    fig = go.Figure(
        data=[
            go.Pie(
                labels=labels,
                values=values,
                hole=0.5,
                marker_colors=colors,
                textinfo="label+percent",
                textposition="outside",
                hovertemplate="%{label}: %{value}<br>%{percent}<extra></extra>",
            )
        ]
    )

    fig.update_layout(
        title=dict(text=title, x=0.5, xanchor="center"),
        showlegend=False,
        margin=dict(t=60, b=20, l=20, r=20),
        height=300,
    )

    # Add center annotation
    fig.add_annotation(
        text=f"<b>{stats['total_posts']}</b><br>posts",
        x=0.5,
        y=0.5,
        font_size=14,
        showarrow=False,
    )

    return fig


def render_time_of_day_chart(
    sentiment_df: pd.DataFrame,
    highlight_current: bool = True,
) -> go.Figure:
    """Render a bar chart showing sentiment by time of day."""
    # Get data for each time period
    time_periods = ["morning", "afternoon", "evening", "night"]
    data = []

    for period in time_periods:
        period_df = sentiment_df[sentiment_df["time_of_day"] == period]
        if not period_df.empty:
            stats = compute_aggregate_stats(period_df, period)
            data.append(
                {
                    "period": period.capitalize(),
                    "positive": stats["positive_pct"],
                    "neutral": stats["neutral_pct"],
                    "negative": stats["negative_pct"],
                    "avg_score": stats["avg_score"],
                    "total": stats["total_posts"],
                }
            )
        else:
            data.append(
                {
                    "period": period.capitalize(),
                    "positive": 0,
                    "neutral": 0,
                    "negative": 0,
                    "avg_score": 0,
                    "total": 0,
                }
            )

    df = pd.DataFrame(data)

    # Determine current period
    current_hour = datetime.now().hour
    if 5 <= current_hour < 12:
        current_period = "Morning"
    elif 12 <= current_hour < 17:
        current_period = "Afternoon"
    elif 17 <= current_hour < 21:
        current_period = "Evening"
    else:
        current_period = "Night"

    # Create stacked bar chart
    fig = go.Figure()

    # Add bars for each sentiment
    for sentiment, color in [
        ("positive", SENTIMENT_COLORS["positive"]),
        ("neutral", SENTIMENT_COLORS["neutral"]),
        ("negative", SENTIMENT_COLORS["negative"]),
    ]:
        fig.add_trace(
            go.Bar(
                name=sentiment.capitalize(),
                x=df["period"],
                y=df[sentiment],
                marker_color=color,
                hovertemplate="%{x}: %{y:.1f}%<extra></extra>",
            )
        )

    # Highlight current period
    if highlight_current:
        current_idx = df[df["period"] == current_period].index
        if len(current_idx) > 0:
            fig.add_vline(
                x=current_idx[0],
                line_width=2,
                line_dash="dash",
                line_color="rgba(0,0,0,0.3)",
                annotation_text="Now",
                annotation_position="top",
            )

    fig.update_layout(
        title=dict(text="Sentiment by Time of Day", x=0.5, xanchor="center"),
        barmode="stack",
        xaxis_title="",
        yaxis_title="Percentage",
        yaxis=dict(range=[0, 100]),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5),
        margin=dict(t=80, b=40, l=40, r=20),
        height=350,
    )

    return fig


def render_sentiment_over_time(
    routes: list[str],
    sources: Optional[list[str]] = None,
) -> Optional[go.Figure]:
    """Render a line chart showing sentiment over time."""
    time_series = load_sentiment_time_series()
    if time_series.empty:
        return None

    # Filter to selected routes
    df = time_series[time_series["route"].isin(routes)]
    if sources:
        df = df[df["source"].isin([s.lower() for s in sources])]

    if df.empty:
        return None

    # Aggregate by date
    daily = (
        df.groupby("date")
        .agg(
            {
                "total_posts": "sum",
                "positive_count": "sum",
                "negative_count": "sum",
                "avg_sentiment_score": "mean",
            }
        )
        .reset_index()
    )

    if len(daily) < 2:
        return None

    # Calculate rolling average
    daily["net_sentiment"] = (
        daily["positive_count"] - daily["negative_count"]
    ) / daily["total_posts"]
    daily["net_sentiment_rolling"] = (
        daily["net_sentiment"].rolling(7, min_periods=1).mean()
    )

    fig = go.Figure()

    # Add raw data
    fig.add_trace(
        go.Scatter(
            x=daily["date"],
            y=daily["net_sentiment"],
            mode="markers",
            name="Daily",
            marker=dict(color="rgba(100,100,100,0.3)", size=6),
            hovertemplate="Date: %{x}<br>Net Sentiment: %{y:.2f}<extra></extra>",
        )
    )

    # Add rolling average
    fig.add_trace(
        go.Scatter(
            x=daily["date"],
            y=daily["net_sentiment_rolling"],
            mode="lines",
            name="7-day Rolling Avg",
            line=dict(color="#1f77b4", width=2),
            hovertemplate="Date: %{x}<br>Rolling Avg: %{y:.2f}<extra></extra>",
        )
    )

    # Add zero line
    fig.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.5)

    fig.update_layout(
        title=dict(text="Sentiment Over Time", x=0.5, xanchor="center"),
        xaxis_title="Date",
        yaxis_title="Net Sentiment (Positive - Negative)",
        yaxis=dict(range=[-1, 1]),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5),
        margin=dict(t=80, b=40, l=40, r=20),
        height=350,
    )

    return fig


def render_source_comparison(
    sentiment_df: pd.DataFrame,
) -> Optional[go.Figure]:
    """Render side-by-side comparison of Reddit vs Bluesky sentiment."""
    sources = sentiment_df["source"].unique()
    if len(sources) < 2:
        return None

    # Get stats for each source
    data = []
    for source in ["reddit", "bluesky"]:
        source_df = sentiment_df[sentiment_df["source"] == source]
        if not source_df.empty:
            stats = compute_aggregate_stats(source_df)
            data.append(
                {
                    "source": source.capitalize(),
                    "positive_pct": stats["positive_pct"],
                    "negative_pct": stats["negative_pct"],
                    "neutral_pct": stats["neutral_pct"],
                    "total_posts": stats["total_posts"],
                }
            )

    if len(data) < 2:
        return None

    df = pd.DataFrame(data)

    fig = make_subplots(
        rows=1,
        cols=2,
        subplot_titles=[f"{d['source']}<br>({d['total_posts']} posts)" for d in data],
        specs=[[{"type": "pie"}, {"type": "pie"}]],
    )

    for i, row in df.iterrows():
        fig.add_trace(
            go.Pie(
                labels=["Positive", "Neutral", "Negative"],
                values=[row["positive_pct"], row["neutral_pct"], row["negative_pct"]],
                marker_colors=[
                    SENTIMENT_COLORS["positive"],
                    SENTIMENT_COLORS["neutral"],
                    SENTIMENT_COLORS["negative"],
                ],
                hole=0.4,
                textinfo="percent",
                name=row["source"],
                showlegend=False,
            ),
            row=1,
            col=i + 1,
        )

    # Add invisible traces for the legend
    for sentiment, color in [
        ("Positive", SENTIMENT_COLORS["positive"]),
        ("Neutral", SENTIMENT_COLORS["neutral"]),
        ("Negative", SENTIMENT_COLORS["negative"]),
    ]:
        fig.add_trace(
            go.Scatter(
                x=[None],
                y=[None],
                mode="markers",
                marker=dict(size=10, color=color),
                name=sentiment,
                showlegend=True,
            )
        )

    fig.update_layout(
        title=dict(text="Source Comparison", x=0.5, xanchor="center"),
        legend=dict(orientation="h", yanchor="bottom", y=-0.1, xanchor="center", x=0.5),
        margin=dict(t=80, b=60, l=20, r=20),
        height=350,
    )

    return fig


def render_route_comparison(
    sentiment_df: pd.DataFrame,
    max_routes: int = 10,
) -> Optional[go.Figure]:
    """Render comparison of sentiment across routes."""
    # Get overall stats per route
    route_stats = []

    for route in sentiment_df["route"].unique():
        route_df = sentiment_df[
            (sentiment_df["route"] == route) & (sentiment_df["time_of_day"] == "all")
        ]
        if not route_df.empty:
            stats = compute_aggregate_stats(route_df)
            route_stats.append(
                {
                    "route": route,
                    "display_name": get_route_display_name(route),
                    "color": get_route_color(route),
                    "positive_pct": stats["positive_pct"],
                    "negative_pct": stats["negative_pct"],
                    "net_sentiment": stats["positive_pct"] - stats["negative_pct"],
                    "total_posts": stats["total_posts"],
                }
            )

    if not route_stats:
        return None

    df = pd.DataFrame(route_stats)

    # Sort by net sentiment
    df = df.sort_values("net_sentiment", ascending=True).tail(max_routes)

    fig = go.Figure()

    # Add horizontal bar chart
    fig.add_trace(
        go.Bar(
            y=df["display_name"],
            x=df["net_sentiment"],
            orientation="h",
            marker_color=[
                SENTIMENT_COLORS["positive"] if x > 0 else SENTIMENT_COLORS["negative"]
                for x in df["net_sentiment"]
            ],
            hovertemplate=("%{y}<br>Net Sentiment: %{x:.1f}%<br><extra></extra>"),
        )
    )

    # Add zero line
    fig.add_vline(x=0, line_color="black", line_width=1)

    fig.update_layout(
        title=dict(text="Route Sentiment Comparison", x=0.5, xanchor="center"),
        xaxis_title="Net Sentiment (Positive% - Negative%)",
        yaxis_title="",
        margin=dict(t=60, b=40, l=120, r=20),
        height=max(300, len(df) * 35),
    )

    return fig


def render_sentiment_charts(
    routes: list[str],
    filters: dict,
):
    """Render all sentiment charts for selected routes."""
    sentiment_df = load_route_sentiment()

    if sentiment_df.empty:
        st.warning("No sentiment data available. Run precomputation scripts first.")
        return

    # Filter data with time_of_day filter applied
    filtered = filter_sentiment_by_routes(
        sentiment_df,
        routes=routes,
        sources=filters.get("sources"),
        time_of_day=filters.get("time_of_day"),
    )

    # Get data with all time periods for the time of day chart
    filtered_all_times = filter_sentiment_by_routes_all_periods(
        sentiment_df,
        routes=routes,
        sources=filters.get("sources"),
    )

    if filtered.empty:
        st.info("No data available for the selected filters.")
        return

    # Create layout - Row 1: Sentiment donut + Source comparison (or Time of day if only one source)
    has_source_comparison = len(filters.get("sources", [])) >= 2

    col1, col2 = st.columns(2)

    with col1:
        # Sentiment donut
        fig = render_sentiment_donut(filtered)
        st.plotly_chart(fig, width="stretch")

    with col2:
        if has_source_comparison:
            # Source comparison
            fig = render_source_comparison(filtered)
            if fig:
                st.plotly_chart(fig, width="stretch")
        else:
            # Time of day chart (when only one source selected)
            fig = render_time_of_day_chart(filtered_all_times)
            st.plotly_chart(fig, width="stretch")

    # Time of day chart (if source comparison was shown above)
    if has_source_comparison:
        fig = render_time_of_day_chart(filtered_all_times)
        st.plotly_chart(fig, width="stretch")

    # Sentiment over time (if date range allows)
    fig = render_sentiment_over_time(routes, filters.get("sources"))
    if fig:
        st.plotly_chart(fig, width="stretch")

    # Route comparison (if multiple routes)
    if len(routes) > 1:
        fig = render_route_comparison(filtered)
        if fig:
            st.plotly_chart(fig, width="stretch")
