"""Sample post previews component."""

import html
from datetime import datetime

import pandas as pd
import streamlit as st

from utils.data_loader import load_top_posts, filter_posts_by_routes
from utils.route_mapping import SENTIMENT_COLORS, get_route_display_name


def escape_html(text: str) -> str:
    """Escape HTML special characters in text."""
    if not text:
        return ""
    return html.escape(str(text))


# Source icons/emojis
SOURCE_DISPLAY = {
    "reddit": {"icon": "https://www.redditstatic.com/desktop2x/img/favicon/android-icon-192x192.png", "emoji": "Reddit"},
    "bluesky": {"icon": "https://bsky.app/static/favicon-32x32.png", "emoji": "Bluesky"},
}


def format_timestamp(ts: str) -> str:
    """Format a timestamp string for display."""
    try:
        if pd.isna(ts):
            return "Unknown date"

        # Parse the timestamp
        ts_str = str(ts)
        if "+" in ts_str:
            ts_str = ts_str.split("+")[0]
        if "Z" in ts_str:
            ts_str = ts_str.replace("Z", "")

        dt = pd.to_datetime(ts_str)

        # Format relative to now
        now = datetime.now()
        diff = now - dt

        if diff.days == 0:
            if diff.seconds < 3600:
                return f"{diff.seconds // 60}m ago"
            return f"{diff.seconds // 3600}h ago"
        elif diff.days == 1:
            return "Yesterday"
        elif diff.days < 7:
            return f"{diff.days}d ago"
        elif diff.days < 30:
            return f"{diff.days // 7}w ago"
        else:
            return dt.strftime("%b %d, %Y")

    except Exception:
        return "Unknown date"


def render_post_card(post: pd.Series, show_route: bool = True):
    """Render a single post as a card."""
    sentiment = post.get("sentiment", "neutral")
    source = post.get("source", "unknown")
    score = post.get("score", 0)

    # Sentiment color
    sentiment_color = SENTIMENT_COLORS.get(sentiment, "#666")

    # Source display
    source_info = SOURCE_DISPLAY.get(source.lower(), {"emoji": source.capitalize()})

    # Build card HTML
    route_badge = ""
    if show_route and post.get("route"):
        route_name = get_route_display_name(post["route"])
        route_badge = f'<span style="background: #e0e0e0; color: #333; padding: 2px 8px; border-radius: 10px; font-size: 12px; margin-left: 8px;">{route_name}</span>'

    # Escape user content to prevent HTML injection
    body_escaped = escape_html(post.get('body', ''))
    author_escaped = escape_html(post.get('author', 'anonymous'))

    card_html = f"""<div style="border: 1px solid #e0e0e0; border-radius: 8px; padding: 12px; margin-bottom: 12px; background: white; border-left: 4px solid {sentiment_color};">
<div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px;">
<div style="display: flex; align-items: center; gap: 8px;">
<span style="font-weight: 500; color: #333;">@{author_escaped}</span>
<span style="background: {sentiment_color}; color: white; padding: 2px 8px; border-radius: 10px; font-size: 11px; text-transform: uppercase;">{sentiment}</span>
{route_badge}
</div>
<div style="color: #666; font-size: 13px;">{source_info['emoji']} | {format_timestamp(post.get('timestamp'))}</div>
</div>
<div style="color: #333; line-height: 1.5;">{body_escaped}</div>
<div style="margin-top: 8px; color: #888; font-size: 13px;">Confidence: {score:.1%}{' | Sarcastic' if post.get('is_sarcastic') else ''}</div>
</div>"""

    st.markdown(card_html, unsafe_allow_html=True)


def select_diverse_posts(
    df: pd.DataFrame,
    min_posts: int = 5,
    max_posts: int = 10,
) -> pd.DataFrame:
    """Select posts with route variety, prioritizing confidence score.

    Strategy:
    1. Get the top post (by score) from each unique route
    2. If we have fewer than min_posts, add more high-scoring posts
    3. Cap at max_posts total
    """
    if df.empty:
        return df

    # Sort by score descending
    df_sorted = df.sort_values("score", ascending=False)

    # Get unique routes in the filtered data
    available_routes = df_sorted["route"].dropna().unique()

    if len(available_routes) == 0:
        # No route info, just return top posts by score
        return df_sorted.head(min_posts)

    selected_indices = []

    # Step 1: Get top post from each route
    for route in available_routes:
        route_posts = df_sorted[df_sorted["route"] == route]
        if not route_posts.empty:
            # Get the highest-scoring post for this route
            selected_indices.append(route_posts.index[0])

    # Step 2: If we have fewer than min_posts, add more high-scoring posts
    if len(selected_indices) < min_posts:
        remaining = df_sorted[~df_sorted.index.isin(selected_indices)]
        needed = min_posts - len(selected_indices)
        additional = remaining.head(needed).index.tolist()
        selected_indices.extend(additional)

    # Cap at max_posts
    selected_indices = selected_indices[:max_posts]

    # Return selected posts, maintaining score order
    result = df_sorted[df_sorted.index.isin(selected_indices)]
    return result.sort_values("score", ascending=False)


def render_post_previews(
    routes: list[str],
    filters: dict,
    min_posts_per_sentiment: int = 5,
    max_posts_per_sentiment: int = 10,
):
    """Render sample posts for selected routes with route variety."""
    top_posts = load_top_posts()

    if top_posts.empty:
        st.info("No sample posts available. Run precomputation scripts first.")
        return

    # Filter posts
    filtered = filter_posts_by_routes(
        top_posts,
        routes=routes,
        sources=filters.get("sources"),
        time_of_day=filters.get("time_of_day"),
    )

    if filtered.empty:
        st.info("No posts available for the selected filters.")
        return

    st.subheader("Sample Posts")

    # Create tabs for each sentiment
    tabs = st.tabs(["Negative", "Positive", "Neutral"])

    for tab, sentiment in zip(tabs, ["negative", "positive", "neutral"]):
        with tab:
            sentiment_posts = filtered[filtered["sentiment"] == sentiment]

            # Select diverse posts across routes
            selected_posts = select_diverse_posts(
                sentiment_posts,
                min_posts=min_posts_per_sentiment,
                max_posts=max_posts_per_sentiment,
            )

            if selected_posts.empty:
                st.caption(f"No {sentiment} posts found.")
                continue

            # Show route count info
            unique_routes = selected_posts["route"].dropna().nunique()
            if unique_routes > 1:
                st.caption(f"Showing {len(selected_posts)} posts across {unique_routes} routes")

            for _, post in selected_posts.iterrows():
                render_post_card(post, show_route=len(routes) > 1)


def render_expanded_posts(
    routes: list[str],
    filters: dict,
):
    """Render expanded view of posts with full text."""
    top_posts = load_top_posts()

    if top_posts.empty:
        return

    filtered = filter_posts_by_routes(
        top_posts,
        routes=routes,
        sources=filters.get("sources"),
        time_of_day=filters.get("time_of_day"),
    )

    if filtered.empty:
        return

    st.subheader("All Sample Posts")

    # Group by route
    for route in routes:
        route_posts = filtered[filtered["route"] == route]
        if route_posts.empty:
            continue

        with st.expander(f"{get_route_display_name(route)} ({len(route_posts)} posts)"):
            for sentiment in ["positive", "negative", "neutral"]:
                sentiment_posts = route_posts[route_posts["sentiment"] == sentiment]
                if sentiment_posts.empty:
                    continue

                st.markdown(f"**{sentiment.capitalize()}** ({len(sentiment_posts)})")

                for _, post in sentiment_posts.iterrows():
                    # Use full_body if available, escape HTML
                    body = escape_html(post.get("full_body", post.get("body", "")))
                    author = escape_html(post.get('author', 'anon'))

                    st.markdown(
                        f"""<div style="padding: 8px; margin: 4px 0; background: #f9f9f9; border-left: 3px solid {SENTIMENT_COLORS[sentiment]}; border-radius: 4px;">
<small><b>@{author}</b> | {post.get('source', '').capitalize()} | {format_timestamp(post.get('timestamp'))}</small><br>
{body}
</div>""",
                        unsafe_allow_html=True,
                    )

                st.markdown("---")
