"""Bluesky data fetcher for CTA transit posts and comments.

This script fetches posts and comments from Bluesky that mention CTA-related
keywords, filters out news/alert accounts and non-CTA transit mentions,
and saves anonymized output to CSV files.

Environment Variables:
    BSKY_USERNAME: Bluesky username (e.g., "username.bsky.social")
    BSKY_PASSWORD: Bluesky app password

Usage:
    # Set environment variables directly
    export BSKY_USERNAME=your-username.bsky.social
    export BSKY_PASSWORD=your-app-password
    python bsky_data_fetch.py

    # Or use a .env file (copy from .env.example)
    python bsky_data_fetch.py
"""

import os
import sys
import time

# Load environment variables from .env file if present
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # python-dotenv not installed, rely on environment variables

import pandas as pd
from atproto import Client
from atproto_client.exceptions import NetworkError

from cta_pipeline.constants import BLUESKY_QUERIES, RAW_DATA_DIR_BSKY
from cta_pipeline.data_fetch import (
    Anonymizer,
    RateLimiter,
    RetryConfig,
    contains_blocked_keywords,
    get_env_var,
    is_blocked_user,
    with_retry,
)
from cta_pipeline.logging_config import configure_logging, get_logger

# Configure logging
configure_logging()
logger = get_logger(__name__)


class BlueskyFetcher:
    """Fetches and processes Bluesky posts and comments."""

    def __init__(self):
        """Initialize the Bluesky fetcher with authentication."""
        self.client = Client()
        self.rate_limiter = RateLimiter(max_calls=2800, window_seconds=300)
        self.retry_config = RetryConfig(max_retries=5, initial_delay=2.0)
        self.anonymizer = Anonymizer()

        # Storage
        self.posts: dict[str, dict] = {}
        self.comments: list[dict] = []
        self.tainted_threads: set[str] = set()

        # Stats
        self.network_errors = 0
        self.total_api_calls = 0

    def authenticate(self) -> None:
        """Authenticate with Bluesky using environment variables."""
        username = get_env_var("BSKY_USERNAME")
        password = get_env_var("BSKY_PASSWORD")

        logger.info("authenticating", username=username)
        self.client.login(username, password)
        logger.info("authentication_successful")

    def _extract_replies(
        self,
        node,
        root_post_uri: str,
        parent_comment_id: str = None,
    ) -> list[dict]:
        """
        Recursively extract all comments in a thread.

        Args:
            node: Thread node from Bluesky API
            root_post_uri: URI of the root post
            parent_comment_id: Anonymized parent comment ID

        Returns:
            List of comment dictionaries
        """
        comments = []

        if node is None or not hasattr(node, "post"):
            return comments

        post_obj = node.post
        record = getattr(post_obj, "record", None)
        text = getattr(record, "text", "") if record is not None else ""

        # Normalize text
        if not isinstance(text, str):
            text = ""

        is_root = (post_obj.uri == root_post_uri) and (parent_comment_id is None)

        # Skip root posts and filtered accounts
        if not is_root and not is_blocked_user(post_obj.author.handle, "bluesky"):
            # Anonymize IDs
            anon_post_id = self.anonymizer.anonymize_post_id(root_post_uri)
            anon_comment_id = self.anonymizer.anonymize_comment_id(post_obj.uri)
            anon_author = self.anonymizer.anonymize_author(post_obj.author.handle)

            comments.append({
                "post_id": anon_post_id,
                "comment_id": anon_comment_id,
                "parent_comment_id": parent_comment_id,
                "author": anon_author,
                "text": text,
                "timestamp": getattr(record, "created_at", None),
            })

            # Update parent for child replies
            parent_comment_id = anon_comment_id

        # Process replies recursively
        if getattr(node, "replies", None):
            for reply in node.replies:
                comments.extend(
                    self._extract_replies(
                        reply,
                        root_post_uri=root_post_uri,
                        parent_comment_id=parent_comment_id,
                    )
                )

        return comments

    def _fetch_thread(self, post_uri: str) -> list[dict]:
        """
        Fetch full thread for a post.

        Args:
            post_uri: URI of the post

        Returns:
            List of comments in the thread
        """
        self.rate_limiter.check()

        def fetch():
            return self.client.app.bsky.feed.get_post_thread(
                params={"uri": post_uri, "depth": 10}
            )

        thread = with_retry(fetch, self.retry_config)
        self.rate_limiter.increment()
        self.total_api_calls += 1

        if thread is None:
            return []

        return self._extract_replies(thread.thread, root_post_uri=post_uri)

    def fetch_query(self, query: str) -> None:
        """
        Fetch all posts matching a query.

        Args:
            query: Search query string
        """
        cursor = None
        logger.info("fetching_query", query=query)

        while True:
            self.rate_limiter.check()

            try:
                results = self.client.app.bsky.feed.search_posts(
                    params={"q": query, "limit": 100, "cursor": cursor}
                )
                self.rate_limiter.increment()
                self.total_api_calls += 1
                self.network_errors = 0  # Reset on success

                new_posts = 0
                new_comments = 0

                for post in results.posts:
                    handle = post.author.handle
                    text = getattr(post.record, "text", "")

                    # Normalize text
                    if not isinstance(text, str):
                        text = ""

                    # Skip if already processed or tainted
                    if post.uri in self.posts or post.uri in self.tainted_threads:
                        continue

                    # Skip news/alert accounts
                    if is_blocked_user(handle, "bluesky"):
                        continue

                    # Check for blocked keywords in root post
                    if contains_blocked_keywords(text):
                        self.tainted_threads.add(post.uri)
                        continue

                    # Fetch full thread
                    thread_comments = self._fetch_thread(post.uri)

                    # Check for blocked keywords in any comment
                    if any(contains_blocked_keywords(c["text"]) for c in thread_comments):
                        self.tainted_threads.add(post.uri)
                        continue

                    # Thread is clean - anonymize and store
                    anon_post_id = self.anonymizer.anonymize_post_id(post.uri)
                    anon_author = self.anonymizer.anonymize_author(handle)

                    self.posts[post.uri] = {
                        "post_id": anon_post_id,
                        "parent_id": None,
                        "author": anon_author,
                        "text": text,
                        "timestamp": getattr(post.record, "created_at", None),
                    }

                    self.comments.extend(thread_comments)
                    new_posts += 1
                    new_comments += len(thread_comments)

                logger.info(
                    "query_page_fetched",
                    query=query,
                    cursor=cursor[:20] if cursor else None,
                    new_posts=new_posts,
                    new_comments=new_comments,
                )

                cursor = results.cursor
                if cursor is None:
                    break

            except NetworkError as e:
                self.network_errors += 1
                logger.warning(
                    "network_error",
                    count=self.network_errors,
                    error=str(e),
                )

                if self.network_errors >= 5:
                    logger.error("too_many_network_errors")
                    raise

                time.sleep(2.0)

    def fetch_all(self) -> None:
        """Fetch posts for all configured queries."""
        for query in BLUESKY_QUERIES:
            self.fetch_query(query)

    def save_output(self) -> None:
        """Save posts and comments to CSV files."""
        os.makedirs(RAW_DATA_DIR_BSKY, exist_ok=True)

        # Create DataFrames
        df_posts = pd.DataFrame(list(self.posts.values()))
        df_comments = pd.DataFrame(self.comments)

        # Deduplicate
        if "post_id" in df_posts.columns and not df_posts.empty:
            df_posts = df_posts.drop_duplicates(subset=["post_id"])

        if "comment_id" in df_comments.columns and not df_comments.empty:
            df_comments = df_comments.drop_duplicates(subset=["comment_id"])

        # Save
        posts_path = os.path.join(RAW_DATA_DIR_BSKY, "bsky_posts.csv")
        comments_path = os.path.join(RAW_DATA_DIR_BSKY, "bsky_comments.csv")

        df_posts.to_csv(posts_path, index=False)
        df_comments.to_csv(comments_path, index=False)

        logger.info(
            "output_saved",
            posts_file=posts_path,
            comments_file=comments_path,
            unique_posts=len(df_posts),
            unique_comments=len(df_comments),
            tainted_threads=len(self.tainted_threads),
            total_api_calls=self.total_api_calls,
        )


def main():
    """Main entry point."""
    print("=" * 60)
    print("BLUESKY CTA DATA FETCHER")
    print("=" * 60)

    try:
        fetcher = BlueskyFetcher()
        fetcher.authenticate()
        fetcher.fetch_all()
        fetcher.save_output()

        logger.info("fetch_completed")

    except ValueError as e:
        logger.error("configuration_error", error=str(e))
        sys.exit(1)
    except Exception as e:
        logger.error("fetch_failed", error=str(e), exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
