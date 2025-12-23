"""Reddit data fetcher for CTA transit posts and comments.

This script fetches posts from configured subreddits that mention CTA-related
keywords, then fetches all comments for those posts, filters out bots and mod
accounts, and saves anonymized output to CSV files.

Combines functionality from:
- reddit_json.py (post fetching)
- combine_posts.py (post aggregation)
- fetch_comments.py (comment fetching)

Usage:
    python reddit_data_fetch.py
"""

import os
import sys
import time
from typing import Optional

import pandas as pd

from cta_pipeline.constants import RAW_DATA_DIR_REDDIT, REDDIT_SUBREDDITS
from cta_pipeline.data_fetch import (
    REDDIT_HEADERS,
    Anonymizer,
    RateLimiter,
    RetryConfig,
    fetch_json,
    is_blocked_user,
    with_retry,
)
from cta_pipeline.logging_config import configure_logging, get_logger

# Configure logging
configure_logging()
logger = get_logger(__name__)


class RedditFetcher:
    """Fetches and processes Reddit posts and comments."""

    def __init__(self):
        """Initialize the Reddit fetcher."""
        self.rate_limiter = RateLimiter(max_calls=30, window_seconds=60)
        self.retry_config = RetryConfig(max_retries=5, initial_delay=5.0)
        self.anonymizer = Anonymizer()

        # Storage
        self.posts: list[dict] = []
        self.comments: list[dict] = []

        # Stats
        self.subreddits_completed = 0

    def _fetch_subreddit_posts(self, subreddit: str, max_pages: int = 20) -> list[dict]:
        """
        Fetch posts from a subreddit matching CTA-related keywords.

        Args:
            subreddit: Subreddit name
            max_pages: Maximum pages to fetch

        Returns:
            List of post dictionaries
        """
        posts = []
        after_param = ""
        page = 0

        logger.info("fetching_subreddit", subreddit=subreddit)

        while page < max_pages:
            self.rate_limiter.check()

            url = f"https://www.reddit.com/r/{subreddit}/search.json"
            params = {
                "q": "cta OR train OR bus OR line",
                "restrict_sr": 1,
                "limit": 100,
                "sort": "new",
                "after": after_param,
            }

            def do_fetch():
                return fetch_json(url, params=params, headers=REDDIT_HEADERS)

            data = with_retry(do_fetch, self.retry_config)
            self.rate_limiter.increment()

            if data is None:
                logger.warning("fetch_failed", subreddit=subreddit, page=page)
                time.sleep(60)
                continue

            page += 1

            # Extract posts
            children = data.get("data", {}).get("children", [])
            for child in children:
                if child.get("kind") != "t3":
                    continue

                post_data = child.get("data", {})
                permalink = post_data.get("permalink", "")

                # Generate a stable post_id from permalink
                original_id = permalink.split("/comments/")[1].split("/")[0] if "/comments/" in permalink else permalink

                posts.append({
                    "original_id": original_id,
                    "subreddit": subreddit,
                    "timestamp": post_data.get("created_utc"),
                    "title": post_data.get("title", ""),
                    "text": (post_data.get("title") or "") + " " + (post_data.get("selftext") or ""),
                    "author": post_data.get("author", ""),
                    "num_comments": post_data.get("num_comments", 0),
                    "permalink": permalink,
                    "score": post_data.get("score", 0),
                })

            # Get next page
            after_param = data.get("data", {}).get("after")
            if after_param is None:
                break

            logger.debug(
                "subreddit_page_fetched",
                subreddit=subreddit,
                page=page,
                posts_on_page=len(children),
            )

            time.sleep(5)  # Polite delay

        logger.info(
            "subreddit_completed",
            subreddit=subreddit,
            total_posts=len(posts),
        )

        return posts

    def fetch_all_posts(self) -> None:
        """Fetch posts from all configured subreddits."""
        for subreddit in REDDIT_SUBREDDITS:
            posts = self._fetch_subreddit_posts(subreddit)
            self.posts.extend(posts)
            self.subreddits_completed += 1

            logger.info(
                "subreddits_progress",
                completed=self.subreddits_completed,
                total=len(REDDIT_SUBREDDITS),
                total_posts=len(self.posts),
            )

    def _extract_comments(
        self,
        data: dict,
        post_id: str,
        parent_id: Optional[str] = None,
    ) -> list[dict]:
        """
        Recursively extract comments from Reddit comment tree.

        Args:
            data: Comment listing data
            post_id: Anonymized post ID
            parent_id: Anonymized parent comment ID

        Returns:
            List of comment dictionaries
        """
        comments = []

        for child in data.get("children", []):
            kind = child.get("kind")
            comment_data = child.get("data", {})

            if kind == "t1":
                author = comment_data.get("author", "")

                # Skip blocked users
                if is_blocked_user(author, "reddit"):
                    continue

                original_comment_id = comment_data.get("id", "")
                anon_comment_id = self.anonymizer.anonymize_comment_id(original_comment_id)
                anon_author = self.anonymizer.anonymize_author(author)

                comments.append({
                    "post_id": post_id,
                    "comment_id": anon_comment_id,
                    "parent_id": parent_id,
                    "timestamp": comment_data.get("created_utc"),
                    "body": comment_data.get("body", ""),
                    "author": anon_author,
                    "score": comment_data.get("score", 0),
                    "is_post": False,
                })

                # Process nested replies
                replies = comment_data.get("replies")
                if replies and isinstance(replies, dict):
                    replies_data = replies.get("data", {})
                    comments.extend(
                        self._extract_comments(
                            replies_data,
                            post_id=post_id,
                            parent_id=anon_comment_id,
                        )
                    )

        return comments

    def _fetch_post_comments(self, post: dict) -> list[dict]:
        """
        Fetch all comments for a post.

        Args:
            post: Post dictionary with permalink

        Returns:
            List of comment dictionaries
        """
        permalink = post.get("permalink", "")
        if not permalink:
            return []

        url = f"https://www.reddit.com{permalink}.json"

        self.rate_limiter.check()

        def do_fetch():
            return fetch_json(url, headers=REDDIT_HEADERS)

        result = with_retry(do_fetch, self.retry_config)
        self.rate_limiter.increment()

        if result is None or not isinstance(result, list) or len(result) < 2:
            return []

        # Anonymize post ID
        original_id = post.get("original_id", "")
        anon_post_id = self.anonymizer.anonymize_post_id(original_id)
        anon_author = self.anonymizer.anonymize_author(post.get("author", ""))

        comments = []

        # Extract post as root comment (for thread relationship)
        post_data = result[0].get("data", {}).get("children", [{}])[0].get("data", {})
        if not is_blocked_user(post_data.get("author", ""), "reddit"):
            comments.append({
                "post_id": anon_post_id,
                "comment_id": anon_post_id,  # Post is its own root
                "parent_id": None,
                "timestamp": post_data.get("created_utc"),
                "body": post_data.get("selftext", ""),
                "author": anon_author,
                "score": post_data.get("score", 0),
                "is_post": True,
            })

        # Extract comments
        comment_listing = result[1].get("data", {})
        comments.extend(
            self._extract_comments(
                comment_listing,
                post_id=anon_post_id,
                parent_id=anon_post_id,
            )
        )

        return comments

    def fetch_all_comments(self) -> None:
        """Fetch comments for all collected posts."""
        total_posts = len(self.posts)

        for idx, post in enumerate(self.posts):
            logger.info(
                "fetching_comments",
                progress=f"{idx + 1}/{total_posts}",
                subreddit=post.get("subreddit"),
            )

            post_comments = self._fetch_post_comments(post)
            self.comments.extend(post_comments)

            logger.debug(
                "post_comments_fetched",
                post_idx=idx + 1,
                comments_fetched=len(post_comments),
                total_comments=len(self.comments),
            )

            time.sleep(5)  # Polite delay

    def save_output(self) -> None:
        """Save posts and comments to CSV files."""
        os.makedirs(RAW_DATA_DIR_REDDIT, exist_ok=True)

        # Create anonymized posts DataFrame
        posts_data = []
        for post in self.posts:
            original_id = post.get("original_id", "")
            anon_post_id = self.anonymizer.anonymize_post_id(original_id)
            anon_author = self.anonymizer.anonymize_author(post.get("author", ""))

            posts_data.append({
                "post_id": anon_post_id,
                "subreddit": post.get("subreddit"),
                "timestamp": post.get("timestamp"),
                "text": post.get("text"),
                "author": anon_author,
                "num_comments": post.get("num_comments"),
                "score": post.get("score"),
            })

        df_posts = pd.DataFrame(posts_data)
        df_comments = pd.DataFrame(self.comments)

        # Deduplicate
        if "post_id" in df_posts.columns and not df_posts.empty:
            df_posts = df_posts.drop_duplicates(subset=["post_id"])

        if "comment_id" in df_comments.columns and not df_comments.empty:
            df_comments = df_comments.drop_duplicates(subset=["comment_id"])

        # Save
        posts_path = os.path.join(RAW_DATA_DIR_REDDIT, "reddit_posts.csv")
        comments_path = os.path.join(RAW_DATA_DIR_REDDIT, "reddit_comments.csv")

        df_posts.to_csv(posts_path, index=False)
        df_comments.to_csv(comments_path, index=False)

        logger.info(
            "output_saved",
            posts_file=posts_path,
            comments_file=comments_path,
            unique_posts=len(df_posts),
            unique_comments=len(df_comments),
        )


def main():
    """Main entry point."""
    print("=" * 60)
    print("REDDIT CTA DATA FETCHER")
    print("=" * 60)

    try:
        fetcher = RedditFetcher()

        # Phase 1: Fetch all posts
        logger.info("phase_start", phase="fetch_posts")
        fetcher.fetch_all_posts()

        # Phase 2: Fetch comments for all posts
        logger.info("phase_start", phase="fetch_comments")
        fetcher.fetch_all_comments()

        # Phase 3: Save output
        logger.info("phase_start", phase="save_output")
        fetcher.save_output()

        logger.info("fetch_completed")

    except Exception as e:
        logger.error("fetch_failed", error=str(e), exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
