"""Common utilities for data fetching from Reddit and Bluesky."""

import hashlib
import os
import time
from dataclasses import dataclass, field
from typing import Callable, Optional, TypeVar

import requests

from cta_pipeline.logging_config import get_logger

logger = get_logger(__name__)

T = TypeVar("T")


# =============================================================================
# Rate Limiting
# =============================================================================


@dataclass
class RateLimiter:
    """Sliding window rate limiter for API calls."""

    max_calls: int = 100
    window_seconds: int = 60
    _call_count: int = field(default=0, init=False)
    _window_start: float = field(default_factory=time.time, init=False)

    def check(self) -> None:
        """Check rate limit and sleep if necessary."""
        now = time.time()

        # Reset window if expired
        if now - self._window_start >= self.window_seconds:
            self._call_count = 0
            self._window_start = now

        # Sleep if at limit
        if self._call_count >= self.max_calls:
            sleep_time = self.window_seconds - (now - self._window_start)
            if sleep_time > 0:
                logger.info(
                    "rate_limit_sleep",
                    sleep_seconds=round(sleep_time, 1),
                    calls_made=self._call_count,
                )
                time.sleep(sleep_time)
            self._call_count = 0
            self._window_start = time.time()

    def increment(self) -> None:
        """Increment the call counter."""
        self._call_count += 1


# =============================================================================
# Retry Logic
# =============================================================================


@dataclass
class RetryConfig:
    """Configuration for retry behavior."""

    max_retries: int = 5
    initial_delay: float = 1.0
    backoff_factor: float = 2.0
    max_delay: float = 60.0


def with_retry(
    func: Callable[[], T],
    config: RetryConfig = None,
    on_error: Optional[Callable[[Exception, int], None]] = None,
) -> Optional[T]:
    """
    Execute a function with retry logic and exponential backoff.

    Args:
        func: Function to execute
        config: Retry configuration
        on_error: Optional callback for error handling (receives exception and attempt number)

    Returns:
        Result of function or None if all retries failed
    """
    if config is None:
        config = RetryConfig()

    delay = config.initial_delay

    for attempt in range(1, config.max_retries + 1):
        try:
            return func()
        except Exception as e:
            if on_error:
                on_error(e, attempt)
            else:
                logger.warning(
                    "retry_attempt",
                    attempt=attempt,
                    max_retries=config.max_retries,
                    error=str(e),
                )

            if attempt == config.max_retries:
                logger.error("all_retries_failed", error=str(e))
                return None

            time.sleep(delay)
            delay = min(delay * config.backoff_factor, config.max_delay)

    return None


# =============================================================================
# Anonymization
# =============================================================================


class Anonymizer:
    """
    Generates consistent anonymous IDs for user data while preserving relationships.

    Uses a salted hash to ensure:
    - Same input always produces same output (for relationship preservation)
    - Cannot reverse the hash to get original value
    - Salt prevents rainbow table attacks
    """

    def __init__(self, salt: Optional[str] = None):
        """
        Initialize anonymizer with a salt.

        Args:
            salt: Secret salt for hashing. If not provided, generates a random one.
                  For consistent anonymization across runs, provide the same salt.
        """
        self.salt = salt or os.urandom(32).hex()
        self._cache: dict[str, str] = {}

    def anonymize(self, value: Optional[str], prefix: str = "") -> Optional[str]:
        """
        Generate an anonymous ID for a value.

        Args:
            value: The value to anonymize (e.g., username, post_id)
            prefix: Optional prefix for the anonymous ID (e.g., "user_", "post_")

        Returns:
            Anonymous ID or None if input is None/empty
        """
        if value is None or value == "":
            return None

        # Check cache for consistency
        cache_key = f"{prefix}:{value}"
        if cache_key in self._cache:
            return self._cache[cache_key]

        # Generate hash
        hash_input = f"{self.salt}:{value}".encode("utf-8")
        hash_digest = hashlib.sha256(hash_input).hexdigest()[:12]

        # Create anonymous ID
        anon_id = f"{prefix}{hash_digest}" if prefix else hash_digest
        self._cache[cache_key] = anon_id

        return anon_id

    def anonymize_author(self, author: Optional[str]) -> Optional[str]:
        """Anonymize an author/username."""
        return self.anonymize(author, prefix="user_")

    def anonymize_post_id(self, post_id: Optional[str]) -> Optional[str]:
        """Anonymize a post ID."""
        return self.anonymize(post_id, prefix="post_")

    def anonymize_comment_id(self, comment_id: Optional[str]) -> Optional[str]:
        """Anonymize a comment ID."""
        return self.anonymize(comment_id, prefix="comment_")


# =============================================================================
# HTTP Utilities
# =============================================================================

# Standard headers for Reddit API requests (mimics browser)
REDDIT_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Accept-Language": "en-US,en;q=0.5",
    "Connection": "keep-alive",
    "Host": "www.reddit.com",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Upgrade-Insecure-Requests": "1",
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:130.0) Gecko/20100101 Firefox/130.0",
}


def fetch_json(
    url: str,
    params: Optional[dict] = None,
    headers: Optional[dict] = None,
    timeout: int = 10,
) -> Optional[dict]:
    """
    Fetch JSON from a URL with error handling.

    Args:
        url: URL to fetch
        params: Query parameters
        headers: HTTP headers (defaults to REDDIT_HEADERS)
        timeout: Request timeout in seconds

    Returns:
        JSON response or None on error
    """
    if headers is None:
        headers = REDDIT_HEADERS

    try:
        response = requests.get(url, params=params, headers=headers, timeout=timeout)
        if response.status_code == 200:
            return response.json()
        else:
            logger.warning(
                "http_error",
                url=url,
                status_code=response.status_code,
            )
            return None
    except requests.RequestException as e:
        logger.warning("request_exception", url=url, error=str(e))
        return None


# =============================================================================
# Filter Lists
# =============================================================================

# Reddit bots and mod accounts to filter out
REDDIT_BLOCKED_USERS = frozenset([
    "AmputatorBot",
    "RemindMeBot",
    "WikiSummarizerBot",
    "TweetsInCommentsBot",
    "sneakpeekbot",
    "AutoModerator",
    "chicago-ModTeam",
    "cta-ModTeam",
    "ChicagoNWsideMods",
    "WindyCityChicagoMods",
])


# Bluesky news and alert accounts to filter out
BLUESKY_NEWS_ACCOUNTS = frozenset([
    "chicago-news.bsky.social",
    "chicago.thesocial.news",
    "thenewshub.bsky.social",
    "some-news.bsky.social",
    "illinews.bsky.social",
    "chicagonewsbot.bertshouse.social.ap.brid.gy",
    "us-news.bsky.social",
    "worldbytenews.bsky.social",
    "omalliecatt.news",
    "wbbmnewsradio.bsky.social",
    "ntsbnewsroom.govmirrors.com",
    "ntsbnewsroom.bsky.social",
    "fox-news.bsky.social",
    "newsbychinny.bsky.social",
    "la-news.bsky.social",
    "worldnewstodayoff.bsky.social",
    "shaw-local-news.bsky.social",
    "news-tribune.bsky.social",
    "morris-herald-news.bsky.social",
    "newsbeep.bsky.social",
    "latestnewsx9.bsky.social",
    "10bmnews.bsky.social",
    "newsoneofficial.bsky.social",
    "100yearsagonews.bsky.social",
    "cavenewstimes.com",
    "21alivenews.com",
    "newsnation.bsky.social",
    "media-news.at.thenote.app",
    "ctvnews.bsky.social",
    "nbcnews.com",
    "abc10news.bsky.social",
    "scrippsnews.bsky.social",
    "apnews.com",
    "victoriannews.com.au",
    "gotanygoodnews.bsky.social",
    "chicagonewsbench.bsky.social",
    "newsjennifer.bsky.social",
    "meidasherefornews.bsky.social",
    "virtualnews360.bsky.social",
    "cbsnewscn.nzcow.com",
    "wkow27news.bsky.social",
    "phatznewsroom.bsky.social",
    "asanews.bsky.social",
    "oldbaseballnews.bsky.social",
    "katz.theracket.news",
    "mprnews.org",
    "bringmethenews.bsky.social",
    "wandnews.bsky.social",
    "southrichmondnews.bsky.social",
    "newsnetworks.bsky.social",
    "news-feed.bsky.social",
    "cybersecuritynews.bsky.social",
    "nycnewsfeed.bsky.social",
    "news-flows-ir.bsky.social",
    "citizenptnewsil.bsky.social",
    "ksntnews.bsky.social",
    "newsfedora.bsky.social",
    "apnews-world-rss.bsky.social",
    "sketchynews.net",
    "buffalonews.com",
    "global-news-2024.bsky.social",
    "expressnews.com",
    "veritynews.bsky.social",
    "american-news.bsky.social",
    "leehamnews.com",
    "mexiconews.bsky.social",
    "newsowl.bsky.social",
    "newsdesk.flipboard.social.ap.brid.gy",
    "disappearednews.bsky.social",
    "tanglenews.bsky.social",
    "technewslit.journa.host.ap.brid.gy",
    "technewslit.bsky.social",
    "atheranews.bsky.social",
    "newsen.bsky.social",
    "newworldnewnews.bsky.social",
    "breathfartnews.bsky.social",
    "sportsnewstimes.bsky.social",
    "bitcoincom-news.bsky.social",
    "hollywood-news.bsky.social",
    "worldnewstimes.bsky.social",
    "12sknnews.bsky.social",
    "citizenptnewsin.bsky.social",
    "newsdarts.bsky.social",
    "revnewsstand.bsky.social",
    "usatopnews.bsky.social",
    "angrydonkeynews.bsky.social",
    "therealnews.com",
    "strikenews.skyfleet.blue",
    "chi.streetsblog.org",
    "wttw.bsky.social",
    "starlinechicago.bsky.social",
    "chi.nws-bot.us",
    "noagendamedia.bsky.social",
    "findsuperdeals.bsky.social",
    "chicagoconnected.bsky.social",
    "illinoisicymi.bsky.social",
    "chicago.suntimes.com",
    "chicagotribune.com",
    "chicagocta.bsky.social",
    "cwbchicago.bsky.social",
    "theurbanist.org",
    "ilroadahead.bsky.social",
    "ilroadnetwork.bsky.social",
    "ilpolitihub.bsky.social",
    "cdupoldt.bsky.social",
    "govpritzker.illinois.gov",
    "chicagobriefly.bsky.social",
    "todayheadline.bsky.social",
    "cbschicago.bsky.social",
    "chicagorevbooks.bsky.social",
    "thefaceoff.net",
    "us-nb.bsky.social",
    "wgntv.com",
])

BLUESKY_ALERT_ACCOUNTS = frozenset([
    "ctaction.org",
    "ctaalerts.bsky.social",
    "chicagoalerts.com",
    "legitctaalerts.bsky.social",
    "metraalerts.bsky.social",
    "path-alerts.paxex.aero",
    "amtrak-status.paxex.aero",
    "chicagocta.bsky.social",
    "metraupdates.bsky.social",
])

# Keywords to filter out (non-CTA transit)
BLOCKED_KEYWORDS = frozenset(["metra", "amtrak", "bnsf"])


def is_blocked_user(username: str, platform: str = "reddit") -> bool:
    """Check if a user should be filtered out."""
    if platform == "reddit":
        return username in REDDIT_BLOCKED_USERS
    elif platform == "bluesky":
        return username in BLUESKY_NEWS_ACCOUNTS or username in BLUESKY_ALERT_ACCOUNTS
    return False


def contains_blocked_keywords(text: str) -> bool:
    """Check if text contains blocked keywords (Metra/Amtrak/BNSF)."""
    if not text:
        return False
    text_lower = text.lower()
    return any(keyword in text_lower for keyword in BLOCKED_KEYWORDS)


# =============================================================================
# Environment Variable Helpers
# =============================================================================


def get_env_var(name: str, required: bool = True, default: Optional[str] = None) -> Optional[str]:
    """
    Get an environment variable with logging.

    Args:
        name: Environment variable name
        required: Whether the variable is required
        default: Default value if not set

    Returns:
        Value or default

    Raises:
        ValueError: If required variable is not set
    """
    value = os.environ.get(name, default)

    if value is None and required:
        logger.error("missing_env_var", name=name)
        raise ValueError(f"Required environment variable {name} is not set")

    if value is not None:
        logger.debug("env_var_loaded", name=name)

    return value
