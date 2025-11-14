"""Common utilities for data fetching from Reddit and Bluesky."""

import time
from dataclasses import dataclass, field

from cta_pipeline.logging_config import get_logger

logger = get_logger(__name__)


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
