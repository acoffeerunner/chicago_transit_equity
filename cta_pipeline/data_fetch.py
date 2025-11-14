"""Common utilities for data fetching from Reddit and Bluesky."""

import time
from dataclasses import dataclass, field
from typing import Callable, Optional, TypeVar

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
