"""Time of day extraction from timestamps and text."""
from datetime import datetime

import pytz

from cta_pipeline.constants import (
    AFTERNOON_KEYWORDS,
    CHICAGO_TZ,
    EVENING_KEYWORDS,
    MORNING_KEYWORDS,
    NIGHT_KEYWORDS,
)
from cta_pipeline.errors import TransformError
from cta_pipeline.logging_config import get_logger

logger = get_logger(__name__)

def _normalize_iso_fraction(ts: str) -> str:
    """
    Ensure ISO timestamp has <= 6 fractional digits so datetime.fromisoformat can parse it.
    """
    ts = ts.strip()
    if not ts:
        return ts
    ts = ts.replace("Z", "+00:00")

    m = _FRACTION_RE.search(ts)
    if not m:
        return ts

    frac = m.group(1)          # like ".401356645"
    digits = frac[1:]          # "401356645"
    digits6 = (digits + "000000")[:6]  # pad/trim to 6
    return ts[:m.start(1)] + "." + digits6 + ts[m.end(1):]

def get_time_of_day_from_timestamp(timestamp_str: str) -> str:
    """
    Convert UTC timestamp to Chicago time and determine time of day.

    Args:
        timestamp_str: ISO format timestamp string (UTC)

    Returns:
        Time of day: "morning", "afternoon", "evening", "night", or "unknown"
    """
    try:
        # Normalize the fractional part of the timestamp
        normalized_timestamp = _normalize_iso_fraction(timestamp_str)
        
        # Parse ISO format timestamp
        dt_utc = datetime.fromisoformat(normalized_timestamp.replace("Z", "+00:00"))

        # Convert to Chicago time (Central Time Zone)
        chicago_tz = pytz.timezone(CHICAGO_TZ)
        dt_chicago = dt_utc.astimezone(chicago_tz)

        # Determine time of day
        hour = dt_chicago.hour
        if 5 <= hour < 12:
            return "morning"
        elif 12 <= hour < 17:
            return "afternoon"
        elif 17 <= hour < 21:
            return "evening"
        else:
            return "night"
    except Exception as e:
        logger.debug("timestamp_parsing_failed", timestamp=timestamp_str, error=str(e))
        return "unknown"


def get_time_of_day_from_text(text: str) -> str:
    """
    Extract time of day from text using keywords.

    Args:
        text: Input text string

    Returns:
        Time of day: "morning", "afternoon", "evening", "night", or "unknown"
    """
    if not isinstance(text, str):
        return "unknown"

    text_lower = text.lower()

    # Check keywords in order of specificity
    if any(keyword in text_lower for keyword in MORNING_KEYWORDS):
        return "morning"

    if any(keyword in text_lower for keyword in AFTERNOON_KEYWORDS):
        return "afternoon"

    if any(keyword in text_lower for keyword in EVENING_KEYWORDS):
        return "evening"

    if any(keyword in text_lower for keyword in NIGHT_KEYWORDS):
        return "night"

    return "unknown"


def extract_time_of_day(batch):
    """
    Extract time of day from both timestamp and text.

    Uses timestamp as primary source, falls back to text if timestamp is unknown.

    Args:
        batch: Dictionary with keys:
            - timestamp: List of timestamp strings
            - text: List of text strings

    Returns:
        Dictionary with keys:
            - time_of_day_from_timestamp: List of time strings
            - time_of_day_from_text: List of time strings
            - time_of_day: List of final time strings (timestamp preferred)
    """
    try:
        time_from_timestamp = [
            get_time_of_day_from_timestamp(ts) for ts in batch["timestamp"]
        ]
        time_from_text = [get_time_of_day_from_text(text) for text in batch["text"]]

        # Use timestamp as primary source, fall back to text if timestamp is unknown
        final_time_of_day = []
        for ts_time, text_time in zip(time_from_timestamp, time_from_text):
            if ts_time != "unknown":
                final_time_of_day.append(ts_time)
            else:
                final_time_of_day.append(text_time)

        return {
            "time_of_day_from_timestamp": time_from_timestamp,
            "time_of_day_from_text": time_from_text,
            "time_of_day": final_time_of_day,
        }
    except Exception as e:
        logger.error("extract_time_of_day_failed", error=str(e), exc_info=True)
        raise TransformError(f"Time extraction failed: {e}") from e



