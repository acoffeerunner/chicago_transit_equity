"""Sentiment analysis utilities - route context extraction."""
import re

from cta_pipeline.errors import TransformError
from cta_pipeline.logging_config import get_logger
from cta_pipeline.stop_extraction import detect_sarcasm

logger = get_logger(__name__)


def extract_route_context(text: str, route: str) -> str:
    """
    Extract sentence(s) containing the route mention.

    Args:
        text: Full text to search
        route: Route identifier (e.g., "red_line", "bus_66")

    Returns:
        Extracted context string (sentences containing route), or original text if no match
    """
    if not isinstance(text, str):
        return text

    try:
        if route.endswith("_line"):
            color = route.replace("_line", "")
            pattern = rf"\b{color}\s+lines?\b"
        else:
            num = route.replace("bus_", "")
            pattern = rf"(?:bus\s*{num}|{num}\s*bus|#{num}|route\s*{num})\b"

        sentences = re.split(r"(?<=[.!?])\s+", text)
        relevant = [s for s in sentences if re.search(pattern, s, re.IGNORECASE)]

        return " ".join(relevant) if relevant else text
    except Exception as e:
        logger.warning("extract_route_context_failed", route=route, error=str(e))
        return text


def add_route_context(batch):
    """
    Map function to add route context for sentiment analysis.

    Args:
        batch: Dictionary with 'body' and 'route' keys

    Returns:
        Dictionary with 'route_context' key
    """
    try:
        contexts = [
            extract_route_context(text, route)
            for text, route in zip(batch["body"], batch["route"])
        ]
        return {"route_context": contexts}
    except Exception as e:
        logger.error("add_route_context_failed", error=str(e), exc_info=True)
        raise TransformError(f"Route context extraction failed: {e}") from e


def adjust_sentiment_for_sarcasm(route_sentiment: str, body: str) -> str:
    """
    Adjust sentiment for sarcasm detection.

    Flips positive to negative if sarcasm is detected.

    Args:
        route_sentiment: Original sentiment label
        body: Text body to check for sarcasm

    Returns:
        Adjusted sentiment label
    """
    if route_sentiment == "positive" and detect_sarcasm(body):
        return "negative"
    return route_sentiment



