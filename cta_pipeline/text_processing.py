"""Text cleaning and preprocessing functions."""
import re

from emoji import demojize
from ftfy import fix_text

from cta_pipeline.errors import TransformError
from cta_pipeline.logging_config import get_logger

logger = get_logger(__name__)


def clean_text(s: str) -> str:
    """
    Clean text by fixing encoding, removing URLs, and normalizing whitespace.

    Args:
        s: Input text string

    Returns:
        Cleaned text string
    """
    if not isinstance(s, str):
        return ""

    try:
        # Fix text encoding issues
        s = fix_text(s)
        # Convert emojis to text
        s = demojize(s)
        # Remove URLs
        s = re.sub(r"http\S+|www\.\S+", "", s)
        # Collapse whitespace
        s = re.sub(r"\s+", " ", s)
        return s.strip()
    except Exception as e:
        logger.warning("text_cleaning_failed", error=str(e), text_preview=str(s)[:50])
        return str(s).strip() if s else ""


def preprocess_fn(batch):
    """
    Preprocess text batch: combine thread text and clean.

    Args:
        batch: Dictionary with keys:
            - text: List of text strings
            - combined_text: Optional list of combined thread text
            - is_thread_continuation: Optional list of boolean flags

    Returns:
        Dictionary with 'body' and 'body_lower' keys
    """
    try:
        body = []
        combined_texts = batch.get("combined_text", [None] * len(batch["text"]))
        is_continuations = batch.get("is_thread_continuation", [False] * len(batch["text"]))

        for text, combined, is_cont in zip(batch["text"], combined_texts, is_continuations):
            # For thread first-members, use combined text for full context
            # For continuations or non-threads, use individual text
            if combined and not is_cont:
                body.append(clean_text(combined))
            else:
                body.append(clean_text(text or ""))

        body_lower = [b.lower() for b in body]
        return {"body": body, "body_lower": body_lower}
    except Exception as e:
        logger.error("preprocess_fn_failed", error=str(e), exc_info=True)
        raise TransformError(f"Preprocessing failed: {e}") from e



