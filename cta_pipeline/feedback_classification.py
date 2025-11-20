"""Feedback detection using rule-based and semantic classification."""
from sentence_transformers import util

from cta_pipeline.constants import (
    FEEDBACK_PATTERN,
    FEEDBACK_RULE_THRESHOLD,
    SEM_MARGIN,
    SEM_THRESHOLD,
)
from cta_pipeline.errors import TransformError
from cta_pipeline.logging_config import get_logger
from cta_pipeline.models import ModelBundle

logger = get_logger(__name__)


def is_feedback_semantic(batch, model_bundle: ModelBundle):
    """
    Semantic feedback detection using GPU-optimized embeddings.

    Uses normalized embeddings with dot-product scoring (faster than cosine).
    Uses top-3 mean instead of max for more robust scoring.

    Args:
        batch: Dictionary with 'body' key containing list of text strings
        model_bundle: ModelBundle with precomputed normalized embeddings

    Returns:
        Dictionary with keys:
            - fb_score: List of feedback similarity scores (top-3 mean)
            - nf_score: List of non-feedback similarity scores (top-3 mean)
            - is_feedback_sem: List of boolean flags
    """
    try:
        txts = batch["body"]
        emb = model_bundle.sbert_model.encode(txts, convert_to_tensor=True)
        emb = emb.to(model_bundle.device)
        emb = util.normalize_embeddings(emb)

        # Use dot-product for normalized embeddings (faster than cosine)
        fb_sim = util.dot_score(emb, model_bundle.feedback_emb)
        nf_sim = util.dot_score(emb, model_bundle.nonfeedback_emb)

        # Top-3 mean instead of max (less sensitive to single anchor)
        k = min(3, fb_sim.shape[1])
        fb = fb_sim.topk(k, dim=1).values.mean(dim=1).cpu().tolist()

        k = min(3, nf_sim.shape[1])
        nf = nf_sim.topk(k, dim=1).values.mean(dim=1).cpu().tolist()

        is_fb = [
            (a > SEM_THRESHOLD and (a - b) > SEM_MARGIN) for a, b in zip(fb, nf)
        ]
        return {"fb_score": fb, "nf_score": nf, "is_feedback_sem": is_fb}
    except Exception as e:
        logger.error("feedback_semantic_failed", error=str(e), exc_info=True)
        raise TransformError(f"Feedback semantic classification failed: {e}") from e


def feedback_rule_match(batch):
    """
    Rule-based feedback detection using keyword patterns.

    Requires feedback keywords, excludes #chicagoscanner, and checks semantic score.

    Args:
        batch: Dictionary with 'body_lower' and 'fb_score' keys

    Returns:
        Dictionary with 'is_feedback' key (list of booleans)
    """
    try:
        is_feedback = []
        for t, fb_score in zip(batch["body_lower"], batch["fb_score"]):
            is_feedback.append(
                bool(
                    (FEEDBACK_PATTERN.search(t))
                    and ("#chicagoscanner" not in t)
                    and (fb_score > FEEDBACK_RULE_THRESHOLD)
                )
            )
        return {"is_feedback": is_feedback}
    except Exception as e:
        logger.error("feedback_rule_match_failed", error=str(e), exc_info=True)
        raise TransformError(f"Feedback rule matching failed: {e}") from e


def classify_feedback_independently(batch):
    """
    For comments on non-feedback parents, evaluate feedback status independently.

    This allows feedback comments on news posts to pass through.

    Args:
        batch: Dictionary with keys:
            - record_type: List of record types ("post" or "comment")
            - is_feedback: List of boolean flags
            - is_feedback_sem: List of boolean flags
            - body: List of text strings

    Returns:
        Dictionary with 'is_feedback_independent' key (list of booleans)
    """
    try:
        is_independent_feedback = []
        for record_type, is_fb, is_fb_sem, text in zip(
            batch["record_type"],
            batch["is_feedback"],
            batch["is_feedback_sem"],
            batch["body"],
        ):
            # If already classified as feedback, keep it
            if is_fb or is_fb_sem:
                is_independent_feedback.append(True)
            # If comment, check independently (even if parent isn't feedback)
            elif record_type == "comment":
                # Re-run feedback keyword check on this comment alone
                has_feedback_keywords = bool(FEEDBACK_PATTERN.search(text.lower()))
                is_independent_feedback.append(has_feedback_keywords)
            else:
                is_independent_feedback.append(False)

        return {"is_feedback_independent": is_independent_feedback}
    except Exception as e:
        logger.error(
            "classify_feedback_independently_failed", error=str(e), exc_info=True
        )
        raise TransformError(f"Independent feedback classification failed: {e}") from e



