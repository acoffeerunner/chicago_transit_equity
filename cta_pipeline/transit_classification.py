"""Transit detection using rule-based and semantic classification."""
import torch
from sentence_transformers import util

from cta_pipeline.constants import (
    BUS_REGEX,
    SEM_MARGIN,
    SEM_THRESHOLD,
    TRANSIT_GROUNDING_KEYWORDS,
    TRANSIT_PATTERN,
)
from cta_pipeline.errors import TransformError
from cta_pipeline.logging_config import get_logger
from cta_pipeline.models import ModelBundle

logger = get_logger(__name__)


def transit_rule_match(batch):
    """
    Rule-based transit detection using keyword patterns.

    Args:
        batch: Dictionary with 'body_lower' key containing list of lowercased text strings

    Returns:
        Dictionary with 'is_transit' key (list of booleans)
    """
    try:
        is_transit = []
        for t in batch["body_lower"]:
            is_transit.append(
                bool(TRANSIT_PATTERN.search(t)) or bool(BUS_REGEX.search(t))
            )
        return {"is_transit": is_transit}
    except Exception as e:
        logger.error("transit_rule_match_failed", error=str(e), exc_info=True)
        raise TransformError(f"Transit rule matching failed: {e}") from e


def is_transit_semantic(batch, model_bundle: ModelBundle):
    """
    Semantic transit detection using GPU-optimized embeddings.

    Uses normalized embeddings with dot-product scoring (faster than cosine).

    Args:
        batch: Dictionary with 'body' and 'body_lower' keys
        model_bundle: ModelBundle with precomputed normalized embeddings

    Returns:
        Dictionary with keys:
            - transit_score: List of maximum transit similarity scores
            - transit_margin: List of margins (transit - non_transit)
            - is_transit_sem: List of boolean flags
    """
    try:
        # Encode text batch
        emb = model_bundle.sbert_model.encode(
            batch["body"], convert_to_tensor=True
        )
        emb = emb.to(model_bundle.device)
        emb = util.normalize_embeddings(emb)

        # Use dot-product for normalized embeddings (faster than cosine)
        transit_sim = util.dot_score(emb, model_bundle.transit_emb).max(dim=1).values
        non_transit_sim = (
            util.dot_score(emb, model_bundle.non_transit_emb).max(dim=1).values
        )

        # Require transit score to be higher than non-transit
        margin = (transit_sim - non_transit_sim).cpu().tolist()
        transit_max = transit_sim.cpu().tolist()

        texts = batch["body_lower"]
        is_transit = [
            t > SEM_THRESHOLD
            and m > SEM_MARGIN
            and any(kw in text for kw in TRANSIT_GROUNDING_KEYWORDS)
            for t, m, text in zip(transit_max, margin, texts)
        ]

        return {
            "transit_score": transit_max,
            "transit_margin": margin,
            "is_transit_sem": is_transit,
        }
    except Exception as e:
        logger.error("transit_semantic_failed", error=str(e), exc_info=True)
        raise TransformError(f"Transit semantic classification failed: {e}") from e



