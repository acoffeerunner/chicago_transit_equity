"""Model loading and embedding cache with GPU optimization."""
from dataclasses import dataclass
from typing import Optional

import torch
from sentence_transformers import SentenceTransformer, util
from transformers import pipeline

from cta_pipeline.constants import (
    FEEDBACK_ANCHORS,
    NONFEEDBACK_ANCHORS,
    NON_TRANSIT_ANCHORS,
    SBERT_MODEL_NAME,
    SENTIMENT_MODEL_NAME,
    TRANSIT_ANCHORS,
)
from cta_pipeline.errors import ModelLoadingError
from cta_pipeline.logging_config import get_logger

logger = get_logger(__name__)


@dataclass
class ModelBundle:
    """Bundle of models and precomputed embeddings for the pipeline."""

    sbert_model: SentenceTransformer
    sentiment_pipeline: pipeline
    transit_emb: torch.Tensor
    non_transit_emb: torch.Tensor
    feedback_emb: torch.Tensor
    nonfeedback_emb: torch.Tensor
    device: str

    def cleanup(self):
        """Explicit memory cleanup."""
        if torch.cuda.is_available() and self.device == "cuda":
            torch.cuda.empty_cache()
            logger.debug("cleared_gpu_cache")


def load_models(device: Optional[str] = None) -> ModelBundle:
    """
    Load all models with GPU optimization for embeddings.

    Precomputes and normalizes anchor embeddings for faster semantic search
    using dot-product instead of cosine similarity.

    Args:
        device: Device to use ("cuda", "cpu", or None for auto-detection)

    Returns:
        ModelBundle with all models and normalized embeddings

    Raises:
        ModelLoadingError: If model loading fails
    """
    if device is None:
        device = "cuda" if torch.cuda.is_available() else "cpu"

    logger.info("loading_models", device=device, sbert_model=SBERT_MODEL_NAME)

    try:
        # Load SentenceTransformer model
        sbert_model = SentenceTransformer(SBERT_MODEL_NAME)
        sbert_model = sbert_model.to(device)

        logger.info("computing_anchor_embeddings")

        # Precompute and normalize anchor embeddings for faster semantic search
        # Normalization allows us to use dot-product instead of cosine similarity
        transit_emb = sbert_model.encode(TRANSIT_ANCHORS, convert_to_tensor=True)
        transit_emb = transit_emb.to(device)
        transit_emb = util.normalize_embeddings(transit_emb)

        non_transit_emb = sbert_model.encode(NON_TRANSIT_ANCHORS, convert_to_tensor=True)
        non_transit_emb = non_transit_emb.to(device)
        non_transit_emb = util.normalize_embeddings(non_transit_emb)

        feedback_emb = sbert_model.encode(FEEDBACK_ANCHORS, convert_to_tensor=True)
        feedback_emb = feedback_emb.to(device)
        feedback_emb = util.normalize_embeddings(feedback_emb)

        nonfeedback_emb = sbert_model.encode(NONFEEDBACK_ANCHORS, convert_to_tensor=True)
        nonfeedback_emb = nonfeedback_emb.to(device)
        nonfeedback_emb = util.normalize_embeddings(nonfeedback_emb)

        logger.info("loading_sentiment_pipeline", model=SENTIMENT_MODEL_NAME)

        # Load sentiment pipeline
        sentiment_pipeline_device = 0 if device == "cuda" else -1
        sentiment_pipeline = pipeline(
            "text-classification",
            model=SENTIMENT_MODEL_NAME,
            top_k=1,
            truncation=False,
            device=sentiment_pipeline_device,
        )

        logger.info("models_loaded_successfully", device=device)

        return ModelBundle(
            sbert_model=sbert_model,
            sentiment_pipeline=sentiment_pipeline,
            transit_emb=transit_emb,
            non_transit_emb=non_transit_emb,
            feedback_emb=feedback_emb,
            nonfeedback_emb=nonfeedback_emb,
            device=device,
        )
    except Exception as e:
        logger.error("model_loading_failed", error=str(e), exc_info=True)
        raise ModelLoadingError(f"Failed to load models: {e}") from e


def semantic_search_normalized(
    query_emb: torch.Tensor, corpus_emb: torch.Tensor, device: str
) -> torch.Tensor:
    """
    Use dot-product for normalized embeddings (faster than cosine).

    Args:
        query_emb: Query embeddings (must be normalized)
        corpus_emb: Corpus embeddings (must be normalized)
        device: Device to use for computation

    Returns:
        Similarity scores (dot-product of normalized embeddings)
    """
    query_emb = query_emb.to(device)
    query_emb = util.normalize_embeddings(query_emb)
    return util.dot_score(query_emb, corpus_emb)



