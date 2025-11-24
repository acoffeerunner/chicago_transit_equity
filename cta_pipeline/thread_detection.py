"""Thread detection and grouping logic."""
from datetime import datetime
from tqdm.auto import tqdm

from sentence_transformers import util

from cta_pipeline.constants import (
    CONTINUATION_PATTERN,
    THREAD_SIMILARITY_HIGH,
    THREAD_SIMILARITY_MODERATE,
    THREAD_TIME_GAP_SECONDS,
)
from cta_pipeline.errors import TransformError
from cta_pipeline.logging_config import get_logger
from cta_pipeline.models import ModelBundle

logger = get_logger(__name__)


def identify_thread_candidates(dataset):
    """
    Identify records that are potential thread continuations.

    A thread candidate is a same-author reply to their own post/comment.

    Args:
        dataset: Dataset with 'original_record_id', 'record_type', 'author',
                 'parent_comment_id', 'parent_post_id' columns

    Returns:
        dict mapping child_record_id -> parent_record_id for thread candidates
    """
    try:
        # Build lookup tables
        record_authors = {}  # record_id -> author
        record_parents = {}  # record_id -> parent_record_id

        for row in tqdm(dataset, desc="Building thread lookup"):
            record_id = row["original_record_id"]
            record_authors[record_id] = row["author"]

            # Determine parent
            if row["record_type"] == "comment":
                # Parent is either parent_comment_id or parent_post_id
                parent = row["parent_comment_id"] or row["parent_post_id"]
                record_parents[record_id] = parent
            else:
                # Posts don't have parents in this context
                record_parents[record_id] = None

        # Find same-author chains
        thread_candidates = {}
        for record_id, parent_id in record_parents.items():
            if parent_id is None:
                continue

            child_author = record_authors.get(record_id)
            parent_author = record_authors.get(parent_id)

            if child_author and parent_author and child_author == parent_author:
                thread_candidates[record_id] = parent_id

        logger.info("thread_candidates_identified", count=len(thread_candidates))
        return thread_candidates
    except Exception as e:
        logger.error("identify_thread_candidates_failed", error=str(e), exc_info=True)
        raise TransformError(f"Thread candidate identification failed: {e}") from e


def score_thread_relevance(dataset, thread_candidates, model_bundle: ModelBundle):
    """
    Score each thread candidate pair using:
    1. Continuation markers
    2. Semantic similarity (SBERT with GPU optimization)
    3. Temporal proximity

    Args:
        dataset: The unified dataset
        thread_candidates: dict of child_id -> parent_id
        model_bundle: ModelBundle with SBERT model

    Returns:
        dict mapping child_id -> {
            'parent_id': str,
            'has_marker': bool,
            'similarity': float,
            'time_gap_seconds': float,
            'should_combine': bool
        }
    """
    if not thread_candidates:
        return {}

    try:
        # Build lookup for text and timestamp
        record_data = {}
        for row in dataset:
            record_id = row["original_record_id"]
            record_data[record_id] = {
                "text": row.get("text", "") or "",
                "timestamp": row.get("timestamp", ""),
            }

        scores = {}

        # Process in batches for SBERT efficiency
        child_ids = list(thread_candidates.keys())
        child_texts = []
        parent_texts = []

        for child_id in child_ids:
            parent_id = thread_candidates[child_id]
            child_texts.append(record_data.get(child_id, {}).get("text", ""))
            parent_texts.append(record_data.get(parent_id, {}).get("text", ""))

        # Batch encode for similarity
        logger.info("encoding_thread_texts_for_similarity", count=len(child_ids))
        if child_texts:
            child_embeddings = model_bundle.sbert_model.encode(
                child_texts, convert_to_tensor=True, show_progress_bar=True
            )
            child_embeddings = child_embeddings.to(model_bundle.device)
            child_embeddings = util.normalize_embeddings(child_embeddings)

            parent_embeddings = model_bundle.sbert_model.encode(
                parent_texts, convert_to_tensor=True, show_progress_bar=True
            )
            parent_embeddings = parent_embeddings.to(model_bundle.device)
            parent_embeddings = util.normalize_embeddings(parent_embeddings)

            # Use dot-product for normalized embeddings (faster than cosine)
            sims = []
            for i in range(len(child_ids)):
                sim = util.dot_score(
                    child_embeddings[i : i + 1], parent_embeddings[i : i + 1]
                )
                sims.append(sim[0][0].item())
        else:
            sims = []

        # Score each pair
        for idx, child_id in enumerate(child_ids):
            parent_id = thread_candidates[child_id]
            child_data = record_data.get(child_id, {})
            parent_data = record_data.get(parent_id, {})

            # Signal 1: Continuation markers
            child_text = child_data.get("text", "")
            has_marker = bool(CONTINUATION_PATTERN.match(child_text.strip()))

            # Signal 2: Semantic similarity
            similarity = sims[idx] if idx < len(sims) else 0.0

            # Signal 3: Temporal proximity
            time_gap = float("inf")
            try:
                child_ts = child_data.get("timestamp", "")
                parent_ts = parent_data.get("timestamp", "")
                if child_ts and parent_ts:
                    t1 = datetime.fromisoformat(parent_ts.replace("Z", "+00:00"))
                    t2 = datetime.fromisoformat(child_ts.replace("Z", "+00:00"))
                    time_gap = (t2 - t1).total_seconds()
            except Exception:
                pass

            # Decision logic
            should_combine = (
                has_marker
                or similarity > THREAD_SIMILARITY_HIGH
                or (
                    similarity > THREAD_SIMILARITY_MODERATE
                    and time_gap < THREAD_TIME_GAP_SECONDS
                )
            )

            scores[child_id] = {
                "parent_id": parent_id,
                "has_marker": has_marker,
                "similarity": similarity,
                "time_gap_seconds": time_gap,
                "should_combine": should_combine,
            }

        combine_count = sum(1 for s in scores.values() if s["should_combine"])
        logger.info("thread_relevance_scored", total=len(scores), will_combine=combine_count)
        return scores
    except Exception as e:
        logger.error("score_thread_relevance_failed", error=str(e), exc_info=True)
        raise TransformError(f"Thread relevance scoring failed: {e}") from e


def build_thread_groups(relevance_scores):
    """
    Build thread groups from scored pairs.

    Uses Union-Find to group connected components.

    Args:
        relevance_scores: dict from score_thread_relevance()

    Returns:
        dict mapping record_id -> thread_id (or None if not in a thread)
    """
    # Filter to pairs that should combine
    combine_pairs = {
        child_id: data["parent_id"]
        for child_id, data in relevance_scores.items()
        if data["should_combine"]
    }

    if not combine_pairs:
        return {}

    try:
        # Union-Find implementation
        parent = {}

        def find(x):
            if x not in parent:
                parent[x] = x
            if parent[x] != x:
                parent[x] = find(parent[x])
            return parent[x]

        def union(x, y):
            px, py = find(x), find(y)
            if px != py:
                parent[px] = py

        # Union all pairs
        for child_id, parent_id in combine_pairs.items():
            union(child_id, parent_id)

        # Assign thread IDs (use root of each group)
        thread_assignments = {}
        for record_id in parent.keys():
            root = find(record_id)
            thread_assignments[record_id] = f"thread_{root}"

        logger.info("thread_groups_built", threads=len(set(thread_assignments.values())))
        return thread_assignments
    except Exception as e:
        logger.error("build_thread_groups_failed", error=str(e), exc_info=True)
        raise TransformError(f"Thread group building failed: {e}") from e


def consolidate_threads(dataset, thread_assignments):
    """
    Add thread metadata to dataset and prepare combined text for threads.

    Adds columns:
    - thread_id: Group ID for thread members (None if not in thread)
    - is_thread_continuation: True if this is a non-first part of thread
    - combined_text: For threads, the concatenated text of all parts

    Args:
        dataset: Dataset with thread assignments
        thread_assignments: dict from build_thread_groups()

    Returns:
        Dataset with thread metadata columns added
    """
    if not thread_assignments:
        # No threads to consolidate, add empty columns
        def add_empty_thread_columns(batch):
            n = len(batch["original_record_id"])
            return {
                "thread_id": [None] * n,
                "is_thread_continuation": [False] * n,
                "combined_text": [None] * n,
            }

        return dataset.map(add_empty_thread_columns, batched=True, batch_size=256)

    try:
        # Build lookup for record data
        record_data = {}
        for row in dataset:
            record_id = row["original_record_id"]
            record_data[record_id] = {
                "text": row.get("text", "") or "",
                "timestamp": row.get("timestamp", ""),
                "author": row.get("author", ""),
            }

        # Build thread -> members mapping, sorted by timestamp
        thread_members = {}
        for record_id, thread_id in thread_assignments.items():
            if thread_id not in thread_members:
                thread_members[thread_id] = []
            thread_members[thread_id].append(record_id)

        # Sort each thread's members by timestamp
        for thread_id in thread_members:
            thread_members[thread_id].sort(
                key=lambda rid: record_data.get(rid, {}).get("timestamp", "")
            )

        # Build combined text for each thread
        thread_combined_text = {}
        for thread_id, members in thread_members.items():
            texts = [record_data.get(rid, {}).get("text", "") for rid in members]
            thread_combined_text[thread_id] = " ".join(texts)

        # Determine first member of each thread
        thread_first = {
            thread_id: members[0] for thread_id, members in thread_members.items()
        }

        # Map function to add columns
        def add_thread_columns(batch):
            thread_ids = []
            is_continuation = []
            combined_texts = []

            for record_id in batch["original_record_id"]:
                t_id = thread_assignments.get(record_id)
                thread_ids.append(t_id)

                if t_id:
                    is_cont = record_id != thread_first.get(t_id)
                    is_continuation.append(is_cont)
                    combined_texts.append(thread_combined_text.get(t_id, ""))
                else:
                    is_continuation.append(False)
                    combined_texts.append(None)

            return {
                "thread_id": thread_ids,
                "is_thread_continuation": is_continuation,
                "combined_text": combined_texts,
            }

        dataset = dataset.map(add_thread_columns, batched=True, batch_size=256)
        logger.info("threads_consolidated", threads=len(thread_members))
        return dataset
    except Exception as e:
        logger.error("consolidate_threads_failed", error=str(e), exc_info=True)
        raise TransformError(f"Thread consolidation failed: {e}") from e



