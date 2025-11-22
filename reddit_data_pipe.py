"""Reddit CTA feedback pipeline orchestrator."""

import os
from datetime import datetime

import pandas as pd
import pytz
from datasets import Dataset
from tqdm.auto import tqdm
from transformers.pipelines.pt_utils import KeyDataset

from cta_pipeline.constants import (
    COMMENTS_PATH_REDDIT,
    DEFAULT_BATCH_SIZE,
    OUTPUT_DIR_REDDIT,
    POSTS_PATH_REDDIT,
)
from cta_pipeline.context_inheritance import (
    apply_route_inheritance,
    apply_time_inheritance,
)
from cta_pipeline.dataset_transforms import explode_routes_batched
from cta_pipeline.errors import ModelLoadingError, TransformError
from cta_pipeline.feedback_classification import (
    classify_feedback_independently,
    feedback_rule_match,
    is_feedback_semantic,
)
from cta_pipeline.gtfs_loader import load_gtfs_bus_intersections
from cta_pipeline.logging_config import configure_logging, get_logger
from cta_pipeline.metrics import PipelineMetrics, StageTimer, log_distribution_snapshot
from cta_pipeline.models import load_models
from cta_pipeline.route_extraction import extract_route_fn
from cta_pipeline.sentiment_analysis import (
    add_route_context,
    adjust_sentiment_for_sarcasm,
)
from cta_pipeline.stop_extraction import detect_sarcasm, extract_stops
from cta_pipeline.text_processing import preprocess_fn
from cta_pipeline.time_extraction import extract_time_of_day
from cta_pipeline.transit_classification import is_transit_semantic, transit_rule_match

# Configure logging
configure_logging()
logger = get_logger(__name__)

os.makedirs(OUTPUT_DIR_REDDIT, exist_ok=True)


def load_reddit_data():
    """
    Load Reddit posts and comments from CSV files and normalize to common format.

    Returns:
        Dataset with unified posts and comments
    """
    logger.info("loading_reddit_data")

    # Load the flat file (contains both posts and comments)
    comments_df = pd.read_csv(COMMENTS_PATH_REDDIT, dtype=str)
    logger.info("reddit_data_loaded_from_file", records=len(comments_df))

    # Separate posts and comments using is_post field
    posts_mask = comments_df["is_post"].fillna("").str.lower() == "true"
    posts_df = comments_df[posts_mask].copy()
    comments_only = comments_df[~posts_mask].copy()

    logger.info(
        "reddit_data_separated", posts=len(posts_df), comments=len(comments_only)
    )

    all_records = []

    # Helper function to safely convert to string
    def safe_str(val, default=""):
        if pd.isna(val) or val is None:
            return default
        return str(val)

    # Process posts
    for _, row in posts_df.iterrows():
        try:
            ts = float(row["timestamp"])
            dt = datetime.fromtimestamp(ts, tz=pytz.UTC)
            timestamp_iso = dt.isoformat()
        except Exception:
            timestamp_iso = ""

        post_id = safe_str(row.get("comment_id") or row.get("post_id", ""))
        all_records.append(
            {
                "post_id": post_id,
                "comment_id": None,
                "author": safe_str(row.get("author", "")),
                "text": safe_str(row.get("body", "")),
                "timestamp": timestamp_iso,
                "record_type": "post",
                "parent_post_id": post_id,
                "parent_comment_id": None,
                "original_record_id": post_id,
                "parent_id": None,
            }
        )

    # Process comments
    for _, row in comments_only.iterrows():
        try:
            ts = float(row["timestamp"])
            dt = datetime.fromtimestamp(ts, tz=pytz.UTC)
            timestamp_iso = dt.isoformat()
        except Exception:
            timestamp_iso = ""

        parent_id_val = row.get("parent_id", "")
        if pd.isna(parent_id_val) or parent_id_val == "":
            parent_comment_id = None
        else:
            parent_comment_id = str(parent_id_val)

        comment_id = safe_str(row["comment_id"])
        all_records.append(
            {
                "post_id": safe_str(row["post_id"]),
                "comment_id": comment_id,
                "author": safe_str(row.get("author", "")),
                "text": safe_str(row.get("body", "")),
                "timestamp": timestamp_iso,
                "record_type": "comment",
                "parent_post_id": safe_str(row["post_id"]),
                "parent_comment_id": parent_comment_id,
                "original_record_id": comment_id,
                "parent_id": parent_comment_id,
            }
        )

    dataset = Dataset.from_list(all_records)
    logger.info("reddit_data_normalized", total=len(all_records))
    return dataset


def build_reddit_threads(dataset):
    """
    Build thread groups using Reddit's native parent_id field.
    A thread is a same-author chain of replies.
    """
    logger.info("building_reddit_threads")

    # Build lookup tables
    record_authors = {}
    record_parents = {}
    record_data = {}

    for row in dataset:
        record_id = row["original_record_id"]
        record_authors[record_id] = row["author"]
        record_parents[record_id] = row.get("parent_id")
        record_data[record_id] = {
            "text": row.get("text", "") or "",
            "timestamp": row.get("timestamp", ""),
        }

    # Find same-author chains
    thread_candidates = {}
    for record_id, parent_id in record_parents.items():
        if not parent_id:
            continue

        child_author = record_authors.get(record_id)
        parent_author = record_authors.get(parent_id)

        if child_author and parent_author and child_author == parent_author:
            thread_candidates[record_id] = parent_id

    logger.info("reddit_thread_candidates_found", count=len(thread_candidates))

    # Union-Find for grouping
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

    for child_id, parent_id in thread_candidates.items():
        union(child_id, parent_id)

    thread_assignments = {}
    for record_id in parent.keys():
        root = find(record_id)
        thread_assignments[record_id] = f"thread_{root}"

    unique_threads = len(set(thread_assignments.values()))
    logger.info("reddit_threads_formed", threads=unique_threads)

    if not thread_assignments:

        def add_empty_thread_columns(batch):
            n = len(batch["original_record_id"])
            return {
                "thread_id": [None] * n,
                "is_thread_continuation": [False] * n,
                "combined_text": [None] * n,
            }

        return dataset.map(add_empty_thread_columns, batched=True, batch_size=256)

    # Build thread -> members mapping
    thread_members = {}
    for record_id, thread_id in thread_assignments.items():
        if thread_id not in thread_members:
            thread_members[thread_id] = []
        thread_members[thread_id].append(record_id)

    for thread_id in thread_members:
        thread_members[thread_id].sort(
            key=lambda rid: record_data.get(rid, {}).get("timestamp", "")
        )

    thread_combined_text = {}
    for thread_id, members in thread_members.items():
        texts = [record_data.get(rid, {}).get("text", "") for rid in members]
        thread_combined_text[thread_id] = " ".join(texts)

    thread_first = {
        thread_id: members[0] for thread_id, members in thread_members.items()
    }

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

    return dataset.map(add_thread_columns, batched=True, batch_size=256)


def main():
    """Main pipeline orchestrator."""
    print("=" * 60)
    print("REDDIT CTA FEEDBACK PIPELINE")
    print("=" * 60)

    pipeline_metrics = PipelineMetrics()

    try:
        # Load models once
        try:
            model_bundle = load_models()
        except ModelLoadingError as e:
            logger.error("model_loading_failed", error=str(e))
            raise

        # Stage 1: Load data
        with StageTimer("data_loading", rows_in=0) as timer:
            unified = load_reddit_data()
            posts_count = sum(1 for x in unified if x["record_type"] == "post")
            comments_count = sum(1 for x in unified if x["record_type"] == "comment")
            timer.rows_in = unified.num_rows
            timer.rows_out = unified.num_rows
            timer.extras["posts"] = posts_count
            timer.extras["comments"] = comments_count
        pipeline_metrics.stages.append(timer)

        # Stage 2: Thread detection (simpler Reddit version)
        with StageTimer("thread_detection", rows_in=unified.num_rows) as timer:
            unified = build_reddit_threads(unified)
            timer.rows_out = unified.num_rows
        pipeline_metrics.stages.append(timer)

        # Stage 3: Text preprocessing
        with StageTimer("text_preprocessing", rows_in=unified.num_rows) as timer:
            unified = unified.map(
                preprocess_fn, batched=True, batch_size=DEFAULT_BATCH_SIZE
            )
            timer.rows_out = unified.num_rows
        pipeline_metrics.stages.append(timer)

        # Stage 4: Time extraction
        with StageTimer("time_extraction", rows_in=unified.num_rows) as timer:
            unified = unified.map(
                extract_time_of_day, batched=True, batch_size=DEFAULT_BATCH_SIZE
            )
            timer.rows_out = unified.num_rows
        pipeline_metrics.stages.append(timer)

        # Stage 5: Route extraction
        with StageTimer("route_extraction", rows_in=unified.num_rows) as timer:
            unified = unified.map(
                extract_route_fn, batched=True, batch_size=DEFAULT_BATCH_SIZE
            )
            routes_found = sum(unified["has_route"])
            timer.extras["routes_found"] = routes_found
            timer.rows_out = unified.num_rows
        pipeline_metrics.stages.append(timer)

        # Stage 6: Route inheritance
        with StageTimer("route_inheritance", rows_in=unified.num_rows) as timer:
            unified = apply_route_inheritance(unified)
            inherited_count = sum(unified["has_inherited_route"])
            timer.extras["inherited_routes"] = inherited_count
            timer.rows_out = unified.num_rows
        pipeline_metrics.stages.append(timer)

        # Stage 7: Transit classification (rule-based)
        with StageTimer(
            "transit_rule_classification", rows_in=unified.num_rows
        ) as timer:
            unified = unified.map(
                transit_rule_match, batched=True, batch_size=DEFAULT_BATCH_SIZE
            )
            timer.rows_out = unified.num_rows
        pipeline_metrics.stages.append(timer)

        # Stage 8: Transit classification (semantic)
        with StageTimer(
            "transit_semantic_classification", rows_in=unified.num_rows
        ) as timer:

            def transit_semantic_wrapper(batch):
                return is_transit_semantic(batch, model_bundle)

            unified = unified.map(
                transit_semantic_wrapper, batched=True, batch_size=DEFAULT_BATCH_SIZE
            )
            unified = unified.filter(lambda x: x["is_transit_sem"] or x["is_transit"])
            timer.rows_out = unified.num_rows
        pipeline_metrics.stages.append(timer)

        # Stage 9: Feedback classification (semantic)
        with StageTimer(
            "feedback_semantic_classification", rows_in=unified.num_rows
        ) as timer:

            def feedback_semantic_wrapper(batch):
                return is_feedback_semantic(batch, model_bundle)

            unified = unified.map(
                feedback_semantic_wrapper, batched=True, batch_size=DEFAULT_BATCH_SIZE
            )
            timer.rows_out = unified.num_rows
        pipeline_metrics.stages.append(timer)

        # Stage 10: Feedback classification (rule-based)
        with StageTimer(
            "feedback_rule_classification", rows_in=unified.num_rows
        ) as timer:
            unified = unified.map(
                feedback_rule_match, batched=True, batch_size=DEFAULT_BATCH_SIZE
            )
            timer.rows_out = unified.num_rows
        pipeline_metrics.stages.append(timer)

        # Stage 11: Independent feedback classification
        with StageTimer(
            "feedback_independent_classification", rows_in=unified.num_rows
        ) as timer:
            unified = unified.map(
                classify_feedback_independently,
                batched=True,
                batch_size=DEFAULT_BATCH_SIZE * 2,
            )
            unified = unified.filter(lambda x: x["is_feedback_independent"])
            timer.rows_out = unified.num_rows
        pipeline_metrics.stages.append(timer)

        # Stage 12: Time inheritance
        with StageTimer("time_inheritance", rows_in=unified.num_rows) as timer:
            unified = apply_time_inheritance(unified)
            timer.rows_out = unified.num_rows
        pipeline_metrics.stages.append(timer)

        # Stage 13: Route explosion
        with StageTimer("route_explosion", rows_in=unified.num_rows) as timer:
            unified = explode_routes_batched(unified)
            timer.rows_out = unified.num_rows
        pipeline_metrics.stages.append(timer)

        if unified.num_rows == 0:
            logger.warning("no_records_after_explosion")
            unified.to_csv(
                os.path.join(OUTPUT_DIR_REDDIT, "reddit_transit_feedback_labeled.csv")
            )
            unified.to_json(
                os.path.join(OUTPUT_DIR_REDDIT, "reddit_transit_feedback_labeled.json")
            )
            return

        # Stage 14: Route context extraction
        with StageTimer("route_context_extraction", rows_in=unified.num_rows) as timer:
            unified = unified.map(
                add_route_context, batched=True, batch_size=DEFAULT_BATCH_SIZE
            )
            timer.rows_out = unified.num_rows
        pipeline_metrics.stages.append(timer)

        # Stage 15: Sentiment analysis
        with StageTimer("sentiment_analysis", rows_in=unified.num_rows) as timer:
            sentiments = []
            scores = []

            for out in tqdm(
                model_bundle.sentiment_pipeline(
                    KeyDataset(unified, "route_context"),
                    batch_size=64,
                    truncation=True,
                    max_length=512,
                ),
                total=len(unified),
                desc="Route sentiment",
            ):
                sentiments.append(out[0]["label"])
                scores.append(out[0]["score"])

            unified = unified.add_column("route_sentiment", sentiments)
            unified = unified.add_column("route_sentiment_score", scores)
            timer.rows_out = unified.num_rows
        pipeline_metrics.stages.append(timer)

        # Stage 16: Stop extraction
        with StageTimer("stop_extraction", rows_in=unified.num_rows) as timer:
            bus_intersections = load_gtfs_bus_intersections()
            stops_col = []
            for row in tqdm(unified, desc="Stop extraction"):
                stops = extract_stops(row["body"], row["route"], bus_intersections)
                stops_col.append(stops)

            unified = unified.add_column("stops", stops_col)
            unified = unified.add_column("stop_count", [len(s) for s in stops_col])
            unified = unified.add_column("has_stop", [len(s) > 0 for s in stops_col])
            timer.rows_out = unified.num_rows
        pipeline_metrics.stages.append(timer)

        # Stage 17: Sarcasm detection and sentiment adjustment
        with StageTimer("sarcasm_detection", rows_in=unified.num_rows) as timer:
            is_sarcastic_col = []
            adjusted_sentiment_col = []

            for row in unified:
                is_sarc = detect_sarcasm(row["body"])
                is_sarcastic_col.append(is_sarc)
                adjusted = adjust_sentiment_for_sarcasm(
                    row["route_sentiment"], row["body"]
                )
                adjusted_sentiment_col.append(adjusted)

            unified = unified.add_column("is_sarcastic", is_sarcastic_col)
            unified = unified.add_column(
                "route_sentiment_adjusted", adjusted_sentiment_col
            )
            timer.rows_out = unified.num_rows
        pipeline_metrics.stages.append(timer)

        # Log distribution snapshots
        log_distribution_snapshot(unified, "route_source", "final")
        log_distribution_snapshot(unified, "route_sentiment_adjusted", "final")
        log_distribution_snapshot(unified, "time_of_day", "final")

        # Save output
        output_csv = os.path.join(
            OUTPUT_DIR_REDDIT, "reddit_transit_feedback_labeled.csv"
        )
        output_json = os.path.join(
            OUTPUT_DIR_REDDIT, "reddit_transit_feedback_labeled.json"
        )

        unified.to_csv(output_csv)
        unified.to_json(output_json)

        logger.info(
            "pipeline_completed",
            output_csv=output_csv,
            output_json=output_json,
            final_rows=unified.num_rows,
            columns=len(unified.column_names),
        )

        # Log pipeline summary
        pipeline_metrics.log_summary()

        # Cleanup
        model_bundle.cleanup()

    except Exception as e:
        logger.error("pipeline_failed", error=str(e), exc_info=True)
        raise


if __name__ == "__main__":
    main()
