"""Atproto (Bluesky) CTA feedback pipeline orchestrator."""

import os

from datasets import Dataset, concatenate_datasets, load_dataset
from tqdm.auto import tqdm
from transformers.pipelines.pt_utils import KeyDataset

from cta_pipeline.constants import (
    COMMENTS_PATH_BSKY,
    DEFAULT_BATCH_SIZE,
    OUTPUT_DIR_BSKY,
    POSTS_PATH_BSKY,
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
from cta_pipeline.thread_detection import (
    build_thread_groups,
    consolidate_threads,
    identify_thread_candidates,
    score_thread_relevance,
)
from cta_pipeline.time_extraction import extract_time_of_day
from cta_pipeline.transit_classification import is_transit_semantic, transit_rule_match

# Configure logging
configure_logging()
logger = get_logger(__name__)

os.makedirs(OUTPUT_DIR_BSKY, exist_ok=True)


def load_and_merge_atproto_data():
    """
    Load posts and comments from CSV files and merge into unified dataset.

    Returns:
        Dataset with unified posts and comments
    """
    logger.info("loading_atproto_data")

    # Load posts
    posts_ds = load_dataset("csv", data_files=POSTS_PATH_BSKY)["train"]

    # Load comments
    comments_ds = load_dataset("csv", data_files=COMMENTS_PATH_BSKY)["train"]

    # DEDUPLICATION: Remove posts that also exist as comments
    comment_ids = set(comments_ds["comment_id"])
    original_post_count = posts_ds.num_rows
    posts_ds = posts_ds.filter(lambda x: x["post_id"] not in comment_ids)
    dedup_count = original_post_count - posts_ds.num_rows
    logger.info("deduplication_complete", duplicates_removed=dedup_count)

    # Transform posts: add metadata columns
    def add_post_metadata(batch):
        n = len(batch["post_id"])
        return {
            "record_type": ["post"] * n,
            "parent_post_id": batch["post_id"],
            "parent_comment_id": [None] * n,
            "original_record_id": batch["post_id"],
        }

    posts_ds = posts_ds.map(add_post_metadata, batched=True, batch_size=256)

    # Transform comments: add metadata columns
    def add_comment_metadata(batch):
        n = len(batch["comment_id"])
        parent_comment_ids = []
        for post_id, parent in zip(batch["post_id"], batch["parent_comment_id"]):
            if parent == post_id:
                parent_comment_ids.append(None)
            else:
                parent_comment_ids.append(parent)

        return {
            "record_type": ["comment"] * n,
            "parent_post_id": batch["post_id"],
            "parent_comment_id": parent_comment_ids,
            "original_record_id": batch["comment_id"],
        }

    comments_ds = comments_ds.map(add_comment_metadata, batched=True, batch_size=256)

    # Standardize columns for merging
    def standardize_posts(batch):
        n = len(batch["post_id"])
        return {"comment_id": [None] * n}

    posts_ds = posts_ds.map(standardize_posts, batched=True, batch_size=256)

    def standardize_comments(batch):
        n = len(batch["post_id"])
        return {"parent_id": [None] * n}

    comments_ds = comments_ds.map(standardize_comments, batched=True, batch_size=256)

    # Concatenate
    unified = concatenate_datasets([posts_ds, comments_ds])

    posts_count = sum(1 for x in unified if x["record_type"] == "post")
    comments_count = sum(1 for x in unified if x["record_type"] == "comment")
    logger.info(
        "data_loaded",
        posts=posts_count,
        comments=comments_count,
        total=unified.num_rows,
    )

    return unified


def main():
    """Main pipeline orchestrator."""

    print("=" * 60)
    print("BLUESKY CTA FEEDBACK PIPELINE")
    print("=" * 60)

    pipeline_metrics = PipelineMetrics()

    try:
        # Load models once
        logger.info("initializing_pipeline")
        try:
            model_bundle = load_models()
        except ModelLoadingError as e:
            logger.error("model_loading_failed", error=str(e))
            raise

        # Stage 1: Load and merge data
        with StageTimer("data_loading", rows_in=0) as timer:
            unified = load_and_merge_atproto_data()
            timer.rows_in = unified.num_rows
            timer.rows_out = unified.num_rows
        pipeline_metrics.stages.append(timer)

        # Stage 2: Thread detection
        with StageTimer("thread_detection", rows_in=unified.num_rows) as timer:
            thread_candidates = identify_thread_candidates(unified)
            logger.info("thread_candidates_found", count=len(thread_candidates))

            relevance_scores = score_thread_relevance(
                unified, thread_candidates, model_bundle
            )
            combine_count = sum(
                1 for s in relevance_scores.values() if s["should_combine"]
            )
            timer.extras["candidates"] = len(thread_candidates)
            timer.extras["will_combine"] = combine_count

            thread_assignments = build_thread_groups(relevance_scores)
            unified = consolidate_threads(unified, thread_assignments)
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
            transit_rule_count = sum(unified["is_transit"])
            timer.extras["transit_rule_matches"] = transit_rule_count
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
            transit_sem_count = sum(unified["is_transit_sem"])
            timer.extras["transit_semantic_matches"] = transit_sem_count
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
            feedback_sem_count = sum(unified["is_feedback_sem"])
            timer.extras["feedback_semantic_matches"] = feedback_sem_count
            timer.rows_out = unified.num_rows

            # Save intermediate result
            unified.to_csv(
                os.path.join(OUTPUT_DIR_BSKY, "bsky_transit_routes_bf_fb_filter.csv")
            )
        pipeline_metrics.stages.append(timer)

        # Stage 10: Feedback classification (rule-based)
        with StageTimer(
            "feedback_rule_classification", rows_in=unified.num_rows
        ) as timer:
            unified = unified.map(
                feedback_rule_match, batched=True, batch_size=DEFAULT_BATCH_SIZE
            )
            feedback_rule_count = sum(unified["is_feedback"])
            timer.extras["feedback_rule_matches"] = feedback_rule_count
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
            timer.extras["explosion_ratio"] = (
                unified.num_rows / timer.rows_in if timer.rows_in > 0 else 0
            )
            timer.rows_out = unified.num_rows
        pipeline_metrics.stages.append(timer)

        if unified.num_rows == 0:
            logger.warning("no_records_after_explosion")
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
            stops_detected = sum(1 for s in stops_col if s)
            timer.extras["stops_detected"] = stops_detected
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
            sarcasm_count = sum(is_sarcastic_col)
            flipped_count = sum(
                1
                for is_sarc, orig in zip(is_sarcastic_col, unified["route_sentiment"])
                if is_sarc and orig == "positive"
            )
            timer.extras["sarcastic_posts"] = sarcasm_count
            timer.extras["sentiment_flipped"] = flipped_count
            timer.rows_out = unified.num_rows
        pipeline_metrics.stages.append(timer)

        # Log distribution snapshots
        log_distribution_snapshot(unified, "route_source", "final")
        log_distribution_snapshot(unified, "route_sentiment_adjusted", "final")
        log_distribution_snapshot(unified, "time_of_day", "final")

        # Save output
        output_csv = os.path.join(OUTPUT_DIR_BSKY, "bsky_transit_feedback_labeled.csv")
        output_json = os.path.join(
            OUTPUT_DIR_BSKY, "bsky_transit_feedback_labeled.json"
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
