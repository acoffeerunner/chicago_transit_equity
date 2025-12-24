"""Dataset transform utilities - batched operations and validation."""
import pickle
from typing import Callable, Optional

from datasets import Dataset

from cta_pipeline.constants import DEFAULT_BATCH_SIZE
from cta_pipeline.errors import TransformError, ValidationError
from cta_pipeline.logging_config import get_logger

logger = get_logger(__name__)


def apply_batched_transform(
    dataset: Dataset,
    transform_fn: Callable,
    batch_size: int = DEFAULT_BATCH_SIZE,
    num_proc: Optional[int] = None,
    remove_columns: Optional[list] = None,
):
    """
    Apply batched transform with proper feature handling and picklability check.

    Args:
        dataset: Input dataset
        transform_fn: Transform function (must be picklable)
        batch_size: Batch size for processing
        num_proc: Number of processes for parallelization (None = sequential)
        remove_columns: Columns to remove after transform

    Returns:
        Transformed dataset

    Raises:
        TransformError: If transform function is not picklable or transform fails
    """
    try:
        # Ensure transform is picklable for caching
        try:
            pickle.dumps(transform_fn)
        except Exception as e:
            logger.warning(
                "transform_not_picklable",
                transform_name=transform_fn.__name__,
                error=str(e),
            )
            # Non-picklable transforms will still work but won't be cached

        return dataset.map(
            transform_fn,
            batched=True,
            batch_size=batch_size,
            num_proc=num_proc,
            remove_columns=remove_columns,
        )
    except Exception as e:
        logger.error("apply_batched_transform_failed", error=str(e), exc_info=True)
        raise TransformError(f"Batched transform failed: {e}") from e


def explode_routes_batched(dataset: Dataset) -> Dataset:
    """
    Explode routes using effective_routes (includes inherited).
    Each route gets its own row for sentiment analysis.
    Adds route_source column to indicate "explicit" or "inherited".
    
    This function uses Dataset.from_dict() approach (like original) to avoid
    schema validation issues with batched map() when changing row count.

    Args:
        dataset: Input dataset with 'effective_routes' and 'routes' columns

    Returns:
        Dataset with exploded routes (one row per route)
    """
    # Columns to skip when copying (will be replaced or removed)
    skip_cols = {"routes", "effective_routes", "inherited_routes"}
    
    # Build new dataset row by row
    new_data = {key: [] for key in dataset.column_names if key not in skip_cols}
    new_data["route"] = []
    new_data["route_source"] = []

    for row in dataset:
        effective_routes = row.get("effective_routes", []) or []
        own_routes = set(row.get("routes", []) or [])

        if not effective_routes:
            continue

        for route in effective_routes:
            # Copy all columns except skipped ones
            for key in new_data.keys():
                if key in ("route", "route_source"):
                    continue
                new_data[key].append(row.get(key))

            new_data["route"].append(route)
            # Track whether route was explicit or inherited
            new_data["route_source"].append(
                "explicit" if route in own_routes else "inherited"
            )

    # Ensure skip_cols are not in output (defensive check)
    for col in skip_cols:
        new_data.pop(col, None)

    return Dataset.from_dict(new_data)


def deduplicate_dataset(dataset: Dataset, key_columns: list) -> tuple[Dataset, int]:
    """
    Explicit deduplication with count reporting.

    Args:
        dataset: Input dataset
        key_columns: Columns to use for deduplication

    Returns:
        Tuple of (deduplicated_dataset, duplicate_count)
    """
    try:
        original_count = dataset.num_rows

        # Get unique indices based on key columns
        seen = set()
        unique_indices = []
        for i, row in enumerate(dataset):
            key = tuple(row.get(col) for col in key_columns)
            if key not in seen:
                seen.add(key)
                unique_indices.append(i)

        deduplicated = dataset.select(unique_indices)
        duplicate_count = original_count - deduplicated.num_rows

        logger.info(
            "dataset_deduplicated",
            original=original_count,
            deduplicated=deduplicated.num_rows,
            duplicates_removed=duplicate_count,
        )
        return deduplicated, duplicate_count
    except Exception as e:
        logger.error("deduplicate_dataset_failed", error=str(e), exc_info=True)
        raise TransformError(f"Dataset deduplication failed: {e}") from e


def validate_dataset_schema(dataset: Dataset, required_columns: list):
    """
    Validate dataset schema - check required columns exist.

    Args:
        dataset: Dataset to validate
        required_columns: List of required column names

    Raises:
        ValidationError: If required columns are missing
    """
    try:
        missing = [col for col in required_columns if col not in dataset.column_names]
        if missing:
            raise ValidationError(
                f"Missing required columns: {missing}. "
                f"Available columns: {dataset.column_names}"
            )
    except ValidationError:
        raise
    except Exception as e:
        logger.error("validate_dataset_schema_failed", error=str(e), exc_info=True)
        raise ValidationError(f"Schema validation failed: {e}") from e
