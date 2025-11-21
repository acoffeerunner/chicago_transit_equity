"""Route and time inheritance from parent records."""
from datasets import Value

from cta_pipeline.errors import TransformError
from cta_pipeline.logging_config import get_logger

logger = get_logger(__name__)


def apply_route_inheritance(dataset):
    """
    Inherit routes from parent for records without explicit routes.

    Adds columns:
    - inherited_routes: Routes inherited from parent
    - has_inherited_route: True if has inherited routes
    - effective_routes: Union of routes and inherited_routes

    Args:
        dataset: Dataset with 'original_record_id', 'record_type', 'routes',
                 'parent_comment_id', 'parent_post_id' columns

    Returns:
        Dataset with inheritance columns added
    """
    try:
        # Build lookup tables
        record_routes = {}
        record_parents = {}

        for row in dataset:
            record_id = row["original_record_id"]
            record_routes[record_id] = row.get("routes", []) or []

            if row["record_type"] == "comment":
                record_parents[record_id] = (
                    row["parent_comment_id"] or row["parent_post_id"]
                )
            else:
                record_parents[record_id] = None

        def get_inherited_routes(record_id, visited=None):
            """Recursively get routes from ancestors."""
            if visited is None:
                visited = set()
            if record_id in visited:
                return []
            visited.add(record_id)

            parent_id = record_parents.get(record_id)
            if not parent_id:
                return []

            parent_routes = record_routes.get(parent_id, [])
            if parent_routes:
                return parent_routes

            # Try grandparent
            return get_inherited_routes(parent_id, visited)

        # Compute inherited routes for all records
        inherited_map = {}
        for record_id in record_routes:
            if not record_routes[record_id]:
                inherited_map[record_id] = get_inherited_routes(record_id)
            else:
                inherited_map[record_id] = []

        # Build columns directly (avoid PyArrow type inference issues with map)
        inherited_col = []
        has_inherited_col = []
        effective_col = []

        for row in dataset:
            record_id = row["original_record_id"]
            routes = row.get("routes")

            # Ensure routes is a list
            own_routes = routes if routes is not None and isinstance(routes, list) else []

            inh = inherited_map.get(record_id, [])
            if not isinstance(inh, list):
                inh = []

            inherited_col.append(inh if inh else [])
            has_inherited_col.append(len(inh) > 0)
            eff = list(set(own_routes + inh))
            effective_col.append(eff if eff else [])

        # Add columns directly
        dataset = dataset.add_column("inherited_routes", inherited_col)
        dataset = dataset.add_column("has_inherited_route", has_inherited_col)
        dataset = dataset.add_column("effective_routes", effective_col)

        inherited_count = sum(has_inherited_col)
        logger.info("route_inheritance_applied", inherited_count=inherited_count)
        return dataset
    except Exception as e:
        logger.error("apply_route_inheritance_failed", error=str(e), exc_info=True)
        raise TransformError(f"Route inheritance failed: {e}") from e


def apply_time_inheritance(dataset):
    """
    Inherit time_of_day from parent for records with unknown time.

    Adds column:
    - inherited_time_of_day: Time inherited from parent (if own is unknown)

    Args:
        dataset: Dataset with 'original_record_id', 'record_type', 'time_of_day',
                 'parent_comment_id', 'parent_post_id' columns

    Returns:
        Dataset with inherited_time_of_day column added
    """
    try:
        # Build lookup tables
        record_times = {}
        record_parents = {}

        for row in dataset:
            record_id = row["original_record_id"]
            record_times[record_id] = row.get("time_of_day", "unknown")

            if row["record_type"] == "comment":
                record_parents[record_id] = (
                    row["parent_comment_id"] or row["parent_post_id"]
                )
            else:
                record_parents[record_id] = None

        def get_inherited_time(record_id, visited=None):
            """Recursively get time from ancestors."""
            if visited is None:
                visited = set()
            if record_id in visited:
                return "unknown"
            visited.add(record_id)

            parent_id = record_parents.get(record_id)
            if not parent_id:
                return "unknown"

            parent_time = record_times.get(parent_id, "unknown")
            if parent_time != "unknown":
                return parent_time

            return get_inherited_time(parent_id, visited)

        # Compute inherited times
        inherited_map = {}
        for record_id, time in record_times.items():
            if time == "unknown":
                inherited_map[record_id] = get_inherited_time(record_id)
            else:
                inherited_map[record_id] = None

        def add_time_inheritance(batch):
            inherited = []
            for record_id, time in zip(batch["original_record_id"], batch["time_of_day"]):
                if time == "unknown":
                    inherited.append(inherited_map.get(record_id, "unknown"))
                else:
                    inherited.append(None)
            return {"inherited_time_of_day": inherited}

        new_features = dataset.features.copy()
        new_features["inherited_time_of_day"] = Value("string")

        dataset = dataset.map(
            add_time_inheritance, batched=True, batch_size=256, features=new_features
        )

        inherited_count = sum(1 for v in inherited_map.values() if v is not None and v != "unknown")
        logger.info("time_inheritance_applied", inherited_count=inherited_count)
        return dataset
    except Exception as e:
        logger.error("apply_time_inheritance_failed", error=str(e), exc_info=True)
        raise TransformError(f"Time inheritance failed: {e}") from e



