"""GTFS data loading functions."""
import re

import pandas as pd

from cta_pipeline.constants import GTFS_STOPS_PATH
from cta_pipeline.errors import ModelLoadingError
from cta_pipeline.logging_config import get_logger

logger = get_logger(__name__)


def load_gtfs_bus_intersections():
    """
    Load GTFS stops and build bus intersection lookup.

    Returns:
        set of tuples: Bus intersection pairs (both orders included)
        e.g., {("state", "lake"), ("lake", "state"), ...}
    """
    try:
        logger.info("loading_gtfs_stops", path=GTFS_STOPS_PATH)
        stops = pd.read_csv(GTFS_STOPS_PATH)

        # Bus stops: have stop_code, no parent_station
        bus_stops_raw = stops[
            stops.stop_code.notna() & stops.parent_station.isna()
        ].stop_name.unique()

        # Build bus intersection lookup (5,021 pairs)
        bus_intersections = set()
        for name in bus_stops_raw:
            match = re.match(
                r"^([A-Za-z0-9]+(?:\s+[A-Za-z]+)?)\s*&\s*([A-Za-z0-9]+(?:\s+[A-Za-z]+)?)",
                str(name),
            )
            if match:
                a, b = match.group(1).lower().strip(), match.group(2).lower().strip()
                bus_intersections.add((a, b))
                bus_intersections.add((b, a))  # Both orders

        logger.info(
            "gtfs_bus_intersections_loaded",
            count=len(bus_intersections) // 2,
            total_pairs=len(bus_intersections),
        )
        return bus_intersections
    except Exception as e:
        logger.error("gtfs_loading_failed", error=str(e), exc_info=True)
        raise ModelLoadingError(f"Failed to load GTFS data: {e}") from e



