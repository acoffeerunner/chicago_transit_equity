"""Stop and station extraction, plus sarcasm detection."""
import re

from cta_pipeline.constants import (
    AMBIGUOUS_TRAIN,
    SARCASM_PATTERNS,
    STATION_CONTEXT_PATTERNS,
    UNAMBIGUOUS_TRAIN,
    USER_INTERSECTION_PATTERN,
)

from cta_pipeline.logging_config import get_logger

logger = get_logger(__name__)


def extract_stops(text: str, route: str, bus_intersections: set) -> list:
    """
    Extract stops from text based on route type.

    - If route is a train line → look for train stations
    - If route is a bus route → look for bus intersections

    Args:
        text: Input text to search
        route: Route identifier (e.g., "red_line", "bus_66")
        bus_intersections: Set of bus intersection tuples (from GTFS)

    Returns:
        List of found stop/station names
    """
    text_lower = text.lower() if isinstance(text, str) else ""
    found_stops = []

    is_train_route = route.endswith("_line")  # red_line, blue_line, etc.
    is_bus_route = route.startswith("bus_")  # bus_66, bus_9, etc.

    if is_train_route:
        # Look for unambiguous train stations
        for station in UNAMBIGUOUS_TRAIN:
            if re.search(rf"\b{re.escape(station)}\b", text_lower):
                found_stops.append(station)

        # Look for ambiguous stations with context
        for station in AMBIGUOUS_TRAIN:
            for pattern in STATION_CONTEXT_PATTERNS:
                if re.search(pattern.format(station=re.escape(station)), text_lower):
                    found_stops.append(station)
                    break

    elif is_bus_route:
        # Look for bus intersections
        for match in USER_INTERSECTION_PATTERN.finditer(text_lower):
            a, b = match.group(1).strip(), match.group(2).strip()
            if (a, b) in bus_intersections:
                found_stops.append(f"{a} & {b}")
            elif (b, a) in bus_intersections:
                found_stops.append(f"{b} & {a}")

    return list(set(found_stops))


def detect_sarcasm(text: str) -> bool:
    """
    Return True if text contains sarcastic patterns.

    Args:
        text: Input text to check

    Returns:
        True if sarcastic patterns detected, False otherwise
    """
    text_lower = text.lower() if isinstance(text, str) else ""
    for pattern in SARCASM_PATTERNS:
        if re.search(pattern, text_lower):
            return True
    return False



