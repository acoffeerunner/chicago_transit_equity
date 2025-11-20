"""Route and line extraction from text."""
import re

from cta_pipeline.constants import (
    BUS_LIST_PATTERN,
    BUS_ROUTES,
    BUS_SCHEDULED_PATTERN,
    HASHTAG_THE_PATTERN,
    HASHTAG_VERB_PATTERN,
    LINE_AT_STATION_PATTERN,
    LINE_LIST_PATTERN,
    LINE_NAMES,
    LINE_TRAIN_PATTERN,
    LINE_TRANSFER_PATTERN,
    LINE_VERB_PATTERN,
    SINGLE_BUS_PATTERN,
    SINGLE_LINE_PATTERN,
)
from cta_pipeline.errors import TransformError
from cta_pipeline.logging_config import get_logger

logger = get_logger(__name__)


def extract_route_fn(batch):
    """
    Extract routes and lines from text using multiple regex patterns.

    Supports:
    - Train lines: red, blue, green, orange, brown, purple, pink, yellow
    - Bus routes: numbered bus routes (e.g., bus_66, bus_49)

    Args:
        batch: Dictionary with 'body_lower' key containing list of lowercased text strings

    Returns:
        Dictionary with keys:
            - routes: List of lists of route strings (e.g., ["red_line", "bus_66"])
            - route_count: List of route counts per text
            - has_route: List of boolean flags
    """
    try:
        routes_list = []
        for t in batch["body_lower"]:
            matched = []
            if not isinstance(t, str):
                routes_list.append([])
                continue

            # Pattern 6: Multiple line mentions (e.g., "red and blue lines")
            for m in LINE_LIST_PATTERN.finditer(t):
                match_text = m.group(0)
                for line in LINE_NAMES:
                    if line in match_text:
                        route = f"{line}_line"
                        if route not in matched:
                            matched.append(route)

            # Pattern 1: Single line (e.g., "red line")
            for m in SINGLE_LINE_PATTERN.finditer(t):
                route = f"{m.group(1).lower()}_line"
                if route not in matched:
                    matched.append(route)

            # Pattern 2: Line at station (e.g., "the red at Belmont")
            for m in LINE_AT_STATION_PATTERN.finditer(t):
                route = f"{m.group(1).lower()}_line"
                if route not in matched:
                    matched.append(route)

            # Pattern 3: Line transfers (e.g., "red to blue")
            for m in LINE_TRANSFER_PATTERN.finditer(t):
                for line in [m.group(1).lower(), m.group(2).lower()]:
                    route = f"{line}_line"
                    if route not in matched:
                        matched.append(route)

            # Pattern 4: Line verbs (e.g., "ride the red")
            for m in LINE_VERB_PATTERN.finditer(t):
                route = f"{m.group(1).lower()}_line"
                if route not in matched:
                    matched.append(route)

            # Pattern 5: Line train (e.g., "red train")
            for m in LINE_TRAIN_PATTERN.finditer(t):
                route = f"{m.group(1).lower()}_line"
                if route not in matched:
                    matched.append(route)

            # Bus list pattern (e.g., "buses 66, 49, 22")
            for m in BUS_LIST_PATTERN.finditer(t):
                match_text = m.group(0)
                bus_nums = re.findall(r"(\d{1,3}[A-Za-z]?)", match_text)
                for num in bus_nums:
                    route = f"bus_{num}".lower()
                    if route in BUS_ROUTES and route not in matched:
                        matched.append(route)

            # Single bus pattern (e.g., "bus 66", "66 bus", "route 66")
            for m in SINGLE_BUS_PATTERN.finditer(t):
                num = m.group(1) or m.group(2) or m.group(3) or m.group(4)
                if num:
                    route = f"bus_{num}".lower()
                    if route in BUS_ROUTES and route not in matched:
                        matched.append(route)

            # Hashtag verb pattern (e.g., "take #66", "ride the #66")
            for m in HASHTAG_VERB_PATTERN.finditer(t):
                # Pattern has two groups (OR), use whichever matched
                num = m.group(1) or m.group(2)
                if num:
                    route = f"bus_{num}".lower()
                    if route in BUS_ROUTES and route not in matched:
                        matched.append(route)

            # Hashtag the pattern (e.g., "the #156 is")
            for m in HASHTAG_THE_PATTERN.finditer(t):
                route = f"bus_{m.group(1)}".lower()
                if route in BUS_ROUTES and route not in matched:
                    matched.append(route)

            # Bus scheduled pattern (e.g., "the 156 scheduled")
            for m in BUS_SCHEDULED_PATTERN.finditer(t):
                route = f"bus_{m.group(1)}".lower()
                if route in BUS_ROUTES and route not in matched:
                    matched.append(route)

            routes_list.append(matched)

        return {
            "routes": routes_list,
            "route_count": [len(r) for r in routes_list],
            "has_route": [len(r) > 0 for r in routes_list],
        }
    except Exception as e:
        logger.error("extract_route_fn_failed", error=str(e), exc_info=True)
        raise TransformError(f"Route extraction failed: {e}") from e



