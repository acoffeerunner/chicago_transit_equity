"""Route mapping utilities between sentiment data and GTFS routes."""

# Mapping from sentiment data route names to GTFS route_ids
SENTIMENT_TO_GTFS = {
    # Train lines
    "red_line": "Red",
    "blue_line": "Blue",
    "green_line": "G",
    "orange_line": "Org",
    "brown_line": "Brn",
    "purple_line": "P",
    "pink_line": "Pink",
    "yellow_line": "Y",
}

# Reverse mapping: GTFS to sentiment
GTFS_TO_SENTIMENT = {v: k for k, v in SENTIMENT_TO_GTFS.items()}

# Train line display names
TRAIN_LINE_DISPLAY = {
    "Red": "Red Line",
    "Blue": "Blue Line",
    "G": "Green Line",
    "Org": "Orange Line",
    "Brn": "Brown Line",
    "P": "Purple Line",
    "Pink": "Pink Line",
    "Y": "Yellow Line",
}

# Train line colors (from GTFS route_color)
TRAIN_LINE_COLORS = {
    "Red": "#c60c30",
    "Blue": "#00a1de",
    "G": "#009b3a",
    "Org": "#f9461c",
    "Brn": "#62361b",
    "P": "#522398",
    "Pink": "#e27ea6",
    "Y": "#f9e300",
}

# Sentiment colors
SENTIMENT_COLORS = {
    "positive": "#2ECC71",
    "negative": "#E74C3C",
    "neutral": "#95A5A6",
}


def sentiment_route_to_gtfs(route: str) -> str:
    """Convert a sentiment data route name to GTFS route_id.

    Examples:
        'red_line' -> 'Red'
        'bus_66' -> '66'
        'bus_X49' -> 'X49'
    """
    if route in SENTIMENT_TO_GTFS:
        return SENTIMENT_TO_GTFS[route]

    # Handle bus routes: bus_66 -> 66, bus_X49 -> X49
    if route.startswith("bus_"):
        return route[4:]  # Remove 'bus_' prefix

    return route


def gtfs_route_to_sentiment(route_id: str, route_type: int | None = None) -> str:
    """Convert a GTFS route_id to sentiment data route name.

    Args:
        route_id: The GTFS route_id
        route_type: Optional GTFS route_type (1=rail, 3=bus)

    Examples:
        'Red' -> 'red_line'
        '66' -> 'bus_66'
    """
    if route_id in GTFS_TO_SENTIMENT:
        return GTFS_TO_SENTIMENT[route_id]

    # Check if it's a known train line
    if route_id in TRAIN_LINE_DISPLAY:
        # Find in reverse mapping
        for sentiment_name, gtfs_id in SENTIMENT_TO_GTFS.items():
            if gtfs_id == route_id:
                return sentiment_name

    # Assume it's a bus route
    return f"bus_{route_id}"


def get_route_display_name(route: str) -> str:
    """Get a human-readable display name for a route.

    Examples:
        'red_line' -> 'Red Line'
        'bus_66' -> 'Bus 66'
    """
    if route.endswith("_line"):
        color = route.replace("_line", "")
        return f"{color.capitalize()} Line"

    if route.startswith("bus_"):
        number = route[4:]
        return f"Bus {number}"

    return route


def is_train_route(route: str) -> bool:
    """Check if a route is a train line."""
    return route.endswith("_line") or route in SENTIMENT_TO_GTFS


def is_bus_route(route: str) -> bool:
    """Check if a route is a bus route."""
    return route.startswith("bus_")


def get_route_color(route: str) -> str:
    """Get the display color for a route.

    Returns official CTA color for train lines, gray for buses.
    """
    gtfs_id = sentiment_route_to_gtfs(route)
    if gtfs_id in TRAIN_LINE_COLORS:
        return TRAIN_LINE_COLORS[gtfs_id]
    return "#666666"  # Gray for buses


def parse_sentiment_routes_from_gtfs_list(gtfs_routes: list[str]) -> list[str]:
    """Convert a list of GTFS route names to sentiment format.

    Useful for converting routes served by a stop.
    """
    result = []
    for route in gtfs_routes:
        # Check if it's a train line name
        if route in ["Red Line", "Blue Line", "Green Line", "Orange Line",
                     "Brown Line", "Purple Line", "Pink Line", "Yellow Line"]:
            color = route.split()[0].lower()
            result.append(f"{color}_line")
        elif route in GTFS_TO_SENTIMENT:
            result.append(GTFS_TO_SENTIMENT[route])
        else:
            # Assume bus route
            result.append(f"bus_{route}")
    return result
