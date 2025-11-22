"""CTA Pipeline package - modular data processing pipeline."""

# Core modules
from cta_pipeline import constants, errors, logging_config, models, metrics

# Processing modules
from cta_pipeline import (
    context_inheritance,
    dataset_transforms,
    feedback_classification,
    gtfs_loader,
    route_extraction,
    sentiment_analysis,
    stop_extraction,
    text_processing,
    thread_detection,
    time_extraction,
    transit_classification,
)

# Initialize logging when package is imported
logging_config.configure_logging()

__all__ = [
    # Core
    "constants",
    "errors",
    "logging_config",
    "models",
    "metrics",
    # Processing
    "context_inheritance",
    "dataset_transforms",
    "feedback_classification",
    "gtfs_loader",
    "route_extraction",
    "sentiment_analysis",
    "stop_extraction",
    "text_processing",
    "thread_detection",
    "time_extraction",
    "transit_classification",
]



