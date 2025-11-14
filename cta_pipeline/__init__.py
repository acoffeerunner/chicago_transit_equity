"""CTA Pipeline package - modular data processing pipeline."""

from cta_pipeline import logging_config

# Initialize logging when package is imported
logging_config.configure_logging()

__all__ = [
    "logging_config",
]
