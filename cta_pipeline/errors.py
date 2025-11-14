"""Custom exceptions for the CTA pipeline."""


class ModelLoadingError(Exception):
    """Raised when model loading fails."""
    pass


class TransformError(Exception):
    """Raised when dataset transform fails."""
    pass


class ValidationError(Exception):
    """Raised when data validation fails."""
    pass
