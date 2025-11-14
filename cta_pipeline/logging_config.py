"""Enhanced structlog configuration with callsite tracking and dev/prod modes."""
import sys
import structlog


def configure_logging():
    """Configure structlog with enhanced processors and callsite tracking."""
    shared_processors = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,  # Structured exception tracebacks
        structlog.processors.UnicodeDecoder(),  # Handle special characters
        structlog.processors.CallsiteParameterAdder(
            {
                structlog.processors.CallsiteParameter.FILENAME,
                structlog.processors.CallsiteParameter.FUNC_NAME,
                structlog.processors.CallsiteParameter.LINENO,
            }
        ),
    ]

    if sys.stderr.isatty():
        # Pretty printing in terminal (dev)
        processors = shared_processors + [structlog.dev.ConsoleRenderer()]
    else:
        # JSON in containers/cloud (prod)
        processors = shared_processors + [
            structlog.processors.dict_tracebacks,  # Structured exception tracebacks
            structlog.processors.JSONRenderer(),
        ]

    structlog.configure(processors=processors)


def get_logger(name: str = None):
    """Get a configured structlog logger."""
    return structlog.get_logger(name)
