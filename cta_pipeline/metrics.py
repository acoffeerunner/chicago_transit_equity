"""Stage timing and metrics collection."""
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Optional

from cta_pipeline.logging_config import get_logger

logger = get_logger(__name__)


@dataclass
class StageMetrics:
    """Metrics for a single pipeline stage."""

    stage_name: str
    rows_in: int
    rows_out: int
    duration_s: float
    extras: dict[str, Any] = field(default_factory=dict)
    error: Optional[str] = None

    @property
    def retention_pct(self) -> float:
        """Calculate retention percentage."""
        if self.rows_in == 0:
            return 0.0
        return (self.rows_out / self.rows_in) * 100


@dataclass
class PipelineMetrics:
    """Aggregated metrics for entire pipeline."""

    stages: list[StageMetrics] = field(default_factory=list)

    @property
    def total_duration_s(self) -> float:
        """Total pipeline duration in seconds."""
        return sum(stage.duration_s for stage in self.stages)

    @property
    def final_rows(self) -> Optional[int]:
        """Final number of rows after all stages."""
        return self.stages[-1].rows_out if self.stages else None

    @property
    def initial_rows(self) -> Optional[int]:
        """Initial number of rows before any stages."""
        return self.stages[0].rows_in if self.stages else None

    def log_summary(self):
        """Log pipeline summary metrics."""
        logger.info(
            "pipeline_completed",
            total_duration_s=self.total_duration_s,
            initial_rows=self.initial_rows,
            final_rows=self.final_rows,
            stages_count=len(self.stages),
        )


@contextmanager
def StageTimer(stage_name: str, rows_in: int, extras: Optional[dict] = None):
    """
    Context manager for timing pipeline stages with error tracking.

    Usage:
        with StageTimer("transit_classification", rows_in=1000) as timer:
            dataset = process(dataset)
            timer.rows_out = len(dataset)
            timer.extras["routes_found"] = 42
    """
    start_time = time.time()
    timer = StageMetrics(
        stage_name=stage_name, rows_in=rows_in, rows_out=0, duration_s=0.0
    )
    if extras:
        timer.extras = extras

    logger.info("stage_started", stage=stage_name, rows_in=rows_in)

    try:
        yield timer
        timer.duration_s = time.time() - start_time

        logger.info(
            "stage_completed",
            stage=stage_name,
            rows_in=timer.rows_in,
            rows_out=timer.rows_out,
            retention_pct=timer.retention_pct,
            duration_s=timer.duration_s,
            **timer.extras,
        )
    except Exception as e:
        timer.duration_s = time.time() - start_time
        timer.error = str(e)
        logger.error(
            "stage_failed",
            stage=stage_name,
            rows_in=timer.rows_in,
            duration_s=timer.duration_s,
            error=str(e),
            exc_info=True,
        )
        raise


def log_distribution_snapshot(dataset, column: str, stage_name: str):
    """
    Log distribution snapshot for a column.

    Args:
        dataset: Dataset to analyze
        column: Column name to analyze
        stage_name: Stage name for logging context
    """
    try:
        if column not in dataset.column_names:
            logger.warning("distribution_column_not_found", column=column, stage=stage_name)
            return

        # Count value frequencies
        distribution = {}
        for value in dataset[column]:
            distribution[value] = distribution.get(value, 0) + 1

        logger.info(
            "distribution_snapshot",
            stage=stage_name,
            column=column,
            distribution=distribution,
            total=sum(distribution.values()),
        )
    except Exception as e:
        logger.warning(
            "distribution_snapshot_failed",
            column=column,
            stage=stage_name,
            error=str(e),
        )



