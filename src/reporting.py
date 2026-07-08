"""Lightweight stage reporting for pipeline observability.

Each pipeline stage reports rows in/out and data-quality counts
(kept / quarantined / dropped) so a reader can watch conservation hold as the
pipeline runs. Deliberately tiny: `report_stage` logs a one-line summary and
returns a `StageReport` that tests and the notebook can assert on.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass

logger = logging.getLogger("freemarket.pipeline")
if not logger.handlers:
    _handler = logging.StreamHandler()
    _handler.setFormatter(
        logging.Formatter("%(asctime)s | %(levelname)-5s | %(message)s", "%H:%M:%S")
    )
    logger.addHandler(_handler)
    logger.setLevel(logging.INFO)


@dataclass
class StageReport:
    """Row-count and data-quality accounting for one pipeline stage."""

    stage: str
    rows_in: int = 0
    rows_out: int = 0
    kept: int = 0
    quarantined: int = 0
    dropped: int = 0

    @property
    def conserved(self) -> bool:
        """True when every input row is accounted for: out + quarantined + dropped == in."""
        return self.rows_out + self.quarantined + self.dropped == self.rows_in

    def __str__(self) -> str:
        return (
            f"[{self.stage}] in={self.rows_in} out={self.rows_out} "
            f"kept={self.kept} quarantined={self.quarantined} dropped={self.dropped} "
            f"conserved={'yes' if self.conserved else 'NO'}"
        )


def report_stage(
    stage: str,
    rows_in: int = 0,
    rows_out: int = 0,
    *,
    kept: int = 0,
    quarantined: int = 0,
    dropped: int = 0,
) -> StageReport:
    """Build, log, and return a `StageReport` for one pipeline stage."""
    report = StageReport(stage, rows_in, rows_out, kept, quarantined, dropped)
    logger.info(str(report))
    return report
