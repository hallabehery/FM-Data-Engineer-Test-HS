"""`submission/WRITEUP.md` — the documentation deliverable (issue #17).

Prose isn't unit-testable, but the *deliverable's completeness* is: this guards that the
write-up exists and still covers each acceptance-criterion topic, so the required record
can't silently regress (a heading gets dropped in an edit, the file goes missing, etc.).
Each check maps to one bullet in issue #17's acceptance criteria.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from src import config

WRITEUP = config.SUBMISSION_DIR / "WRITEUP.md"


@pytest.fixture(scope="module")
def text() -> str:
    assert WRITEUP.exists(), f"documentation deliverable missing: {WRITEUP}"
    return WRITEUP.read_text().lower()


# (topic label, substrings that must all be present) — one row per acceptance criterion.
# NB: this is a completeness tripwire — it guards that each required *topic* is still present,
# not that the prose is *accurate* (the pipeline's own tests own correctness). So it survives
# rewording of the numbers but catches a whole section being dropped.
REQUIRED_TOPICS = [
    ("FX approach: as-of match", ["as-of", "validfrom", "validtill"]),
    ("FX approach: quarantine policy", ["quarantine", "fx_out_of_coverage", "fx_no_rate_point"]),
    ("FX approach: GBP special case", ["gbp", "1.0", "base"]),
    ("Layer boundaries Bronze/Silver/Gold", ["bronze", "silver", "gold", "data_mart", "curated"]),
    ("DQ classified dropped/quarantined/kept", ["dropped", "quarantined", "kept"]),
    ("DQ: the 42 null-currency fees", ["42", "fx_missing_currency"]),
    ("Live-stream ingestion strategy", ["live-stream", "append-only", "idempoten"]),
    ("Gold satisfies the network deliverable", ["directed", "circle", "diamond", "inflow", "outflow"]),
    ("Slicing & drill", ["month", "year", "drill"]),
    ("Warehouse topology (catalog vs schema)", ["catalog", "schema", "single", "attach"]),
]


@pytest.mark.parametrize("label,needles", REQUIRED_TOPICS, ids=[t[0] for t in REQUIRED_TOPICS])
def test_writeup_covers_topic(text: str, label: str, needles: list[str]) -> None:
    missing = [n for n in needles if n not in text]
    assert not missing, f"WRITEUP.md § {label!r} missing expected content: {missing}"


def test_writeup_is_substantial(text: str) -> None:
    # A trust document, not a stub — guards against an accidentally emptied deliverable.
    assert len(text) > 4000, "WRITEUP.md looks too short to be the documentation deliverable"
