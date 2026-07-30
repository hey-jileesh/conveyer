"""MUST-FAIL: `.count()` inside `frames/` materializes an eager Spark action
and turns its result into control flow — banned by name (§12.3's
`frames-transforms` profile `banned_attr_names`, critique F1, bead
conveyer-azr.30) the same way `.collect()`/`.take()` already are: `frames/`
must stay a pure DataFrame-in/DataFrame-out plan builder, never a place that
counts a violating subset and raises mid-composition (the exact seam
`frames/quarantine.py::_assert_business_reason_grammar` used to sit in,
before this fix relocated the count+raise to `stages/post_check.py`).

Simulated scope: `spine/frames/**` (`frames-transforms` profile applies).
"""

from __future__ import annotations


def nonconforming_count(viol_df):
    offending = viol_df.filter(viol_df.reason.isNull())
    return offending.count()
