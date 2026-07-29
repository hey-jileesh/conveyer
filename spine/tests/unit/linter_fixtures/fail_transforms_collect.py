"""MUST-FAIL: `.collect()` inside a transform pulls the whole DataFrame to the
driver — banned by name (§12.3's `frames-transforms` profile
`banned_attr_names`) as part of the Spark-API surface a pure transform must
never touch (I-9: `apply`/`post_check` are plain DataFrame plans, no actions).

Simulated scope: pipeline `transforms.py` (`frames-transforms` profile
applies via the `("pipelines",)` path prefix, D-2).
"""

from __future__ import annotations


def apply(candidate_df):
    rows = candidate_df.collect()
    return rows
