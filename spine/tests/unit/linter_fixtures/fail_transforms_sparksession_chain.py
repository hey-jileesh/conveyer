"""MUST-FAIL: `df.sparkSession.read.parquet(path)` — the classic "just one
lookup" erosion vector (design-critique finding F3(a), bead
`conveyer-nvh.42`). Before this fix, `banned_attr_names` only fired on the
outermost `Call.func.attr` of an `ast.Call` node — here that's `parquet`,
which isn't banned — so `sparkSession` and `read`, both intermediate
`ast.Attribute` nodes in the chain that are never themselves the target of a
call, passed every profile silently. The engine now walks EVERY
`ast.Attribute` node in the file regardless of call position, so both
`sparkSession` and `read` trip here even though only `parquet` sits in call
position.

Simulated scope: `spine/frames/**` or a pipeline's `transforms.py`
(`frames-transforms` profile applies).
"""

from __future__ import annotations


def apply(df, path):
    return df.sparkSession.read.parquet(path)
