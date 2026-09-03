"""Drift guard: `spine/terraform/modules/spine-pipeline/main.tf`'s
`local.pipeline_table_names` list must include every DERIVED (never
authored) per-pipeline table-name suffix `spine.core.naming` composes --
LLD 007.1 S6.3/S6.5, I-21/S-5 ("never database-wide").

Originating gap (critique gate `wf_a0ef7f3b-6aa` finding N1, bead
`conveyer-swb.28`, MAJOR): the marker table (`naming.markers_table`,
`<lake db>.<slug>__markers`) had NO Glue-catalog IAM grant at all --
`main.tf`'s `pipeline_table_names` only ever listed the four AUTHORED
tables (`raw`/`quarantine`/`facts`/`state`, freeform `PipelineSpecModel`
fields with no fixed suffix `naming.py` derives), never the marker table,
which IS a fixed, code-derived suffix. In any real Glue-catalog deployment
every commit/bind marker-table touch (`effects/spark.py::
_require_marker_table`/`append_marker_row`/`read_marker_completions`/
`read_marker_presence`, `entrypoints/glue_main.py::_committed_tables`)
would `AccessDenied` -- pre-land, fail-closed (no wrong data), but the
record path could not run.

Pure file-read/text-parse -- no Terraform binary, no AWS, and (unlike
`test_glue_job_argv_wiring.py`) not even a `pyspark`-adjacent import:
`spine.core.naming` is stdlib-pure (its own module docstring: "stdlib +
boto3 only" zip-purity constraint), so this test needs nothing beyond
importing it directly. Fast, CI-cheap companion to `spine/terraform/
modules/spine-pipeline/tests/spine_pipeline.tftest.hcl`'s own
`terraform test` assertion of the same fact -- that one exercises the
module's actual planned IAM policy document; this one never invokes
`terraform` and needs nothing on PATH.

**The expected suffix is DERIVED, not hand-copied.** `naming.markers_
table` is the ONE function in `core/naming.py` that composes a per-
pipeline table name from a FIXED suffix rather than accepting one as an
authored `PipelineSpecModel` field (`raw_table`/`quarantine_table`/
`fact_table`/`state_table` are all freeform authored strings, validated
only for identifier SHAPE by `check_qualified_table` -- Terraform has no
way to derive their names structurally, so `main.tf` hardcodes the
existing `<slug>__raw`/`__quarantine`/`__facts`/`__state` convention
by hand, unchanged by this test). `_derived_table_suffix` calls `naming.
markers_table` itself (never hardcodes the literal `"__markers"` string)
so a FUTURE change to that function's suffix -- or a future SECOND
naming.py function of the same "derived, fixed-suffix" shape -- keeps
this test's expectation honest with zero edits here, the same
derive-don't-hardcode discipline `test_glue_job_argv_wiring.py` already
established for `RunnerConfig`'s required argv keys.

**F-1 addendum (security gate `wf_c9aadeb2-8eb`, MEDIUM, this same bead).**
`pipeline_table_names` composes each table name off `local.table_slug` --
the TRAILING `/`-segment (mirrors `naming.table_slug` EXACTLY), never
`local.slug` (the "--"-joined ARN/exec-path form) -- because the module's
own `pipeline` variable is genuinely multi-segment for the only deployed
pipeline (`"pipelines/identity"`), and `naming.py`'s own runner resolves
bare `identity__raw`/etc, never `pipelines--identity__raw`. `_SLUG_SUFFIX_
RE` below is updated to match `${local.table_slug}` for this reason (a
regression back to `local.slug` would make `_slug_suffixes` find nothing,
already failing `test_main_tf_pipeline_table_names_includes_every_naming_
derived_suffix` below on its own). `test_main_tf_table_slug_derivation_
agrees_with_naming_table_slug` below is the NEW, dedicated check: it
extracts `main.tf`'s own `local.table_slug = ...` HCL expression text
(pinned to the exact reviewer-fix formula, so a silent formula edit fails
loudly) and cross-checks a faithful pure-Python mirror of that EXACT
formula against `naming.table_slug` for both a multi- and single-segment
probe -- the same "derive, don't hand-copy" discipline as `naming.markers_
table` above, applied to a Terraform-side formula this time.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
from spine.core import naming

_REPO_ROOT = Path(__file__).resolve().parents[3]  # .../conveyer
_MAIN_TF_PATH = _REPO_ROOT / "spine" / "terraform" / "modules" / "spine-pipeline" / "main.tf"

_SLUG_SUFFIX_RE = re.compile(r'"\$\{local\.table_slug\}(__[a-zA-Z0-9_]+)"')

# F-1: the exact reviewer-fix HCL formula for `local.table_slug` (main.tf) --
# the pipeline's own trailing `/`-segment, Terraform's own idiom for what
# `naming.table_slug`'s `pipeline.rsplit("/", 1)[-1]` computes in Python.
_TABLE_SLUG_EXPR = 'element(split("/", var.pipeline), length(split("/", var.pipeline)) - 1)'
_TABLE_SLUG_LOCAL_RE = re.compile(r"table_slug\s*=\s*(?P<expr>element\([^\n]+?- 1\))")


def _extract_table_slug_expr(hcl_text: str) -> str:
    """Textual extraction of `main.tf`'s `local.table_slug = <expr>`
    right-hand side -- same non-HCL-parsing posture as `_extract_pipeline_
    table_names_block` above, single-line assignment instead of a bracketed
    block."""
    match = _TABLE_SLUG_LOCAL_RE.search(hcl_text)
    if match is None:
        raise ValueError("no 'local.table_slug = element(...)' assignment found in HCL text")
    return match.group("expr")


def _tf_table_slug(pipeline: str) -> str:
    """Pure-Python mirror of `_TABLE_SLUG_EXPR`'s own semantics (the
    trailing '/'-segment) -- NEVER trusted on its own, always cross-checked
    against `naming.table_slug` below, the same "two independently-built
    computations must agree" discipline `agent-memory` notes elsewhere in
    this codebase for Spark-vs-reference-model checks."""
    parts = pipeline.split("/")
    return parts[len(parts) - 1]


def _extract_pipeline_table_names_block(hcl_text: str) -> str:
    """Textual (non-HCL-parsing) extraction of the `pipeline_table_names = [
    ... ]` body from a `main.tf`-shaped Terraform file: finds the marker,
    then the FIRST `[` after it, then returns everything up to the matching
    close `]` via depth counting -- exact for this file's shape (every
    entry is a single-line quoted string, no nested `[`/`]` appears).
    Raises `ValueError` if the marker or a matching close bracket is not
    found, so a future file-shape drift fails loudly rather than silently
    returning an empty/wrong slice (mirrors `test_glue_job_argv_wiring.py`::
    `_extract_default_arguments_block`'s identical depth-counting shape,
    bracket instead of brace)."""
    marker = "pipeline_table_names"
    marker_idx = hcl_text.find(marker)
    if marker_idx == -1:
        raise ValueError(f"{marker!r} not found in HCL text")
    open_idx = hcl_text.find("[", marker_idx)
    if open_idx == -1:
        raise ValueError(f"no '[' found after {marker!r}")
    depth = 0
    for i in range(open_idx, len(hcl_text)):
        ch = hcl_text[i]
        if ch == "[":
            depth += 1
        elif ch == "]":
            depth -= 1
            if depth == 0:
                return hcl_text[open_idx + 1 : i]
    raise ValueError(f"no matching ']' found for {marker!r} block")


def _slug_suffixes(block_text: str) -> frozenset[str]:
    """Every literal `"${local.slug}__<suffix>"` entry's `__<suffix>` part,
    inside a `pipeline_table_names` block body."""
    return frozenset(m.group(1) for m in _SLUG_SUFFIX_RE.finditer(block_text))


def _derived_table_suffix() -> str:
    """The one Terraform-relevant DERIVED (not authored) per-pipeline
    table-name suffix, computed by actually CALLING `naming.markers_table`
    with a synthetic single-segment pipeline (so `table_slug(pipeline) ==
    pipeline`, isolating exactly the fixed suffix the function appends)
    rather than hardcoding the literal `"__markers"` string anywhere in
    this test."""
    probe_slug = "probeslug"
    raw_table = f"lakedb.{probe_slug}__raw"
    markers = naming.markers_table(raw_table, probe_slug)
    prefix = f"lakedb.{probe_slug}"
    assert markers.startswith(prefix), markers
    return markers[len(prefix) :]


def test_main_tf_pipeline_table_names_includes_every_naming_derived_suffix() -> None:
    hcl_text = _MAIN_TF_PATH.read_text()
    block = _extract_pipeline_table_names_block(hcl_text)
    found = _slug_suffixes(block)

    expected = _derived_table_suffix()
    assert expected in found, (
        f"{_MAIN_TF_PATH} pipeline_table_names is missing the Glue-catalog "
        f"ARN for naming.py's derived {expected!r} table -- every commit/bind "
        f"marker-table touch would AccessDenied against a real Glue catalog "
        f"(N1, critique gate wf_a0ef7f3b-6aa)"
    )


# --- F-1 (security gate wf_c9aadeb2-8eb, MEDIUM): `local.table_slug` must --
# agree with `naming.table_slug`, not `local.slug` ---------------------------


def test_main_tf_table_slug_expr_is_the_pinned_reviewer_fix_formula() -> None:
    hcl_text = _MAIN_TF_PATH.read_text()
    expr = _extract_table_slug_expr(hcl_text)
    assert expr == _TABLE_SLUG_EXPR, (
        f"{_MAIN_TF_PATH}'s local.table_slug formula changed shape ({expr!r}) -- "
        f"re-derive this test's Python mirror before trusting the cross-check below"
    )


@pytest.mark.parametrize(
    "pipeline",
    [
        "pipelines/identity",  # the only deployed pipeline (envs/dev/main.tf) -- multi-segment
        "identity",  # single-segment: table_slug must be a no-op here
    ],
)
def test_main_tf_table_slug_agrees_with_naming_table_slug(pipeline: str) -> None:
    # Proves `main.tf`'s own `local.table_slug` HCL formula computes the
    # SAME value as `naming.table_slug` for both a multi-segment
    # (`envs/dev`'s real deployed shape) and single-segment pipeline --
    # never `naming.slug` (the "--"-joined ARN/exec-path form), which would
    # wrongly leave `local.table_slug`-composed table names as
    # `pipelines--identity__facts` instead of `identity__facts`.
    hcl_text = _MAIN_TF_PATH.read_text()
    _extract_table_slug_expr(hcl_text)  # fails loudly if the formula is gone entirely
    assert _tf_table_slug(pipeline) == naming.table_slug(pipeline)


def test_extract_pipeline_table_names_block_raises_without_marker() -> None:
    with pytest.raises(ValueError, match="'pipeline_table_names' not found"):
        _extract_pipeline_table_names_block("no marker here")


def test_extract_pipeline_table_names_block_raises_on_unterminated_block() -> None:
    with pytest.raises(ValueError, match="no matching '\\]' found"):
        _extract_pipeline_table_names_block("pipeline_table_names = [ unterminated")
