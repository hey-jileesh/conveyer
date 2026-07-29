"""`slug`/`unslug`, table identifiers, execution names, object-URI shape check. LLD §5, §7.7.

Every composition function here **validates before composing** (fail-fast at
the boundary, matching `core/model.py`'s idiom for its own field validators —
§7.0's "defects are values in the pure zones" governs `core/guards.py`,
`core/checks.py`, `core/merge.py`'s planners, but the functions in *this*
module are boundary-validation helpers called from entrypoints/effects at
composition time, the same class as a pydantic `field_validator`): a
malformed input never reaches an ARN/name/table-identifier string.

**This module is now the SINGLE SOURCE of the shared grammar** (critique F5,
bead conveyer-nvh.43): `BATCH_ID_RE`, the identifier grammar
(`_IDENTIFIER_RE`), the pipeline-slug grammar (`_PIPELINE_SEGMENT`/
`_PIPELINE_RE`/`_check_pipeline_slug_grammar`), and `check_qualified_table`
are all defined here, stdlib-pure, and `core/model.py` **imports** them
directly rather than keeping its own copies. This is the correct dependency
direction (`core/model.py` -> `core/naming.py`), not the reverse: the ONLY
reason these were ever duplicated (conveyer-nvh.26, M5) is that
`entrypoints/router.py` imports this module and is packaged as a **stdlib +
boto3 only** zip (§7.1, I-8 -- "no pydantic, no pyspark", enforced by an
import test), while `core/model.py` is a pydantic-shaped module (`from
pydantic import ...` at module scope) -- importing ANYTHING from it, pure or
not, would force `import pydantic` transitively into that zip. The
constraint only forbids THIS module importing FROM `model.py`; `model.py`
importing from this stdlib-only module is fine and is what removes the
duplication (verified: this module still imports nothing beyond the
standard library, so `entrypoints/router.py`'s zip-purity test is
unaffected by `model.py`'s side of the change).
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from datetime import UTC, datetime

_CATALOG = "spine_cat"  # §5: Spark catalog name, bound to Glue in prod / Hadoop-on-tmpdir in tests

# UUIDv5, I-22. THE single source (§5 grammar) -- `core/model.py` imports
# this rather than keeping its own copy (see module docstring).
BATCH_ID_RE = r"^[0-9a-f]{8}-[0-9a-f]{4}-5[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"

# §6.2 table identifiers: bare "<db>.<table>"; EACH dot-component checked
# against this identifier grammar. THE single source -- see module docstring.
_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# §5 "Pipeline slug" grammar, per segment: `^[a-z0-9]([a-z0-9]|-(?!-))*$` --
# `--` forbidden inside a segment (makes `slug` injective, [S-11]). THE
# single source -- see module docstring.
_PIPELINE_SEGMENT = r"[a-z0-9](?:[a-z0-9]|-(?!-))*"
_PIPELINE_RE = re.compile(rf"^{_PIPELINE_SEGMENT}(?:/{_PIPELINE_SEGMENT})*$")

# Decodes a `slug()` output back to segments. The grammar allows a segment to
# END in a single "-" (`-(?!-)`'s lookahead is satisfied trivially at a
# segment's end) but never to START with one (`[a-z0-9]` is the segment's
# first char) and never to contain "--". Consequence: at every former "/"
# boundary the slugged string carries a run of EITHER 2 dashes (the
# separator alone) or 3 (one trailing segment-dash + the 2-dash separator;
# 3 is the max since a leading dash on the following segment is
# grammar-impossible) -- and nowhere else, since within-segment dashes are
# always isolated singles. A naive `"--" -> "/"` replace is NOT the correct
# inverse of `"/" -> "--"` for this reason (a 3-dash run naively splits as
# 2+1 instead of 1+2, e.g. `unslug(slug("a-/a"))` would wrongly yield
# `"a/-a"`) -- decoding must special-case the 3-dash run as `"-/"`, which is
# what makes `slug` genuinely injective as [S-11] claims (property-tested).
_DASH_RUN_RE = re.compile(r"-{2,3}")

_BATCH_ID_RE = re.compile(BATCH_ID_RE)

# `check_object_uris` (I-22): the canonical shape is prefix + exactly ONE
# object-name segment. This grammar governs that trailing segment --
# non-empty, no further `/`/`\` separators (rules out multi-segment and
# trailing-slash forms), and no `%` at all. Object names in this canonical
# landing shape have no legitimate need for percent-encoding, so rejecting
# `%` outright (rather than only decoding and re-checking `%2e`/`%2f`/`%5c`)
# is the conservative choice: it closes the encoded-separator/dot-segment
# class of bypass without having to reason about decode order or double
# encoding (conveyer-nvh.46).
_OBJECT_NAME_RE = re.compile(r"^[^/\\%]+$")

# §5 deliberate-rerun grammar: `<batch_id>--r<n>`. Structurally disjoint from
# every routed execution name (I-13): `_BATCH_ID_RE` is a fixed-length,
# fully-anchored UUIDv5 pattern, so no string matching it can also carry a
# `--rN` suffix and match it again -- disjointness holds by construction, not
# by this regex's design.
_RERUN_RE = re.compile(r"^(?P<batch_id>.+)--r(?P<n>[0-9]+)$")


def _check_pipeline_slug_grammar(value: str) -> str:
    """§5's pipeline-slug grammar, shared by `core/model.py`'s `pipeline`
    field validators (both `DeliveryRegisteredV1` and `PipelineSpecModel`,
    imported from here rather than re-derived — critique F5) and this
    module's own `slug`. Returns `value` unchanged on success (so a pydantic
    `field_validator` can use it directly as its return) -- raises
    `ValueError` otherwise. Kept module-private (leading underscore,
    unchanged name): `tools/linter_configs/spine.py`'s `_TRY_RAISE_ALLOWLIST`
    already names `("spine/core/naming.py", "_check_pipeline_slug_grammar")`
    for this raise-only helper — renaming it would need a linter-config edit
    outside this bead's file scope for no behavioral gain, since a private
    name is importable by an explicit `from ... import _name` just as a
    public one would be.

    `.fullmatch`, not `.match`: Python's `$` matches just before a trailing
    "\\n" even without MULTILINE, so `.match()` against a `^...$` pattern
    would wrongly accept e.g. "pipelines/commissions\\n". `.fullmatch`
    requires the ENTIRE string to be consumed, closing that gap."""
    if not _PIPELINE_RE.fullmatch(value):
        raise ValueError(
            f"pipeline must be one or more '/'-joined slug segments "
            f"(LLD §5 grammar, no '--' inside a segment): {value!r}"
        )
    return value


def slug(pipeline: str) -> str:
    """`slug(pipeline)` = `pipeline` with `/` -> `--` (§5). Validates the
    grammar BEFORE composing -- a malformed `pipeline` never reaches an
    ARN/name."""
    _check_pipeline_slug_grammar(pipeline)
    return pipeline.replace("/", "--")


def unslug(value: str) -> str:
    """Inverse of `slug`. NOT a naive `"--" -> "/"` replace -- see
    `_DASH_RUN_RE`'s comment: a 3-dash run (a segment ending in `-`,
    immediately followed by the `--` separator) decodes to `-/`, a 2-dash
    run to `/`. `unslug(slug(p)) == p` for every grammar-conforming `p`
    ([S-11], property-tested)."""

    def _replace(match: re.Match[str]) -> str:
        return "-/" if len(match.group(0)) == 3 else "/"

    return _DASH_RUN_RE.sub(_replace, value)


def check_qualified_table(value: str) -> str:
    """Bare "<db>.<table>" (or deeper) identifier check: split on ".", every
    non-empty component must match `_IDENTIFIER_RE`, and there must be at
    least one dot (§6.2, §6.7). THE single source (see module docstring) --
    `core/model.py` imports this rather than keeping its own copy (and
    `core/merge.py` in turn imports it from `core/model.py`, unaffected by
    this change).
    """
    parts = value.split(".")
    if len(parts) < 2 or any(part == "" for part in parts):
        raise ValueError(f"table must be a qualified '<db>.<table>' identifier: {value!r}")
    # `.fullmatch`, not `.match`: Python's `$` matches just before a trailing
    # "\n" even without MULTILINE, so `.match()` would wrongly accept e.g.
    # "lake.commissions__raw\n" as a conforming component. `.fullmatch`
    # closes that gap.
    bad = [part for part in parts if not _IDENTIFIER_RE.fullmatch(part)]
    if bad:
        raise ValueError(f"invalid identifier component(s) in {value!r}: {bad!r}")
    return value


def qualified(table: str) -> str:
    """Bare `<db>.<table>` -> `spine_cat.<db>.<table>` (§5, §7.6). Validates
    before composing via this module's own `check_qualified_table`."""
    check_qualified_table(table)
    return f"{_CATALOG}.{table}"


def execution_name(batch_id: str) -> str:
    """The (non-rerun) SFN execution name IS the `batch_id`, exactly (§5) --
    validated as UUIDv5 (I-22) before use."""
    if not _BATCH_ID_RE.fullmatch(batch_id):
        raise ValueError(f"batch_id must be a UUIDv5 (I-22): {batch_id!r}")
    return batch_id


def rerun_execution_name(batch_id: str, n: int) -> str:
    """Deliberate-rerun execution name `f"{batch_id}--r{n}"` (§5); `n` >= 1.
    Unreachable from the event path -- only the operator role starts these
    (§10.3)."""
    if not _BATCH_ID_RE.fullmatch(batch_id):
        raise ValueError(f"batch_id must be a UUIDv5 (I-22): {batch_id!r}")
    if n < 1:
        raise ValueError(f"rerun number must be >= 1: {n!r}")
    return f"{batch_id}--r{n}"


def is_rerun_execution_name(name: str) -> bool:
    """True iff `name` is `--rN`-shaped (the deliberate-rerun grammar, §5) --
    NOT a validity check of the embedded `batch_id`."""
    return _RERUN_RE.fullmatch(name) is not None


def _format_received_at(ts: datetime) -> str:
    """Mirrors `ingestion/core/naming.py::format_received_at` (own copy, 004
    D-13 -- spine owns its own model, no shared code between lanes):
    UTC, microseconds, basic ISO8601 (no dashes/colons). Converts to UTC
    first so a URI check is a same-instant comparison, not a string-format
    coincidence."""
    utc = ts.astimezone(UTC)
    return utc.strftime("%Y%m%dT%H%M%S") + f"{utc.microsecond:06d}Z"


def _canonical_prefix(feed_id: str, received_at: datetime, delivery_id: str) -> str:
    """Mirrors `ingestion/core/naming.py::canonical_prefix`: the canonical
    landing prefix (no bucket, no object name) for one delivery."""
    return f"{feed_id}/received_at={_format_received_at(received_at)}/dl-{delivery_id}/"


def check_object_uris(
    feed_id: str,
    delivery_id: str,
    received_at: datetime,
    object_uris: Sequence[str],
    landing_bucket: str,
) -> None:
    """I-22: every `object_uris` entry must match the canonical landing shape
    `s3://<landing_bucket>/<feed_id>/received_at=.../dl-<delivery_id>/<name>`
    for THIS delivery's own `feed_id`/`delivery_id`, in the configured
    landing bucket -- self-consistency, not merely well-formedness. A
    forged/foreign URI (wrong feed, wrong delivery, wrong bucket, or one
    missing a name past the prefix) is a binding defect: raise, fail fast,
    pre-land -- no raw write, no `batch-started`, no ledger row (§9).

    The prefix match alone is not sufficient (security HIGH,
    conveyer-nvh.46): everything after the prefix must be exactly ONE
    object-name segment, checked against `_OBJECT_NAME_RE` and the literal
    `.`/`..` dot-segments -- otherwise a partner-controlled name like
    `../../incoming/attacker.csv` composes a URI that passes the bare
    `startswith` check and (if S3A normalizes the key) lands ungated bytes
    outside this delivery's own landing prefix."""
    prefix = f"s3://{landing_bucket}/{_canonical_prefix(feed_id, received_at, delivery_id)}"

    def _is_bad(uri: str) -> bool:
        if not uri.startswith(prefix):
            return True
        remainder = uri[len(prefix) :]
        if remainder in ("", ".", ".."):
            return True
        return not _OBJECT_NAME_RE.fullmatch(remainder)

    bad = [uri for uri in object_uris if _is_bad(uri)]
    if bad:
        raise ValueError(
            f"object_uris must match this delivery's own canonical landing "
            f"prefix {prefix!r} (I-22): {bad!r}"
        )
