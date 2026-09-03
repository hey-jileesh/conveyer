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

# F-6 (security gate `wf_c9aadeb2-8eb`, LOW): the grammar above is
# length-UNBOUNDED -- every rendered use is a validated/quoted identifier
# (F.col, `qualified()`) or a Glue table/database name, and Glue itself
# caps identifier length, so an over-long value fails closed rather than
# reaching wrong data. The asymmetry with `ColumnSpec`/`FactColumnSpec`/
# check-id (`core/model.py`, all `max_length=128`) still invites drift, so
# `check_qualified_table` caps each dot-component at this length -- 255,
# the AWS Glue Data Catalog table/database name limit (the more permissive
# of the two ceilings a `<db>.<table>` value here can name).
_MAX_IDENTIFIER_COMPONENT_LENGTH = 255

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

# 007.1 §6.3 answer 1: the marker table's commit-completion row-kind sentinel
# for `table_name`. Sits STRUCTURALLY outside `_IDENTIFIER_RE` (a leading
# dash; `_IDENTIFIER_RE` requires `[A-Za-z_]` first) -- "by grammar, not by
# convention" (the LLD's own words) is made a provable, not merely asserted,
# fact by the module-import-time check right below: no declared fact table
# (whose bare name component must ALSO pass `_IDENTIFIER_RE` via `check_
# qualified_table`) can ever collide with this sentinel.
COMMIT_COMPLETION_SENTINEL = "-completion-"
assert not _IDENTIFIER_RE.fullmatch(COMMIT_COMPLETION_SENTINEL), (
    "the commit-completion sentinel must sit outside the identifier grammar (007.1 §6.3 "
    "answer 1) -- if this ever fires, the sentinel and a real table name could collide"
)


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


def table_slug(pipeline: str) -> str:
    """The pipeline's own TRAILING `/`-segment -- a deliberately DIFFERENT
    derivation from `slug()`'s "--"-joined form, reserved for composing
    IDENTIFIER-GRAMMAR-CONSTRAINED names (Iceberg/Glue table names, via
    `<slug>__<suffix>`, 004.1 §5 / 007.1 §6.3/§6.5) rather than ARN/URI path
    segments.

    **Recorded deviation, found empirically this bead (007.1 §6.5's new
    prefix-assertion mechanic, `bootstrap/create_record_tables.py`).**
    004.1 §5's own naming table defines "Pipeline slug" as `slug(pipeline)`
    (`/` -> `--`) and, in the SAME table row, spells the data-table name
    literally as `<slug>__raw` -- i.e. the text names ONE slug function for
    both uses. That reading is unusable for identifier composition: `slug()`
    output can carry `--`, which (a) `check_qualified_table`'s own
    `_IDENTIFIER_RE` already rejects as a table-name component (no declared
    `raw_table`/`quarantine_table`/`fact_table`/`state_table` value could
    ever legally equal a `slug()`-composed name for a multi-segment
    `pipeline`, at PARSE time, today) and (b) is actively dangerous even where
    validation is skipped: an UNQUOTED `--` inside a Spark SQL identifier is
    parsed as a line-comment marker, silently truncating everything after it
    (kernel-verified this bead: `CREATE TABLE spine_cat.probe.pipelines--
    identity__markers (x STRING) USING iceberg` actually creates a table
    named bare `pipelines`, no error). The shipped exemplar (`tests/exemplar/
    identity/pipeline.yaml`, `pipeline: pipelines/identity`) already resolves
    this the way this function does: its own `raw_table` etc. use bare
    `identity`, not `pipelines--identity` -- this function makes that
    observed, already-relied-upon convention a named, reusable one rather
    than a fixed which-existing-fixtures-happen-to-do-it accident. A
    genuinely hierarchical multi-segment pipeline identity (beyond a
    fixed leading namespace segment) has no table-safe rendering yet under
    this rule -- 009's authoring surface owns resolving that properly;
    flagged here, not silently chosen around.
    """
    _check_pipeline_slug_grammar(pipeline)
    return pipeline.rsplit("/", 1)[-1]


def check_qualified_table(value: str) -> str:
    """Bare "<db>.<table>" (or deeper) identifier check: split on ".", every
    non-empty component must match `_IDENTIFIER_RE`, be no longer than
    `_MAX_IDENTIFIER_COMPONENT_LENGTH` (F-6), and there must be at least one
    dot (§6.2, §6.7). THE single source (see module docstring) --
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
    too_long = [part for part in parts if len(part) > _MAX_IDENTIFIER_COMPONENT_LENGTH]
    if too_long:
        raise ValueError(
            f"identifier component(s) exceed {_MAX_IDENTIFIER_COMPONENT_LENGTH} chars "
            f"in {value!r}: {too_long!r}"
        )
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
    coincidence.

    **Overflow guard (absorbs `conveyer-azr.25`, the `core/canonical.py::
    _timestamp_str` idiom from `conveyer-azr.24`).** `ts.astimezone(UTC)`
    raises a bare `OverflowError` (not a documented, catchable defect) for
    an aware value at/near `datetime.min` with a positive UTC offset (or
    `datetime.max` with a negative offset) -- there is no year 0, so the
    UTC-converted instant falls outside `[MINYEAR, MAXYEAR]`. `core/**`
    bans `try` (§12.3), so this is a pure arithmetic pre-check, not
    exception-based: comparing the aware value's UTC offset against its
    span to each boundary (both spans and the offset are always safely
    representable -- the max possible span within `[MINYEAR, MAXYEAR]` is
    ~9998 years, well inside `timedelta`'s range, and a conformant
    `tzinfo.utcoffset()` is always < 24h)."""
    tzinfo = ts.tzinfo
    assert tzinfo is not None, (
        "AwareDatetime narrows this at the pydantic boundary (I-19's seed field)"
    )
    offset = tzinfo.utcoffset(ts)
    assert offset is not None, "same aware-datetime contract as above"
    naive = ts.replace(tzinfo=None)
    if offset > naive - datetime.min or -offset > datetime.max - naive:
        raise ValueError(f"received_at out of representable range in UTC: {ts!r}")
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


def markers_table(raw_table: str, pipeline: str) -> str:
    """`<lake db>.<slug>__markers` (007.1 §6.3) -- one marker table per
    pipeline, name DERIVED (never an authored spec field, unlike `raw_table`/
    `quarantine_table`/`fact_table`/`state_table`): §6.5 step 1 names it
    literally as "the derived `<slug>__markers`". `<lake db>` = `raw_table`'s
    own db component -- every table one pipeline's deploy provisions shares
    one lake db by the deployed-spec's own convention (`tests/exemplar/
    identity/pipeline.yaml`'s literal shape: `conveyer_dev_lake.identity__raw`
    / `.identity__quarantine` / `.identity__facts` / `.identity__state` all
    share `conveyer_dev_lake`) -- no separate `lake_db` field exists or is
    needed. Takes plain `str` args, not `PipelineSpecModel` (this module's
    own zip-purity constraint, see module docstring): both call sites
    (`bootstrap/create_record_tables.py`, `entrypoints/glue_main.py`) already
    hold `spec.raw_table`/`spec.pipeline` in hand. Shared here (nvh.43's own
    "single source" precedent for this module) so the writer (bootstrap,
    §6.5) and the reader (the entrypoint's `committed_tables(batch_id)`
    probe, §4.3's [DC-1] discharge) can never derive two different names for
    the same table.

    Uses `table_slug`, NOT `slug` -- see `table_slug`'s own docstring for why
    `slug()`'s "--"-joined form must never compose an actual table
    identifier."""
    check_qualified_table(raw_table)
    db = raw_table.split(".", 1)[0]
    return f"{db}.{table_slug(pipeline)}__markers"


def table_class_inventory_uri(spec_uri: str) -> str:
    """`table-classes.json` sits beside the deployed spec (007.1 §6.5 step 6,
    F-10's own words: "shipped content-pinned beside the deployed spec (the
    I-23 idiom)") -- same directory as `spec_uri`, sibling filename. Shared
    by `bootstrap/create_record_tables.py` (emits it, deriving the sibling
    URI from `--spec-uri`) and `entrypoints/glue_main.py` (loads it, deriving
    the sibling URI from `RunnerConfig.pipeline_spec_uri` -- the SAME URI
    `check_spec_uri_allowlist` already validated) -- one naming convention,
    write here / read there, both call sites import it from here rather than
    deriving it twice."""
    directory, _, _ = spec_uri.rpartition("/")
    return f"{directory}/table-classes.json"
