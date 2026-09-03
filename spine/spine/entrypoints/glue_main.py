"""Glue entrypoint — argv -> config -> bind -> session -> `spine.run`. No `awsglue`. LLD §8.3.

`main(argv)` is pure COMPOSITION (004 §7.2, restated by §8.3: "any logic in
the entrypoint is a review defect") — a straight-line sequence of calls to
small, independently-testable functions defined below; it contains no `if`/
`try` of its own. Order (normative, §8.3, plus the leading observability
install below, conveyer-nvh.47; 006.1 P-4's bind step and its K5 gate-2
call, bead conveyer-6pg.12, added at the end): `observability.install_
json_handler()` -> argv -> `RunnerConfig` -> seed parse + `check_object_uris`
(I-22) -> fetch/parse `PipelineSpec` (URI allowlist, I-23) -> binding-defect
asserts (`spec.pipeline == seed.pipeline`, `spec.sla_minutes == config.
sla_minutes` [H-5]) -> `bind_transforms` (I-10, fail-fast) -> build
`SparkSession` (explicit catalog conf, §7.6) -> assert Iceberg extensions
active [T-16] -> K5's engine-compile gate (`_assert_check_expressions_
compile`, P-2 gate 2/P-9 rule 2) -> `fx = make_runner_fx(spark, config)` ->
P-4's bind-time validator inventory (`fx.describe_table` acquisition,
`validate_bindings`, 007.1 F-10's `table-classes.json` load and [DC-1]'s
marker-table `committed_tables` probe -- bead conveyer-6pg.18, B7, replacing
B2's interim stubs) -> seed `BatchContext` -> `spine.run.run(seed, fx)`.
Every fail-fast branch above
`session.build_session` raises before any effect runs (no raw write, no
`batch-started`, no ledger row — the I-10/I-22 "invisible to the run
ledger" class, §9's "Config/binding/seed-validation defect" row); the bind
step (K5 and P-4's own asserts) stays in that same pre-land class too —
`fx.describe_table`/`_assert_bind_checks_pass` raise before `run_sequence`
is ever called, so no raw write/`batch-started`/ledger row happens there
either.

`observability.install_json_handler()` runs FIRST, before `from_args` even
parses argv (conveyer-nvh.47: `install_json_handler` previously had no
caller anywhere, so every §7.3 stage-transition INFO line from `record_run`
was silently dropped by the root logger's default WARNING level, and every
surviving WARNING lost its `batch_id`/`pipeline`/`feed_id`/`attempt_id`
extras to the default formatter). Installing it first, not just before
`run_sequence`, means even an I-10/I-22 fail-fast raised before any stage
runs is captured in the same structured JSON shape. `install_json_handler`
is itself idempotent (a named-handler marker guards a second call, see its
own docstring in `spine/observability.py`) — calling it unconditionally on
every invocation, including a warm process where `main()` runs more than
once, is therefore always safe and never accumulates duplicate handlers.

**005.1 §3.2 [DC-4] + §6.1's pinned obligation #1 (bead conveyer-azr.18,
n3-context-wiring):** two binding-defect asserts land in the §8.3 sequence,
both AFTER `session.build_session`/`session.assert_iceberg_extensions_active` (they need a
live JVM) and BEFORE `fx_factory`/`run_sequence` (still pre-land: no raw
write, no `batch-started`, no ledger row reaches storage for either).

* `_assert_patterns_compile_in_jvm` — `pattern`'s NORMATIVE grammar is Java
  regex ([DC-4]), not the best-effort Python `re.compile` typo check
  `core/model.py::ColumnSpec._check_pattern_compiles` already ran at spec
  parse; a pattern that is Python-valid but Java-invalid (e.g. `(?P<name>
  ...)`, PCRE-only named-group syntax) would otherwise only surface the
  first time `frames/checks.py::_pattern_check`'s `rlike` expression
  actually executes against a real batch. This function compiles the SAME
  fullmatch-anchored string that check will `rlike` against (`\\A(?:
  pattern)\\z` — `frames/checks.py::_pattern_check`'s own literal template,
  duplicated here rather than imported: that module is out of this bead's
  scope, and the three-line template is cheaper to keep in sync by
  cross-reference than to couple a private function across the frames/
  entrypoints boundary) through `spark._jvm.java.util.regex.Pattern.compile`
  — the driver-side JVM, no `SparkSession` action needed, so this stays a
  cheap, one-object-per-pattern probe.
* `_assert_temporal_bounds_bind` — discharges 005.1 §6.1's own pinned
  obligation #1, flagged unmet by `core/model.py`'s docstring ("temporal
  `min <= max` ordering has no spec-parse-time check ... must be enforced
  at compile time") and by `frames/checks.py::_bounds_check`'s own docstring
  (a malformed fmt SEQUENCE, as opposed to a bad bound VALUE, "only surfaces
  the first time `evaluate`/`typed_projection` genuinely executes against
  real batch data" — `frames/` cannot force that validation itself, being
  banned from every Spark-action attribute name file-wide, see that
  module's docstring). This module has legitimate Spark-execution access
  (an entrypoint, not `frames/`), so it closes both gaps in one probe: for
  every `date`/`timestamp` column declaring `min`/`max`, it evaluates
  `frames.checks.compile_contract`'s OWN `typed_exprs[column.name]` — the
  IDENTICAL `Column` expression the real cast/bounds checks use (D-5's "no
  second cast to disagree", carried one layer further out) — over a
  one-row driver-side `DataFrame` holding the bound literal, and raises a
  binding defect when: the JVM formatter itself rejects the fmt SEQUENCE
  (`.collect()` on `to_date`/`to_timestamp` is a real Spark action, so an
  otherwise-alphabet-valid-but-structurally-invalid fmt like `"V"`, count
  1, throws here); the bound literal fails to parse under an otherwise-valid
  fmt; or (both present) the parsed `min > max`. Columns with no declared
  `min`/`max` get no BOUND-LITERAL probe here (there is no bound literal to
  evaluate) — that residual fmt-SEQUENCE gap is closed separately, below.

**005.1 §6.1's "a malformed fmt fails at bind, pre-land" claim, closed for
EVERY temporal column (bead conveyer-azr.26, n3-fmt-probe fix):**
`_assert_temporal_bounds_bind` only forces the JVM formatter build
incidentally, via the bound literal it evaluates for columns that declare
`min`/`max` — a `date`/`timestamp` column declaring NEITHER never received a
probe at all, so a malformed fmt SEQUENCE on it was still only surfacing the
first time a real batch executed against it (an unnamed loud failure, the
exact residual `_typed_expr`'s/`_bounds_check`'s own docstrings named as
still open after conveyer-azr.18). `_assert_temporal_fmt_compiles_in_jvm`
closes it: for EVERY `date`/`timestamp` column, regardless of `min`/`max`,
it evaluates `compile_contract`'s SAME `typed_exprs[column.name]` (D-5,
identical reuse to `_assert_temporal_bounds_bind`) over a one-row
driver-side `DataFrame` holding a plain SQL `NULL` in that column's cell,
via a genuine `.collect()`. A `NULL` probe value is sufficient — empirically
verified (scratch script, this bead): `to_date`/`to_timestamp` build their
`DateTimeFormatter` eagerly enough that `IllegalArgumentException: Pattern
letter count must be 2: V` raises identically whether the probed cell is
`NULL` or a real string (`nullSafeEval` does not defer formatter
construction past the null check), so no rendered/synthesized non-null
value is needed. Runs BEFORE `_assert_temporal_bounds_bind` in `main()`
(§8.3 composition) and unconditionally for every temporal column, including
ones `_assert_temporal_bounds_bind` will also probe via its own bound
literal — deliberately redundant JVM work for bound-declaring columns (one
cheap one-row driver-side job per column), traded for one function whose
job is exactly "is this fmt SEQUENCE valid", not entangled with bound-value
parseability. Message grammar is A-10's (`admission-defect/<code>: <machine
detail>`, no URIs — column names and fmt strings only, both spec-authored,
reviewed strings, never a row value), per this bead's own instruction.

Two DELIBERATE, DOCUMENTED DI seams beyond the literal §8.3 shape, both
following the codebase's own established precedent for "a real
implementation, swappable for a still-real (never mocked) alternative in
tests" (`spine.run.run`'s own `stages` parameter; `tests/conftest.py`'s
`make_wrapped_fx` mechanism):

* `fetch_spec: Callable[[str], str]` (defaults to `default_fetch_spec`) —
  the I-23 spec fetch is a real effect (S3 `GetObject` in production); the
  URI-allowlist VALIDATION (`check_spec_uri_allowlist`, below) stays pure and
  always runs, regardless of what `fetch_spec` is. Tests inject a local-file
  reader (or `default_fetch_spec` itself against a `file://` URI — it
  natively supports both schemes) with zero mocking framework involved.
* `fx_factory: Callable[[SparkSession, RunnerConfig], RunnerFx]` (defaults to
  `spine.effects.build.make_runner_fx` — the production assembly). Recorded
  reason this seam exists although §8.3 doesn't name it: `naming.
  check_object_uris` (I-22, frozen — out of this bead's file list) hardcodes
  the canonical prefix as literally `s3://...`, so any seed that survives
  that check has `object_uris` starting with `s3://`; the local test
  substrate has no S3-compatible filesystem wired into Spark (no
  `hadoop-aws`/S3A, matching why `tests/integration/test_scenarios_core.py`
  bypasses this entrypoint entirely and hands stages plain local paths
  directly). `fx_factory` lets a genuine end-to-end drive of `main(argv)`
  still use the REAL production `make_runner_fx` for every field except
  `read_objects`, which the test wraps to translate the (real, allowlisted)
  `s3://` URIs to real local fixture files by basename — the same
  "real assembly, one named field swapped" shape `local_runner_fx` already
  uses for `now`. No `awsglue` anywhere in this module (I-14).
"""

from __future__ import annotations

import importlib
import json
from collections.abc import Callable, Mapping, Sequence
from typing import TYPE_CHECKING

from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    ByteType,
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from spine import observability
from spine.binding import Transforms, bind_transforms
from spine.config import RunConfig, RunnerConfig, from_args
from spine.context import BatchContext
from spine.core import bind_checks, naming
from spine.core.check_grammar import (
    CompiledAggregate,
    Family,
    ValidatedExpr,
    compile_aggregate,
    family_of_kind,
    validate_expression,
)
from spine.core.checks import checks_version
from spine.core.contract import check_version, parse_column_type, read_spec_version
from spine.core.model import (
    BatchCheckModel,
    DeliveryRegisteredV1,
    FactSchemaModel,
    FactTypeModel,
    PipelineSpecModel,
    RowCheckModel,
    parse_pipeline_spec_yaml,
)
from spine.effects.build import make_runner_fx
from spine.entrypoints import session
from spine.frames.business_checks import aggregate_column
from spine.frames.checks import compile_contract
from spine.run import run as run_sequence

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.types import DataType

    from spine.core.model import RawContractModel
    from spine.effects.records import RunnerFx

# §5/I-23 allowlisted specs root, bucket-pinned for `s3://` (6pg.35 item 4;
# see `check_spec_uri_allowlist`'s own docstring for why `file://` stays
# bucket-agnostic): every `pipeline_spec_uri` must resolve under
# `.../spine/specs/<this job's own pipeline slug>/<name>` IN the pinned
# `RunnerConfig.artifacts_bucket`.
_SPECS_ROOT_SEGMENTS: tuple[str, str] = ("spine", "specs")
_ALLOWED_SPEC_URI_SCHEMES: tuple[str, ...] = ("s3://", "file://")  # file:// — I-14 EMR/local parity


# --- pure: I-23 spec-URI allowlist -------------------------------------------


def check_spec_uri_allowlist(uri: str, pipeline_slug: str, artifacts_bucket: str) -> None:
    """I-23: `uri` must resolve under `spine/specs/<pipeline_slug>/<name>` for
    THIS job's own pipeline, IN the pinned `artifacts_bucket`, checked BEFORE
    any fetch. Pure, no I/O.

    Bucket-pinned for `s3://` (6pg.35 security-gate LOW item 4, left undone
    by conveyer-swb.10): a spec/inventory URI is forgeable path-shape-only if
    the job role ever gains broad S3 read or argv becomes event-derived
    (today Terraform-pinned + bucket-scoped grant; priced residual) — so this
    check now ALSO refuses any `s3://` URI whose bucket disagrees with
    `RunnerConfig.artifacts_bucket` (Terraform-pinned, §6.4), before even
    looking at path shape. The refusal message is deliberately value-free —
    it names only the PINNED (expected) bucket, never the offending URI or
    the bucket found in it, since both are attacker-reachable input the
    moment this check's precondition (job role has only scoped read) is
    ever weakened; nothing attacker-controlled is echoed back.

    **Every other refusal branch below is value-free the same way (critique
    gate wf_78ea4599-a5b M6, bead conveyer-swb.26).** The scheme, traversal,
    and root-shape messages name only the fixed allowlist, the literal
    `'..'` grammar, and this job's own PINNED `pipeline_slug` — never the
    offending `uri` itself: the function's own rationale above ("attacker-
    reachable the moment the precondition weakens") applies to every branch,
    not just the bucket one.

    `file://` (tests, I-14 EMR/local-file parity) has no bucket concept — a
    local filesystem path is not an S3 bucket, and `file://` is never a real
    production path (only `s3://` reaches this function from a Glue job) —
    so the bucket check is skipped entirely for that scheme; only the PATH
    shape below still applies to it.

    Path shape (both schemes, unchanged by this bucket-pin): reject a spec
    URI whose path escapes `spine/specs/<pipeline_slug>/` even if it somehow
    named the right bucket — traversal segments (`..`/`.`), an empty
    segment, a different pipeline's slug, or a path that never reaches a
    file under that prefix. `s3://` (production) and `file://` (tests) are
    the only accepted schemes.

    Implementation note: segments are found via `segments.index("spine")` —
    the FIRST literal `"spine"` path segment. A coincidental earlier `"spine"`
    segment elsewhere in the path (e.g. inside the bucket name broken across
    segments) that is not immediately followed by `specs/<pipeline_slug>/`
    fails this check even if a later, genuine root exists further down the
    path — a false-negative direction only (fails closed, never open), so it
    trades a theoretical over-rejection for zero risk of under-rejection.
    """
    for scheme in _ALLOWED_SPEC_URI_SCHEMES:
        if uri.startswith(scheme):
            break
    else:
        raise ValueError(f"pipeline_spec_uri must be one of {_ALLOWED_SPEC_URI_SCHEMES!r} (I-23)")

    if uri.startswith("s3://"):
        bucket = uri[len("s3://") :].split("/", 1)[0]
        if bucket != artifacts_bucket:
            raise ValueError(
                "pipeline_spec_uri must resolve inside the pinned artifacts bucket "
                f"{artifacts_bucket!r} (I-23)"
            )

    segments = [s for s in uri.split("/") if s not in ("", ".")]
    if ".." in segments:
        raise ValueError("pipeline_spec_uri must not contain '..' traversal segments (I-23)")
    try:
        root_idx = segments.index(_SPECS_ROOT_SEGMENTS[0])
    except ValueError:
        root_idx = -1
    expected = [*_SPECS_ROOT_SEGMENTS, pipeline_slug]
    has_filename = len(segments) > root_idx + len(expected) if root_idx >= 0 else False
    matches_root = root_idx >= 0 and segments[root_idx : root_idx + len(expected)] == expected
    if not (matches_root and has_filename):
        raise ValueError(
            f"pipeline_spec_uri must resolve under '.../spine/specs/{pipeline_slug}/' "
            "for this job's own pipeline (I-23)"
        )


# --- effect (default): I-23 spec fetch ---------------------------------------


def default_fetch_spec(uri: str) -> str:
    """Production default fetch: `s3://` via boto3 `GetObject`; `file://`
    (or a bare local path — tolerated for direct test convenience) via a
    plain read. `boto3` is imported lazily, function-local: the `s3://`
    branch is the only one that needs it, so a test exercising only the
    `file://` branch never pays a boto3-client-construction cost or needs
    AWS credentials configured. Any failure (missing key, bad credentials,
    unreadable path) propagates as-is — an infra/config failure at job
    start, before any land-stage effect, not something this module maps to
    `TransientError` (that class is reserved for `effects/*.py`, §7.6)."""
    if uri.startswith("s3://"):
        import boto3  # type: ignore[import-untyped]  # local import — see docstring above

        bucket, _, key = uri[len("s3://") :].partition("/")
        client = boto3.client("s3")
        body = client.get_object(Bucket=bucket, Key=key)["Body"]
        return body.read().decode("utf-8")
    path = uri[len("file://") :] if uri.startswith("file://") else uri
    from pathlib import Path  # local import — mirrors the boto3 branch's lazy-import symmetry

    return Path(path).read_text()


# --- pure: seed parse, spec parse, binding-defect asserts --------------------


def _parse_seed(config: RunnerConfig) -> DeliveryRegisteredV1:
    """Full pydantic parse of the SFN-supplied delivery detail (§6.1). A
    `ValidationError` here is the I-10 fail-fast class (§7.0's boundary-parse
    carve-out) -- raised before any effect."""
    return DeliveryRegisteredV1.model_validate_json(config.delivery_json)


def _parse_spec(spec_text: str) -> PipelineSpecModel:
    """`core.model.parse_pipeline_spec_yaml` — the strict duplicate-key-
    rejecting loader (006.1 §4/S1) + full pydantic parse (§6.2). A
    malformed spec (or one with a duplicate YAML mapping key) raises here,
    before `bind_transforms` ever runs."""
    return parse_pipeline_spec_yaml(spec_text)


def _assert_binding_matches(
    spec: PipelineSpecModel, seed: DeliveryRegisteredV1, config: RunnerConfig
) -> None:
    """Two binding-defect asserts, both fail-fast, both pre-`bind_transforms`:
    `spec.pipeline == seed.pipeline` (the spec fetched for this job must be
    the seed's OWN pipeline's spec) and `spec.sla_minutes == config.
    sla_minutes` — the [H-5] two-sources-of-truth guard: the DEPLOYED
    per-attempt timeout (`RunnerConfig.sla_minutes`, a Terraform default
    argument) must equal the authored spec's own declared budget, so a spec
    edit that silently changes nothing fails loudly instead."""
    if spec.pipeline != seed.pipeline:
        raise ValueError(
            f"pipeline_spec_uri resolved to spec.pipeline={spec.pipeline!r}, but the seed "
            f"event's own pipeline is {seed.pipeline!r} (binding defect)"
        )
    if spec.sla_minutes != config.sla_minutes:
        raise ValueError(
            f"spec.sla_minutes={spec.sla_minutes!r} != RunnerConfig.sla_minutes="
            f"{config.sla_minutes!r} (the deployed per-attempt budget) -- binding defect [H-5]"
        )


# --- session build + extensions assert: `entrypoints/session.py`, the ONE --
# --- authored source (F2, bead conveyer-swb.25) -- `_catalog_conf`/---------
# --- `_build_session`/`_assert_iceberg_extensions_active` used to be -------
# --- duplicated locally here; `rebuild_main.py` imports the identical ------
# --- functions from `entrypoints/session.py`, so the two entrypoints' own --
# --- session confs cannot drift apart again. ---------------------------


# --- effect (real JVM, driver-side only): 005.1 §3.2 [DC-4] + §6.1's --------
# --- pinned obligation #1 -- bind-time admission-grammar validation ---------


def _pattern_fullmatch_expr(pattern: str) -> str:
    """The exact fullmatch-anchoring template `frames/checks.py::
    _pattern_check` builds for its own `rlike` expression (`\\A(?:…)\\z`,
    lowercase `\\z` -- [DC-4]) -- duplicated here (not imported: `frames/`
    is out of this bead's scope, and this is a three-line literal, cheaper
    kept in sync by cross-reference than by coupling a private function
    across the frames/entrypoints boundary) so the JVM compiles the SAME
    string that will actually execute at real check time, not just the bare
    author-declared `pattern`."""
    return f"\\A(?:{pattern})\\z"


def _assert_patterns_compile_in_jvm(spark: SparkSession, contract: RawContractModel) -> None:
    """005.1 §3.2 [DC-4]: `pattern`'s NORMATIVE grammar is Java regex (the
    engine that actually executes it via `rlike`), not the best-effort
    Python `re.compile` typo check `core/model.py::ColumnSpec.
    _check_pattern_compiles` already ran at spec parse -- a pattern that is
    Python-valid but Java-invalid (e.g. `(?P<name>foo)`, PCRE-only named-
    group syntax; Java spells it `(?<name>foo)`) must not silently diverge
    between the two engines. Compiles `_pattern_fullmatch_expr(column.
    pattern)` through `spark._jvm.java.util.regex.Pattern.compile` -- a
    driver-side JVM call, no `DataFrame`/action needed -- for every column
    that declares one; raises a binding defect pre-land on the first
    rejection (message names the column and the JVM's own error, never a
    row value -- both are spec-authored, reviewed strings, not payload)."""
    # pyspark types `SparkSession._jvm` as `Optional` (only `None` before a
    # live session/gateway exists) -- this function only ever runs after
    # `session.build_session` has already returned a live session (§8.3 ordering),
    # so the assert narrows the type for mypy rather than silencing it.
    assert spark._jvm is not None, "SparkSession._jvm is only None before a live session exists"  # noqa: SLF001
    for column in contract.columns:
        if column.pattern is None:
            continue
        fullmatch = _pattern_fullmatch_expr(column.pattern)
        try:
            spark._jvm.java.util.regex.Pattern.compile(fullmatch)  # noqa: SLF001
        except Exception as exc:  # noqa: BLE001 -- re-raised as our own binding defect below
            raise ValueError(
                f"column {column.name!r} declares pattern {column.pattern!r}, which the JVM "
                f"(java.util.regex.Pattern, the executing engine, [DC-4]) rejects: {exc} -- "
                "binding defect, 005.1 §3.2"
            ) from exc


def _assert_temporal_fmt_compiles_in_jvm(spark: SparkSession, contract: RawContractModel) -> None:
    """Bead conveyer-azr.26 (n3-fmt-probe fix): closes the residual gap left
    open by `_assert_temporal_bounds_bind` (conveyer-azr.18) -- a `date`/
    `timestamp` column declaring NEITHER `min` NOR `max` has no bound
    literal to feed that function's probe, so a malformed fmt SEQUENCE on it
    (alphabet-valid-but-structurally-invalid, e.g. `"V"` at count 1 -- Spark
    requires exactly `"VV"`) was still only surfacing the first time a real
    batch genuinely executed against it -- an unnamed loud failure,
    contradicting 005.1 §6.1's own "a malformed fmt fails at bind, pre-land"
    claim for this one case (see `frames/checks.py::_typed_expr`'s own
    "Recorded gap" paragraph).

    Reuses the .18 probe machinery exactly: `compile_contract`'s SAME
    `typed_exprs[column.name]` (D-5's "no second cast to disagree", carried
    out to this bind-time probe too) evaluated over a one-row driver-side
    `DataFrame`, via a genuine `.collect()` to force the JVM to actually
    build the `DateTimeFormatter` -- but for EVERY `date`/`timestamp`
    column, unconditionally, not just ones declaring `min`/`max`.

    The synthetic probe value is a bare SQL `NULL` in the column's cell, not
    a rendered value -- empirically verified sufficient (scratch script,
    this bead, real Spark 3.5/JVM): `to_date`/`to_timestamp` build their
    `DateTimeFormatter` eagerly enough that `IllegalArgumentException:
    Pattern letter count must be 2: V` raises identically whether the probed
    cell is `NULL` or a real non-null string (`nullSafeEval` does not defer
    formatter construction past the null check) -- a rendered probe value
    (e.g. via `date_format` of a fixed instant under the column's own fmt)
    is unnecessary once this is known, and would itself be unreliable for a
    malformed fmt (rendering under a malformed fmt can fail for reasons
    unrelated to parsing).

    Deliberately redundant for columns `_assert_temporal_bounds_bind` also
    probes via their own bound literal (one extra cheap one-row driver-side
    job each) -- traded for one function whose job is exactly "is this fmt
    SEQUENCE valid", independent of bound-value parseability. Message
    grammar is A-10's (`admission-defect/<code>: <machine detail>`, no URIs
    -- column name and fmt string only, both spec-authored, reviewed
    strings, never a row value)."""
    compiled = compile_contract(contract)
    for column in contract.columns:
        column_type = parse_column_type(column.type)
        if column_type.kind not in ("date", "timestamp"):
            continue
        typed_expr = compiled.typed_exprs[column.name]
        schema = StructType([StructField(column.name, StringType(), True)])
        probe_df = spark.createDataFrame([(None,)], schema=schema)
        try:
            probe_df.select(typed_expr.alias("_v")).collect()
        except Exception as exc:  # noqa: BLE001 -- re-raised as our own binding defect below
            raise ValueError(
                f"admission-defect/malformed-fmt-sequence: column={column.name!r} "
                f"fmt={column_type.fmt!r} rejected by the JVM formatter: {exc} -- binding "
                "defect, 005.1 §6.1 (conveyer-azr.26)"
            ) from exc


def _assert_temporal_bounds_bind(spark: SparkSession, contract: RawContractModel) -> None:
    """005.1 §6.1's pinned obligation #1 (flagged unmet by `core/model.py`'s
    own docstring and `frames/checks.py::_bounds_check`'s own docstring):
    for every `date`/`timestamp` column declaring `min`/`max`, evaluates the
    SAME compiled `typed_exprs[column.name]` `frames/checks.py::
    compile_contract` builds (D-5's one-cast-expression rule, carried out
    to this bind-time probe too) over a one-row driver `DataFrame` holding
    each bound literal, via a genuine `.collect()` -- a real Spark action,
    so it forces the JVM to actually interpret the column's `fmt`. Two
    bound-VALUE-specific things this function alone still checks: an
    unparseable bound literal under an otherwise-valid fmt (`typed_expr`
    casts it to `NULL`); and, once both bounds parse, `min > max` (never
    checked at spec-parse time for temporals, unlike the numeric kinds --
    `core/model.py`'s own recorded gap), caught by a plain Python comparison
    of the two parsed driver-side values. An alphabet-valid-but-
    structurally-invalid fmt SEQUENCE (e.g. `"V"` at count 1 -- Spark
    requires exactly `"VV"`) also still raises HERE incidentally, for
    columns that reach this function's own bound-literal probe -- but the
    general case (ANY temporal column, with or without `min`/`max`) is now
    `_assert_temporal_fmt_compiles_in_jvm`'s job (bead conveyer-azr.26),
    which `main()` runs first; `_bounds_check`'s own runtime
    "100%-quarantine on an unparseable bound" predicate remains as
    defense-in-depth regardless of either bind-time probe."""
    compiled = compile_contract(contract)
    for column in contract.columns:
        column_type = parse_column_type(column.type)
        if column_type.kind not in ("date", "timestamp"):
            continue
        if column.min is None and column.max is None:
            continue
        typed_expr = compiled.typed_exprs[column.name]
        schema = StructType([StructField(column.name, StringType(), True)])
        parsed: dict[str, str] = {}
        for label, literal in (("min", column.min), ("max", column.max)):
            if literal is None:
                continue
            row_df = spark.createDataFrame([(literal,)], schema=schema)
            try:
                collected = row_df.select(typed_expr.alias("_v")).collect()
            except Exception as exc:  # noqa: BLE001 -- re-raised as our own binding defect below
                raise ValueError(
                    f"column {column.name!r} declares {label}={literal!r} under "
                    f"fmt={column_type.fmt!r}, which the JVM formatter rejects: {exc} -- "
                    "binding defect, 005.1 §6.1's pinned obligation #1"
                ) from exc
            value = collected[0]["_v"]
            if value is None:
                raise ValueError(
                    f"column {column.name!r} declares {label}={literal!r}, which does not "
                    f"parse under its own fmt={column_type.fmt!r} -- binding defect, 005.1 "
                    "§6.1's pinned obligation #1"
                )
            parsed[label] = value
        if "min" in parsed and "max" in parsed and parsed["min"] > parsed["max"]:
            raise ValueError(
                f"column {column.name!r} declares min={column.min!r} > max={column.max!r} "
                "(parsed under the column's own fmt) -- binding defect, 005.1 §6.1's pinned "
                "obligation #1"
            )


# --- effect (real JVM, driver-side only): K5, the engine compile gate ------
# --- (P-2 gate 2 / P-9 rule 2's compiled-dtype inspection, [EM-3]) ----------


_FACT_COLUMN_SPARK_TYPES: Mapping[str, Callable[[], DataType]] = {
    "string": StringType,
    "int": IntegerType,
    "long": LongType,
    "bool": BooleanType,
    "date": DateType,
    "timestamp": TimestampType,
}

# P-9 rule 2 [EM-3]: `aggregate`/`control.expr` must compile to an
# INTEGRAL or decimal type -- the engine-verified DOUBLE producers
# (`avg(int|long)`, `/` over integrals) are exactly what this excludes.
_INTEGRAL_OR_DECIMAL_TYPES: tuple[type[DataType], ...] = (
    ByteType,
    ShortType,
    IntegerType,
    LongType,
    DecimalType,
)


def _fact_column_spark_type(type_str: str) -> DataType:
    """006.1 §4.1's `FACT_COLUMN_TYPE_RE`-shaped type string -> its Spark
    `DataType`, for K5's probe schema -- the six bare kinds via a lookup
    table, `decimal(p,s)` parsed directly (`type_str` is already
    `FactColumnSpec.type`-pattern-valid by the time a `PipelineSpecModel`
    exists)."""
    kind = type_str.split("(", 1)[0]
    if kind == "decimal":
        precision_str, scale_str = type_str[len("decimal(") : -1].split(",")
        return DecimalType(int(precision_str), int(scale_str))
    return _FACT_COLUMN_SPARK_TYPES[kind]()


def _fact_type_probe_schema(fact_type: FactTypeModel) -> StructType:
    return StructType(
        [
            StructField(column.name, _fact_column_spark_type(column.type), True)
            for column in fact_type.schema_.columns
        ]
    )


def _compile_probe(probe_df: DataFrame, text: str, check_id: str, label: str) -> DataType:
    """P-2 gate 2: engine-compiles `text` -- the raw authored field, byte-
    exact; gate 1/K9 already accepted it at spec-parse (`core/model.py`) by
    the time a `PipelineSpecModel` exists, so no re-derivation is needed
    here -- against `probe_df`'s bound schema. `.select(...)` alone forces
    full Catalyst analysis (verified this bead: an unresolved column raises
    `AnalysisException` right here, no `.collect()`/action needed) -- a
    defensive net K3/K4 should already have made unreachable for a validly-
    parsed spec, kept because engine-compile failure is its own named §5.4
    code, never silently assumed impossible."""
    try:
        compiled = probe_df.select(F.expr(text).alias("_v"))
    except Exception as exc:  # noqa: BLE001 -- re-raised as our own binding defect below
        raise ValueError(
            f"bind-defect/check-expression-uncompilable: check {check_id!r} {label} "
            f"rejected by the engine (P-2 gate 2): {exc}"
        ) from exc
    return compiled.schema.fields[0].dataType


def _assert_row_expr_boolean(probe_df: DataFrame, check_id: str, text: str) -> None:
    dtype = _compile_probe(probe_df, text, check_id, "expr")
    if not isinstance(dtype, BooleanType):
        raise ValueError(
            f"bind-defect/check-expression-not-boolean: check {check_id!r} expr compiles to "
            f"{dtype.simpleString()!r}, not boolean (P-9 rule 2 [EM-3])"
        )


def _assert_aggregate_dtype_exact(
    probe_df: DataFrame, check_id: str, label: str, text: str
) -> DataType:
    """Returns the compiled `dtype` on success (conveyer-swb.15/D006-1: the
    [DC-2] wiring below reuses this SAME oracle compile rather than
    re-running `_compile_probe` a second time for the identical text) --
    otherwise unchanged: every existing raise/condition below is exactly as
    it was before this return-value addition (K5's own behaviour, untouched
    per this bead's brief)."""
    dtype = _compile_probe(probe_df, text, check_id, label)
    if not isinstance(dtype, _INTEGRAL_OR_DECIMAL_TYPES):
        raise ValueError(
            f"bind-defect/check-expression-inexact-type: check {check_id!r} {label} compiles "
            f"to {dtype.simpleString()!r}, not integral or decimal (P-9 rule 2 [EM-3])"
        )
    return dtype


def _schema_family_map(schema: FactSchemaModel) -> dict[str, Family | None]:
    """The SAME one-line, grammar-anchored reduction `core/model.py::_fact_
    schema_family_map`/`frames/business_checks.py::_schema_family_map`
    already perform -- re-derived here rather than imported (both of those
    are module-private, and the derivation is mechanical, not business
    logic that could drift; `business_checks.py`'s own docstring names this
    exact precedent)."""
    return {column.name: family_of_kind(column.type.split("(", 1)[0]) for column in schema.columns}


def _assert_dtype_agreement(
    check_id: str, structural_dtype: DataType, oracle_dtype: DataType
) -> None:
    """§7.5's [DC-2]: bind additionally asserts equality between the
    structural Column tree's (`business_checks.aggregate_column`, built
    from `check_grammar.compile_aggregate`'s pure value tree) engine-
    analyzed dtype and gate 2's own oracle answer for the SAME authored
    text (`_assert_aggregate_dtype_exact`'s own `_compile_probe` call,
    passed in rather than recomputed). Disagreement is a FRAMEWORK defect
    (the `compile_aggregate` translator disagreeing with the engine's own
    typing of the authored text) -- never an authored-spec `bind-defect/*`
    -- a plain `ValueError`, checked at the WHOLE aggregate-expression
    grain: each individual `sum` node's own coalesce-cast target is
    independently drawn from a LIVE probe of that exact structural sub-
    column against the same frame (`aggregate_column`'s own construction),
    so it cannot itself disagree by construction -- what this assertion
    actually guards is the translator's ARITHMETIC/DISPATCH composition
    (e.g. a wrong operator or aggregate-kind mapping), which this bead's
    own kernel session confirmed a whole-expression dtype comparison does
    catch (combined aggregate+arithmetic dtypes agree end to end exactly
    when the composition is correct, verified over `sum(net)+sum(fees)`,
    `sum(net)/count(1)`, and a decimal-overflow `sum` row)."""
    if structural_dtype != oracle_dtype:
        raise ValueError(
            f"framework-defect/compile-aggregate-dtype-mismatch: check {check_id!r} aggregate "
            f"structural compile yields {structural_dtype.simpleString()!r}, engine oracle "
            f"yields {oracle_dtype.simpleString()!r} (sec7.5 [DC-2])"
        )


def _assert_compile_aggregate_dtype_agreement(
    probe_df: DataFrame,
    check_id: str,
    aggregate_text: str,
    family_map: Mapping[str, Family | None],
    oracle_dtype: DataType,
) -> None:
    """Wires [DC-2] into K5 (§7.5, conveyer-swb.15/D006-1): re-derives the
    SAME gate-1 `ValidatedExpr` (never a second grammar -- the `_compiled_
    expr`/`business_checks.py` precedent, re-deriving from raw text since
    bind already validated it once), compiles it via `check_grammar.
    compile_aggregate` (the pure, per-sum-coalesce-typed value tree), turns
    it into a real `Column` via `business_checks.aggregate_column`, engine-
    probes ITS dtype against the SAME `probe_df` K5 already built, and
    asserts it against `oracle_dtype`. A gate-1 disagreement between this
    re-validation and `core/model.py`'s own K4/K9 call (i.e. THIS call
    rejects text that already passed at spec-parse) is itself a framework
    defect -- the two call sites must always agree, since both run the
    identical pure `validate_expression` over the identical text and
    schema. A construct outside `compile_aggregate`'s own scoped surface
    (its own docstring) is ALSO a framework defect here: it means a
    `batch_check` was authored using an aggregate shape this translator
    does not yet cover, caught pre-land rather than mistranslated."""
    validated = validate_expression(aggregate_text, "aggregate", family_map)
    if not isinstance(validated, ValidatedExpr):
        raise ValueError(
            f"framework-defect/compile-aggregate-gate1-disagreement: check {check_id!r} "
            "aggregate failed re-validation at K5 after already passing gate 1 at bind-parse "
            "(core/model.py) -- the two gate-1 call sites disagree"
        )
    compiled = compile_aggregate(validated, family_map)
    if not isinstance(compiled, CompiledAggregate):
        raise ValueError(
            f"framework-defect/compile-aggregate-unsupported-construct: check {check_id!r} "
            f"aggregate {aggregate_text!r} uses a construct outside compile_aggregate's scoped "
            f"surface ({compiled.code}: {compiled.detail}) -- sec7.5 [DC-2] cannot be asserted"
        )
    structural_col = aggregate_column(compiled.tree, probe_df)
    structural_dtype = probe_df.agg(structural_col.alias("_v")).schema.fields[0].dataType
    _assert_dtype_agreement(check_id, structural_dtype, oracle_dtype)


def _assert_check_expressions_compile(spark: SparkSession, spec: PipelineSpecModel) -> None:
    """K5 (P-2 gate 2 + P-9 rule 2's compiled-dtype inspection, [EM-3]),
    006.1 §5.4 -- deliberately NOT folded into `bind_checks.validate_
    bindings` (`core/bind_checks.py`'s own module docstring: that function
    is plain-value-pure; K5 needs a live engine -- "a THIRD, separate
    bind-time call the entrypoint makes", P-4's own text). Still pre-land,
    still before `fx_factory`: this function needs only `spark` + `spec`,
    unlike the F-10/[DC-1] wiring below (which needs `fx.describe_table`).

    The `BatchCheckModel` branch (aggregate/`control.expr`) is currently
    UNREACHABLE through any validly-parsed `PipelineSpecModel`: `ChecksModel`
    (`core/model.py`) refuses ANY `batch_check` at spec-PARSE time (K7,
    P-6's structural wait on 005 v1.x's member grammar) -- a spec
    containing one never becomes a `PipelineSpecModel` instance in the
    first place. Implemented anyway, never guarded behind "if batch checks
    were ever reachable": P-9 rule 2 defines K5 over BOTH check kinds, and
    005 v1.x landing must not require revisiting this function. Tested at
    `_assert_aggregate_dtype_exact`'s own grain, against a hand-built
    `BatchCheckModel` (which carries no refusal of its own -- only
    `ChecksModel` does), since a real spec cannot exercise it.

    **[DC-2] (conveyer-swb.15/D006-1), wired into the `aggregate` branch
    only.** `control.expr` is a SCALAR-position expression over one already-
    extracted row (§9), not an aggregate -- `compile_aggregate` does not
    apply to it, so only `_assert_aggregate_dtype_exact(..., "aggregate",
    ...)`'s own oracle dtype feeds `_assert_compile_aggregate_dtype_
    agreement` below; K5's `control.expr` check is otherwise unchanged."""
    for type_name, fact_type in spec.fact_types.items():
        probe_df = spark.createDataFrame([], schema=_fact_type_probe_schema(fact_type))
        for check in spec.checks.checks:
            if check.fact_type != type_name:
                continue
            if isinstance(check, RowCheckModel):
                _assert_row_expr_boolean(probe_df, check.id, check.expr)
            elif isinstance(check, BatchCheckModel):
                aggregate_dtype = _assert_aggregate_dtype_exact(
                    probe_df, check.id, "aggregate", check.aggregate
                )
                family_map = _schema_family_map(fact_type.schema_)
                _assert_compile_aggregate_dtype_agreement(
                    probe_df, check.id, check.aggregate, family_map, aggregate_dtype
                )
                _assert_aggregate_dtype_exact(
                    probe_df, check.id, "control.expr", check.control.expr
                )


# --- effect: P-4's CatalogFacts acquisition + the F-10/[DC-1] bind step ----
# --- (006.1 §5/P-4; 007.1 F-10/[DC-1], the B2<->B7 seam) --------------------


def _referenced_tables(spec: PipelineSpecModel) -> tuple[str, ...]:
    """P-4's `CatalogFacts` population set (`core/bind_checks.py::
    CatalogFacts`'s own docstring): every declared co-effect's table,
    (existence-only) every declared fact type's `fact_table`/`state_table`,
    plus (existence-only, critique gate wf_78ea4599-a5b F3, bead
    conveyer-swb.26) this pipeline's own marker table (`naming.markers_
    table`'s own derivation — the SAME one `_committed_tables`'s call site
    below and `bootstrap/create_record_tables.py` already use, never a
    second, independent naming rule). Folding the marker table into this
    ONE `fx.describe_table`-per-table acquisition (rather than a bespoke
    existence probe) means P-4's own bind-defect matrix (`core/bind_checks.
    py::validate_bindings`'s new `marker-table-missing` row) discovers an
    un-bootstrapped marker table PRE-LAND — before any raw write or
    `batch-started` event — instead of only at commit, where `effects/
    spark.py::_require_marker_table` raises a retry-class `TransientError`
    on every SFN attempt until a redeploy (006.1 P-4's own "the pre-land
    class exists precisely so misconfiguration never half-starts a batch")."""
    tables = {decl.table for decl in spec.co_effects.values()}
    for fact_type in spec.fact_types.values():
        tables.add(fact_type.fact_table)
        tables.add(fact_type.state_table)
    tables.add(naming.markers_table(spec.raw_table, spec.pipeline))
    return tuple(sorted(tables))


def _acquire_catalog_facts(fx: RunnerFx, tables: tuple[str, ...]) -> bind_checks.CatalogFacts:
    """P-4's effectful acquisition step: one `fx.describe_table` call per
    referenced table (004.1-erratum class, §16.4 item 3)."""
    return {table: fx.describe_table(table) for table in tables}


def _acquire_transforms_meta(spec: PipelineSpecModel) -> bind_checks.TransformsMeta:
    """S4's raw-module inspection (006.1 §4.4/§5.1), plus its sibling
    `stale-fold-export` check (critique gate wf_24a3125f-ecc F2, bead
    conveyer-6pg.31). `importlib.import_module` is a cache HIT here --
    `bind_transforms` (earlier in this same `main()` sequence) already
    imported this exact module, so `sys.modules` already holds it; this is
    a dict lookup, not a re-execution. Kept deliberately independent of
    `binding.py::bind_transforms`'s OWN contract (`Transforms` still
    required `post_check`, pre-B3's stage-rewrite migration, `conveyer-
    6pg.13`; it still required `fold`, pre-F2's fold-defaulting-wiring
    removal) -- this reads the raw module's own `post_check`/`fold`
    attributes directly, so S4 and its sibling are wireable independent of
    what `bind_transforms` itself currently requires or ignores."""
    module = importlib.import_module(spec.transforms_module)
    return bind_checks.TransformsMeta(
        has_post_check_export=hasattr(module, "post_check"),
        has_fold_export=hasattr(module, "fold"),
    )


def _load_table_class_inventory(
    fetch_spec: Callable[[str], str], pipeline_spec_uri: str
) -> Mapping[str, str]:
    """F-10 (007.1 §6.5 step 6, `core/bind_checks.py`'s own "F-10 / [DC-1]"
    docstring section): loads the content-pinned `table-classes.json` the
    deploy step (`bootstrap.create_record_tables.py::main`) emits beside the
    deployed spec (the I-23 idiom) -- reuses the SAME `fetch_spec` DI seam
    `main()` already threads through for the spec fetch itself (`s3://`
    boto3 / `file://` local, zero new seams), at the sibling URI `naming.
    table_class_inventory_uri(pipeline_spec_uri)` derives. `core/naming.py`
    is the single source both the writer (bootstrap) and this reader import
    the URI-derivation rule from (that module's own docstring), so the two
    can never disagree on where the inventory lives. `json.loads` -> the
    bind-time AUTHORITY `validate_bindings`'s C4 check reads."""
    inventory_uri = naming.table_class_inventory_uri(pipeline_spec_uri)
    inventory: Mapping[str, str] = json.loads(fetch_spec(inventory_uri))
    return inventory


def _committed_tables(spark: SparkSession, markers_table: str, batch_id: str) -> tuple[str, ...]:
    """[DC-1] discharge (007.1 §4.3, `core/bind_checks.py`'s own "F-10 /
    [DC-1]" docstring section): bind's ONE ruled data-read -- the DISTINCT
    guard-twin `table_name` for `(batch_id, stage='commit')`, sentinel
    excluded (`naming.COMMIT_COMPLETION_SENTINEL`). `markers_table` is the
    BARE `<db>.<slug>__markers` name (`naming.markers_table`'s own output);
    `naming.qualified` adds the catalog prefix here, matching `effects/
    spark.py`'s own convention (bare names flow through business logic,
    catalog-qualification happens at the effect that actually touches
    Spark). Returns `()` when the marker table does not exist yet --
    `spark.catalog.tableExists` first, mirroring `bootstrap/create_record_
    tables.py`'s own idempotent-creation precedent; an empty set is a subset
    of every deployed fact-table set by construction, so `fact-type-removed-
    in-flight` correctly stays silent in that case too. DataFrame-API
    predicate, not string-interpolated SQL (I-3's own `table_has_batch`
    precedent, 004.1 §5) -- `batch_id`/the sentinel never touch a raw SQL
    string.

    **This function's own missing-table tolerance is now unreachable-after-
    bind (critique gate wf_78ea4599-a5b F3, bead conveyer-swb.26).** `main()`
    calls this BEFORE `_assert_bind_checks_pass`, so the `return ()` branch
    above still executes when the marker table is absent -- but by
    construction its result can never reach `run_sequence`: `_referenced_
    tables` now includes this pipeline's own marker table in `CatalogFacts`,
    so the SAME absence `validate_bindings`'s `marker-table-missing` row
    refuses at bind, right after this call returns, before this function's
    `()` result (or anything downstream of it) has any observable effect.
    Left in place, not removed: `spark.catalog.tableExists` here is a real,
    independently-tested Spark-catalog read (`test_committed_tables_returns_
    empty_when_marker_table_absent`) that this docstring note, not a code
    change, is the lower-risk way to record the new fact about."""
    qt = naming.qualified(markers_table)
    if not spark.catalog.tableExists(qt):
        return ()
    rows = (
        spark.table(qt)
        .where(
            (F.col("stage") == "commit")
            & (F.col("batch_id") == F.lit(batch_id))
            & (F.col("table_name") != F.lit(naming.COMMIT_COMPLETION_SENTINEL))
        )
        .select("table_name")
        .distinct()
        .collect()
    )
    return tuple(sorted(r["table_name"] for r in rows))


def _assert_bind_checks_pass(
    spec: PipelineSpecModel,
    catalog_facts: bind_checks.CatalogFacts,
    transforms_meta: bind_checks.TransformsMeta,
    table_class_inventory: Mapping[str, str],
    committed_tables: tuple[str, ...],
    markers_table: str,
) -> None:
    """P-4's pure half, called: any defect -> one plain `ValueError` naming
    every `bind-defect/<code>: <detail>` found (A-10's grammar; every
    finding named, not just the first -- the `describe_raw_diff` precedent,
    `bootstrap/create_admission_tables.py`).

    `markers_table` (critique gate wf_78ea4599-a5b F3, bead conveyer-swb.26):
    this pipeline's own bare marker-table identifier -- the SAME value
    `_committed_tables` above already reads with, threaded through so
    `validate_bindings` can look it up in `catalog_facts` (already populated
    by `_referenced_tables`/`_acquire_catalog_facts`, never a second read
    here) and refuse `marker-table-missing` pre-land."""
    defects = bind_checks.validate_bindings(
        spec, catalog_facts, transforms_meta, table_class_inventory, committed_tables, markers_table
    )
    if not defects:
        return
    detail = "; ".join(f"bind-defect/{d.code}: {d.detail}" for d in defects)
    raise ValueError(detail)


# --- pure: seed `BatchContext` -----------------------------------------------


def _seed_batch_context(
    seed: DeliveryRegisteredV1,
    spec: PipelineSpecModel,
    config: RunnerConfig,
    transforms: Transforms,
) -> BatchContext:
    """Assembles the seed `BatchContext` (§6.3) from the parsed seed event,
    the bound spec, the framework-owned `RunConfig` (parsed once more here
    from `config.run_config_json` -- the identical, harmless re-parse
    `effects/spark.py::build_spark_fx` already performs internally for its
    own `append`/`RunConfig` needs; both are pure parses of the same JSON
    string, so no state diverges), and the bound `Transforms` record.

    005.1 A-11/§3.5 (bead conveyer-azr.18): `read_spec_version`/
    `check_version` are computed exactly ONCE here, right after spec parse
    (`core.contract.read_spec_version`/`check_version`, pure functions of
    the already-parsed `spec.read`/`spec.raw_contract`) and carried on the
    seed context unchanged for the rest of the run -- this function is
    called exactly once per `main()` invocation, so "computed once" falls
    out of that, the same way `RunConfig.model_validate_json` above already
    relies on being called from exactly one call site.

    006.1 P-3/§4.5 (bead conveyer-6pg.13, B3): `checks_version` joins the
    same seed-adjacent class, computed once here via `core.checks.
    checks_version(spec.checks)` -- a pure function of the already-parsed
    `spec.checks` -- and carried unchanged for the rest of the run."""
    return BatchContext(
        pipeline=seed.pipeline,
        feed_id=seed.feed_id,
        delivery_id=seed.delivery_id,
        batch_id=seed.batch_id,
        delivery_key=seed.delivery_key,
        content_hash=seed.content_hash,
        object_uris=tuple(seed.object_uris),
        received_at=seed.received_at,
        spec=spec,
        run=RunConfig.model_validate_json(config.run_config_json),
        transforms=transforms,
        attempt_id=config.attempt_id,
        sfn_retry_count=config.sfn_retry_count,
        sfn_redrive_count=config.sfn_redrive_count,
        read_spec_version=read_spec_version(spec.read),
        check_version=check_version(spec.raw_contract, spec.read),
        checks_version=checks_version(spec.checks),
    )


# --- the entrypoint: pure composition, no decisions of its own --------------


def main(
    argv: Sequence[str],
    *,
    fetch_spec: Callable[[str], str] = default_fetch_spec,
    fx_factory: Callable[[SparkSession, RunnerConfig], RunnerFx] = make_runner_fx,
) -> BatchContext:
    """`install_json_handler() -> argv -> RunnerConfig -> seed -> spec ->
    transforms -> session -> fx -> BatchContext -> spine.run.run(seed, fx)`
    (§8.3 plus the leading observability install, conveyer-nvh.47), plus
    three more binding-defect asserts (005.1 §3.2 [DC-4], §6.1's pinned
    obligation #1 -- bead conveyer-azr.18; the fmt-SEQUENCE probe for every
    temporal column -- bead conveyer-azr.26), K5's engine-compile gate
    (P-2 gate 2/P-9 rule 2, bead conveyer-6pg.12) right after the session/
    extensions asserts, still strictly before `fx_factory`, and P-4's bind-
    time validator inventory (006.1 §5, 007.1 F-10/[DC-1]) right after
    `fx_factory` (needs `fx.describe_table`) but still strictly before
    `run_sequence`: every step below is a named call to a function defined
    above (or imported) — no branching in this function itself."""
    observability.install_json_handler()
    config = from_args(argv)
    seed = _parse_seed(config)
    naming.check_object_uris(
        seed.feed_id, seed.delivery_id, seed.received_at, seed.object_uris, config.landing_bucket
    )
    check_spec_uri_allowlist(
        config.pipeline_spec_uri, naming.slug(seed.pipeline), config.artifacts_bucket
    )
    spec = _parse_spec(fetch_spec(config.pipeline_spec_uri))
    _assert_binding_matches(spec, seed, config)
    transforms = bind_transforms(spec)
    spark = session.build_session(config, app_name=f"conveyer-spine-{config.env}")
    session.assert_iceberg_extensions_active(spark)
    _assert_patterns_compile_in_jvm(spark, spec.raw_contract)
    _assert_temporal_fmt_compiles_in_jvm(spark, spec.raw_contract)
    _assert_temporal_bounds_bind(spark, spec.raw_contract)
    _assert_check_expressions_compile(spark, spec)
    fx = fx_factory(spark, config)
    markers_table = naming.markers_table(spec.raw_table, spec.pipeline)
    catalog_facts = _acquire_catalog_facts(fx, _referenced_tables(spec))
    transforms_meta = _acquire_transforms_meta(spec)
    table_class_inventory = _load_table_class_inventory(fetch_spec, config.pipeline_spec_uri)
    committed_tables = _committed_tables(spark, markers_table, seed.batch_id)
    _assert_bind_checks_pass(
        spec, catalog_facts, transforms_meta, table_class_inventory, committed_tables, markers_table
    )
    seed_ctx = _seed_batch_context(seed, spec, config, transforms)
    return run_sequence(seed_ctx, fx)
