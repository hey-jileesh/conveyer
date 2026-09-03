"""Rebuild run-mode entrypoint — argv -> config -> bind -> session ->
`spine.effects.rebuild.rebuild_pipeline`. LLD 007.1 §9.4 (run-mode
trigger), A007-1.

**A SEPARATE entrypoint, not a `--run-mode` flag on `entrypoints/glue_main.
py::main`** (recorded decision, this bead). `glue_main.main`'s own
docstring states it is "pure COMPOSITION ... contains no `if`/`try` of its
own", and its `RunnerConfig`/`from_args` contract HARD-REQUIRES seed/
delivery/SFN-retry/SLA fields (`delivery_json`, `sfn_retry_count`,
`sfn_redrive_count`, `sla_minutes`, `landing_bucket`, `event_bus`) that no
rebuild invocation has a real value for — there is no seed batch to
rebuild. Folding a branch into that composition would either fabricate a
fake seed (a value-free-obligation smell, [S-7]) or break the "no `if` of
its own" invariant `main()`'s own docstring states as a design law, not a
style preference. `rebuild_main.py` is therefore its OWN, equally-thin,
pure-composition `main(argv)`, with its OWN minimal argv contract (§6.4's
`--conveyer-<kebab-field-name>` convention, extended with exactly ONE new
key this run mode alone needs: `--conveyer-pipeline` — the pipeline slug
I-23's allowlist check needs BEFORE any spec fetch; a rebuild invocation
names WHICH pipeline it rebuilds explicitly, since there is no seed event
to derive it from the way `glue_main.main` derives it from `seed.pipeline`).

**Reuse, not duplication (coordinator scope note, A007-1; sharpened F2/M3,
bead conveyer-swb.25).** `glue_main.check_spec_uri_allowlist` (I-23,
already public) and `glue_main.default_fetch_spec` are imported directly,
never re-derived — the SAME allowlist path and the SAME production/
`file://` fetch `glue_main.main` uses. The session-building idiom
(`catalog_conf`/`build_session`/`assert_iceberg_extensions_active`) now
lives in `entrypoints/session.py` (F2's fix): this module used to
reproduce it locally (`_catalog_conf`/`_build_session`/`_assert_iceberg_
extensions_active`) under `effects.rebuild`'s own "duplicate narrow shapes
rather than reach into another module's private surface" precedent — but
that copy had ALREADY DRIFTED from `glue_main.py`'s own (it omitted
`frames.checks.SESSION_PINS` entirely, the exact "two evaluators, same
code, different engine semantics" class the precedent exists to name, not
license). Both entrypoints now import the SAME public `entrypoints/
session.py` functions instead — one authored source, so they cannot drift
apart again. `RebuildConfig` satisfies `session.SessionConfig`'s narrow
Protocol structurally (`catalog_kind`/`warehouse_uri` — N3 fix, bead
conveyer-swb.28: `env` was dropped from the Protocol, unread by either
function), with no adapter.

**No `--force` flag anywhere in this module's own argv contract, by
construction (RB-2).** This is a wholly separate script with its own flag
surface — there is no shared surface with `glue_main.py`'s own flags to
accidentally widen, and nothing here ever threads an operator-supplied
override into `effects.rebuild.swap_with_retry`'s own refuse/retry loop.

**The ledger channel (`record_run`) reuses `effects/ledger.py`'s
production `build_catalog`/`build_record_run` unchanged, now typed against
the narrow `LedgerConfig` Protocol (M3, bead conveyer-swb.25).** Both used
to be typed against the full `spine.config.RunnerConfig` (18 fields), so
this module's own `_as_runner_config` fabricated a `RunnerConfig` from
`RebuildConfig`, filling the seed/SFN/SLA fields `effects/ledger.py` never
reads with harmless placeholders ([S-7]'s own fabrication smell, named as
such in this module's prior docstring). `effects/ledger.py::LedgerConfig`
now types those two functions against exactly the SIX fields they
genuinely read; `RebuildConfig` satisfies it structurally, with no
adapter — `_as_runner_config` is deleted outright, and `config` (this
module's own `RebuildConfig`) flows straight into both `ledger.
build_catalog`/`ledger.build_record_run`. The catalog is still built via a
DEFERRED factory (`functools.partial`, the SAME shape `effects/build.py::
make_runner_fx` uses for its own `record_run`), not eagerly — a
catalog-construction hiccup at job start must not prevent `record_run`'s
own best-effort, never-raising posture (§11.3) from degrading gracefully
per attempt, rather than crashing this entrypoint outright before any swap
runs.

**Out-of-band interim runbook**: `README.md`'s own "Out-of-band rebuild
(interim)" section names the manual `domainDB` re-materialization step
this entrypoint's own successful return does NOT discharge (§9.3, `effects/
rebuild.py`'s own module docstring — no `RebuildCompletedV1` event exists
yet).
"""

from __future__ import annotations

import functools
from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, cast

from spine import observability
from spine.core import args as core_args
from spine.core import naming
from spine.core.model import parse_pipeline_spec_yaml
from spine.effects import ledger
from spine.effects.rebuild import rebuild_pipeline
from spine.entrypoints import session
from spine.entrypoints.glue_main import check_spec_uri_allowlist, default_fetch_spec

if TYPE_CHECKING:
    from collections.abc import Callable

    from spine.core.model import PipelineSpecModel
    from spine.effects.rebuild import RebuildResult

_ARGV_KEYS: dict[str, str] = {
    "pipeline": "conveyer-pipeline",  # NEW: this run mode's own -- no seed to derive it from
    "pipeline_spec_uri": "conveyer-pipeline-spec-uri",
    "env": "conveyer-env",
    "aws_region": "conveyer-aws-region",
    "catalog_kind": "conveyer-catalog-kind",
    "warehouse_uri": "conveyer-warehouse-uri",  # optional (tests only)
    "ledger_catalog_kind": "conveyer-ledger-catalog-kind",
    "ledger_sql_uri": "conveyer-ledger-sql-uri",  # optional (SqlCatalog, tests only)
    "spine_db": "conveyer-spine-db",
    "run_ledger_table": "conveyer-run-ledger-table",
    # 6pg.35 item 4: I-23's `check_spec_uri_allowlist` bucket pin -- this run
    # mode calls the SAME shared allowlist function `glue_main.main` does
    # (module docstring's "reuse, not duplication"), so its own signature
    # change ripples here too.
    "artifacts_bucket": "conveyer-artifacts-bucket",
}


@dataclass(frozen=True)
class RebuildConfig:
    """The rebuild run mode's own MINIMAL argv contract — deliberately NOT
    `spine.config.RunnerConfig`: that dataclass hard-requires seed/SFN/SLA
    fields no rebuild invocation has a real value for (module docstring).
    Every field here is one this run mode genuinely needs."""

    pipeline: str
    pipeline_spec_uri: str
    artifacts_bucket: str  # 6pg.35 item 4: I-23's `check_spec_uri_allowlist` bucket pin
    env: str
    aws_region: str
    catalog_kind: Literal["glue", "hadoop"]
    warehouse_uri: str | None
    ledger_catalog_kind: Literal["glue", "sql"]
    ledger_sql_uri: str | None
    spine_db: str
    run_ledger_table: str


def _check_literal(value: str, options: tuple[str, ...], key: str) -> str:
    if value not in options:
        raise ValueError(f"{key} must be one of {options!r}, got {value!r}")
    return value


def from_args(argv: Sequence[str]) -> RebuildConfig:
    """Pure parse — the SAME `core.args.parse_args`/`--conveyer-<kebab-
    field-name>` convention `spine.config.from_args` follows (`config.py`'s
    own module docstring), scoped to this run mode's own minimal field
    set. Missing required key -> `KeyError` naming it, matching `spine.
    config.from_args`'s own failure shape."""
    parsed = core_args.parse_args(argv)

    def required(field: str) -> str:
        return parsed[_ARGV_KEYS[field]]

    def optional(field: str) -> str | None:
        return parsed.get(_ARGV_KEYS[field])

    return RebuildConfig(
        pipeline=required("pipeline"),
        pipeline_spec_uri=required("pipeline_spec_uri"),
        artifacts_bucket=required("artifacts_bucket"),
        env=required("env"),
        aws_region=required("aws_region"),
        catalog_kind=cast(
            Literal["glue", "hadoop"],
            _check_literal(required("catalog_kind"), ("glue", "hadoop"), "catalog_kind"),
        ),
        warehouse_uri=optional("warehouse_uri"),
        ledger_catalog_kind=cast(
            Literal["glue", "sql"],
            _check_literal(required("ledger_catalog_kind"), ("glue", "sql"), "ledger_catalog_kind"),
        ),
        ledger_sql_uri=optional("ledger_sql_uri"),
        spine_db=required("spine_db"),
        run_ledger_table=required("run_ledger_table"),
    )


def _assert_pipeline_matches(spec: PipelineSpecModel, config: RebuildConfig) -> None:
    """The rebuild-mode twin of `glue_main._assert_binding_matches`'s first
    check: the spec fetched for this invocation must be the OPERATOR-NAMED
    pipeline's own spec — I-23's allowlist already constrained the URI
    PATH; this closes the loop by checking the fetched spec's own declared
    `pipeline` field agrees, a binding defect otherwise."""
    if spec.pipeline != config.pipeline:
        raise ValueError(
            f"pipeline_spec_uri resolved to spec.pipeline={spec.pipeline!r}, but "
            f"--conveyer-pipeline={config.pipeline!r} (binding defect)"
        )


# --- session build + extensions assert: `entrypoints/session.py`, the ONE --
# --- authored source (F2, bead conveyer-swb.25) -- `_catalog_conf`/---------
# --- `_build_session`/`_assert_iceberg_extensions_active` used to be -------
# --- duplicated locally here; `glue_main.py` imports the identical ---------
# --- functions from `entrypoints/session.py`, so the two entrypoints' own --
# --- session confs cannot drift apart again. `RebuildConfig` satisfies -----
# --- `session.SessionConfig`'s narrow Protocol structurally. ---------------


# --- the entrypoint: pure composition, no decisions of its own --------------


def main(
    argv: Sequence[str],
    *,
    fetch_spec: Callable[[str], str] = default_fetch_spec,
) -> dict[str, RebuildResult]:
    """`install_json_handler() -> RebuildConfig -> I-23 allowlist -> spec ->
    binding-defect assert -> session -> ledger record_run -> rebuild_
    pipeline` — pure composition, no `if`/`try` of its own (the SAME design
    law `glue_main.main`'s own docstring states for itself). No `--force`
    flag exists anywhere in this sequence, by construction (RB-2)."""
    observability.install_json_handler()
    config = from_args(argv)
    check_spec_uri_allowlist(
        config.pipeline_spec_uri, naming.slug(config.pipeline), config.artifacts_bucket
    )
    spec = parse_pipeline_spec_yaml(fetch_spec(config.pipeline_spec_uri))
    _assert_pipeline_matches(spec, config)
    spark = session.build_session(config, app_name=f"conveyer-spine-rebuild-{config.env}")
    session.assert_iceberg_extensions_active(spark)
    catalog_factory = functools.partial(ledger.build_catalog, config)
    record_run = ledger.build_record_run(catalog_factory, config)
    return rebuild_pipeline(spark, spec, record_run=record_run)
