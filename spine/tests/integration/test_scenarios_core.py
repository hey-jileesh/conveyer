"""Standing scenario suite (M3 gate) — R-01, R-02, R-04, R-09. LLD §12.4, §7.5,
I-7, I-12, I-19, [H-1][E-1].

Drives `spine.run.run(seed, fx)` end-to-end over `local_runner_fx` (the real
production assembly, §12.1) with NO explicit `stages` argument, so this suite
is also `spine/stages/__init__.py::SEQUENCE`'s own wiring test — the
entrypoint (M5) is bypassed entirely: the seed `BatchContext` is built
directly, and `transforms` is bound via the real `spine.binding.
bind_transforms` against the real `pipelines.identity`/
`pipelines.identity_violations` modules (not an inline test double), matching
the bead's own ask.

Table shapes are pinned once here (`_create_raw_table` et al.) rather than
re-derived per test: `identity`'s `apply` is a byte-identical column
projection (see `pipelines/identity/transforms.py`'s own docstring for why),
so raw/candidate/admitted/fact/state all share the SAME 9-column shape
(`domain_id, event_time, source_ts, content_hash, payload, batch_id,
delivery_id, feed_id, received_at`); the quarantine table adds `reason` +
`check_stage` on top (11 columns) — the one table two stages (`pre_check`/
`post_check`) share, disambiguated by `check_stage` (§7.5's two-writer note).
The state table additionally carries `write.merge.mode = merge-on-read`
(the fold no-op detection precondition, `effects/spark.py`'s own documented
empirical finding).

Goldens are reconstructed in-test from literals (sorted tuples of the columns
under test), not stored parquet: the fixture data is tiny and fully spelled
out just above each test, so a literal expected-rows tuple is both the
easier-to-read AND the easier-to-maintain form here — a schema/column-order
change in a stored parquet golden would be a silent, unreviewable diff,
whereas a literal tuple change shows up in the PR text itself.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING

import yaml
from pyspark.sql import SparkSession
from scenario_helpers import (
    EXEMPLAR_DIR as _EXEMPLAR_DIR,
)
from scenario_helpers import (
    FIXTURES_DIR as _FIXTURES_DIR,
)
from scenario_helpers import (
    bare as _bare,
)
from scenario_helpers import (
    batch_id as _batch_id,
)
from scenario_helpers import (
    create_fact_table as _create_fact_table,
)
from scenario_helpers import (
    create_quarantine_table as _create_quarantine_table,
)
from scenario_helpers import (
    create_raw_table as _create_raw_table,
)
from scenario_helpers import (
    create_state_table as _create_state_table,
)
from scenario_helpers import (
    make_seed as _make_seed,
)
from scenario_helpers import (
    make_spec as _make_spec,
)
from scenario_helpers import (
    quarantine_rows as _quarantine_rows,
)
from snapshot_asserts import assert_no_new_snapshot, snapshot_ids
from spine.core.model import PipelineSpecModel
from spine.effects.records import RunnerFx
from spine.run import run as run_sequence

if TYPE_CHECKING:
    from tests.conftest import MotoEventsBus


# --- a test asserts the deployed-shape yaml parses into PipelineSpecModel ---


def test_pipeline_yaml_parses_into_pipeline_spec_model() -> None:
    data = yaml.safe_load((_EXEMPLAR_DIR / "pipeline.yaml").read_text())
    spec = PipelineSpecModel(**data)
    assert spec.pipeline == "pipelines/identity"
    assert spec.transforms_module == "pipelines.identity.transforms"
    assert spec.raw_table == "conveyer_dev_lake.identity__raw"
    assert spec.quarantine_table == "conveyer_dev_lake.identity__quarantine"
    assert spec.fact_table == "conveyer_dev_lake.identity__facts"
    assert spec.state_table == "conveyer_dev_lake.identity__state"
    assert spec.required_columns == ["domain_id"]
    assert spec.fold == "default-lww"


# --- R-01: fixture delivery -> run; goldens; both events on the bus --------


def test_r01_identity_e2e_matches_goldens_and_emits_both_events(
    spark: SparkSession,
    local_runner_fx: RunnerFx,
    unique_table: Callable[[str], str],
    moto_events_bus: MotoEventsBus,
) -> None:
    raw_qt = unique_table("r01_raw")
    qtn_qt = unique_table("r01_qtn")
    fact_qt = unique_table("r01_fact")
    state_qt = unique_table("r01_state")
    _create_raw_table(spark, raw_qt)
    _create_quarantine_table(spark, qtn_qt)
    _create_fact_table(spark, fact_qt)
    _create_state_table(spark, state_qt)
    spec = _make_spec(
        transforms_module="pipelines.identity.transforms",
        raw_table=_bare(raw_qt),
        quarantine_table=_bare(qtn_qt),
        fact_table=_bare(fact_qt),
        state_table=_bare(state_qt),
    )
    batch_id = _batch_id(101)
    object_uris = (
        str(_FIXTURES_DIR / "clean" / "object_1.csv"),
        str(_FIXTURES_DIR / "clean" / "object_2.csv"),
    )
    seed = _make_seed(spec=spec, batch_id=batch_id, object_uris=object_uris)

    result = run_sequence(seed, local_runner_fx)

    assert result.raw_count == 3
    assert result.pre_quarantined_count == 0
    assert result.post_quarantined_count == 0
    assert result.facts_appended == 3
    assert result.guard_skips == ()
    assert result.published is True

    fact_golden = [
        ("id-001", "alpha"),
        ("id-002", "bravo"),
        ("id-003", "charlie"),
    ]
    fact_rows = sorted((r["domain_id"], r["payload"]) for r in spark.table(fact_qt).collect())
    assert fact_rows == fact_golden

    state_rows = sorted((r["domain_id"], r["payload"]) for r in spark.table(state_qt).collect())
    assert state_rows == fact_golden  # distinct domain_ids -- one winner each, all 3 kept

    envelopes = [e for e in moto_events_bus.read_events() if e["detail"]["batch_id"] == batch_id]
    assert len(envelopes) == 2
    by_type = {e["detail-type"]: e["detail"] for e in envelopes}
    assert set(by_type) == {"batch-started", "batch-completed"}
    started = by_type["batch-started"]
    assert started["raw_count"] == 3
    assert started["land_snapshot_id"] == result.land_snapshot_id
    completed = by_type["batch-completed"]
    assert completed["raw_count"] == 3
    assert completed["pre_quarantined"] == 0
    assert completed["post_quarantined"] == 0
    assert completed["fact_count"] == 3
    assert completed["fact_snapshot_id"] == result.fact_snapshot_id
    assert completed["state_snapshot_id"] == result.state_snapshot_id
    assert completed["state_snapshot_id"] is not None


# --- R-02: rerun same seed, fresh context -----------------------------------


def test_r02_rerun_same_batch_id_zero_new_rows_and_guard_skips(
    spark: SparkSession,
    local_runner_fx: RunnerFx,
    unique_table: Callable[[str], str],
    moto_events_bus: MotoEventsBus,
) -> None:
    raw_qt = unique_table("r02_raw")
    qtn_qt = unique_table("r02_qtn")
    fact_qt = unique_table("r02_fact")
    state_qt = unique_table("r02_state")
    _create_raw_table(spark, raw_qt)
    _create_quarantine_table(spark, qtn_qt)
    _create_fact_table(spark, fact_qt)
    _create_state_table(spark, state_qt)
    spec = _make_spec(
        transforms_module="pipelines.identity.transforms",
        raw_table=_bare(raw_qt),
        quarantine_table=_bare(qtn_qt),
        fact_table=_bare(fact_qt),
        state_table=_bare(state_qt),
    )
    batch_id = _batch_id(102)
    object_uris = (
        str(_FIXTURES_DIR / "clean" / "object_1.csv"),
        str(_FIXTURES_DIR / "clean" / "object_2.csv"),
    )

    first_seed = _make_seed(spec=spec, batch_id=batch_id, object_uris=object_uris)
    first = run_sequence(first_seed, local_runner_fx)
    moto_events_bus.read_events()  # drain the first attempt's events

    raw_before = snapshot_ids(spark, raw_qt)
    qtn_before = snapshot_ids(spark, qtn_qt)
    fact_before = snapshot_ids(spark, fact_qt)

    # A FRESH seed context, same batch_id -- R-02's own precondition (a real
    # rerun never reuses the first attempt's in-memory BatchContext).
    second_seed = _make_seed(spec=spec, batch_id=batch_id, object_uris=object_uris)
    second = run_sequence(second_seed, local_runner_fx)

    assert_no_new_snapshot(spark, raw_qt, raw_before)
    assert_no_new_snapshot(spark, qtn_qt, qtn_before)  # no violations either attempt
    assert_no_new_snapshot(spark, fact_qt, fact_before)
    # NOT a snapshot_ids equality check on state_qt: a healthy-rerun MERGE
    # still commits a harmless PHYSICAL no-op snapshot (effects/spark.py's
    # own documented empirical finding; see test_stages_post_commit_fold_
    # publish.py's identical carve-out) -- state_snapshot_id is the LOGICAL
    # no-op signal this test cares about, asserted below.

    assert second.guard_skips == ("land", "commit")  # pre_check/post_check: zero-violation
    # branch every attempt (never a guard-skip, §7.5) -- fold has no presence
    # guard at all (idempotence is the fold's own contract).
    assert second.raw_count == first.raw_count == 3
    assert second.facts_appended == 0  # this attempt's own delta -- the ledger signature
    assert second.fact_snapshot_id == first.fact_snapshot_id  # I-19 own-commit resolution
    assert second.land_snapshot_id == first.land_snapshot_id
    # Known erratum (LLD R-02 vs. [C-7][T-8], recorded in the handoff report):
    # a fold no-op rerun's state_snapshot_id is None even though the ORIGINAL
    # attempt's fold produced a real snapshot id -- MERGE commits carry no
    # conveyer.batch-id/stage stamp (only `append` stamps), so there is no
    # channel to recover the original id on this path. Do not assert equality
    # here; assert the documented None explicitly instead.
    assert first.state_snapshot_id is not None
    assert second.state_snapshot_id is None

    envelopes = [e for e in moto_events_bus.read_events() if e["detail"]["batch_id"] == batch_id]
    assert len(envelopes) == 2  # unconditional re-emit, I-7 -- guard-skip never skips events
    by_type = {e["detail-type"]: e["detail"] for e in envelopes}
    completed = by_type["batch-completed"]
    first_completed = first.completed_event
    assert completed["raw_count"] == first_completed.raw_count
    assert completed["pre_quarantined"] == first_completed.pre_quarantined
    assert completed["post_quarantined"] == first_completed.post_quarantined
    assert completed["fact_count"] == first_completed.fact_count
    assert completed["fact_snapshot_id"] == first_completed.fact_snapshot_id
    # state_snapshot_id excluded from the field-equality claim -- see the
    # erratum note above; R-09 pins this same None for the empty-batch case.
    assert completed["state_snapshot_id"] is None
    assert first_completed.state_snapshot_id is not None


# --- R-04: quarantine never drops --------------------------------------------


def test_r04_violations_variant_quarantine_never_drops(
    spark: SparkSession,
    local_runner_fx: RunnerFx,
    unique_table: Callable[[str], str],
) -> None:
    raw_qt = unique_table("r04_raw")
    qtn_qt = unique_table("r04_qtn")
    fact_qt = unique_table("r04_fact")
    state_qt = unique_table("r04_state")
    _create_raw_table(spark, raw_qt)
    _create_quarantine_table(spark, qtn_qt)
    _create_fact_table(spark, fact_qt)
    _create_state_table(spark, state_qt)
    spec = _make_spec(
        transforms_module="pipelines.identity_violations.transforms",
        raw_table=_bare(raw_qt),
        quarantine_table=_bare(qtn_qt),
        fact_table=_bare(fact_qt),
        state_table=_bare(state_qt),
    )
    batch_id = _batch_id(104)
    object_uris = (str(_FIXTURES_DIR / "violations" / "object_1.csv"),)
    seed = _make_seed(spec=spec, batch_id=batch_id, object_uris=object_uris)

    result = run_sequence(seed, local_runner_fx)

    assert result.raw_count == 4
    assert result.pre_quarantined_count == 1  # the null-domain_id row
    valid_count = result.raw_count - result.pre_quarantined_count
    assert valid_count == 3
    assert result.candidate_facts_df.count() == valid_count  # apply is a pure projection
    assert result.post_quarantined_count == 1  # the payload == "INVALID" row
    admitted_count = result.candidate_facts_df.count() - result.post_quarantined_count
    assert admitted_count == 2
    assert result.facts_appended == 2

    # I-12's own count identities, both stages:
    assert result.raw_count == valid_count + result.pre_quarantined_count
    assert result.candidate_facts_df.count() == admitted_count + result.post_quarantined_count

    pre_rows = _quarantine_rows(spark, qtn_qt, batch_id, "pre_check")
    assert len(pre_rows) == 1
    assert pre_rows[0]["domain_id"] is None
    assert pre_rows[0]["reason"]  # non-empty, present

    post_rows = _quarantine_rows(spark, qtn_qt, batch_id, "post_check")
    assert len(post_rows) == 1
    assert post_rows[0]["payload"] == "INVALID"
    assert post_rows[0]["reason"]  # non-empty, present

    fact_rows = sorted((r["domain_id"], r["payload"]) for r in spark.table(fact_qt).collect())
    assert fact_rows == [("id-101", "delta"), ("id-103", "foxtrot")]


# --- R-09: empty/all-quarantined batch completes, fold skipped --------------


def test_r09_all_quarantined_batch_completes_with_zero_counts(
    spark: SparkSession,
    local_runner_fx: RunnerFx,
    unique_table: Callable[[str], str],
    moto_events_bus: MotoEventsBus,
) -> None:
    raw_qt = unique_table("r09_raw")
    qtn_qt = unique_table("r09_qtn")
    fact_qt = unique_table("r09_fact")
    state_qt = unique_table("r09_state")
    _create_raw_table(spark, raw_qt)
    _create_quarantine_table(spark, qtn_qt)
    _create_fact_table(spark, fact_qt)
    _create_state_table(spark, state_qt)
    spec = _make_spec(
        transforms_module="pipelines.identity.transforms",
        raw_table=_bare(raw_qt),
        quarantine_table=_bare(qtn_qt),
        fact_table=_bare(fact_qt),
        state_table=_bare(state_qt),
    )
    batch_id = _batch_id(109)
    object_uris = (str(_FIXTURES_DIR / "all_quarantined" / "object_1.csv"),)
    seed = _make_seed(spec=spec, batch_id=batch_id, object_uris=object_uris)
    state_before = snapshot_ids(spark, state_qt)

    result = run_sequence(seed, local_runner_fx)

    assert result.raw_count == 2
    assert result.pre_quarantined_count == 2  # every row -- all-quarantined at pre_check
    assert result.candidate_facts_df.count() == 0
    assert result.post_quarantined_count == 0  # nothing reached post_check
    assert result.facts_appended == 0
    assert result.committed_facts_df.count() == 0

    # fold skipped entirely (§7.5: "Empty committed_facts_df => skip the
    # merge entirely") -- no fx.merge call at all, so a bare snapshot-log
    # equality check IS valid here (unlike R-02's fold no-op case, which
    # makes a real fx.merge call that leaves a harmless physical snapshot).
    assert_no_new_snapshot(spark, state_qt, state_before)
    assert result.state_snapshot_id is None
    assert result.state_read_snapshot_id is None
    assert result.merge_summary is None
    assert result.published is True

    envelopes = [e for e in moto_events_bus.read_events() if e["detail"]["batch_id"] == batch_id]
    completed = next(e["detail"] for e in envelopes if e["detail-type"] == "batch-completed")
    assert completed["raw_count"] == 2
    assert completed["pre_quarantined"] == 2
    assert completed["post_quarantined"] == 0
    assert completed["fact_count"] == 0
    assert completed["state_snapshot_id"] is None
