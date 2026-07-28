"""Golden test: supersession reconciliation end-to-end -- LLD §9.4 step 3.

Seeds two live `registered` deliveries sharing one `delivery_key` directly
into the REAL local ledger (`local_effects` -- moto + `SqlCatalog`, the same
fixture every other golden test in this suite uses, `tests/conftest.py`),
then runs step 3 with "the query replaced by the local fold" (brief's
literal instruction -- Athena has no moto coverage, §12.5, see
`maintenance/optimize.py`'s module docstring):
`maintenance.optimize.live_duplicates_from_rows` is the SAME pure grouping
function the production Athena path also feeds into
`reconcile_supersessions`, applied here directly to a real
`fx.ledger.scan_feed(...)` result instead of to Athena-query rows.

Asserts the older delivery gets a `superseded` accretion row (exactly one
`fx.ledger.append`, per §9.4), and that reconciling AGAIN from the ledger's
now-current content is a no-op -- append-on-change idempotency (§9.4:
"Deterministic, idempotent").

No golden ID is assigned to this scenario in LLD §12.4's table (that table
enumerates driver/registration scenarios only, §12.4's own scope); this
module documents its coverage against §9.4's normative text directly rather
than inventing an unassigned ID.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from ingestion.core import decisions
from ingestion.core.model import DeliveryRecord
from ingestion.effects.records import Effects
from ingestion.maintenance import optimize

_FEED_ID = "carrier-y/renewal-statements"
_DELIVERY_KEY = "renewal-2026-07-24.manifest.json"
NOW = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)


def _registered_row(
    delivery_id: str, received_at: datetime, content_hash_digit: str
) -> DeliveryRecord:
    return decisions._build_row(
        delivery_id=delivery_id,
        feed_id=_FEED_ID,
        delivery_key=_DELIVERY_KEY,
        received_at=received_at,
        recorded_at=received_at,
        driver="s3-push",
        driver_run_id="run0",
        completeness_mode="manifest",
        asserted_record_count=None,
        disposition="registered",
        supersedes=None,
        content_hash="sha256:" + content_hash_digit * 64,
        batch_id=f"batch-{delivery_id}",
        size_bytes=1,
        objects=[],
        object_uris=[],
        manifest_ref=None,
    )


def test_reconciliation_appends_accretion_row_once_then_second_run_is_a_no_op(
    local_effects: Effects, clock_box: list[datetime]
) -> None:
    fx = local_effects
    clock_box[0] = NOW

    older = _registered_row("d-older", NOW - timedelta(hours=2), "1")
    newer = _registered_row("d-newer", NOW - timedelta(hours=1), "2")
    fx.ledger.append([older, newer])

    live_duplicates = optimize.live_duplicates_from_rows(fx.ledger.scan_feed(_FEED_ID, None))
    assert len(live_duplicates) == 1
    (records,) = live_duplicates.values()
    assert {r.delivery_id for r in records} == {"d-older", "d-newer"}

    result = optimize.reconcile_supersessions(fx, live_duplicates, fx.now())

    assert len(result) == 1
    accretion = result[0]
    assert accretion.delivery_id == "d-older"
    assert accretion.disposition == "superseded"
    assert accretion.recorded_at == NOW
    assert accretion.delivery_key == _DELIVERY_KEY  # identity columns copied verbatim (§6.2)

    all_rows = fx.ledger.scan_feed(_FEED_ID, None)
    assert len(all_rows) == 3  # 2 seeded registered rows + 1 accretion row

    # Second run: fold the ledger's NOW-current content again -- the
    # reconciled delivery_key no longer has more than one live registered
    # row, so the query (here, the local fold) reports nothing and the
    # append is a genuine no-op (idempotent by construction, §9.4).
    live_duplicates_2 = optimize.live_duplicates_from_rows(fx.ledger.scan_feed(_FEED_ID, None))
    assert live_duplicates_2 == {}

    result_2 = optimize.reconcile_supersessions(fx, live_duplicates_2, fx.now())
    assert result_2 == ()
    assert len(fx.ledger.scan_feed(_FEED_ID, None)) == 3  # unchanged
