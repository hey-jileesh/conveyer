"""`apply` for the identity exemplar pipeline. LLD 006.1 §12.2/G-12, I-10.

**006.1 migration (bead conveyer-6pg.13, B3): `post_check` is GONE.** The
exemplar's clean half used to be a `post_check` that admitted every row
unconditionally (`.limit(0)` over the candidate's own columns); under the
framework's own interpreter (§7), "admit everything" is simply the absence
of any declared check for this fact type -- `tests/exemplar/identity/
pipeline.yaml`'s `checks:` section declares none for the `identity` type,
so `frames/business_checks.py` compiles just the one framework-reserved
implicit check (`business/missing-domain-id`) and nothing else. The
violations VARIANT (`pipelines.identity_violations`, R-04/G-12) now
declares its own rule in `checks.yaml` (`business/negative-amount`) instead
of a second Python `post_check` -- see that package's own module docstring.

**`apply`** returns the 006.1 §4.4 one-entry MAPPING `{"identity":
<candidate frame>}` (G-12: "identity exemplar: `apply` returns the one-entry
mapping") -- a column projection of `valid_df` (005.1 §6.2's
declared-columns-ONLY projection, post pre_check) down to the `identity`
fact type's own declared candidate columns, `domain_id`/`event_time`/
`payload` (`_FACT_COLUMNS`) — I-11: `domain_id_col` is the merge key.
`source_ts`/`content_hash` are DELIBERATELY NOT projected through as of
this exemplar's 006.1 migration (bead conveyer-6pg.13): both names are now
`core/record.py::FACT_STAMP_COLUMNS`, 007.1 §5.1 fragment 4's
framework-derived stamp set (F-1's own commit-time derivation; a
`FactSchemaModel` may not declare either as an authored column, F3) —
DIFFERENT from the raw admission grammar's reserved set, so the two raw
columns of the same NAME (`source_ts`, `content_hash`, upstream-supplied
provenance on the fixture) still admit cleanly; `apply` simply no longer
carries them into the candidate frame, leaving their derivation to 007.1's
own commit-side stamping (`frames/facts.py`, B9a/B9b's territory) — lineage
(`batch_id`, `delivery_id`, `feed_id`, `received_at`) was never `apply`'s
concern either, stamped after `post_check` by `frames.lineage.
stamp_fact_lineage`, for every pipeline uniformly (§7.5).

**Historical note (superseded by n3-admission-cut, bead conveyer-azr.19):**
prior to 005.1's real `pre_check`/quarantine rewrite, `valid_df` was an
unfiltered projection of `raw_df` (carrying lineage columns), and `apply`'s
byte-identical projection was load-bearing for a DIFFERENT reason — I-P2's
provisional pre_check wrote raw-shaped violations and post_check wrote
candidate-shaped violations into the SAME literal quarantine table, which
`DataFrameWriterV2.append()`'s exact-column-match requirement made only
possible if `apply` never reshaped a column. 005.1 §4.2's quarantine table
is now a FIXED, versionless, candidate-independent shape (`row_snapshot` is
a JSON blob), so that constraint no longer applies — `apply` stays a plain
projection here because the exemplar has no actual transformation to do,
not because a shared-table schema forces it to.

Both `event_time`/`source_ts` stay the reader's native `string` type (raw
columns are always string, D-5) rather than being cast to `timestamp`. This
still orders correctly: the fixture's fixed-width, zero-padded, UTC
(`Z`-suffixed) ISO-8601 strings sort identically under plain string
comparison and true chronological order, so the fold's native Spark
`struct(...) > struct(...)` ordering comparison (I-11) is exactly as correct
over these strings as it would be over parsed timestamps — a fixture-format
contract, not a general one. `content_hash` arrives as upstream-supplied
provenance data on the fixture (a realistic shape: some sources already
carry their own content hash) rather than being derived here.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import functions as F

if TYPE_CHECKING:
    from collections.abc import Mapping

    from pyspark.sql import DataFrame

# The `identity` fact type's own declared `FactSchemaModel.columns` (§4.1)
# -- ONE shape, authored once, in `tests/integration/scenario_helpers.py::
# IDENTITY_FACT_SCHEMA` (test-constructed specs) / `tests/exemplar/identity/
# pipeline.yaml` (the deployed-shape doc) and here, matching by construction
# (all three enumerate the same three names -- see the module docstring for
# why `source_ts`/`content_hash` are excluded).
_FACT_COLUMNS: tuple[str, ...] = (
    "domain_id",
    "event_time",
    "payload",
)

# 006.1 §4.4: the one declared fact type's own key in `apply`'s returned
# mapping -- matches `pipeline.yaml`'s `fact_types:` key exactly (S2's
# check-id-shaped grammar).
FACT_TYPE = "identity"


def apply(valid_df: DataFrame, co_effects: Mapping[str, DataFrame]) -> Mapping[str, DataFrame]:
    """Pure column projection into the one-entry candidate mapping (G-12):
    pins the exact candidate-row shape by name (no rename/cast/add/drop —
    see module docstring)."""
    del co_effects  # identity declares no co-effects
    return {FACT_TYPE: valid_df.select(*(F.col(name) for name in _FACT_COLUMNS))}
