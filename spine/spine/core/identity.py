"""`derive_record_key` — the one shared `record_key` derivation function.
LLD 007.1 F-2 §5.2 (mechanics), §5.3 (the committed vector surface).

**One shared function, two call sites, never a second implementation (006
D-6's law, cited by 007.1 §5.2).** Both call sites wrap this function:
`frames/facts.py`'s commit-side UDF (007.1 F-1 §5.1's completion block —
totality, no gate) and `frames/quarantine.py::shape_post_quarantine`'s
post_check writer (006.1 §10 — derives iff every declared key column is
non-null, else `record_key` stays NULL). **The gate is call-site policy;
this function never gates** — it is total over complete maps (§5.2's own
text): a caller decides WHETHER to call it and with what, this function
never refuses a `None`-bearing map.

Plain-value pure, stdlib + `core.canonical` only — no Spark, no pydantic.
Column *names* participate in identity (the hashed object is the map, not a
positional value tuple, 007.1 §5.2): a declared key-column rename changes
every derived `record_key`, correctly, since a rename is a breaking
declaration change and, under additive-only schema evolution, implies a new
table. Declared order is irrelevant — canonical rendering sorts keys
bytewise (`core.canonical.canonical_json`'s own rule), so reordering a
`record_key:` declaration never changes derived keys (pinned by
`contracts/fixtures/record-key/basic.json`).

Implementation note: this *is* `core.canonical.row_hash` applied to its
third subject (007.1 §5.2's own text) — declared as its own named function
because the seam is the name + subject contract 006 D-6 attaches to, not
because the derivation differs from `row_hash` in any way.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import date, datetime
from decimal import Decimal

from spine.core import canonical

# 007.1 §5.2's input domain: every declared `record_key:` column name ->
# its typed value, or the value's canonical-string pre-rendering (007.1 §5.1
# fragment 2 -- the [DC-3] in-plan Spark rendering rule for timestamp key
# material, applied by the caller before this function ever sees the map).
KeyValue = str | int | Decimal | date | datetime | bool | None


def derive_record_key(key_values: Mapping[str, KeyValue]) -> str:
    """`sha256(canonical_json(key_values)).hexdigest()` — 64 lowercase hex.
    Total over complete maps (§5.2): `None` renders as canonical `null`, a
    value, not a refusal. `key_values` is expected to already carry every
    declared `record_key:` column name (a caller bug otherwise, per §5.2 --
    this function does not itself know a fact type's declaration, so it
    cannot check completeness; K-01 pins reproduction of every committed
    vector, which is this function's real contract)."""
    return canonical.row_hash(dict(key_values))
