"""batch_id minting via uuid5 — LLD §6.6 (D-4).

`mint_batch_id` is deterministic content addressing for a delivery: the same
(feed_id, content_hash) pair always mints the same batch_id, so a rerun or a
crash-recovery replay reproduces the same identity rather than minting a new
one (idempotency, LLD D-4). `uuid.uuid5` is deterministic and is the one
`uuid` call the purity linter allows in `core/` (§12.2) — `uuid1`/`uuid4` are
banned because they are nondeterministic ("now" must always be a parameter).
"""

import uuid

# Fixed forever (LLD §6.6): this namespace UUID is part of the contract.
# Changing it re-mints every batch identity in the system — every delivery
# ever registered would compute a different batch_id on the next fold/replay.
# Do not change it; if the algorithm must change, that is a new function/name,
# not a mutation of this constant.
CONVEYER_INGESTION_NS = uuid.UUID("6ba7b811-9dad-11d1-80b4-00c04fd430c8")


def mint_batch_id(feed_id: str, content_hash: str) -> str:
    return str(uuid.uuid5(CONVEYER_INGESTION_NS, f"{feed_id}\n{content_hash}"))
