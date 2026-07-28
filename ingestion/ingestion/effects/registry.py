"""feeds.json registry loader -- LLD §6.8.

Single cached implementation, consolidating what were three near-identical
copies previously duplicated across `drivers/s3_push.py`,
`drivers/sftp_pull.py`, and `absence/detector.py` (critique-gate finding
F-2). Effect-side (real I/O via `fx.store.get_bytes`) -- outside the purity
linter's PURITY scope (`ingestion/core/**` + `sources/**` only).

`cache` is an explicit dict PARAMETER threaded by every caller, not a bare
module global: each Lambda function (registrar, a per-feed sftp-pull
driver, the absence sweep) is a separate process with its own imported
module state, so each caller owning its own container-lifetime default dict
is both harmless and simpler than sharing one across unrelated call sites --
see [[m3-registration-s3push-design-notes]] for why an `id(fx)`-keyed cache
was rejected in favor of this explicit-dict shape in the first place; golden
tests pass a fresh `{}` per test for isolation, same convention.
"""

from __future__ import annotations

import json

from ingestion.core.completeness import Defect
from ingestion.core.model import FeedConfig
from ingestion.core.naming import split_s3_uri
from ingestion.effects.records import Effects

RegistryCache = dict[str, tuple[float, dict[str, FeedConfig]]]

_REGISTRY_MAX_BYTES = 1024 * 1024  # §6.8 registry object cap
_REGISTRY_TTL_SECONDS = 60.0  # §6.8: "cached per Lambda container for <= 60 s"


def load_feed_registry(fx: Effects, cache: RegistryCache) -> dict[str, FeedConfig]:
    """Read+parse `feeds.json` via `fx.store.get_bytes` -- never the
    filesystem -- cached in `cache` for `_REGISTRY_TTL_SECONDS`, keyed by
    `registry_uri`.
    """
    registry_uri = fx.config.registry_uri
    now_ts = fx.now().timestamp()
    cached = cache.get(registry_uri)
    if cached is not None:
        fetched_at, feeds = cached
        if now_ts - fetched_at < _REGISTRY_TTL_SECONDS:
            return feeds
    bucket, key = split_s3_uri(registry_uri)
    raw = fx.store.get_bytes(bucket, key, _REGISTRY_MAX_BYTES)
    if isinstance(raw, Defect):
        raise ValueError(f"feed registry unreadable at {registry_uri}: {raw.reason}")
    payload = json.loads(raw)
    feeds = {entry["feed_id"]: FeedConfig.model_validate(entry) for entry in payload["feeds"]}
    cache[registry_uri] = (now_ts, feeds)
    return feeds
