"""Runtime config from CONVEYER_* env vars (effect-side only) -- LLD S7.2.

Not under `ingestion/core/` -- the purity linter's PURITY scope (banned
`os` import, banned `raise`) does not apply here; only the IDIOM rule
(class-shape) does, and `RuntimeConfig` is an allowed frozen dataclass.

`from_env()` fails loudly (raises) rather than defaulting silently on a
missing required var -- this is deliberately NOT `TransientError`:
`TransientError` (`effects/records.py`) is reserved for the `Callable`
fields effect factories build (S7.3, "raised by effect functions only");
config loading happens once at Lambda cold start, before any `Effects`
record exists, and `effects/records.py` importing this module (for
`RuntimeConfig`) forbids the reverse import. A plain `RuntimeError` is
loud enough: an uncaught exception at import/init time fails the
invocation and surfaces in logs/alarms same as any other bug (S7.3's
"anything else is a bug and follows the same path").
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass

_ENV_PREFIX = "CONVEYER_"


@dataclass(frozen=True)
class RuntimeConfig:
    env: str
    aws_region: str
    landing_bucket: str
    lake_bucket: str
    artifacts_bucket: str
    glue_database: str
    ledger_table: str  # table name only; db is `glue_database`
    cas_table: str
    event_bus: str
    registry_uri: str
    athena_workgroup: str  # maintenance only
    athena_output_uri: str  # maintenance only
    feed_id: str | None  # set only on per-feed driver functions


def _required(env: Mapping[str, str], name: str, missing: list[str]) -> str:
    value = env.get(f"{_ENV_PREFIX}{name}")
    if not value:
        missing.append(f"{_ENV_PREFIX}{name}")
        return ""
    return value


def from_env() -> RuntimeConfig:
    env = os.environ
    missing: list[str] = []

    config = RuntimeConfig(
        env=_required(env, "ENV", missing),
        aws_region=_required(env, "AWS_REGION", missing),
        landing_bucket=_required(env, "LANDING_BUCKET", missing),
        lake_bucket=_required(env, "LAKE_BUCKET", missing),
        artifacts_bucket=_required(env, "ARTIFACTS_BUCKET", missing),
        glue_database=_required(env, "GLUE_DATABASE", missing),
        ledger_table=_required(env, "LEDGER_TABLE", missing),
        cas_table=_required(env, "CAS_TABLE", missing),
        event_bus=_required(env, "EVENT_BUS", missing),
        registry_uri=_required(env, "REGISTRY_URI", missing),
        athena_workgroup=_required(env, "ATHENA_WORKGROUP", missing),
        athena_output_uri=_required(env, "ATHENA_OUTPUT_URI", missing),
        feed_id=env.get(f"{_ENV_PREFIX}FEED_ID") or None,
    )
    if missing:
        raise RuntimeError("missing required environment variable(s): " + ", ".join(missing))
    return config
