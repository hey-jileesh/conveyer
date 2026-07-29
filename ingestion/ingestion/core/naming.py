"""Pure naming/URI helpers -- LLD §5, shared across drivers/absence/decisions.

Consolidates three previously-independent copies of the canonical-landing-key
convention (received-at formatting + canonical URI) and `s3://` URI parsing
that lived in `drivers/s3_push.py`, `drivers/sftp_pull.py`,
`absence/detector.py`, and `core/decisions.py::_parse_s3_uri` (critique-gate
finding F-2). Pure string/datetime manipulation only, no I/O -- lives in
`core/` precisely so `core/decisions.py` can use it without crossing the
core/effects boundary; the purity linter's PURITY scope (banned imports,
banned `raise`/`try`) applies here same as any other `ingestion/core/*.py`
module, and this module needs none of those constructs.
"""

from __future__ import annotations

import re
from datetime import datetime

# Producer-side companion to spine's `check_object_uris`/`_OBJECT_NAME_RE`
# (conveyer-nvh.46) -- this is ingestion's own copy, not a shared import (004
# D-13: no shared code between lanes). The canonical landing shape is prefix
# + exactly ONE object-name segment: non-empty, no further `/`/`\`
# separators (rules out multi-segment and traversal forms), and no `%` at
# all. Object names here have no legitimate need for percent-encoding, so
# rejecting `%` outright (rather than only decoding and re-checking
# `%2e`/`%2f`/`%5c`) is the conservative choice: it closes the
# encoded-separator/dot-segment class of bypass without having to reason
# about decode order or double encoding (conveyer-nvh.46/nvh.48).
_OBJECT_NAME_RE = re.compile(r"^[^/\\%]+$")


def is_clean_object_name(name: str) -> bool:
    """True iff `name` is a single clean object-name segment: not `""`, not
    `"."`/`".."`, and matching `_OBJECT_NAME_RE` (no `/`, `\\`, `%`). Pure,
    raise-free -- the ONE authored grammar a caller composing a canonical
    URI from a partner/server-supplied name must check first (conveyer
    nvh.46's ingestion-side companion, nvh.48).

    `.fullmatch`, not `.match`: the general convention this module and
    spine's `core/naming.py` both follow for every `^...$`-anchored check
    (`.match()` only guarantees a matching PREFIX, `.fullmatch()` the whole
    string) -- verified in the kernel that for THIS specific character
    class the two happen to agree on every input (the class excludes only
    `/`, `\\`, `%`, so it never stops short of a trailing "\\n" the way a
    narrower class like an identifier grammar would), but `.fullmatch` is
    still the correct default so this doesn't silently regress if the
    grammar is ever tightened.
    """
    if name in ("", ".", ".."):
        return False
    return _OBJECT_NAME_RE.fullmatch(name) is not None


def format_received_at(ts: datetime) -> str:
    """§5: UTC, microseconds, basic ISO8601 (no dashes/colons)."""
    return ts.strftime("%Y%m%dT%H%M%S") + f"{ts.microsecond:06d}Z"


def canonical_prefix(feed_id: str, received_at: datetime, delivery_id: str) -> str:
    """§5 canonical landing prefix (no bucket, no object name):
    `<source>/<feed>/received_at=<ts>/dl-<delivery_id>/`.
    """
    ts = format_received_at(received_at)
    return f"{feed_id}/received_at={ts}/dl-{delivery_id}/"


def canonical_uri(
    landing_bucket: str, feed_id: str, received_at: datetime, delivery_id: str, name: str
) -> str:
    """§5 canonical landing key: `<source>/<feed>/received_at=<ts>/dl-<delivery_id>/<name>`."""
    return f"s3://{landing_bucket}/{canonical_prefix(feed_id, received_at, delivery_id)}{name}"


def split_s3_uri(uri: str) -> tuple[str, str]:
    """`s3://bucket/key...` -> (bucket, key)."""
    without_scheme = uri.removeprefix("s3://")
    bucket, _, key = without_scheme.partition("/")
    return bucket, key
