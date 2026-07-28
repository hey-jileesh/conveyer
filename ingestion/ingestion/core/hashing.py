"""Canonical content hash — LLD §6.5 (D-6).

`canonical_content_hash` is the normative algorithm: data objects only (the
manifest object is always excluded by the caller), each contributing a
sha256sum-style "<sha256_hex>  <name>" line (two spaces), lines sorted
bytewise, SHA-256 over the joined UTF-8 bytes, `sha256:` prefix. Independent
of received-at prefixes, upload order, and manifest regeneration; a
renamed-but-identical file is a *different* delivery (names participate
deliberately — the partner's naming is part of what was asserted).
"""

import hashlib
from collections.abc import Sequence


def canonical_content_hash(objects: Sequence[tuple[str, str]]) -> str:
    """objects: (name, sha256_hex) pairs for the delivery's data objects."""
    lines = [f"{sha256_hex}  {name}" for name, sha256_hex in objects]
    lines.sort()
    digest = hashlib.sha256("\n".join(lines).encode("utf-8")).hexdigest()
    return f"sha256:{digest}"
