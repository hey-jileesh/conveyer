"""Remote candidate selection for pull drivers — LLD §7.4.

Trailer/timer-mode selection only; manifest-mode selection is manifest-first
(§9.2) and lives in `select_manifests` below. All functions are pure: plain
values in, plain values out, no I/O, `now` is always a parameter.
"""

import fnmatch
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime

from ingestion.core.completeness import quiet_window_satisfied
from ingestion.core.model import Completeness
from ingestion.core.naming import is_clean_object_name


@dataclass(frozen=True)
class RemoteFile:
    name: str
    bytes: int
    mtime: datetime


def select_candidates(
    listing: Sequence[RemoteFile],
    acquired: frozenset[tuple[str, int]],
    defective: frozenset[tuple[str, int]],
    file_pattern: str,
    completeness: Completeness,
    now: datetime,
    force: bool = False,
) -> list[RemoteFile]:
    """Trailer/timer-mode candidate selection (LLD §7.4):

    0. keep only `is_clean_object_name(name)` -- a single clean object-name
       segment (conveyer-nvh.46/nvh.48's producer-side companion guard).
       `force` does NOT bypass this step: `force` is documented (below) as
       bypassing ONLY step 2 (the acquired/defective drop), never this one.
       A dropped hostile entry gets NO ledger row at all, not even a
       `record_nondelivery` -- a nondelivery row would put the hostile
       string into the append-only `delivery_key` column, and the remote
       server is untrusted here (unlike a manifest-declared name, which is
       at least schema-validated first): admitting it would let a hostile
       or compromised SFTP server spam an unbounded number of
       attacker-controlled rows into the ledger merely by listing
       differently-named garbage, with no partner-side cost.
    1. keep `fnmatch(name, file_pattern)`
    2. unless `force`: drop `(name, bytes)` already in `acquired | defective`
       (re-listing an already-acquired-or-defective file is a no-op, D-16)
    3. `mode == "timer"`: keep only `quiet_window_satisfied(mtime, now, minutes)`
    4. sort by `(mtime, name)` ascending — oldest first, deterministic

    `force=True` bypasses ONLY step 2 — bytes are re-streamed and the
    turnstile dedups them into explicit `duplicate` rows (D-16); the
    cleanliness filter (step 0), the pattern filter (step 1), and the timer
    quiet-window filter (step 3) still apply.
    """
    candidates = [f for f in listing if is_clean_object_name(f.name)]
    candidates = [f for f in candidates if fnmatch.fnmatch(f.name, file_pattern)]
    if not force:
        skip = acquired | defective
        candidates = [f for f in candidates if (f.name, f.bytes) not in skip]
    if completeness.mode == "timer":
        minutes = completeness.timer.quiet_window_minutes if completeness.timer else 0
        candidates = [f for f in candidates if quiet_window_satisfied(f.mtime, now, minutes)]
    candidates.sort(key=lambda f: (f.mtime, f.name))
    return candidates


def select_manifests(
    listing: Sequence[RemoteFile],
    acquired: frozenset[tuple[str, int]],
    manifest_pattern: str,
    force: bool = False,
) -> list[RemoteFile]:
    """Manifests matching `manifest_pattern`, minus `(name, bytes)` already
    in `acquired` (unless `force`), oldest first (LLD §7.4):

    0. keep only `is_clean_object_name(name)` -- same guard and rationale as
       `select_candidates` step 0 above: NOT bypassable by `force`, and a
       dropped hostile manifest OBJECT name (the listing entry itself, not
       yet anything parsed from inside it) gets NO ledger row, for the same
       reason -- an untrusted remote listing must never be able to spam the
       append-only ledger with attacker-chosen `delivery_key` values.
    1. keep `fnmatch(name, manifest_pattern)`
    2. unless `force`: drop `(name, bytes)` already in `acquired`

    Non-final (incomplete/unreadable) manifests are never added to
    `acquired` (§6.2/§7.4's `acquired_final`), so they are re-examined every
    run by design — the bounded (≤1 MiB) re-read is the accepted cost of
    eventually catching late-arriving parts.
    """
    candidates = [f for f in listing if is_clean_object_name(f.name)]
    candidates = [f for f in candidates if fnmatch.fnmatch(f.name, manifest_pattern)]
    if not force:
        candidates = [f for f in candidates if (f.name, f.bytes) not in acquired]
    candidates.sort(key=lambda f: (f.mtime, f.name))
    return candidates
