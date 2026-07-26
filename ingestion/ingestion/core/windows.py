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

    1. keep `fnmatch(name, file_pattern)`
    2. unless `force`: drop `(name, bytes)` already in `acquired | defective`
       (re-listing an already-acquired-or-defective file is a no-op, D-16)
    3. `mode == "timer"`: keep only `quiet_window_satisfied(mtime, now, minutes)`
    4. sort by `(mtime, name)` ascending — oldest first, deterministic

    `force=True` bypasses ONLY step 2 — bytes are re-streamed and the
    turnstile dedups them into explicit `duplicate` rows (D-16); the
    pattern filter (step 1) and the timer quiet-window filter (step 3)
    still apply.
    """
    candidates = [f for f in listing if fnmatch.fnmatch(f.name, file_pattern)]
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
    in `acquired` (unless `force`), oldest first (LLD §7.4). Non-final
    (incomplete/unreadable) manifests are never added to `acquired`
    (§6.2/§7.4's `acquired_final`), so they are re-examined every run by
    design — the bounded (≤1 MiB) re-read is the accepted cost of
    eventually catching late-arriving parts.
    """
    candidates = [f for f in listing if fnmatch.fnmatch(f.name, manifest_pattern)]
    if not force:
        candidates = [f for f in candidates if (f.name, f.bytes) not in acquired]
    candidates.sort(key=lambda f: (f.mtime, f.name))
    return candidates
