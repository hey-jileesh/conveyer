"""Manifest/trailer/quiet-window completeness evaluation — LLD §7.3.

Value types (`ObjectStat`, `Defect`, `CompletenessResult`) plus the four
evaluator functions: `parse_manifest`, `evaluate_manifest`, `evaluate_trailer`,
`quiet_window_satisfied`. `core/model.py` deliberately does NOT import this
module (its docstring records why) so the import graph stays acyclic; this
module imports `ingestion.core.model` one-directionally for `ManifestV1` and
`TrailerSpec`.
"""

import re
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Literal

from pydantic import ValidationError

from ingestion.core.model import ManifestV1, TrailerSpec

_TRAILER_COUNT_RE = re.compile(r"[0-9]+")
_MANIFEST_DEFECT_MAX_VIOLATIONS = 5  # H-4 (security-gate): cap, not the raw error count


@dataclass(frozen=True)
class ObjectStat:  # what the effect side observed in the vestibule / remote dir
    name: str
    bytes: int
    sha256: str | None  # sha256 present once streamed


@dataclass(frozen=True)
class Defect:  # a defect is a VALUE (§7.0 rule 4), never an exception
    reason: str  # human-readable; becomes ledger `notes`


@dataclass(frozen=True)
class CompletenessResult:
    verdict: Literal["complete", "incomplete", "defective"]
    reason: str | None  # None iff complete
    asserted_record_count: int | None
    data_object_names: tuple[str, ...]  # the objects composing the delivery (excl. manifest)


def _manifest_validation_reason(exc: ValidationError) -> str:
    """(loc, type) pairs only, capped to the first `_MANIFEST_DEFECT_MAX_VIOLATIONS`
    plus a count of the rest -- H-4 (security-gate): `raw` is UNTRUSTED partner
    manifest bytes (up to 1000 declared files); pydantic's default `str(exc)`
    rendering embeds `input_value=...` verbatim for every violation, so a
    naive `Defect(reason=str(exc))` lets partner-controlled content (e.g. a
    PII-shaped filename) land permanently in `Defect.reason` -> ledger
    `notes` -> an append-only, never-deleted, Athena-queryable column,
    unbounded in length. Neither risk applies to `(loc, type)`: `loc` is a
    manifest-shape path (`files.0.sha256`, never file content) and `type`
    is pydantic's own closed error-code vocabulary (`int_parsing`,
    `value_error`, ...) -- never partner-supplied.
    """
    errors = exc.errors()
    parts = []
    for err in errors[:_MANIFEST_DEFECT_MAX_VIOLATIONS]:
        loc = ".".join(str(part) for part in err["loc"])
        parts.append(f"{loc}: {err['type']}" if loc else err["type"])
    reason = "; ".join(parts)
    remaining = len(errors) - _MANIFEST_DEFECT_MAX_VIOLATIONS
    if remaining > 0:
        reason += f" (+{remaining} more violation(s))"
    return reason


def parse_manifest(raw: bytes) -> ManifestV1 | Defect:
    """The ONE place a pydantic exception is caught-and-reified (§7.3) — the
    single linter-allowlisted `try` in `core/` (`tools/purity_linter.py`).
    Pure: bytes in, value out.
    """
    try:
        return ManifestV1.model_validate_json(raw)
    except ValidationError as exc:
        return Defect(reason=_manifest_validation_reason(exc))
    except UnicodeError:
        # No `.errors()` shape to draw (loc, type) pairs from, but the same
        # rule applies: never interpolate the exception (its message can
        # include a snippet of the offending bytes) into a `Defect.reason`
        # that becomes permanent ledger `notes`.
        return Defect(reason="manifest bytes are not valid UTF-8")


def evaluate_manifest(
    manifest: ManifestV1, present: Sequence[ObjectStat], feed_id: str
) -> CompletenessResult:
    names = tuple(f.name for f in manifest.files)
    asserted_record_count = (
        sum(f.record_count for f in manifest.files if f.record_count is not None)
        if all(f.record_count is not None for f in manifest.files)
        else None
    )

    if manifest.feed_id != feed_id:
        return CompletenessResult(
            verdict="defective",
            reason=f"manifest feed_id {manifest.feed_id!r} does not match expected {feed_id!r}",
            asserted_record_count=asserted_record_count,
            data_object_names=names,
        )

    seen: set[str] = set()
    duplicates: list[str] = []
    for name in names:
        if name in seen:
            duplicates.append(name)
        seen.add(name)
    if duplicates:
        return CompletenessResult(
            verdict="defective",
            reason=f"duplicate file name(s) in manifest: {', '.join(duplicates)}",
            asserted_record_count=asserted_record_count,
            data_object_names=names,
        )

    present_by_name = {stat.name: stat for stat in present}
    defective_reasons: list[str] = []
    incomplete_reasons: list[str] = []
    for f in manifest.files:
        stat = present_by_name.get(f.name)
        if stat is None:
            incomplete_reasons.append(f"{f.name} not present")
            continue
        if stat.bytes != f.bytes:
            incomplete_reasons.append(
                f"{f.name} byte-size mismatch (expected {f.bytes}, observed {stat.bytes})"
            )
            continue
        if stat.sha256 is not None and stat.sha256 != f.sha256:
            defective_reasons.append(
                f"{f.name} sha256 mismatch (expected {f.sha256}, observed {stat.sha256})"
            )

    if defective_reasons:
        return CompletenessResult(
            verdict="defective",
            reason="; ".join(defective_reasons),
            asserted_record_count=asserted_record_count,
            data_object_names=names,
        )
    if incomplete_reasons:
        return CompletenessResult(
            verdict="incomplete",
            reason="; ".join(incomplete_reasons),
            asserted_record_count=asserted_record_count,
            data_object_names=names,
        )
    return CompletenessResult(
        verdict="complete",
        reason=None,
        asserted_record_count=asserted_record_count,
        data_object_names=names,
    )


def evaluate_trailer(tail_text: str, spec: TrailerSpec) -> CompletenessResult:
    non_empty_lines = [line for line in tail_text.splitlines() if line.strip() != ""]
    if not non_empty_lines:
        return CompletenessResult(
            verdict="incomplete",
            reason="trailer missing or malformed",
            asserted_record_count=None,
            data_object_names=(),
        )

    match = re.fullmatch(spec.pattern, non_empty_lines[-1])
    if match is None:
        return CompletenessResult(
            verdict="incomplete",
            reason="trailer missing or malformed",
            asserted_record_count=None,
            data_object_names=(),
        )

    if spec.count_group is None:
        return CompletenessResult(
            verdict="complete",
            reason=None,
            asserted_record_count=None,
            data_object_names=(),
        )

    raw_count = match.groupdict().get(spec.count_group)
    if raw_count is None or not _TRAILER_COUNT_RE.fullmatch(raw_count):
        return CompletenessResult(
            verdict="defective",
            reason=f"trailer {spec.count_group} is not an integer: {raw_count!r}",
            asserted_record_count=None,
            data_object_names=(),
        )

    return CompletenessResult(
        verdict="complete",
        reason=None,
        asserted_record_count=int(raw_count),
        data_object_names=(),
    )


def quiet_window_satisfied(mtime: datetime, now: datetime, minutes: int) -> bool:
    return now - mtime >= timedelta(minutes=minutes)
