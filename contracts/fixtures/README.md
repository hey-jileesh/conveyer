# `contracts/fixtures/` — committed vector families

This directory holds the **committed vector fixtures** that pin conveyer's
pure-derivation functions (canonical JSON, `record_key`, `content_hash`,
check-verdict evaluation) and the event-contract examples used across
lanes. Vectors here are a **normative surface**: on commit, a family's own
`*.json` files — not this prose — become the ground truth its consumer
tests must reproduce-or-fail against (005.1 §15.2, 007.1 §5.4's "designed
under 005.1's tagged-JSON convention").

This file is the **fixture-review checklist** for anyone adding, editing,
or regenerating a family here. Read it before touching a file in this
directory or reviewing a PR that does.

## The tagged-JSON convention (005.1 §15.2)

Fixture values ride plain JSON wherever JSON already has a native
representation (string, int, bool, null, array, object). For the three
types plain JSON does not represent exactly — `Decimal`, `date`,
`timestamp` — a value is a single-key wrapper object instead:

```json
{"$decimal": "10.50"}
{"$date": "2026-01-02"}
{"$timestamp": "2026-01-02T03:04:05+09:00"}
```

Some families extend this locally for a typed NULL — `{"$decimal": null}`
— where a bare JSON `null` would lose which column FAMILY the null belongs
to (see `tests/frames/test_check_verdicts.py`'s own docstring for the
worked case). Extending the convention per-family this way is licensed by
the rule immediately below; it must never leak into a *shared* parser.

## Checklist

- [ ] **Synthetic-only [DS-5].** Every value in every vector is
  **fabricated**, never derived from a real partner delivery, a real lake
  row, or any other production data. "A real row that failed" is never a
  test vector — committing real partner/member key material (the values
  *and* their derived hashes) into a git repository gives it a far broader
  audience than the lake's own IAM posture permits (007.1 §5.3, `[DS-5]`,
  verified synthetic at the `conveyer-hpp.6` security gate for all 25
  originally-committed `record-key` vectors; the rule generalizes to every
  family in this directory, 005.1 §15.2 convention amendment).
- [ ] **Regenerate via the resident serializer, reproducing every already-
  committed family first.** A family here is never hand-typed against a
  second, ad-hoc implementation — it is *computed* by the real production
  serializer (`spine.core.canonical` for canonical-json/record-key/
  fact-hash; the real check evaluator for check-verdicts) run as a
  generator script. Before regenerating or extending any one family, the
  generator's own validation gate must first reproduce every OTHER
  already-committed family byte-for-byte (007.1 §5.4: "the generator first
  reproducing all committed canonical-json vectors (31) and all committed
  record-key vectors (25) as its validation gate"). This is what keeps
  "one serializer per lane behind every derived identity" true instead of
  aspirational — a second, drifted serializer is exactly the trap two
  evaluators (005.1 A-7) would reintroduce.
- [ ] **Every consumer writes its own untagging parser — shared vectors,
  never shared code (004 D-13).** `test_canonical.py`, `test_identity.py`,
  and `test_fact_hash.py` (see each module's own docstring) each define
  their own private `_parse_fixture_value`, byte-identical in shape but
  never imported across files. This is deliberate, not an oversight: a
  shared parsing helper would silently couple three otherwise-independent
  reproduce-or-fail gates through one piece of code neither the vectors
  nor the design doc govern. `tests/frames/test_check_verdicts.py` follows
  the identical rule for the same reason. Do not "clean up" the
  duplication by extracting a shared helper.
- [ ] **NFC/NFD pairs are authored as `\uXXXX` escapes, verified after
  every write.** A byte-distinct-but-visually-identical Unicode pair (e.g.
  `é` as U+00E9 vs. the decomposed U+0065 U+0301) is a **deliberate,
  named test case** — the canonical grammar performs no normalization, so
  the two are two distinct keys/hashes by design (007.1 §5.3 `[AE-10]`).
  Author such a pair using literal `\uXXXX` Python escapes in the
  generator source (never a literal Unicode character pasted into a
  string), and **re-open and byte-diff the written file after any tool or
  editor write touches it**: several tool-mediated writes have been
  observed silently normalizing NFC/NFD pairs to one canonical form,
  which would silently collapse the very discriminator the vector exists
  to pin. If a diff ever shows an edit near an NFC/NFD vector you did not
  intend, treat it as a write-time normalization bug, not a content
  change, and re-author from the `\uXXXX`-escaped source.
- [ ] **Additive only, per family.** Extending a family's coverage is a
  vector *addition*; changing or removing a previously-committed vector's
  value is a breaking change to the normative surface itself and must be
  argued as such (007.1 §5.4's "the vectors become the normative surface
  under §5.3's on-divergence rule").

## Families in this directory

| Directory | Consumer test(s) | Notes |
|---|---|---|
| `canonical-json/` | `spine/tests/unit/test_canonical.py` | `core.canonical.canonical_json` — the reference untagging parser other families' docstrings point back to |
| `record-key/` | `spine/tests/unit/test_identity.py` | `core.identity.derive_record_key` |
| `fact-hash/` | `spine/tests/unit/test_fact_hash.py` | `core.canonical.row_hash` / `content_hash` — its generator reproduces all committed canonical-json + record-key vectors first |
| `check-verdicts/` | `spine/tests/frames/test_check_verdicts.py` | real check-grammar evaluation (G-10); its own typed-NULL extension of the tagged convention |
| `events/` | `spine/tests/contracts/test_parse_fixtures.py`, `test_emit_fixtures.py`, `spine/tests/unit/test_naming.py` (`delivery-registered`) | plain (untagged) event-contract examples — a different family, not part of the tagged-JSON convention above |

See 005.1 §15.2 (the tagged-JSON convention's normative home) and 007.1
§5.3/§5.4 (`[DS-5]`, the fact-hash family's own generation provenance) for
the full design text this checklist summarizes.
