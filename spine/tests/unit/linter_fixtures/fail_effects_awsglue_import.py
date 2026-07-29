"""MUST-FAIL: `import awsglue` inside `spine/effects/**` (design-critique
finding F3(b), bead `conveyer-nvh.42`). I-14 says "no `awsglue` anywhere in
`spine/`", but before this fix only the `core` and `frames-transforms`
profiles carried an `awsglue` import ban — `effects/`, `stages/`,
`entrypoints/`, `bootstrap/`, and spine's top-level modules (`run.py`,
`binding.py`, `config.py`, `context.py`, `observability.py`) had NO
enforcement at all. A single package-wide profile
(`path_prefixes=(("spine",), ("pipelines",))`) now covers the whole tree.

Simulated scope: `spine/effects/**` (`effects-stages` profile applies for
the string-SQL rule; the package-wide profile is what actually catches this
import).
"""

from __future__ import annotations

import awsglue


def job_name() -> str:
    return awsglue.__name__
