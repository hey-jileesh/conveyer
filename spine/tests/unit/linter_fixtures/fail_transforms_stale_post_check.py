"""MUST-FAIL (bind-defect corpus, not the purity linter -- 006.1 §13.4 item
2): a transforms module that still exports `post_check`, superseded by
006.1 §4.4's hard cut (bead conveyer-6pg.13, B3 -- `Transforms` drops
`post_check`; business-rule evaluation moved entirely into the framework's
own `post_check` STAGE). `purity_linter.py` has no rule about a function
NAMED `post_check` (this file trips zero `purity-*`/`idiom-*` rules on its
own -- it is syntactically/structurally clean transforms code); the defect
this fixture exercises is `core/bind_checks.py`'s S4 check
(`bind-defect/stale-post-check-export`), fed by `entrypoints/glue_main.py::
_acquire_transforms_meta`'s raw `hasattr(module, "post_check")` read over a
REAL imported module -- this fixture IS that real module, committed so the
scenario is a reviewable, versioned artifact rather than a `tmp_path`-
authored throwaway string.

Consumed by `tests/unit/test_glue_main.py`
(`test_acquire_transforms_meta_flags_the_committed_stale_post_check_corpus_fixture`,
line 667), via the `pipelines.__path__.append(...)` technique `tests/unit/
test_binding.py`/`tests/unit/test_glue_main.py` already established for
throwaway `pipelines.<name>` modules -- pointed at THIS directory instead
of a `tmp_path`, so the committed fixture is the thing under test.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Mapping

    from pyspark.sql import DataFrame


def apply(valid_df: DataFrame, co_effects: Mapping[str, DataFrame]) -> Mapping[str, DataFrame]:
    return {"detail": valid_df}


def post_check(candidate_df: DataFrame, co_effects: Mapping[str, DataFrame]) -> DataFrame:
    # The STALE export itself -- 005.1-era shape, no longer looked for by
    # `bind_transforms` (which drops `post_check` entirely, §4.4) but still
    # a real attribute a raw `hasattr` read over the imported module finds.
    return candidate_df
