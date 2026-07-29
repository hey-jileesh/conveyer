"""Unit tests for `ingestion.core.naming.is_clean_object_name` -- conveyer
nvh.46/nvh.48. The rejected-suffix corpus mirrors
`spine/tests/unit/test_naming.py`'s `test_check_object_uris_rejects_
traversal_suffixes` parametrization (~line 304) -- same 15 cases, same ids
-- since this predicate is ingestion's own copy of the same grammar
(`_OBJECT_NAME_RE`), applied directly to a bare name rather than a URI
suffix (004 D-13: no shared code between lanes).
"""

import pytest
from ingestion.core.naming import is_clean_object_name

_REJECTED_CORPUS = [
    "..",
    "../x",
    "../../incoming/attacker.csv",
    ".",
    "./x",
    "a/b",
    "a/b/c",
    "",
    "%2e%2e/x",
    "%2e%2e%2fattacker.csv",
    "a%2fb",
    "a%5cb",
    "statement-2026-07.csv/",
    "a\\b",
    "..\\x",
]

_REJECTED_IDS = [
    "dotdot",
    "dotdot-slash-x",
    "dotdot-traversal-to-incoming",
    "dot",
    "dot-slash-x",
    "multi-segment",
    "multi-segment-3",
    "empty",
    "percent-encoded-dotdot",
    "percent-encoded-dotdot-slash",
    "percent-encoded-slash",
    "percent-encoded-backslash",
    "trailing-slash",
    "backslash-segment",
    "backslash-traversal",
]


@pytest.mark.parametrize("name", _REJECTED_CORPUS, ids=_REJECTED_IDS)
def test_is_clean_object_name_rejects_traversal_corpus(name: str) -> None:
    assert is_clean_object_name(name) is False


@pytest.mark.parametrize(
    "name",
    [
        "statement-2026-07.csv",  # clean basename
        "a..b.csv",  # interior dots, not a dot-segment
        "..hidden",  # leading dots but not the literal ".." segment
        "a b.csv",  # space
        "héllo.csv",  # unicode
        "a=b",  # '='
        "a-b",  # '-'
        "a_b",  # '_'
    ],
    ids=[
        "clean-basename",
        "interior-dots",
        "dotdot-prefix-not-bare",
        "space",
        "unicode",
        "equals",
        "dash",
        "underscore",
    ],
)
def test_is_clean_object_name_accepts_clean_names(name: str) -> None:
    assert is_clean_object_name(name) is True
