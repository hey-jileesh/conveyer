"""Unit tests for `spine.core.args.parse_args` — LLD §6.4, §7.7.

Example-based for the two accepted flag forms and the edge cases (`--flag`
with no value token, a value that itself starts with `--`, non-flag tokens
ignored), then property-tested (§12.4) for the round-trip: any dict of plain
string keys/values, rendered as `["--k", "v", ...]`, parses back to the same
dict.
"""

from hypothesis import given, settings
from hypothesis import strategies as st
from spine.core import args

# --- example-based -----------------------------------------------------------


def test_two_token_form() -> None:
    assert args.parse_args(["--conveyer-env", "dev"]) == {"conveyer-env": "dev"}


def test_equals_form() -> None:
    assert args.parse_args(["--conveyer-env=dev"]) == {"conveyer-env": "dev"}


def test_mixed_forms_in_one_argv() -> None:
    assert args.parse_args(["--a=1", "--b", "2", "--c=3"]) == {"a": "1", "b": "2", "c": "3"}


def test_bare_flag_with_no_value_maps_to_empty_string() -> None:
    assert args.parse_args(["--flag"]) == {"flag": ""}


def test_flag_immediately_followed_by_another_flag_maps_to_empty_string() -> None:
    assert args.parse_args(["--a", "--b", "v"]) == {"a": "", "b": "v"}


def test_non_flag_tokens_are_ignored() -> None:
    assert args.parse_args(["positional", "--k", "v"]) == {"k": "v"}


def test_empty_argv() -> None:
    assert args.parse_args([]) == {}


def test_job_run_id_style_key_parses() -> None:
    # Glue's own injected key (I-5 fallback) -- no leading "conveyer-" prefix,
    # just a plain "--k v" pair like every other flag.
    assert args.parse_args(["--JOB_RUN_ID", "jr_abc123"]) == {"JOB_RUN_ID": "jr_abc123"}


# --- property-based (§12.4: "args.parse_args round-trip") -------------------

_key_strategy = st.text(alphabet="abcdefghijklmnopqrstuvwxyz0123456789-", min_size=1, max_size=20)
_value_strategy = st.text(
    alphabet="abcdefghijklmnopqrstuvwxyz0123456789-_/:.", min_size=0, max_size=30
).filter(lambda v: not v.startswith("--"))


@given(pairs=st.dictionaries(_key_strategy, _value_strategy, min_size=0, max_size=8))
@settings(max_examples=200)
def test_parse_args_round_trips_over_dicts(pairs: dict[str, str]) -> None:
    argv: list[str] = []
    for key, value in pairs.items():
        argv += [f"--{key}", value]
    assert args.parse_args(argv) == pairs
