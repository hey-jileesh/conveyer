"""`parse_args`: argv -> dict, pure. LLD §6.4, §7.7."""

from collections.abc import Sequence


def parse_args(argv: Sequence[str]) -> dict[str, str]:
    """Pure: ``["--k", "v", "--x=y", ...]`` -> ``{"k": "v", "x": "y"}``.

    Two accepted forms per flag (I-14 -- no Glue/`awsglue`-specific argv
    assumptions beyond plain `--k v` pairs, so this stays engine-agnostic):

    * ``--key value`` -- two tokens, the second consumed as the value unless
      it itself starts with ``--`` (in which case ``key`` maps to ``""``).
    * ``--key=value`` -- one token, split on the first ``=`` (the trivial
      case §6.4 tolerates; anything richer than a first-``=`` split is out of
      scope for this pure parser).

    A dict, not a validated shape: this function does not know which keys
    are required or what they mean -- `RunnerConfig.from_args` (`config.py`)
    picks known keys out of the result and raises `KeyError` naming any
    missing one. Unknown keys (Glue injects its own, e.g. `--JOB_NAME`,
    `--scriptLocation`) simply ride along in the dict, unused.

    Tokens that don't start with ``--`` are ignored (no positional arguments
    are expected from Glue/SFN argv, but tolerating stray ones costs nothing
    and avoids inventing a rejection this design has no stake in).
    """
    out: dict[str, str] = {}
    i = 0
    n = len(argv)
    while i < n:
        token = argv[i]
        if not token.startswith("--"):
            i += 1
            continue
        body = token[2:]
        if "=" in body:
            key, _, value = body.partition("=")
            out[key] = value
            i += 1
            continue
        key = body
        has_value = i + 1 < n and not argv[i + 1].startswith("--")
        out[key] = argv[i + 1] if has_value else ""
        i += 2 if has_value else 1
    return out
