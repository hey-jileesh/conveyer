"""Local fixtures for the post-deploy smoke suite -- LLD §12.1/§12.5,
`make smoke ENV=<env>`.

Real AWS only (never moto): every fixture here either SKIPS cleanly (`--env`
or AWS credentials absent -- so a bare `uv run pytest`/CI sweep that
recurses into `tests/smoke` never fails the build; `make test`'s three
directory-scoped targets, §12.6, never touch `tests/smoke` at all) or FAILS
loudly on a genuine misconfiguration (`--env` disagrees with the deployed
`CONVEYER_ENV`) -- those two outcomes are deliberately different: absence
is normal (or an accident of test collection), disagreement is an operator
mistake worth surfacing, not silently skipping past.
"""

from __future__ import annotations

import boto3  # type: ignore[import-untyped]
import pytest
from botocore.exceptions import BotoCoreError, ClientError  # type: ignore[import-untyped]
from ingestion.config import RuntimeConfig, from_env


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--env",
        action="store",
        default=None,
        help=(
            "Deploy environment for smoke tests (e.g. dev). Required -- smoke "
            "tests skip cleanly without it (run via `make smoke ENV=<env>`)."
        ),
    )


@pytest.fixture(scope="session")
def env_name(request: pytest.FixtureRequest) -> str:
    value = request.config.getoption("--env")
    if not value:
        pytest.skip("smoke tests require --env (run via `make smoke ENV=<env>`)")
    return value


@pytest.fixture(scope="session")
def smoke_config(env_name: str) -> RuntimeConfig:
    """The same `RuntimeConfig` a deployed Lambda builds (§7.2) -- the
    operator's shell must export the `CONVEYER_*` set (sourced from
    `terraform output`, README §10.8) before running `make smoke`. Absence
    of the whole set skips cleanly (mirrors `env_name`); presence but
    disagreement with `--env` fails loudly (a real environment mixup).
    """
    try:
        config = from_env()
    except RuntimeError as exc:
        pytest.skip(
            "smoke tests require the CONVEYER_* runtime env vars "
            f"(see ingestion/config.py::from_env): {exc}"
        )
    if config.env != env_name:
        pytest.fail(
            f"--env {env_name!r} does not match CONVEYER_ENV={config.env!r} -- "
            "refusing to run smoke against a mismatched environment"
        )
    return config


@pytest.fixture(scope="session")
def aws_credentials(smoke_config: RuntimeConfig) -> None:
    try:
        sts = boto3.client("sts", region_name=smoke_config.aws_region)
        sts.get_caller_identity()
    except (ClientError, BotoCoreError) as exc:
        pytest.skip(f"AWS credentials not available for smoke testing: {exc}")
