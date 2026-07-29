"""Local fixtures for the spine post-deploy smoke suite -- LLD 004.1
S10.6 step 6, `make -C spine smoke ENV=<env>`.

Mirrors `ingestion/tests/smoke/conftest.py` exactly in spirit (same skip/
fail split): real AWS only (never moto). Every fixture here either SKIPS
cleanly (`--env` or AWS credentials absent, so a bare `uv run pytest` /
`testpaths = ["tests"]` sweep -- 004.1 S12.1 -- never fails the build) or
FAILS loudly on a genuine operator mistake (kept symmetrical with
ingestion's own conftest even though this module has no `--env`-vs-deployed
mismatch to detect, since spine has no `RuntimeConfig.env` equivalent to
read back).

Unlike ingestion, spine has no `config.from_env()`/`RuntimeConfig` -- the
spine package's own `RunnerConfig` (`spine/config.py`) is argv-driven for
the Glue entrypoint, not env-driven, and this test module deliberately does
NOT import anything from the `ingestion` package (a different uv-workspace
member, not a declared dependency of `conveyer-spine`, S4). Every deployed
resource name below is instead RE-DERIVED from `name_prefix`/`env` via the
LLD's own S5 naming grammar (`${p} = ${name_prefix}-${env}`) -- the same
values `terraform output` would show, computed locally rather than read
back, so this suite needs no cross-package config-loading machinery.
"""

from __future__ import annotations

import boto3  # type: ignore[import-untyped]
import pytest
from _names import SmokeNames
from botocore.exceptions import BotoCoreError, ClientError  # type: ignore[import-untyped]


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--env",
        action="store",
        default=None,
        help=(
            "Deploy environment for smoke tests (e.g. dev). Required -- smoke "
            "tests skip cleanly without it (run via `make -C spine smoke ENV=<env>`)."
        ),
    )
    parser.addoption(
        "--name-prefix",
        action="store",
        default="conveyer",
        help="Physical-name prefix (LLD S5 `${p}`); matches Terraform's own default.",
    )
    parser.addoption(
        "--region",
        action="store",
        default="us-east-1",
        help="AWS region the env was deployed into (matches envs/dev/dev.tfvars).",
    )


@pytest.fixture(scope="session")
def env_name(request: pytest.FixtureRequest) -> str:
    value = request.config.getoption("--env")
    if not value:
        pytest.skip("smoke tests require --env (run via `make -C spine smoke ENV=<env>`)")
    return value


@pytest.fixture(scope="session")
def smoke_names(env_name: str, request: pytest.FixtureRequest) -> SmokeNames:
    return SmokeNames(
        name_prefix=request.config.getoption("--name-prefix"),
        env=env_name,
        region=request.config.getoption("--region"),
    )


@pytest.fixture(scope="session")
def aws_credentials(smoke_names: SmokeNames) -> None:
    try:
        sts = boto3.client("sts", region_name=smoke_names.region)
        sts.get_caller_identity()
    except (ClientError, BotoCoreError) as exc:
        pytest.skip(f"AWS credentials not available for smoke testing: {exc}")
