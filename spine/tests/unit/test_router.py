"""Unit tests for `spine.entrypoints.router` — LLD §8.1, §8.2, I-8, I-13, I-22.

Covers: R-06 (two duplicate events -> exactly one execution start, one
`ExecutionAlreadyExists` swallowed as success, `SingleFlightCollisions`
metric); the declared invariant that the SFN execution name is byte-equal
to `batch_id`; the `--rN` deliberate-rerun grammar's structural
disjointness from every routed (UUIDv5) `batch_id`, property-tested; the
[S-2] allowlist projection (extra detail fields never ride into
`start_execution`'s `input`); the full stdlib validation matrix (missing
field, malformed `batch_id`, malformed pipeline slug, oversized detail ->
a metric DISTINCT from `SingleFlightCollisions`); unknown pipeline
(`StateMachineDoesNotExist`) and any other error propagating (never
swallowed); `RouterConfig`'s env-var parsing; `handler`'s wiring.

Per I-13: no moto (moto's SFN cannot run `glue:startJobRun.sync` anyway;
the property under test is router logic + an AWS-documented API contract,
not emergent AWS behavior) -- a plain record-of-functions SFN double
(`_StubSfnClient`) models boto3's `client.exceptions.<Name>` shape (a
nested `exceptions` namespace of plain `Exception` subclasses), the same
class of double `test_events.py` already uses for a shape moto can't
fabricate. No mocking framework, ever (engine-wide idiom rule).
"""

from __future__ import annotations

import json
import uuid
from pathlib import Path
from typing import Any

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from spine.core import naming
from spine.entrypoints import router

_FIXTURES_DIR = (
    Path(__file__).resolve().parents[3]
    / "contracts"
    / "fixtures"
    / "events"
    / "delivery-registered"
)

_ARN_PREFIX = "arn:aws:states:us-east-1:123456789012:stateMachine:conveyer-dev-spine-"


def _minimal_detail() -> dict[str, Any]:
    return json.loads((_FIXTURES_DIR / "v1-minimal.json").read_text())  # type: ignore[no-any-return]


# --- record-of-functions SFN double (I-13) -----------------------------------


class _StubSfnExceptions:
    """Models boto3's per-client `client.exceptions.<Name>` shape -- each
    name is a plain `Exception` subclass, matched by `except sfn.exceptions.
    ExecutionAlreadyExists:` in `router.route` exactly as it would a real
    boto3 client's dynamically-generated exception class."""

    class ExecutionAlreadyExists(Exception):
        pass

    class StateMachineDoesNotExist(Exception):
        pass


class _StubSfnClient:
    """Records every `start_execution` call; raises `ExecutionAlreadyExists`
    for a name it has already started, `StateMachineDoesNotExist` for an ARN
    outside `known_arns` (`None` = "every ARN known" -- the common case),
    else records success."""

    exceptions = _StubSfnExceptions

    def __init__(self, known_arns: set[str] | None = None) -> None:
        self.calls: list[dict[str, Any]] = []
        self._started_names: set[str] = set()
        self._known_arns = known_arns

    def start_execution(self, *, stateMachineArn: str, name: str, input: str) -> dict[str, Any]:
        self.calls.append({"stateMachineArn": stateMachineArn, "name": name, "input": input})
        if self._known_arns is not None and stateMachineArn not in self._known_arns:
            raise self.exceptions.StateMachineDoesNotExist(
                f"State Machine Does Not Exist: {stateMachineArn}"
            )
        if name in self._started_names:
            raise self.exceptions.ExecutionAlreadyExists(f"Execution Already Exists: {name}")
        self._started_names.add(name)
        return {"executionArn": f"{stateMachineArn}:{name}", "startDate": 0.0}


class _AnythingElseFailsSfnClient:
    """Models the "everything else: raise" branch with a generic error that
    is neither of boto3's named exceptions -- must NOT be swallowed."""

    exceptions = _StubSfnExceptions

    def start_execution(self, **_kwargs: Any) -> dict[str, Any]:
        raise RuntimeError("simulated unrelated AWS failure")


# --- route(): happy path + allowlist projection ------------------------------


def test_route_starts_execution_and_returns_started_true() -> None:
    detail = _minimal_detail()
    sfn = _StubSfnClient()
    config = router.RouterConfig(sfn_arn_prefix=_ARN_PREFIX)

    result = router.route(detail, sfn, config)

    assert result == {"batch_id": detail["batch_id"], "started": True}
    assert len(sfn.calls) == 1
    assert sfn.calls[0]["stateMachineArn"] == _ARN_PREFIX + naming.slug(detail["pipeline"])


def test_route_execution_name_is_byte_equal_to_batch_id() -> None:
    """Declared invariant (I-13): the SFN execution name IS `batch_id`,
    exactly -- not a derived/prefixed/suffixed form."""
    detail = _minimal_detail()
    sfn = _StubSfnClient()

    router.route(detail, sfn, router.RouterConfig(sfn_arn_prefix=_ARN_PREFIX))

    assert sfn.calls[0]["name"] == detail["batch_id"]


def test_route_forwards_only_the_v1_fields_allowlist() -> None:
    """[S-2]: extra bus-event fields must never ride into `start_execution`'s
    `input` -- the allowlisted projection, not the raw detail."""
    detail = {**_minimal_detail(), "secret_internal_field": "must-not-ride", "another": 123}
    sfn = _StubSfnClient()

    router.route(detail, sfn, router.RouterConfig(sfn_arn_prefix=_ARN_PREFIX))

    forwarded = json.loads(sfn.calls[0]["input"])
    assert forwarded == {field: detail[field] for field in router.V1_FIELDS}
    assert "secret_internal_field" not in forwarded
    assert "another" not in forwarded


# --- R-06: duplicate event single-flight -------------------------------------


def test_r06_two_duplicate_events_single_flight(capsys: pytest.CaptureFixture[str]) -> None:
    """R-06: two `delivery-registered` events for the SAME batch_id ->
    exactly one execution actually starts (`started=True`), the second's
    `ExecutionAlreadyExists` is swallowed as success (`started=False`) --
    both events still individually reach `start_execution` (single-flight is
    SFN's name-uniqueness doing the work, not client-side dedup) -- and the
    swallowed collision emits a `SingleFlightCollisions` EMF metric."""
    detail = _minimal_detail()
    sfn = _StubSfnClient()
    config = router.RouterConfig(sfn_arn_prefix=_ARN_PREFIX)

    first = router.route(detail, sfn, config)
    second = router.route(detail, sfn, config)

    assert first == {"batch_id": detail["batch_id"], "started": True}
    assert second == {"batch_id": detail["batch_id"], "started": False}
    assert len(sfn.calls) == 2  # both events reached start_execution once each
    assert sfn.calls[0] == sfn.calls[1]  # byte-identical name + input (idempotent retry)

    metrics = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    collision_metrics = [m for m in metrics if "SingleFlightCollisions" in m]
    assert len(collision_metrics) == 1
    assert collision_metrics[0]["pipeline"] == detail["pipeline"]
    assert collision_metrics[0]["feed_id"] == detail["feed_id"]


def test_route_does_not_swallow_a_different_client_error() -> None:
    """ "Everything else: raise" (I-8) -- only `ExecutionAlreadyExists` is
    swallowed; any other failure (including one shaped nothing like a named
    boto3 exception) must propagate."""
    detail = _minimal_detail()
    sfn = _AnythingElseFailsSfnClient()

    with pytest.raises(RuntimeError, match="simulated unrelated AWS failure"):
        router.route(detail, sfn, router.RouterConfig(sfn_arn_prefix=_ARN_PREFIX))


def test_route_unknown_pipeline_raises_state_machine_does_not_exist() -> None:
    """Unknown pipeline (unprovisioned state machine) is an ops signal, not a
    silent drop -- raises, does not return success."""
    detail = _minimal_detail()
    sfn = _StubSfnClient(known_arns=set())  # no ARN is provisioned

    with pytest.raises(_StubSfnExceptions.StateMachineDoesNotExist):
        router.route(detail, sfn, router.RouterConfig(sfn_arn_prefix=_ARN_PREFIX))


# --- validation matrix: missing fields ----------------------------------------


@pytest.mark.parametrize("field", router.V1_FIELDS)
def test_route_missing_required_field_raises(field: str) -> None:
    detail = _minimal_detail()
    del detail[field]

    with pytest.raises(ValueError, match=field):
        router.route(detail, _StubSfnClient(), router.RouterConfig(sfn_arn_prefix=_ARN_PREFIX))


# --- validation matrix: malformed batch_id ------------------------------------


@pytest.mark.parametrize(
    "bad_batch_id",
    [
        str(uuid.uuid4()),  # UUIDv4, not v5
        "not-a-uuid-at-all",
        "",
        "b2c3d4e5-f6a7-5b2c-9d3e-4f5a6b7c8d9e\n",  # trailing newline (`.fullmatch` gap)
        "b2c3d4e5-f6a7-5b2c-9d3e-4f5a6b7c8d9e--r1",  # rerun-shaped, I-13 disjointness
    ],
)
def test_route_malformed_batch_id_raises(bad_batch_id: str) -> None:
    detail = {**_minimal_detail(), "batch_id": bad_batch_id}

    with pytest.raises(ValueError, match="UUIDv5"):
        router.route(detail, _StubSfnClient(), router.RouterConfig(sfn_arn_prefix=_ARN_PREFIX))


# --- validation matrix: malformed pipeline slug -------------------------------


@pytest.mark.parametrize(
    "bad_pipeline",
    [
        "Pipelines/Commissions",  # uppercase
        "a--b",  # "--" inside a segment
        "",
        "a/",
        "/a",
    ],
)
def test_route_malformed_pipeline_slug_raises(bad_pipeline: str) -> None:
    detail = {**_minimal_detail(), "pipeline": bad_pipeline}

    with pytest.raises(ValueError, match="pipeline"):
        router.route(detail, _StubSfnClient(), router.RouterConfig(sfn_arn_prefix=_ARN_PREFIX))


# --- validation matrix: oversized detail [T-5, E-10] --------------------------


def test_route_oversized_detail_raises_and_emits_distinct_metric(
    capsys: pytest.CaptureFixture[str],
) -> None:
    detail = _minimal_detail()
    sfn = _StubSfnClient()
    tiny_config = router.RouterConfig(sfn_arn_prefix=_ARN_PREFIX, argv_budget_bytes=10)

    with pytest.raises(ValueError, match="argv budget"):
        router.route(detail, sfn, tiny_config)

    assert len(sfn.calls) == 0  # never reaches start_execution

    metrics = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    oversized = [m for m in metrics if "RouterDetailOversized" in m]
    assert len(oversized) == 1
    # Distinct from SingleFlightCollisions -- oversized-detail and duplicate-
    # event are different failure modes and must be separately dashboarded.
    assert "SingleFlightCollisions" not in oversized[0]


def test_route_under_budget_detail_does_not_raise() -> None:
    detail = _minimal_detail()
    generous_config = router.RouterConfig(sfn_arn_prefix=_ARN_PREFIX, argv_budget_bytes=8192)

    result = router.route(detail, _StubSfnClient(), generous_config)

    assert result["started"] is True


# --- I-13 structural disjointness: --rN grammar vs. UUIDv5 batch_id ----------


@given(name=st.text(min_size=1, max_size=8), n=st.integers(min_value=1, max_value=999))
@settings(max_examples=200)
def test_rerun_grammar_structurally_disjoint_from_every_routed_batch_id(name: str, n: int) -> None:
    """I-13's structural (not merely tested) disjointness claim: for ANY
    UUIDv5 `batch_id` (the only kind `route`/`naming.execution_name` ever
    accepts as a routed execution name), the corresponding `--rN` rerun name
    can NEVER itself validate as a routed `batch_id` -- so a duplicate event
    can never collide with a deliberate rerun's execution name, and vice
    versa. `naming.execution_name`'s UUIDv5 regex is what `route` calls;
    this property pins that regex and `naming`'s own `--rN` grammar are
    disjoint by construction, not by coincidence of today's test corpus."""
    batch_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, name))
    rerun_name = naming.rerun_execution_name(batch_id, n)

    # The routed batch_id itself is never rerun-shaped...
    assert naming.is_rerun_execution_name(naming.execution_name(batch_id)) is False
    # ...and the rerun name it produces can never pass the router's own
    # batch_id validation (the SAME check `route` applies to every event).
    with pytest.raises(ValueError, match="UUIDv5"):
        naming.execution_name(rerun_name)


# --- RouterConfig / env parsing -----------------------------------------------


def test_config_from_env_reads_prefix_and_default_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("CONVEYER_SFN_ARN_PREFIX", _ARN_PREFIX)
    monkeypatch.delenv("CONVEYER_ARGV_BUDGET_BYTES", raising=False)

    config = router._config_from_env()

    assert config.sfn_arn_prefix == _ARN_PREFIX
    assert config.argv_budget_bytes == router._DEFAULT_ARGV_BUDGET_BYTES


def test_config_from_env_override_budget(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CONVEYER_SFN_ARN_PREFIX", _ARN_PREFIX)
    monkeypatch.setenv("CONVEYER_ARGV_BUDGET_BYTES", "4096")

    config = router._config_from_env()

    assert config.argv_budget_bytes == 4096


def test_config_from_env_missing_prefix_raises_keyerror(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("CONVEYER_SFN_ARN_PREFIX", raising=False)

    with pytest.raises(KeyError):
        router._config_from_env()


# --- handler(): wiring only ----------------------------------------------------


def test_handler_wires_event_detail_through_to_route(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    detail = _minimal_detail()
    stub = _StubSfnClient()
    monkeypatch.setenv("CONVEYER_SFN_ARN_PREFIX", _ARN_PREFIX)
    monkeypatch.setattr(router.boto3, "client", lambda *_a, **_kw: stub)

    result = router.handler({"detail": detail}, context=None)

    assert result == {"batch_id": detail["batch_id"], "started": True}
    assert len(stub.calls) == 1
