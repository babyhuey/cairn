"""compute/acquire + compute/release inside a governed pipeline (ADR-0151 D6, #201).

Two properties the operators exist for, proven on the real paths:

* ``compute/acquire`` reaches the approval chokepoint (``enforce_gate``) as
  ``paid_compute`` — the risk the manifest declares, rated high — so a profile that
  gates paid compute pauses it, and ``compute/release`` (``external_write``) is not
  gated by the same profile.
* a ``finalizer: true`` ``compute/release`` stops the box when the pipeline ends
  done, failed or cancelled, on the in-process bridge.

Hermetic: EC2 is a fake behind ``CAIRN_EC2_CLIENT_FACTORY`` (the dev seam the real
``ec2_client`` reads), the connection store is stubbed at ``resolve_connection``, and
the SSH readiness probe answers at once. No AWS call is made.
"""
from __future__ import annotations

import asyncio
import os
import time

os.environ.setdefault("OPENAI_API_KEY", "sk-test-dummy")

import pytest
from pydantic import BaseModel, ConfigDict

import cairn.core.approval.enforce as enforce
from cairn.core.approval.enforce import NeedsApprovalError
from cairn.core.connections import ResolvedConnection
from cairn.core.dispatch.dispatcher import RunDispatcher
from cairn.core.dispatch.pipeline_bridge import PipelineBridge
from cairn.core.engine.pipeline import parse_pipeline_def, plan_pipeline
from cairn.core.operator import Operation, default_registry, load_all_operators, register_operator
from cairn.core.operator.binding import OperationBinding
from cairn.core.operator.runtime import invoke_operator
from cairn.core.pack.manifest import OperatorEntry
from cairn.core.storage import build_storage
from cairn.core.storage.pipeline_store import InMemoryPipelineStore
from tests.core.test_trigger_server_dispatch import FakeRunner

BOX = {"host": "10.0.0.9", "port": "22", "instance_id": "i-abc123",
       "region": "eu-central-1", "hourly_usd": "0.66"}


class FakeEC2:
    def __init__(self, state: str = "stopped") -> None:
        self.state = state
        self.calls: list[str] = []
        self.tags: dict[str, str] = {}

    def describe_instances(self, **kw):
        self.calls.append("describe")
        return {"Reservations": [{"Instances": [
            {"InstanceId": "i-abc123", "State": {"Name": self.state}}]}]}

    def start_instances(self, **kw):
        self.calls.append("start")
        self.state = "running"
        return {"StartingInstances": [{"CurrentState": {"Name": "running"}}]}

    def stop_instances(self, **kw):
        self.calls.append("stop")
        self.state = "stopped"
        return {"StoppingInstances": [{"CurrentState": {"Name": "stopping"}}]}

    def create_tags(self, **kw):
        self.calls.append("tags")
        for t in kw.get("Tags") or []:
            self.tags[t["Key"]] = t["Value"]


_SEAM: dict[str, FakeEC2] = {}


def _seam_factory(region: str = "") -> FakeEC2:
    return _SEAM["ec2"]


@pytest.fixture()
def ec2(monkeypatch):
    fake = _SEAM["ec2"] = FakeEC2()
    monkeypatch.setenv("CAIRN_EC2_CLIENT_FACTORY", f"{__name__}:_seam_factory")

    async def _resolve(name, **kw):
        return ResolvedConnection(name=name, kind="compute.ssh", config=dict(BOX))

    monkeypatch.setattr("cairn.core.connections.resolve_connection", _resolve)

    async def _ready(host, port, *, timeout_s, interval_s=5.0):
        return True

    monkeypatch.setattr("operator_remote.operators.acquire.wait_port_open", _ready)
    yield fake
    _SEAM.clear()


@pytest.fixture(autouse=True)
def _installed_operators():
    """The suite may reset the default registry: re-load the installed operators
    (entry points + pack.toml metadata, which is where `risk` comes from)."""
    load_all_operators()
    for cls in (_Work,):
        try:
            default_registry.register(cls, strict=False)
        except Exception:  # noqa: BLE001 — already registered is fine
            pass
    default_registry.attach_metadata(OperatorEntry(name="_t201/work", risk="read_only"))


# ── enforce_gate reaches acquire as paid_compute ─────────────────────────────
def _profile_gates_paid_compute(monkeypatch):
    monkeypatch.setattr(enforce, "_profile_approval",
                        lambda: {"mode": "enforce", "default_on_risk": ["paid_compute"]})
    monkeypatch.setattr(enforce, "_runtime_policy_engine", lambda: None)


async def test_acquire_reaches_enforce_gate_as_paid_compute(ec2, monkeypatch):
    _profile_gates_paid_compute(monkeypatch)
    assert default_registry.get_metadata("compute/acquire").risk == "paid_compute"
    cls = default_registry.get("compute/acquire")
    binding = OperationBinding(operator="compute/acquire", input={"connection": "gpu"}, output={})

    with pytest.raises(NeedsApprovalError) as ei:
        await invoke_operator(binding, cls, state={}, pipeline_id="pl-1", node_name="acquire")
    assert ei.value.request.operator == "compute/acquire"
    assert ei.value.request.sources == ("profile",)
    assert ec2.calls == [], "nothing may start before the approval"


async def test_release_is_not_gated_by_the_paid_compute_profile(ec2, monkeypatch):
    _profile_gates_paid_compute(monkeypatch)
    ec2.state = "running"
    cls = default_registry.get("compute/release")
    binding = OperationBinding(operator="compute/release", input={"connection": "gpu"},
                               output={"stopped": "$state.stopped"})

    patch = await invoke_operator(binding, cls, state={}, pipeline_id="pl-1", node_name="release")
    assert patch == {"stopped": True}
    assert "stop" in ec2.calls


# ── a finalizer release runs on done, failed and cancelled ───────────────────
class _P(BaseModel):
    model_config = ConfigDict(extra="allow")


@register_operator(name="_t201/work", version="0.1.0")
class _Work(Operation[_P, _P]):
    """The pipeline's own work: finishes, raises, or runs until cancelled."""

    input_schema = _P
    output_schema = _P

    def run(self, inp, ctx):
        mode = getattr(inp, "mode", "ok")
        if mode == "fail":
            raise RuntimeError("the work failed")
        time.sleep(float(getattr(inp, "sleep_s", 0.0)))
        return _P(worked=True)


def _def(mode: str, sleep_s: float = 0.0):
    return parse_pipeline_def(
        "name: box\nkind: pipeline\n"
        "inputs:\n  compute_connection: {type: str, default: gpu}\n"
        "steps:\n"
        "  - id: acquire\n    task: compute/acquire\n"
        "    with: {connection: $input.compute_connection, max_hours: 1}\n"
        f"  - id: work\n    task: _t201/work\n    depends_on: [acquire]\n"
        f"    with: {{mode: {mode}, sleep_s: {sleep_s}}}\n"
        "  - id: release\n    task: compute/release\n    finalizer: true\n"
        "    with: {connection: $input.compute_connection}\n",
        pack="t201", source="<test>")


async def _wait(store, pid, pred, timeout=10.0):
    deadline = asyncio.get_event_loop().time() + timeout
    while True:
        rec = await store.get(pid)
        if pred(rec):
            return rec
        if asyncio.get_event_loop().time() > deadline:
            raise AssertionError(f"stuck: {rec.status!r} "
                                 + str({k: v.status for k, v in rec.step_state.items()}))
        await asyncio.sleep(0.05)


async def _run(mode: str, *, cancel: bool = False):
    d = RunDispatcher(FakeRunner(), backend=build_storage())
    b = PipelineBridge(d, store=InMemoryPipelineStore())
    await b.start()
    try:
        rec = await b.start_pipeline(plan_pipeline(_def(mode, 3.0 if cancel else 0.0)),
                                     inputs={"compute_connection": "gpu"})
        if cancel:
            await _wait(b.store, rec.pipeline_id,
                        lambda r: getattr(r.step_state.get("work"), "status", "") == "running")
            await b.cancel_pipeline(rec.pipeline_id)
        return await _wait(b.store, rec.pipeline_id,
                           lambda r: r.status in ("done", "failed", "cancelled"))
    finally:
        b.stop()


@pytest.mark.parametrize("mode, cancel, terminal", [
    ("ok", False, "done"),
    ("fail", False, "failed"),
    ("ok", True, "cancelled"),
])
async def test_the_finalizer_release_stops_the_box_on_every_outcome(ec2, mode, cancel, terminal):
    rec = await _run(mode, cancel=cancel)

    assert rec.status == terminal
    assert rec.step_state["acquire"].status == "done"
    assert rec.step_state["release"].status == "done"
    assert ec2.calls.index("tags") < ec2.calls.index("start") < ec2.calls.index("stop")
    assert ec2.state == "stopped", "the box must not bill after the pipeline ended"
