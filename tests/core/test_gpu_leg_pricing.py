"""ADR-0151 D1–D5 (#200) — a flow leg on a priced ``compute.ssh`` box is priced,
checked at the launch door, shown to the approver, and settled on every outcome.

* D1 one core function prices a leg: ``hourly_usd × timeout`` from the target's
  connection config; a target with no ``hourly_usd`` is "not priced", never $0;
* D2 the launch door sums the estimates against ``remaining_usd`` (read-only) and
  refuses before any record; the estimate is recorded on ``PipelineRecord.admission``
  and the leg reserves that figure, never a re-derived one;
* D3 a priced leg with no ``timeout`` is refused at the door;
* D4 the gate before the first priced leg carries ``estimate_usd`` + ``estimate_basis``;
* D5 every attempt settles ``hourly_usd × its wall-clock`` on any outcome, the leg's
  ``Run.cost.gpu_usd`` is the sum of its attempts, an adopted attempt starts at the
  persisted ``run.started_at``; both owners make the same call.
"""
from __future__ import annotations

import asyncio
import os
import textwrap
import types
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

os.environ.setdefault("OPENAI_API_KEY", "sk-test-dummy")

import pytest  # noqa: E402

from cairn.core import budget_reservations as B  # noqa: E402
from cairn.core import workspace as W  # noqa: E402
from cairn.core.admission_door import REFUSED, AdmissionRefusedError  # noqa: E402
from cairn.core.connections import Connection, InMemoryConnectionStore  # noqa: E402
from cairn.core.dataflow.contract import StageRef  # noqa: E402
from cairn.core.dispatch import leg_executor as LE  # noqa: E402
from cairn.core.dispatch import leg_hooks as LH  # noqa: E402
from cairn.core.dispatch.leg_executor import LegOutcome, execute_leg  # noqa: E402
from cairn.core.dispatch.leg_hooks import flow_leg_record  # noqa: E402
from cairn.core.dispatch.pipeline_launch import launch_pipeline  # noqa: E402
from cairn.core.runtime.retry import RetryPolicy  # noqa: E402

T0 = datetime(2026, 10, 24, 8, 0, tzinfo=timezone.utc)
BOX = "gpu-box"
#: the admission key (D2) — spelled out so a missing implementation fails per test
COST_ESTIMATE_KEY = "cost_estimate"


def price_flow_leg(*a, **kw):
    return LH.price_flow_leg(*a, **kw)


def leg_price_kwargs(*a, **kw):
    return LH.leg_price_kwargs(*a, **kw)


def estimate_evidence(*a, **kw):
    return LH.estimate_evidence(*a, **kw)


# ── fixtures ────────────────────────────────────────────────────────────────

@pytest.fixture()
def conns(monkeypatch):
    """One in-memory connection store the pricing reads; ``set_rate`` edits the box."""
    store = InMemoryConnectionStore()

    def set_rate(rate, *, kind="compute.ssh", name=BOX):
        cfg = {"host": "10.0.0.5", "instance_id": "i-1", "region": "eu-west-1"}
        if rate is not None:
            cfg["hourly_usd"] = str(rate)
        store._by_id[name] = Connection(id=name, name=name, kind=kind, config=cfg)

    store.set_rate = set_rate
    monkeypatch.setattr("cairn.core.connections.default_connection_store", lambda: store)

    async def _account(workspace_id=None, **_kw):
        return "default"

    monkeypatch.setattr(W, "workspace_account", _account)
    # every flow leg dispatches to the box
    monkeypatch.setattr(LH, "compute_target_for_kind", lambda kind: BOX)
    return store


@pytest.fixture()
def memory_budget(monkeypatch):
    monkeypatch.delenv("OBS_STORAGE", raising=False)
    monkeypatch.delenv("CAIRN_STORAGE", raising=False)
    B.reset_reservations()
    yield
    B.reset_reservations()


_PIPE = """
name: sft
kind: pipeline
governance: {{series: s-gpu, budget: {{usd: {budget}}}}}
steps:
  - {{id: approve-run, gate: approve-run}}
  - {{id: train, flow: gpupack/train{timeout}, depends_on: [approve-run]}}
"""


def _install(tmp_path, monkeypatch, *, budget=25, timeout=", timeout: 12h"):
    import cairn.core.engine.pipeline_resolve as R
    from cairn.core import pull_worker_door as pw

    d = tmp_path / "gpupack" / "pipelines"
    d.mkdir(parents=True, exist_ok=True)
    (d / "sft.yaml").write_text(textwrap.dedent(_PIPE.format(budget=budget, timeout=timeout)))
    monkeypatch.setattr(R, "all_pipeline_dirs", lambda: [str(d)])

    async def _never():
        return None

    monkeypatch.setattr(pw, "last_worker_checkin", _never)

    async def _no_doors(plan, **kw):
        return None

    monkeypatch.setattr("cairn.core.dispatch.pipeline_doors.assert_pipeline_services", _no_doors)
    # the flow itself is not installed here: the door's pricing is what is under test
    monkeypatch.setattr("cairn.core.dispatch.pipeline_launch.assert_runnable", lambda d: None)


class _Bridge:
    """Records every start_pipeline — the launch door's only side effect (the record)."""

    def __init__(self):
        self.calls: list[dict] = []

    async def start_pipeline(self, plan, **kw):
        self.calls.append({"plan": plan, **kw})
        return SimpleNamespace(pipeline_id="pl_test", status="running", step_state={})


class _Clock:
    """``leg_executor._wall_now``: each attempt reads it at its start and its end."""

    def __init__(self, *ticks_h: float):
        self.ticks = [T0 + timedelta(hours=h) for h in ticks_h]

    def __call__(self):
        return self.ticks.pop(0) if len(self.ticks) > 1 else self.ticks[0]


def _stub_flow(monkeypatch, *, kind="ml"):
    stage = StageRef(stage="train.run")
    monkeypatch.setattr("cairn.core.dataflow.defs.load_flow_defs",
                        lambda: {"gpupack/train": types.SimpleNamespace(kind=kind)})
    monkeypatch.setattr("cairn.core.dataflow.defs.resolve_flow_ref",
                        lambda ref, cfg, kind="data": [stage])
    monkeypatch.setattr(LE, "_POLL_INTERVAL_S", 0.01)


class _Engine:
    """Answers each submitted job's polls from ``outcomes`` in order."""

    name = "stub"

    def __init__(self, *outcomes: str):
        self.outcomes = list(outcomes)
        self.submits = 0

    def validate(self, spec):
        return []

    async def submit(self, spec, ctx):
        self.submits += 1
        return types.SimpleNamespace(job_id=f"j{self.submits}", engine=self.name, meta={})

    async def poll(self, handle):
        idx = int(handle.job_id[1:]) - 1 if handle.job_id.startswith("j") else 0
        state = self.outcomes[min(idx, len(self.outcomes) - 1)]
        return types.SimpleNamespace(state=state, metrics={}, error="" if state == "succeeded" else "box said no",
                                     retriable=state == "failed")

    async def cancel(self, handle):
        pass

    async def result(self, handle):
        return types.SimpleNamespace(stats={"train.output": "s3://m"}, output_artifacts=[])


def _ledger(monkeypatch):
    """Spy on the one reserve/settle/release path the executor uses."""
    seen: dict[str, list] = {"reserve": [], "settle": [], "release": []}
    real_reserve, real_settle, real_release = LE.reserve, LE.settle, LE.release

    async def _reserve(series_id, **kw):
        seen["reserve"].append(kw)
        return await real_reserve(series_id, **kw)

    async def _settle(res, actual_usd=0.0):
        seen["settle"].append(round(actual_usd, 6))
        return await real_settle(res, actual_usd=actual_usd)

    async def _release(res):
        seen["release"].append(res)
        return await real_release(res)

    monkeypatch.setattr(LE, "reserve", _reserve)
    monkeypatch.setattr(LE, "settle", _settle)
    monkeypatch.setattr(LE, "release", _release)
    return seen


# ── D1: one function prices a leg ───────────────────────────────────────────

async def test_a_priced_box_is_hourly_rate_times_timeout(conns):
    conns.set_rate(0.53)
    p = await price_flow_leg(BOX, 12 * 3600)
    assert p["priced"] is True and p["connection"] == BOX
    assert p["hourly_usd"] == 0.53 and p["hours"] == 12.0 and p["estimate_usd"] == pytest.approx(6.36)


@pytest.mark.parametrize("rate,kind", [(None, "compute.ssh"), ("", "compute.ssh"), (0, "compute.ssh"),
                                       (0.53, "compute.kube")])
async def test_an_unpriced_target_reads_not_priced_never_zero(conns, rate, kind):
    conns.set_rate(rate, kind=kind)
    p = await price_flow_leg(BOX, 3600)
    assert p["priced"] is False and "estimate_usd" not in p
    assert p["basis"].startswith("not priced")
    # …the leg reserves what it did before and records no box time
    rec = SimpleNamespace(admission={COST_ESTIMATE_KEY: {"estimate_usd": 0.0, "legs": [{"step": "train", **p}]}},
                          governance={}, plan=None)
    assert leg_price_kwargs(rec, "train", SimpleNamespace(started_at=None)) == {"estimated_usd": 0.0}
    rec_ = flow_leg_record({}, LegOutcome(state="failed"))
    assert "cost" not in rec_ and "gpu_usd" not in rec_["result_payloads"][0]


async def test_no_target_is_not_a_priced_leg():
    assert await price_flow_leg("", 3600) is None


# ── D2/D3: the launch door ──────────────────────────────────────────────────

async def test_an_estimate_over_the_budget_is_refused_at_launch_with_no_record(
        tmp_path, monkeypatch, conns, memory_budget):
    conns.set_rate(0.53)
    _install(tmp_path, monkeypatch, budget=5)          # 0.53 × 12 h = $6.36 > $5
    bridge = _Bridge()
    with pytest.raises(AdmissionRefusedError) as ei:
        await launch_pipeline("gpupack/sft", inputs={}, bridge=bridge, workspace_id="default")
    assert bridge.calls == [], "a refused launch must leave no record"
    rec = ei.value.decision.as_record()
    assert rec["verdict"] == REFUSED
    assert "$6.36" in rec["refusing_reasons"][0] and "$5.00" in rec["refusing_reasons"][0]
    assert rec[COST_ESTIMATE_KEY]["estimate_usd"] == pytest.approx(6.36)


async def test_a_live_hold_in_the_series_counts_against_the_launch(
        tmp_path, monkeypatch, conns, memory_budget):
    conns.set_rate(0.53)
    _install(tmp_path, monkeypatch, budget=25)
    await B.reserve("s-gpu", est_usd=20.0, spent_usd=0.0, budget_usd=25.0, run_id="other")
    bridge = _Bridge()
    with pytest.raises(AdmissionRefusedError, match=r"\$5\.00 of \$25\.00 left"):
        await launch_pipeline("gpupack/sft", inputs={}, bridge=bridge, workspace_id="default")
    assert bridge.calls == []
    assert await B.reserved_total("s-gpu") == 20.0, "the door reserves nothing"


async def test_a_priced_leg_without_a_timeout_is_refused(tmp_path, monkeypatch, conns, memory_budget):
    conns.set_rate(0.53)
    _install(tmp_path, monkeypatch, budget=1000, timeout="")
    bridge = _Bridge()
    with pytest.raises(AdmissionRefusedError, match="declare a timeout to price this leg"):
        await launch_pipeline("gpupack/sft", inputs={}, bridge=bridge, workspace_id="default")
    assert bridge.calls == []


async def test_the_estimate_is_recorded_and_reserved_unchanged_after_a_rate_change(
        tmp_path, monkeypatch, conns, memory_budget):
    conns.set_rate(0.53)
    _install(tmp_path, monkeypatch, budget=25)
    bridge = _Bridge()
    await launch_pipeline("gpupack/sft", inputs={}, bridge=bridge, workspace_id="default")
    admission = bridge.calls[0]["admission"]
    leg = admission[COST_ESTIMATE_KEY]["legs"][0]
    assert {k: leg[k] for k in ("step", "priced", "connection", "hourly_usd", "hours")} == {
        "step": "train", "priced": True, "connection": BOX, "hourly_usd": 0.53, "hours": 12.0}
    assert leg["estimate_usd"] == pytest.approx(6.36)

    conns.set_rate(9.99)                          # the box's rate changes after launch
    rec = SimpleNamespace(admission=admission, governance={"series": "s-gpu", "budget": {"usd": 25}},
                          plan=bridge.calls[0]["plan"])
    kw = leg_price_kwargs(rec, "train", SimpleNamespace(started_at=None))
    assert kw["estimated_usd"] == pytest.approx(6.36) and kw["hourly_usd"] == 0.53

    _stub_flow(monkeypatch)
    seen = _ledger(monkeypatch)
    monkeypatch.setattr(LE, "_wall_now", _Clock(0, 1))
    out = await execute_leg("flow:gpupack/train", inputs={}, engine=_Engine("succeeded"), run_id="r1",
                            series_id="s-gpu", spent_usd=0.0, budget_usd=25.0, **kw)
    assert out.ok
    assert [r["est_usd"] for r in seen["reserve"]] == [pytest.approx(6.36)], "the recorded figure"
    assert seen["settle"] == [0.53], "settled at the recorded rate, not the new one"


async def test_a_pipeline_with_no_compute_target_records_nothing_new(tmp_path, monkeypatch, memory_budget):
    monkeypatch.setattr(LH, "compute_target_for_kind", lambda kind: "")
    _install(tmp_path, monkeypatch, budget=1)
    bridge = _Bridge()
    await launch_pipeline("gpupack/sft", inputs={}, bridge=bridge, workspace_id="default")
    assert COST_ESTIMATE_KEY not in bridge.calls[0]["admission"]


# ── D4: the gate carries the estimate ───────────────────────────────────────

def _rec_with_estimate(legs, *, total, steps=("approve-run", "train", "later-gate")):
    plan = SimpleNamespace(steps=[SimpleNamespace(id=s) for s in steps], loops={})
    return SimpleNamespace(admission={COST_ESTIMATE_KEY: {"estimate_usd": total, "legs": legs}},
                           plan=plan, governance={})


def test_only_the_gate_before_the_first_priced_leg_carries_the_estimate():
    leg = {"step": "train", "priced": True, "connection": BOX, "hourly_usd": 0.53, "hours": 12.0,
           "estimate_usd": 6.36, "basis": "$0.53/h × 12h on gpu-box = $6.36"}
    rec = _rec_with_estimate([leg], total=6.36)
    assert estimate_evidence(rec, "approve-run") == {
        "estimate_usd": 6.36, "estimate_basis": "train: $0.53/h × 12h on gpu-box = $6.36"}
    assert estimate_evidence(rec, "later-gate") == {}


def test_an_unpriced_box_reads_not_priced_on_the_gate():
    leg = {"step": "train", "priced": False, "connection": BOX,
           "basis": "not priced: connection 'gpu-box' sets no hourly_usd"}
    ev = estimate_evidence(_rec_with_estimate([leg], total=0.0), "approve-run")
    assert "estimate_usd" not in ev and "not priced" in ev["estimate_basis"]


async def test_the_bridge_gate_evidence_carries_the_estimate(monkeypatch):
    from cairn.core.dispatch.dispatcher import RunDispatcher
    from cairn.core.dispatch.pipeline_bridge import PipelineBridge
    from cairn.core.engine.pipeline import parse_pipeline_def, plan_pipeline
    from cairn.core.storage import build_storage
    from cairn.core.storage.pipeline_store import InMemoryPipelineStore
    from tests.core.test_trigger_server_dispatch import FakeRunner

    seen: list[dict] = []
    real = PipelineBridge._mark_awaiting

    async def _capture(self, rec, s, status, *, gate_ctx=None):
        seen.append(dict(gate_ctx or {}))
        return await real(self, rec, s, status, gate_ctx=gate_ctx)

    monkeypatch.setattr(PipelineBridge, "_mark_awaiting", _capture)
    plan = plan_pipeline(parse_pipeline_def(textwrap.dedent(_PIPE.format(budget=25, timeout=", timeout: 12h")),
                                            pack="gpupack", source="<test>"))
    leg = {"step": "train", "priced": True, "connection": BOX, "hourly_usd": 0.53, "hours": 12.0,
           "estimate_usd": 6.36, "basis": "$0.53/h × 12h on gpu-box = $6.36"}
    d = RunDispatcher(FakeRunner(), backend=build_storage())
    bridge = PipelineBridge(d, store=InMemoryPipelineStore())
    await bridge.start()
    try:
        rec = await bridge.start_pipeline(plan, inputs={}, admission={
            "verdict": "admitted", COST_ESTIMATE_KEY: {"estimate_usd": 6.36, "legs": [leg]}})
        for _ in range(200):
            if seen:
                break
            await asyncio.sleep(0.01)
        assert (await bridge.store.get(rec.pipeline_id)).step_state["approve-run"].status == "awaiting_approval"
    finally:
        bridge.stop()
    ev = seen[0]["evidence"]
    assert ev["estimate_usd"] == 6.36 and "$0.53/h × 12h" in ev["estimate_basis"]
    assert "cost_usd" in ev, "beside the #2358 spend line, not instead of it"


# ── D5: settle on every outcome ─────────────────────────────────────────────

@pytest.mark.parametrize("final", ["succeeded", "failed", "cancelled", "timeout"])
async def test_a_priced_leg_settles_its_wall_clock_on_every_outcome(monkeypatch, memory_budget, final):
    _stub_flow(monkeypatch)
    seen = _ledger(monkeypatch)
    monkeypatch.setattr(LE, "_wall_now", _Clock(0, 2))          # each attempt: 2 h
    engine = _Engine("running" if final == "timeout" else final)
    out = await execute_leg(
        "flow:gpupack/train", inputs={}, engine=engine, run_id="r1",
        series_id="s-gpu", estimated_usd=6.36, spent_usd=0.0, budget_usd=25.0,
        hourly_usd=0.53, poll_budget_s=0.05 if final == "timeout" else 3600.0)
    assert out.ok is (final == "succeeded")
    assert seen["settle"] == [1.06] and seen["release"] == [], "settled, never released as 'no spend'"
    assert out.gpu_usd == 1.06
    rec = flow_leg_record({}, out)
    assert rec["cost"]["gpu_usd"] == 1.06 and rec["cost"]["total_usd"] == 1.06
    assert rec["cost"]["llm_usd"] == 0.0, "box time never folds into the LLM bucket"
    assert await B.reserved_total("s-gpu") == 0.0


async def test_engine_reported_llm_cost_stays_in_llm_usd(monkeypatch, memory_budget):
    out = LegOutcome(state="succeeded", cost_usd=0.25, gpu_usd=1.06)
    cost = flow_leg_record({}, out)["cost"]
    assert cost == {"llm_usd": 0.25, "gpu_usd": 1.06, "storage_usd": 0.0, "total_usd": 1.31}


async def test_an_unpriced_failed_leg_still_releases(monkeypatch, memory_budget):
    _stub_flow(monkeypatch)
    seen = _ledger(monkeypatch)
    out = await execute_leg("flow:gpupack/train", inputs={}, engine=_Engine("failed"), run_id="r1",
                            series_id="s-gpu", estimated_usd=1.0, spent_usd=0.0, budget_usd=25.0)
    assert not out.ok and out.gpu_usd == 0.0
    assert seen["settle"] == [] and len(seen["release"]) == 1


async def test_a_retried_priced_leg_settles_each_attempt_once(monkeypatch, memory_budget):
    """Fails once (retriable), succeeds on retry: two windows settled, the total is
    their sum, the retry's reservation counts the first window, nothing twice."""
    _stub_flow(monkeypatch)
    seen = _ledger(monkeypatch)
    # attempt 1: 0 → 1 h; attempt 2: 1.5 → 4.5 h
    monkeypatch.setattr(LE, "_wall_now", _Clock(0, 1, 1.5, 4.5))
    out = await execute_leg(
        "flow:gpupack/train", inputs={}, engine=_Engine("failed", "succeeded"), run_id="r1",
        series_id="s-gpu", estimated_usd=2.0, spent_usd=0.5, budget_usd=25.0, hourly_usd=0.5,
        retry=RetryPolicy(maximum_attempts=2, initial_interval_seconds=0.0))
    assert out.ok
    assert seen["settle"] == [0.5, 1.5], "one settle per attempt window"
    assert [r["spent_usd"] for r in seen["reserve"]] == [0.5, 1.0], "the retry counts attempt 1"
    assert out.gpu_usd == 2.0
    assert flow_leg_record({}, out)["cost"]["gpu_usd"] == 2.0


async def test_an_adopted_leg_after_a_restart_settles_from_run_started_at(monkeypatch, memory_budget):
    _stub_flow(monkeypatch)
    seen = _ledger(monkeypatch)
    monkeypatch.setattr(LE, "_wall_now", lambda: T0 + timedelta(hours=3))
    run = SimpleNamespace(started_at=T0.isoformat())
    rec = SimpleNamespace(governance={}, plan=None, admission={COST_ESTIMATE_KEY: {
        "estimate_usd": 6.36, "legs": [{"step": "train", "priced": True, "connection": BOX,
                                        "hourly_usd": 0.5, "hours": 12.0, "estimate_usd": 6.0}]}})
    kw = leg_price_kwargs(rec, "train", run)
    assert kw["adopted_started_at"] == run.started_at
    engine = _Engine("succeeded")
    out = await execute_leg("flow:gpupack/train", inputs={}, engine=engine, run_id="r1",
                            series_id="s-gpu", spent_usd=0.0, budget_usd=25.0,
                            adopt={"job_id": "old", "engine": "stub", "meta": {}}, **kw)
    assert out.ok and engine.submits == 0
    assert seen["reserve"] == [], "an adopted attempt was admitted before; it reserves nothing"
    assert out.gpu_usd == 1.5, "0.5 $/h × (now − run.started_at) = 3 h"
    assert flow_leg_record({}, out)["cost"]["gpu_usd"] == 1.5


# ── twins: the bridge and the Temporal activity make the same call ──────────

def _priced_record(**kw):
    from cairn.core.engine.bundle_pipeline import PipelinePlan, PipelinePlanStep
    from cairn.core.storage.pipeline_store import PipelineRecord, StepState

    return PipelineRecord(
        pipeline_id="p1", workspace_id="default", status="running",
        governance={"series": "s-gpu", "budget": {"usd": 25}, "estimated_usd": 99.0},
        plan=PipelinePlan(name="sft", steps=[PipelinePlanStep(id="train", ref="flow:gpupack/train",
                                                               timeout_s=43200.0)]),
        admission={"verdict": "admitted", COST_ESTIMATE_KEY: {"estimate_usd": 6.36, "legs": [
            {"step": "train", "priced": True, "connection": BOX, "hourly_usd": 0.53,
             "hours": 12.0, "estimate_usd": 6.36}]}},
        step_state={"train": StepState(**kw)},
    )


async def _bridge_kwargs(monkeypatch, rec, run):
    from cairn.core.dispatch import pipeline_bridge as PB
    from cairn.core.dispatch.pipeline_bridge import PipelineBridge
    from cairn.core.storage.pipeline_store import InMemoryPipelineStore

    captured: dict = {}

    async def _fake(ref, **kw):
        captured.update(kw)
        return LegOutcome(state="succeeded")

    monkeypatch.setattr(PB, "execute_leg", _fake)
    monkeypatch.setattr(PipelineBridge, "_leg_engine", lambda self, s: object())

    class _RS:
        async def update(self, *a, **k):
            pass

        async def get(self, rid):
            return run

    async def _emit(*a, **k):
        return None

    async def _audit(*a, **k):
        return None

    bridge = PipelineBridge.__new__(PipelineBridge)
    bridge.store = InMemoryPipelineStore()
    await bridge.store.put(rec)
    bridge.dispatcher = SimpleNamespace(store=_RS(), _emit=_emit)
    bridge._bus = None
    bridge._audit = _audit
    bridge._checkpoint_store = lambda: None
    bridge._lock = None
    monkeypatch.setattr(PB, "pipeline_spent_usd", lambda *a, **k: _zero())
    await bridge._execute_and_settle_flow_leg(rec, rec.plan.steps[0], run, {}, adopt=None)
    return captured


async def _zero():
    return 0.0


async def _temporal_kwargs(monkeypatch, rec, run):
    pytest.importorskip("temporalio")
    from temporalio.testing import ActivityEnvironment

    import cairn.core.runtime.temporal.pipeline_activities as PA
    from cairn.core.storage.pipeline_store import InMemoryPipelineStore

    pstore = InMemoryPipelineStore()
    await pstore.put(rec)
    monkeypatch.setattr("cairn.core.storage.pipeline_store.default_pipeline_store", lambda: pstore)

    class _RS:
        async def get(self, rid):
            return run

        async def update(self, *a, **k):
            pass

        async def create(self, r):
            pass

    async def _emit(*a, **kw):
        return None

    async def _no_gate(*a, **kw):
        return None

    monkeypatch.setattr(PA, "_dispatcher", lambda: SimpleNamespace(store=_RS(), _emit=_emit))
    monkeypatch.setattr(PA, "_gate_flow_leg", _no_gate)
    monkeypatch.setattr(LE, "parse_flow_ref", lambda ref: None)
    monkeypatch.setattr("cairn.core.dataflow.engine_registry.resolve_engine", lambda *a, **kw: object())
    monkeypatch.setattr("cairn.core.storage.artifacts.build_artifact_store", lambda: None)
    captured: dict = {}

    async def _execute_leg(ref, **kw):
        captured.update(kw)
        return LegOutcome(state="succeeded")

    monkeypatch.setattr(LE, "execute_leg", _execute_leg)

    async def _spent(*a, **k):
        return 0.0

    monkeypatch.setattr(LH, "pipeline_spent_usd", _spent)
    await ActivityEnvironment().run(PA.pipeline_submit_step, "flow:gpupack/train", {}, "p1", "train", None,
                                    {"id": "train", "ref": "flow:gpupack/train", "timeout_s": 43200.0})
    return captured


async def test_both_owners_reserve_the_recorded_estimate_and_price_the_box(monkeypatch):
    run = SimpleNamespace(id="run-1", status="running", started_at=T0.isoformat(), inputs={})
    rec = _priced_record(status="running", run_id="run-1",
                         handle={"job_id": "j9", "engine": "stub", "meta": {}})
    bridge = await _bridge_kwargs(monkeypatch, rec, run)
    temporal = await _temporal_kwargs(monkeypatch, _priced_record(
        status="running", run_id="run-1", handle={"job_id": "j9", "engine": "stub", "meta": {}}), run)
    keys = ("estimated_usd", "hourly_usd", "adopted_started_at", "series_id", "budget_usd")
    want = {"estimated_usd": 6.36, "hourly_usd": 0.53, "adopted_started_at": T0.isoformat(),
            "series_id": "s-gpu", "budget_usd": 25.0}
    assert {k: bridge.get(k) for k in keys} == want
    assert {k: temporal.get(k) for k in keys} == want


def test_the_admission_key_is_the_one_the_code_writes():
    assert LH.COST_ESTIMATE_KEY == COST_ESTIMATE_KEY


# ── the Postgres reservation backend (live; skipped without PGSTORE_DSN) ────

async def test_live_pg_a_retried_priced_leg_settles_each_window_as_a_row(monkeypatch):
    """Same ledger, durable backend: each attempt's hold becomes a `settled` row with
    its own window as `actual_usd`; nothing is left held and nothing is `released`."""
    import uuid

    dsn = os.environ.get("PGSTORE_DSN")
    if not dsn:
        pytest.skip("PGSTORE_DSN not set — skipping live Postgres integration")
    asyncpg = pytest.importorskip("asyncpg")
    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=2)
    sid = f"t-gpu-{uuid.uuid4().hex[:12]}"
    try:
        monkeypatch.setattr(B, "_backend_instance", B._PostgresBackend(pool=pool))
        _stub_flow(monkeypatch)
        monkeypatch.setattr(LE, "_wall_now", _Clock(0, 1, 1.5, 4.5))
        out = await execute_leg(
            "flow:gpupack/train", inputs={}, engine=_Engine("failed", "succeeded"), run_id=f"r-{sid}",
            series_id=sid, estimated_usd=2.0, spent_usd=0.0, budget_usd=25.0, hourly_usd=0.5,
            retry=RetryPolicy(maximum_attempts=2, initial_interval_seconds=0.0))
        assert out.ok and out.gpu_usd == 2.0
        assert await B.reserved_total(sid) == 0.0
        async with pool.acquire() as c:
            rows = await c.fetch("SELECT state, actual_usd::float8 AS a FROM series_reservations "
                                 "WHERE series_id = $1 ORDER BY created_at", sid)
        assert [(r["state"], r["a"]) for r in rows] == [("settled", 0.5), ("settled", 1.5)]
    finally:
        await pool.close()
