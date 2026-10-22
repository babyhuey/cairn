"""ADR-0142 — a pipeline input may declare the role (and require the intake verdict) of the
dataset version it takes; the launch door refuses a version that does not match, and records
every check. Hermetic: sqlite, no policies."""
from __future__ import annotations

import os

import pytest

os.environ.setdefault("OPENAI_API_KEY", "sk-test-dummy")

from cairn.core.admission_door import decide_admission
from cairn.core.dataflow.contract import ArtifactDescriptor
from cairn.core.engine.pipeline import PipelineDefError, dataset_expectations, parse_pipeline_def
from cairn.core.storage.artifacts import SqliteArtifactStore

_PIPELINE = """
name: sft
kind: pipeline
inputs:
  train_version: {type: str, role: training, verdict: required}
  val_version: {type: str, role: Validation}
  test_version: {type: str, role: test}
  base_model: {type: str, default: m}
steps:
  - gate: approve
    approvers: owners
"""


def test_the_declarations_are_read_and_normalised():
    d = parse_pipeline_def(_PIPELINE, pack="finetune")
    assert dataset_expectations(d) == {
        "train_version": {"role": "training", "verdict": "required"},
        "val_version": {"role": "validation"},
        "test_version": {"role": "test"},
    }


@pytest.mark.parametrize("spec, message", [
    ("{type: str, role: train}", "unknown dataset role"),
    ("{type: str, verdict: optional}", "may only be 'required'"),
])
def test_a_typo_fails_when_the_pipeline_loads(spec, message):
    text = _PIPELINE.replace("{type: str, role: test}", spec)
    with pytest.raises(PipelineDefError, match=message):
        parse_pipeline_def(text, pack="finetune")


@pytest.fixture
async def store(tmp_path):
    s = SqliteArtifactStore(str(tmp_path / "a.db"))
    for name, role in (("demo-train", "training"), ("demo-val", "validation"), ("demo-test", "test"),
                       ("demo-unjudged", "training")):
        h = f"h-{name}"
        await s.register(ArtifactDescriptor(uri=f"file:///d/{name}.jsonl", kind="dataset", content_hash=h),
                         producer_run_id="prep")
        await s.register_dataset_version(name, h, actor="prep", role=role)
        if name != "demo-unjudged":
            await s.set_dataset_version_verdict(name, 1, verdict="admitted", reasons=[], report={})
    yield s
    await s.close()


_GOOD = {"train_version": "dataset:demo-train@1", "val_version": "dataset:demo-val@1",
         "test_version": "dataset:demo-test@1", "base_model": "m"}


async def _decide(store, **over):
    want = dataset_expectations(parse_pipeline_def(_PIPELINE, pack="finetune"))
    return await decide_admission({**_GOOD, **over}, workspace_id="default", store=store, policies=[],
                                  expectations=want)


async def test_the_right_versions_are_admitted_and_every_check_is_recorded(store):
    decision = await _decide(store)
    assert not decision.refused, decision.refusing_reasons
    assert [(e["input"], e["role"], e["ok"]) for e in decision.as_record()["expectations"]] == [
        ("test_version", "test", True), ("train_version", "training", True), ("val_version", "validation", True)]


@pytest.mark.parametrize("over, reason", [
    ({"train_version": "dataset:demo-test@1"}, "is a test version; this input takes a training one"),
    ({"val_version": "dataset:demo-train@1"}, "is a training version; this input takes a validation one"),
    ({"train_version": "dataset:demo-unjudged@1"}, "has no intake verdict; this input requires one"),
    ({"train_version": "/data/train.jsonl"}, "is not a dataset: ref"),
    ({"test_version": "dataset:nobody@1"}, "is not registered"),
])
async def test_a_version_that_does_not_match_refuses_the_launch(store, over, reason):
    decision = await _decide(store, **over)
    assert decision.refused
    assert any(reason in r for r in decision.refusing_reasons), decision.refusing_reasons
    assert not all(e["ok"] for e in decision.expectations)


async def test_a_pipeline_that_declares_nothing_is_decided_as_before(store):
    decision = await decide_admission({**_GOOD, "train_version": "dataset:demo-test@1"}, workspace_id="default",
                                      store=store, policies=[])
    assert decision.expectations == [] and not decision.refused


async def test_the_launch_door_refuses_before_any_record_exists(store, monkeypatch):
    """Through `launch_pipeline_def`: the declarations reach the door, and a refusal leaves the
    bridge untouched."""
    import cairn.core.storage.artifacts as artifacts
    from cairn.core.admission_door import AdmissionRefusedError
    from cairn.core.dispatch.pipeline_launch import launch_pipeline_def

    monkeypatch.setattr(artifacts, "build_artifact_store", lambda *a, **k: store)

    class _Bridge:
        started = False

        async def start_pipeline(self, *a, **k):
            _Bridge.started = True

    d = parse_pipeline_def(_PIPELINE, pack="finetune")
    with pytest.raises(AdmissionRefusedError, match="is a test version; this input takes a training one"):
        await launch_pipeline_def(d, inputs={**_GOOD, "train_version": "dataset:demo-test@1"}, bridge=_Bridge())
    assert _Bridge.started is False
