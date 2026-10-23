"""A ``dataset:`` ref resolves to the version's durable copy when it has one.

A version registered from a control-plane path keeps that path as its ``uri``
(provenance). When the bytes were also copied to the object store, the
registration carries ``durable_uri`` (reachability). A remote box can read only
the durable copy, and a control-plane path such as ``/tmp/…`` may already be
gone, so resolution hands out the durable copy whenever the workspace's own
registration has one, and the original location otherwise.

Each workspace reads its own registration (ADR-0148): another workspace's
durable copy of the same bytes is never handed out.

Hermetic: sqlite in-memory, no network (the byte relay is a stub).
"""
from __future__ import annotations

import pytest

import cairn.core.storage.artifacts as A
from cairn.core.dataflow.contract import ArtifactDescriptor
from cairn.core.storage.artifacts import (
    SqliteArtifactStore,
    resolve_dataset_ref,
    resolve_dataset_refs_in_config,
)
from cairn.core.workspace import workspace_scope

LOCAL = "file:///tmp/cairn-dataflow-x/splits/train.jsonl"
DURABLE = "s3://bucket/artifacts/hash-1/train.jsonl"


def _desc(h: str, uri: str) -> ArtifactDescriptor:
    return ArtifactDescriptor(content_hash=h, kind="dataset", uri=uri)


@pytest.fixture()
async def store(monkeypatch):
    monkeypatch.delenv("CAIRN_JWT_SECRET", raising=False)
    monkeypatch.delenv("CAIRN_WORKSPACE", raising=False)
    s = SqliteArtifactStore(":memory:")
    yield s
    await s.close()


def _relay_to(monkeypatch, target: str) -> None:
    async def _relay(uri, content_hash):
        return target

    monkeypatch.setattr(A, "_relay_bytes", _relay)


async def test_a_version_with_a_durable_copy_resolves_to_it(store, monkeypatch):
    _relay_to(monkeypatch, DURABLE)
    await store.register(_desc("hash-1", LOCAL), workspace_id="default")
    await store.register_dataset_version("train", "hash-1")

    out = await resolve_dataset_ref(store, "dataset:train@1")
    assert out["uri"] == DURABLE
    assert out["content_hash"] == "hash-1"
    cfg = await resolve_dataset_refs_in_config(store, {"dataset_uri": "dataset:train",
                                                       "sources": ["dataset:train@1"]})
    assert cfg["dataset_uri"] == f"{DURABLE}#sha256=hash-1"
    assert cfg["sources"] == [f"{DURABLE}#sha256=hash-1"]
    # the registration itself is unchanged: where the bytes were made stays recorded
    rec = await store.get("hash-1")
    assert (rec.uri, rec.durable_uri) == (LOCAL, DURABLE)


async def test_a_version_without_a_durable_copy_resolves_to_its_location(store, monkeypatch):
    _relay_to(monkeypatch, "")
    await store.register(_desc("hash-1", LOCAL), workspace_id="default")
    await store.register_dataset_version("train", "hash-1")
    assert (await resolve_dataset_ref(store, "dataset:train"))["uri"] == LOCAL


async def test_a_durable_copy_filled_by_a_later_registration_is_used(store, monkeypatch):
    """The 2026-10-22 shape: the bytes were first registered with no object store
    configured, so the first registration kept only the local path. A later
    registration of the same bytes with the store on fills the durable copy, and
    resolution then hands that copy out."""
    _relay_to(monkeypatch, "")
    await store.register(_desc("hash-1", LOCAL), workspace_id="default")
    await store.register_dataset_version("train", "hash-1")
    _relay_to(monkeypatch, DURABLE)
    await store.register(_desc("hash-1", LOCAL), workspace_id="default")
    assert (await resolve_dataset_ref(store, "dataset:train"))["uri"] == DURABLE


async def test_another_workspaces_durable_copy_is_not_handed_out(store, monkeypatch):
    """Workspace scoping holds: ws-b registered the same bytes with no durable
    copy of its own, so it resolves its own location, not ws-a's copy."""
    _relay_to(monkeypatch, DURABLE)
    await store.register(_desc("hash-1", LOCAL), workspace_id="ws-a")
    _relay_to(monkeypatch, "")
    await store.register(_desc("hash-1", "file:///srv/b/train.jsonl"), workspace_id="ws-b")
    with workspace_scope("ws-a"):
        await store.register_dataset_version("train", "hash-1")
        assert (await resolve_dataset_ref(store, "dataset:train"))["uri"] == DURABLE
    with workspace_scope("ws-b"):
        await store.register_dataset_version("train", "hash-1")
        assert (await resolve_dataset_ref(store, "dataset:train"))["uri"] == "file:///srv/b/train.jsonl"
