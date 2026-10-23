"""Artifact tags and artifact reads are scoped to the workspace.

A tag is keyed by ``(workspace_id, name)``: each workspace has its own ``prod``.
An artifact is visible to the workspaces that registered it (``artifact_owners``):
identical bytes registered by two workspaces stay one content-addressed row with
one owner row per workspace, and each sees it only through its own ownership.
``get``, ``list_artifacts``, ``find_by_uri``, ``lineage``, ``resolve`` and
``promote`` take the workspace from ``workspace_id=`` or the scope; with auth on
and neither they raise ``WorkspaceScopeRequired``. Single-workspace deployments,
where everything is ``default``, behave as before. An existing database migrates:
tags move to ``default``; every artifact is owned by ``default``, by the workspace
of the run that produced it, and by any workspace that named it as a dataset
version.

The stores run on an in-memory SQLite database, a SQLite file, and Postgres when
``PGSTORE_DSN`` is set.
"""
from __future__ import annotations

import os
import sqlite3

os.environ.setdefault("OPENAI_API_KEY", "sk-test-dummy")

import pytest

from cairn.core.dataflow.contract import ArtifactDescriptor
from cairn.core.storage.artifacts import PostgresArtifactStore, SqliteArtifactStore
from cairn.core.workspace import WorkspaceScopeRequired, workspace_scope

_TABLES = ("artifact_owners", "artifact_tags", "artifact_parents", "artifacts",
           "dataset_versions", "dataset_meta", "runs")


def _desc(h: str, uri: str = "", parents: tuple[str, ...] = (), **stats) -> ArtifactDescriptor:
    return ArtifactDescriptor(content_hash=h, kind="dataset", uri=uri or f"s3://bkt/{h}.jsonl",
                              parents=list(parents), stats=stats)


def _pg_dsn() -> str:
    dsn = os.environ.get("PGSTORE_DSN")
    if not dsn:
        pytest.skip("PGSTORE_DSN not set — skipping live Postgres integration")
    try:
        import asyncpg  # noqa: F401
    except ImportError:
        pytest.skip("storage-postgres optional dep group not installed")
    return dsn


async def _pg_reset(dsn: str) -> None:
    import asyncpg  # noqa: PLC0415

    conn = await asyncpg.connect(dsn)
    try:
        for t in _TABLES:
            await conn.execute(f"DROP TABLE IF EXISTS {t} CASCADE")
    finally:
        await conn.close()


@pytest.fixture(autouse=True)
def _auth_off(monkeypatch):
    monkeypatch.delenv("CAIRN_JWT_SECRET", raising=False)
    monkeypatch.delenv("CAIRN_WORKSPACE", raising=False)


@pytest.fixture(params=["memory", "sqlite", "postgres"])
async def store(request, tmp_path):
    if request.param == "memory":
        s = SqliteArtifactStore(":memory:")
    elif request.param == "sqlite":
        s = SqliteArtifactStore(str(tmp_path / "artifacts.db"))
    else:
        dsn = _pg_dsn()
        await _pg_reset(dsn)
        s = PostgresArtifactStore(dsn)
    await s.ensure_schema()
    yield s
    await s.close()


# ── reads ────────────────────────────────────────────────────────────────────


async def test_another_workspace_cannot_read_an_artifact(store):
    with workspace_scope("ws-a"):
        parent = await store.register(_desc("a-parent", "s3://a/raw.jsonl"), producer_run_id="run-a")
        child = await store.register(_desc("a-child", "s3://a/clean.jsonl", parents=(parent,)),
                                     producer_run_id="run-a")
        assert (await store.get(child)).parents == [parent]
        assert {r.id for r in await store.list_artifacts()} == {parent, child}
        assert await store.find_by_uri("s3://a/raw.jsonl") == parent
        assert [p["id"] for p in (await store.lineage(child))["parents"]] == [parent]
    with workspace_scope("ws-b"):
        assert await store.get(child) is None
        assert await store.list_artifacts() == []
        assert await store.list_artifacts(kind="dataset") == []
        assert await store.find_by_uri("s3://a/raw.jsonl") is None
        assert await store.lineage(child) is None
    # an explicit workspace is honoured the same way, whatever is in scope
    with workspace_scope("ws-a"):
        assert await store.get(child, workspace_id="ws-b") is None
        assert await store.list_artifacts(workspace_id="ws-b") == []
        assert await store.find_by_uri("s3://a/raw.jsonl", workspace_id="ws-b") is None
        assert await store.lineage(child, workspace_id="ws-b") is None
    assert (await store.get(child, workspace_id="ws-a")).id == child


async def test_lineage_and_parents_leave_out_another_workspaces_artifacts(store):
    with workspace_scope("ws-a"):
        theirs = await store.register(_desc("a-raw"), producer_run_id="run-a")
    with workspace_scope("ws-b"):
        mine = await store.register(_desc("b-raw"), producer_run_id="run-b")
        child = await store.register(_desc("b-child", parents=(theirs, mine, "s3://ext/raw.jsonl")),
                                     producer_run_id="run-b")
        rec = await store.get(child)
        # an external URI and the workspace's own parent stay; the other workspace's id does not
        assert sorted(rec.parents) == sorted([mine, "s3://ext/raw.jsonl"])
        tree = await store.lineage(child)
        assert [p["id"] for p in tree["parents"]] == [mine]


async def test_identical_bytes_are_visible_to_each_registering_workspace(store):
    with workspace_scope("ws-a"):
        first = await store.register(_desc("same-bytes", "s3://a/x.jsonl"), producer_run_id="run-a")
    with workspace_scope("ws-c"):
        assert await store.get(first) is None
    with workspace_scope("ws-b"):
        again = await store.register(_desc("same-bytes", "s3://b/x.jsonl"), producer_run_id="run-b")
    assert again == first
    for ws in ("ws-a", "ws-b"):
        with workspace_scope(ws):
            rec = await store.get(first)
            assert rec is not None and rec.producer_run_id == "run-a"  # one row, first producer kept
            assert [r.id for r in await store.list_artifacts()] == [first]
            assert (await store.lineage(first))["id"] == first
    with workspace_scope("ws-c"):
        assert await store.get(first) is None
        assert await store.list_artifacts() == []


async def test_a_dataset_version_names_only_an_artifact_its_workspace_registered(store):
    with workspace_scope("ws-a"):
        aid = await store.register(_desc("a-data"), producer_run_id="run-a")
        assert await store.register_dataset_version("facts", aid) == 1
    with workspace_scope("ws-b"):
        with pytest.raises(KeyError, match="not registered"):
            await store.register_dataset_version("facts", aid)
        assert await store.get_dataset_version("facts") is None


async def test_registry_health_counts_the_workspaces_own_artifacts(store):
    from cairn.core.storage.artifact_bytes import registry_health

    await store.register(_desc("h-a", "s3://a/x.jsonl"), workspace_id="ws-a")
    await store.register(_desc("h-b1", "s3://b/x.jsonl"), workspace_id="ws-b")
    await store.register(_desc("h-b2", "s3://b/y.jsonl"), workspace_id="ws-b")
    assert (await registry_health(store, workspace_id="ws-a"))["total"] == 1
    with workspace_scope("ws-b"):
        assert (await registry_health(store))["total"] == 2
    assert (await registry_health(store, workspace_id="default"))["total"] == 0


async def test_doctor_checks_the_process_workspace(tmp_path, monkeypatch):
    from cairn.cli.commands.doctor import check_artifact_bytes

    path = tmp_path / "doc.db"
    s = SqliteArtifactStore(str(path))
    await s.register(_desc("h-d", "s3://d/x.jsonl"), workspace_id="default")
    await s.register(_desc("h-t", "s3://t/x.jsonl"), workspace_id="ws-t")
    await s.close()
    monkeypatch.setenv("CAIRN_STORAGE", f"sqlite:///{path}")
    (res,) = [r for r in await check_artifact_bytes() if r.name == "artifact_registry_bytes"]
    assert "1 registered artifacts in workspace 'default'" in res.detail
    monkeypatch.setenv("CAIRN_WORKSPACE", "ws-t")
    (res,) = [r for r in await check_artifact_bytes() if r.name == "artifact_registry_bytes"]
    assert "in workspace 'ws-t'" in res.detail


# ── tags ─────────────────────────────────────────────────────────────────────


async def test_a_tag_belongs_to_its_workspace(store):
    with workspace_scope("ws-a"):
        a1 = await store.register(_desc("a1"), producer_run_id="run-a")
        await store.promote("dataset@prod", a1, actor="alice")
        assert (await store.resolve("dataset@prod")).id == a1
    with workspace_scope("ws-b"):
        assert await store.resolve("dataset@prod") is None
        with pytest.raises(KeyError, match="not registered"):
            await store.promote("dataset@prod", a1, actor="bob")
        with pytest.raises(KeyError):
            await store.promote("dataset@mine", a1, actor="bob")
        b1 = await store.register(_desc("b1"), producer_run_id="run-b")
        await store.promote("dataset@prod", b1, actor="bob")
        assert (await store.resolve("dataset@prod")).id == b1
    # the same name in two workspaces: neither moved the other's
    assert (await store.resolve("dataset@prod", workspace_id="ws-a")).id == a1
    assert (await store.resolve("dataset@prod", workspace_id="ws-b")).id == b1
    assert await store.resolve("dataset@mine", workspace_id="ws-b") is None


async def test_promote_with_an_explicit_workspace(store):
    aid = await store.register(_desc("x1"), producer_run_id="run-x", workspace_id="ws-x")
    rec = await store.promote("model@prod", aid, actor="ops", workspace_id="ws-x")
    assert rec.id == aid
    assert (await store.resolve("model@prod", workspace_id="ws-x")).id == aid
    assert await store.resolve("model@prod", workspace_id="default") is None


# ── auth on: nothing in scope is refused ─────────────────────────────────────


async def test_auth_on_with_no_workspace_in_scope_is_refused(store, monkeypatch):
    aid = await store.register(_desc("w1"), producer_run_id="run-w", workspace_id="ws-w")
    monkeypatch.setenv("CAIRN_JWT_SECRET", "s" * 40)
    calls = [
        lambda: store.register(_desc("w2")),
        lambda: store.get(aid),
        lambda: store.list_artifacts(),
        lambda: store.find_by_uri("s3://bkt/w1.jsonl"),
        lambda: store.lineage(aid),
        lambda: store.resolve("prod"),
        lambda: store.promote("prod", aid),
    ]
    for call in calls:
        with pytest.raises(WorkspaceScopeRequired):
            await call()
    # nothing was written by the refused register
    assert await store.get("w2", workspace_id="ws-w") is None
    # a scope or an explicit workspace is honoured
    with workspace_scope("ws-w"):
        assert (await store.get(aid)).id == aid
        await store.promote("prod", aid)
    assert (await store.resolve("prod", workspace_id="ws-w")).id == aid


# ── single-workspace deployments: unchanged ──────────────────────────────────


async def test_default_workspace_behaves_as_before(store):
    p = await store.register(_desc("d-parent", "s3://d/raw.jsonl"), producer_run_id="run-1")
    c = await store.register(_desc("d-child", "s3://d/clean.jsonl", parents=(p,)), producer_run_id="run-2")
    assert (await store.get(c)).parents == [p]
    assert {r.id for r in await store.list_artifacts()} == {p, c}
    assert await store.find_by_uri("s3://d/raw.jsonl") == p
    assert [n["id"] for n in (await store.lineage(c))["parents"]] == [p]
    await store.promote("prod", c, actor="me")
    assert (await store.resolve("prod")).id == c
    await store.promote("prod", p, actor="me")
    assert (await store.resolve("prod")).id == p
    assert await store.count() == 2
    with workspace_scope("default"):
        assert (await store.resolve("prod")).id == p


# ── migration of an existing database ────────────────────────────────────────

_OLD_SQLITE = [
    """CREATE TABLE artifacts (
        id TEXT PRIMARY KEY, kind TEXT NOT NULL, uri TEXT NOT NULL,
        stats TEXT, data_schema TEXT, size_bytes INTEGER, row_count INTEGER,
        producer_run_id TEXT, producer_job TEXT, account_id TEXT,
        created_at TEXT NOT NULL, durable_uri TEXT)""",
    "CREATE TABLE artifact_parents (child_id TEXT NOT NULL, parent_id TEXT NOT NULL, "
    "PRIMARY KEY (child_id, parent_id))",
    "CREATE TABLE artifact_tags (name TEXT PRIMARY KEY, artifact_id TEXT NOT NULL, "
    "updated_at TEXT NOT NULL, updated_by TEXT)",
    "CREATE TABLE dataset_versions (name TEXT NOT NULL, version INTEGER NOT NULL, "
    "artifact_id TEXT NOT NULL, created_at TEXT NOT NULL, created_by TEXT, "
    "workspace_id TEXT NOT NULL DEFAULT 'default', PRIMARY KEY (name, version))",
    "CREATE TABLE runs (id TEXT PRIMARY KEY, workspace_id TEXT NOT NULL DEFAULT 'default', "
    "data TEXT NOT NULL)",
]

_OLD_ROWS = [
    ("INSERT INTO artifacts (id, kind, uri, producer_run_id, created_at) VALUES "
     "('legacy-x', 'dataset', 's3://x/a.jsonl', 'run-x', '2026-01-01T00:00:00+00:00')"),
    ("INSERT INTO artifacts (id, kind, uri, producer_run_id, created_at) VALUES "
     "('legacy-d', 'dataset', 's3://d/a.jsonl', '', '2026-01-02T00:00:00+00:00')"),
    ("INSERT INTO artifacts (id, kind, uri, producer_run_id, created_at) VALUES "
     "('legacy-y', 'dataset', 's3://y/a.jsonl', 'run-gone', '2026-01-03T00:00:00+00:00')"),
    "INSERT INTO runs (id, workspace_id, data) VALUES ('run-x', 'ws-x', '{}')",
    ("INSERT INTO dataset_versions (name, version, artifact_id, created_at, workspace_id) VALUES "
     "('ys', 1, 'legacy-y', '2026-01-03T00:00:00+00:00', 'ws-y')"),
    ("INSERT INTO artifact_tags (name, artifact_id, updated_at, updated_by) VALUES "
     "('prod', 'legacy-x', '2026-01-04T00:00:00+00:00', 'ops')"),
]


async def _assert_migrated(s) -> None:
    # tags moved to `default`
    assert (await s.resolve("prod", workspace_id="default")).id == "legacy-x"
    assert await s.resolve("prod", workspace_id="ws-x") is None
    # `default` owns every existing artifact
    assert {r.id for r in await s.list_artifacts(workspace_id="default")} == {
        "legacy-x", "legacy-d", "legacy-y"}
    # the producing run's workspace, and a workspace that named it as a version, own it too
    assert [r.id for r in await s.list_artifacts(workspace_id="ws-x")] == ["legacy-x"]
    assert [r.id for r in await s.list_artifacts(workspace_id="ws-y")] == ["legacy-y"]
    assert (await s.get_dataset_version("ys", workspace_id="ws-y"))[1].id == "legacy-y"
    assert await s.list_artifacts(workspace_id="ws-other") == []


async def test_an_existing_sqlite_database_migrates(tmp_path):
    path = str(tmp_path / "old.db")
    conn = sqlite3.connect(path)
    for stmt in (*_OLD_SQLITE, *_OLD_ROWS):
        conn.execute(stmt)
    conn.commit()
    conn.close()

    s = SqliteArtifactStore(path)
    await _assert_migrated(s)
    pk = [r[1] for r in sorted((r for r in s._conn.execute("PRAGMA table_info(artifact_tags)")
                                if r[5]), key=lambda r: r[5])]
    assert pk == ["workspace_id", "name"]
    # after the migration, a new registration is owned only by its workspace …
    await s.register(_desc("new-z"), producer_run_id="run-z", workspace_id="ws-z")
    await s.close()
    # … and reopening the database does not migrate again
    s2 = SqliteArtifactStore(path)
    await _assert_migrated(s2)
    assert await s2.get("new-z", workspace_id="default") is None
    assert (await s2.get("new-z", workspace_id="ws-z")).id == "new-z"
    await s2.close()


async def test_an_existing_postgres_database_migrates():
    import asyncpg  # noqa: PLC0415

    dsn = _pg_dsn()
    await _pg_reset(dsn)
    conn = await asyncpg.connect(dsn)
    try:
        for stmt in (*_OLD_SQLITE, *_OLD_ROWS):
            await conn.execute(stmt)
    finally:
        await conn.close()

    s = PostgresArtifactStore(dsn)
    await _assert_migrated(s)
    await s.register(_desc("new-z"), producer_run_id="run-z", workspace_id="ws-z")
    s2 = PostgresArtifactStore(dsn)  # a second process starting on the migrated database
    await _assert_migrated(s2)
    assert await s2.get("new-z", workspace_id="default") is None
    assert (await s2.get("new-z", workspace_id="ws-z")).id == "new-z"
    conn = await asyncpg.connect(dsn)
    try:
        pk = await conn.fetchval(
            "SELECT string_agg(a.attname, ',' ORDER BY array_position(c.conkey, a.attnum)) "
            "FROM pg_constraint c JOIN pg_attribute a ON a.attrelid = c.conrelid "
            "AND a.attnum = ANY(c.conkey) WHERE c.conrelid = 'artifact_tags'::regclass "
            "AND c.contype = 'p'")
    finally:
        await conn.close()
    assert pk == "workspace_id,name"
