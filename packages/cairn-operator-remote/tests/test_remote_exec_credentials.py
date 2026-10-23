"""remote/exec: the job's env file and the credential cancel reconnects with.

* ``runner.sh`` exports the job's environment (the spec, the job-channel token,
  forwarded settings). It is made owner-only BEFORE any of that is written, so
  another account on the box can never read it, even briefly.
* Submit authenticates with the compute.ssh connection's ``PRIVATE_KEY``. Cancel
  runs later with no operation context, and used to reconnect with
  ``SSH_PRIVATE_KEY`` from the control plane's env instead: with the documented
  setup it could not authenticate, said nothing, and the job kept running. The
  handle now names the connection, and cancel resolves the same key.

Hermetic: ``_connect`` and the connection lookup are monkeypatched.
"""
from __future__ import annotations

import logging

import pytest

import cairn.core.connections as connections
from cairn.core.operator import OperationContext
from cairn.core.operator.jobs import JobHandle
from operator_remote.operators import exec as remote_exec
from operator_remote.operators.exec import RemoteExecOp
from operator_remote.schemas.exec import RemoteExecInput


@pytest.fixture(autouse=True)
def _fresh(monkeypatch):
    monkeypatch.delenv("SSH_PRIVATE_KEY", raising=False)
    remote_exec._reset_conn_cache()
    yield
    remote_exec._reset_conn_cache()


def _ctx() -> OperationContext:
    ctx = OperationContext(pipeline_id="t", node_name="exec")
    ctx.compute = type("C", (), {
        "name": "gpu",
        "config": {"host": "box", "user": "ubuntu", "port": "22", "known_hosts": "/kh"},
        "secrets": {"PRIVATE_KEY": "CONN-KEY"},
    })()
    return ctx


class _Run:
    def __init__(self, stdout=""):
        self.stdout = stdout


class _Conn:
    def __init__(self, events: list):
        self.events = events

    async def run(self, cmd, check=False):
        self.events.append(("run", cmd))
        return _Run("")

    def start_sftp_client(self):
        events = self.events

        class _SFTP:
            async def __aenter__(s): return s
            async def __aexit__(s, *a): return False
            async def put(s, local, remote): events.append(("put", local))
            async def chmod(s, path, mode): events.append(("chmod", path, mode))

            def open(s, path, mode):
                events.append(("open", path, mode))

                class _F:
                    async def __aenter__(f): return f
                    async def __aexit__(f, *a): return False
                    async def write(f, data): events.append(("write", path, data))
                return _F()
        return _SFTP()


def _wire(monkeypatch) -> tuple[list, list]:
    events: list = []
    params_seen: list = []

    async def fake_connect(params):
        params_seen.append(params)
        return _Conn(events)
    monkeypatch.setattr(remote_exec, "_connect", fake_connect)
    return events, params_seen


def _bind_connection(monkeypatch, key="CONN-KEY", *, exists=True):
    seen: list = []

    async def fake_resolve(name, **_kw):
        seen.append(name)
        if not exists:
            return None
        return type("RC", (), {
            "name": name,
            "config": {"host": "box", "user": "ubuntu", "port": 22, "known_hosts": "/kh"},
            "secrets": {"PRIVATE_KEY": key},
        })()
    monkeypatch.setattr(connections, "resolve_connection", fake_resolve)
    return seen


async def test_runner_is_owner_only_before_anything_is_written(monkeypatch):
    events, _ = _wire(monkeypatch)
    await RemoteExecOp().submit(RemoteExecInput(command="run", env={"CAIRN_ENGINE_TOKEN": "tok"}), _ctx())
    runner = [e for e in events if len(e) > 1 and str(e[1]).endswith("runner.sh")]
    kinds = [e[0] for e in runner]
    assert kinds == ["open", "chmod", "write"], kinds
    assert runner[1][2] == 0o600
    # the value is in the file, not on any command line
    assert not any("tok" in e[1] for e in events if e[0] == "run")


async def test_submit_records_the_connection_it_used(monkeypatch):
    _wire(monkeypatch)
    h = await RemoteExecOp().submit(RemoteExecInput(command="run"), _ctx())
    assert h.meta["connection"] == "gpu"


async def test_cancel_reconnects_with_the_submitting_connections_key(monkeypatch):
    events, params_seen = _wire(monkeypatch)
    seen = _bind_connection(monkeypatch, key="CONN-KEY")
    monkeypatch.setenv("SSH_PRIVATE_KEY", "AMBIENT-KEY")
    h = JobHandle(job_id="box:/w", backend="ssh", meta={
        "host": "box", "user": "ubuntu", "port": 22, "workdir": "/w",
        "known_hosts": "/kh", "connection": "gpu"})
    await RemoteExecOp().cancel(h)
    assert seen == ["gpu"]
    assert params_seen[0]["private_key"] == "CONN-KEY"
    assert params_seen[0]["known_hosts"] == "/kh"
    assert any(e[0] == "run" and 'kill -TERM -"$p"' in e[1] for e in events)


async def test_cancel_of_a_handle_from_before_the_connection_was_recorded(monkeypatch):
    """A handle persisted by an earlier version names no connection: cancel keeps
    the old behaviour (the control plane's ``SSH_PRIVATE_KEY``)."""
    _, params_seen = _wire(monkeypatch)
    monkeypatch.setenv("SSH_PRIVATE_KEY", "AMBIENT-KEY")
    h = JobHandle(job_id="box:/w", backend="ssh",
                  meta={"host": "box", "user": "ubuntu", "port": 22, "workdir": "/w"})
    await RemoteExecOp().cancel(h)
    assert params_seen[0]["private_key"] == "AMBIENT-KEY"


async def test_cancel_with_an_unresolvable_connection_says_so(monkeypatch, caplog):
    _, params_seen = _wire(monkeypatch)
    _bind_connection(monkeypatch, exists=False)
    monkeypatch.setenv("SSH_PRIVATE_KEY", "AMBIENT-KEY")
    h = JobHandle(job_id="box:/w", backend="ssh", meta={
        "host": "box", "user": "ubuntu", "port": 22, "workdir": "/w", "connection": "gpu"})
    with caplog.at_level(logging.WARNING):
        assert await remote_exec._cancel(h) is False
    assert params_seen == []                 # never fell back to another credential
    assert "gpu" in caplog.text and "/w" in caplog.text
    assert "AMBIENT-KEY" not in caplog.text


async def test_cancel_that_cannot_authenticate_says_so(monkeypatch, caplog):
    _bind_connection(monkeypatch, key="CONN-KEY")

    async def fake_connect(params):
        raise PermissionError("Permission denied")
    monkeypatch.setattr(remote_exec, "_connect", fake_connect)
    h = JobHandle(job_id="box:/w", backend="ssh", meta={
        "host": "box", "user": "ubuntu", "port": 22, "workdir": "/w", "connection": "gpu"})
    with caplog.at_level(logging.WARNING):
        assert await remote_exec._cancel(h) is False
    assert "PermissionError" in caplog.text and "box" in caplog.text
    assert "CONN-KEY" not in caplog.text
