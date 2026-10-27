"""#230 — `cairn remote` renews its sign-in on a 401, or says once that it cannot.

`cairn remote login` keeps the refresh credential the server issues on `POST /token`
(the renewable sign-in, ADR-0142) in the profile, beside the access token, in a 0600
file. A verb that meets a 401 trades it at `POST /auth/refresh` for a new access token,
saves both, and retries once. When it cannot renew (no refresh credential, or the server
refuses it), it prints one line — `session expired at HH:MM — run cairn remote login` —
and exits 77, instead of a watch going quiet.

The end-to-end tests drive a real app with auth on through the CLI's HTTP seams, as
`test_remote.py` does; the watch test uses a fake client.
"""
from __future__ import annotations

import os
import stat
import time
import types
from datetime import timedelta

os.environ.setdefault("OPENAI_API_KEY", "sk-test-dummy")

import pytest  # noqa: E402
from typer.testing import CliRunner  # noqa: E402

from cairn.cli.commands import remote  # noqa: E402

cli = CliRunner()
SECRET = "test-secret-long-enough-for-hs256-abc"


@pytest.fixture()
def app(tmp_path, monkeypatch):
    """A real app, auth on, env admin `admin`/`pw`, every store in tmp; the CLI's
    two HTTP seams pointed at it and its config in tmp."""
    import httpx
    from cairn_client import CairnClient
    from fastapi.testclient import TestClient

    from cairn.core import account as A
    from cairn.core import refresh_tokens as RT
    from cairn.core import workspace as _ws
    from cairn.core.triggers import auth
    from cairn.core.triggers.server import build_app

    for name in ("JWT_SECRET", "ACCESS_TOKEN_EXPIRE_MINUTES", "API_USER", "API_PASSWORD",
                 "API_PASSWORD_HASH", "PUBLIC_URL", "REMOTE_ENDPOINT", "REMOTE_TOKEN"):
        monkeypatch.delenv(f"CAIRN_{name}", raising=False)
        monkeypatch.delenv(f"OBS_{name}", raising=False)
    monkeypatch.setenv("CAIRN_JWT_SECRET", SECRET)
    monkeypatch.setenv("CAIRN_API_USER", "admin")
    monkeypatch.setenv("CAIRN_API_PASSWORD", "pw")
    monkeypatch.setenv("CAIRN_CONFIG_HOME", str(tmp_path / "cfg"))
    monkeypatch.setenv("CAIRN_RUN_DB", str(tmp_path / "r.db"))
    monkeypatch.setattr(A, "_store", A.SqliteAccountStore(path=str(tmp_path / "a.db")))
    monkeypatch.setattr(_ws, "_store", _ws.SqliteWorkspaceStore(path=str(tmp_path / "w.db")))
    monkeypatch.setattr(RT, "_default_store", RT.SqliteRefreshTokenStore(path=str(tmp_path / "rt.db")))
    monkeypatch.setattr(auth, "_login_failures", {})
    monkeypatch.setattr(remote, "_TOKEN_PROFILE", {})

    class FakeRunner:
        default_environment = "production"
        skill_engine = types.SimpleNamespace(list_skills=lambda: [])

        async def run(self, *a, **k):
            return {}

    application = build_app(FakeRunner(), enable_scheduler=False)

    def _api(endpoint, token, *, timeout=30.0):
        return CairnClient("http://testserver", token=token, timeout=timeout,
                           transport=TestClient(application)._transport)

    def _sse(endpoint, token, *, timeout=30.0):
        headers = {"Authorization": f"Bearer {token}"} if token else {}
        return httpx.Client(base_url="http://testserver", headers=headers, timeout=timeout,
                            transport=TestClient(application)._transport)

    monkeypatch.setattr(remote, "_api", _api)
    monkeypatch.setattr(remote, "_sse_client", _sse)
    return application


def _expired_token(minutes_ago: int = 5) -> str:
    from cairn.core.auth_tokens import create_access_token

    return create_access_token({"sub": "admin", "account_id": "default"},
                               expires_delta=timedelta(minutes=-minutes_ago))


def _offline_config(tmp_path, monkeypatch) -> None:
    """A config in tmp and no endpoint/token from the environment."""
    for name in ("REMOTE_ENDPOINT", "REMOTE_TOKEN"):
        monkeypatch.delenv(f"CAIRN_{name}", raising=False)
        monkeypatch.delenv(f"OBS_{name}", raising=False)
    monkeypatch.setenv("CAIRN_JWT_SECRET", SECRET)
    monkeypatch.setenv("CAIRN_CONFIG_HOME", str(tmp_path))
    monkeypatch.setattr(remote, "_TOKEN_PROFILE", {})


def _login() -> dict:
    res = cli.invoke(remote.app, ["login", "-u", "admin", "-p", "pw", "-e", "http://testserver"])
    assert res.exit_code == 0, res.output
    return remote._load_config()["profiles"]["default"]


def _set_profile(**fields) -> None:
    cfg = remote._load_config()
    cfg["profiles"]["default"].update(fields)
    for k in [k for k, v in cfg["profiles"]["default"].items() if v is None]:
        del cfg["profiles"]["default"][k]
    remote._save_config(cfg)


def test_login_keeps_the_refresh_credential_in_a_private_file(app):
    prof = _login()
    assert prof["token"] and prof["refresh_token"]
    mode = stat.S_IMODE(os.stat(remote._config_path()).st_mode)
    assert mode == 0o600


def test_a_401_renews_the_session_and_saves_it(app):
    prof = _login()
    stale = _expired_token()
    _set_profile(token=stale)

    res = cli.invoke(remote.app, ["ps"])
    assert res.exit_code == 0, res.output
    assert "no runs" in res.output

    renewed = remote._load_config()["profiles"]["default"]
    assert renewed["token"] != stale and remote._token_exp(renewed["token"]) > time.time()
    assert renewed["refresh_token"] != prof["refresh_token"]  # rotated, as the server does
    # the renewed session works on its own in the next command
    assert cli.invoke(remote.app, ["ps"]).exit_code == 0
    assert renewed["token"] not in res.output and renewed["refresh_token"] not in res.output


def test_a_session_that_cannot_renew_says_so_and_exits_77(app):
    _login()
    stale = _expired_token(minutes_ago=7)
    _set_profile(token=stale, refresh_token=None)
    exp = remote._token_exp(stale)

    res = cli.invoke(remote.app, ["ps"])
    assert res.exit_code == remote.EXIT_SESSION_EXPIRED == 77
    line = f"session expired at {time.strftime('%H:%M', time.localtime(exp))} — run `cairn remote login`"
    assert line in res.output
    assert stale not in res.output


def test_a_refused_refresh_credential_ends_the_same_way(app):
    _login()
    _set_profile(token=_expired_token(), refresh_token="not-a-real-refresh-token")
    res = cli.invoke(remote.app, ["findings", "pl_x"])
    assert res.exit_code == 77, res.output
    assert "run `cairn remote login`" in res.output


def test_an_explicit_token_is_never_swapped_for_the_profile_session(app):
    _login()
    res = cli.invoke(remote.app, ["ps", "--token", _expired_token()])
    assert res.exit_code == 77, res.output


def test_whoami_shows_the_expiry_and_whether_it_renews(app):
    _login()
    res = cli.invoke(remote.app, ["whoami"])
    assert res.exit_code == 0, res.output
    assert "admin" in res.output and "expires" in res.output and "renewable: yes" in res.output


def test_a_session_renewed_by_another_process_is_adopted_without_a_refresh(tmp_path, monkeypatch):
    """A long `--watch` and a short `findings` share one profile: the first to meet
    the 401 renews (rotating the refresh credential), the other adopts what it saved."""
    from cairn.core.auth_tokens import create_access_token

    _offline_config(tmp_path, monkeypatch)
    stale = _expired_token()
    fresh = create_access_token({"sub": "admin"}, expires_delta=timedelta(minutes=30))
    remote._save_config({"default_profile": "default", "profiles": {"default": {
        "endpoint": "http://e", "token": stale, "refresh_token": "r1"}}})
    assert remote._resolve(None, None, None) == ("http://e", stale)
    remote._save_config({"default_profile": "default", "profiles": {"default": {
        "endpoint": "http://e", "token": fresh, "refresh_token": "r2"}}})

    def _no_server(*a, **k):
        raise AssertionError("must not call the server")

    monkeypatch.setattr(remote, "_api", _no_server)
    assert remote._renew("http://e", stale) == fresh


def test_launch_watch_renews_mid_poll(tmp_path, monkeypatch):
    """`launch --watch` polls `GET /pipelines/{id}`; a 401 mid-watch renews and the
    watch keeps reporting instead of exiting."""
    from cairn_client import CairnAPIError

    from cairn.core.auth_tokens import create_access_token

    _offline_config(tmp_path, monkeypatch)
    stale = _expired_token()
    fresh = create_access_token({"sub": "admin"}, expires_delta=timedelta(minutes=30))
    remote._save_config({"default_profile": "default", "profiles": {"default": {
        "endpoint": "http://e", "token": stale, "refresh_token": "r1"}}})
    _, tok = remote._resolve(None, None, None)

    class _Client:
        base_url = "http://e"

        def __init__(self, token=None):
            self.token, self.refresh_token = token, None

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return None

        def refresh(self, raw=None):
            assert raw == "r1"
            self.refresh_token = "r2"
            return fresh

        def pipeline(self, pid):
            if self.token != fresh:
                raise CairnAPIError(401, "token expired", "get", f"/pipelines/{pid}")
            return {"pipeline_id": pid, "status": "running", "step_state": {}}

    monkeypatch.setattr(remote, "_api", lambda ep, token, timeout=30.0: _Client(token))
    c = _Client(tok)
    assert remote._poll_pipeline(c, "pl_1")["status"] == "running"
    assert c.token == fresh
    saved = remote._load_config()["profiles"]["default"]
    assert saved["token"] == fresh and saved["refresh_token"] == "r2"
