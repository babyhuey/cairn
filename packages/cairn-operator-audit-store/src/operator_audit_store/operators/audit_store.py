"""audit/store — zero-dep hash-chained append-only audit sink (ADR-0028 builtin).

The storage lives HERE, in the operator pack — core holds only the ``AuditSink``
interface + ``AuditEvent`` + the emission seams (`cairn.core.obs.audit`). This
class is dual-purpose:

  - ``record(event)`` — the service path. ``get_audit_sink()`` resolves this
    operator by name (``audit/store``), instantiates it once, and the seams
    (LLM gateway / MCP boundary / action handler) call ``record`` per event.
  - ``run(inp, ctx)``  — the node path, so a template ``operation`` can write an
    audit row explicitly.

Tamper-evidence by hash-chain: each row carries ``prev_hash`` + ``hash`` where
``hash = sha256(prev_hash + canonical(event))``. Altering any row breaks every
later link. We reuse the ``events.jsonl`` file convention (ADR-0028: do NOT
build a new audit store); richer sinks (`operator-audit-otel`, `-immudb`,
`-siem`) ship later and swap the binding.
"""
from __future__ import annotations
from cairn.core.operator import getenv

import asyncio
import json
import os
from datetime import datetime, timezone
from pathlib import Path

from cairn.core.operator import AuditEvent, GENESIS_HASH, chain_hash
from cairn.core.operator import Operation, OperationContext, ReportSpec, register_operator

from operator_audit_store.schemas.audit_store import (
    AuditWriteInput,
    AuditWriteOutput,
)

def _default_path() -> Path:
    """Override with ``CAIRN_AUDIT_LOG``; defaults to ``audit.jsonl`` (CWD)."""
    return Path(getenv("CAIRN_AUDIT_LOG", "audit.jsonl"))


class _HashChainLog:
    """Append-only JSONL with a sha256 hash-chain over canonicalized rows.

    On construction the chain head is recovered from the last row of an
    existing file, so the chain continues unbroken across process restarts.
    Writes are serialized with an ``asyncio.Lock`` so concurrent seams cannot
    interleave and corrupt the chain.
    """

    def __init__(self, path: Path) -> None:
        self._path = path
        self._lock = asyncio.Lock()
        self._prev = self._recover_prev()

    def _recover_prev(self) -> str:
        try:
            if not self._path.exists():
                return GENESIS_HASH
            last = GENESIS_HASH
            with self._path.open("r") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        last = json.loads(line).get("hash", last)
                    except Exception:  # noqa: BLE001 — skip a corrupt row, keep going
                        continue
            return last
        except Exception:  # noqa: BLE001 — unreadable file → start a fresh chain
            return GENESIS_HASH

    def _write_row(self, row: dict) -> None:
        """Blocking file append — run via asyncio.to_thread so the audit write
        (fired on every governed op) never stalls the event loop (#86)."""
        self._path.parent.mkdir(parents=True, exist_ok=True)
        with self._path.open("a") as f:
            f.write(json.dumps(row, default=str) + "\n")

    async def append(self, payload: dict) -> str:
        async with self._lock:
            h = chain_hash(self._prev, payload)
            row = {**payload, "prev_hash": self._prev, "hash": h}
            await asyncio.to_thread(self._write_row, row)
            self._prev = h
            return h


@register_operator(
    name="audit/store",
    version="0.1.0",
    capabilities=["audit.sink"],
    implements=["contract://audit.sink/1.0.0"],  # #2242 V1-B — cairn-contracts-audit
)
class AuditStoreOp(Operation[AuditWriteInput, AuditWriteOutput]):
    """Hash-chained append-only audit sink. Backs the substrate ``audit.sink``."""

    input_schema = AuditWriteInput
    output_schema = AuditWriteOutput
    supports_mock = True
    # ADR-0103: one append per call, nothing long-running to report. Stated,
    # never left out — the worked example for the rest of the packs.
    reports = ReportSpec.NONE

    def __init__(self) -> None:
        # One log per instance; get_audit_sink() instantiates + caches once.
        self._log = _HashChainLog(_default_path())

    # ── service path (called by obs.audit.get_audit_sink) ──────────────
    async def record(self, event: AuditEvent) -> None:
        await self._log.append(event.to_dict())

    # ── node path (usable as a template `operation`) ──────────────────
    async def run(self, inp: AuditWriteInput, ctx: OperationContext) -> AuditWriteOutput:
        # stamp the active workspace so node-authored rows are workspace-scoped
        # too (#288) — the service path already stamps it via audit_emit.
        from cairn.core.operator import current_workspace

        event = AuditEvent(
            action=inp.action,
            outcome=inp.outcome,
            actor=inp.actor,
            target=inp.target,
            run_id=inp.run_id,
            workspace_id=current_workspace(),
            # #1233 — honor the schema's contract ("filled by the sink if
            # blank"): templates never pass ts, so node-authored rows landed
            # with ts="" and the console audit WHEN column rendered "—". The
            # service path (audit_emit) already stamps at emission.
            ts=inp.ts or datetime.now(timezone.utc).isoformat(),
            metadata=inp.metadata,
        )
        h = await self._log.append(event.to_dict())
        return AuditWriteOutput(ok=True, hash=h)

    def mock(self, inp: AuditWriteInput, ctx: OperationContext) -> AuditWriteOutput:
        return AuditWriteOutput(ok=True, hash="")
