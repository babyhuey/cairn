"""OpenTelemetry tracing for the agent system.

Meta-observability: we instrument our own agent system with the same kind of
telemetry the system is designed to investigate. Every LLM call, every MCP tool
call, every graph node becomes a span.

Configure via env vars:
- OTEL_EXPORTER_OTLP_ENDPOINT: send to a collector (e.g. http://localhost:4318)
- OTEL_CONSOLE=1: print spans to stdout (for local dev)
- (neither set) → no-op tracer, zero overhead

Spans emitted:
- agent.<name>.handle — top-level agent invocation
- agent.<name>.<node> — each graph node
- llm.complete — every LLM call (model, tokens, latency, cost)
- mcp.<server>.<tool> — every MCP tool call
"""
from __future__ import annotations
from cairn.core.settings import getenv

import functools
import logging
import os
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any, Callable, Optional

#: #1465 — the cairn run this task is executing FOR, so LLM spans are joinable
#: to runs regardless of which backend (Langfuse / MLflow OTLP) receives them.
#: A contextvar so concurrent runs in ONE worker process don't cross-stamp;
#: falls back to the env the remote-job paths already export (CAIRN_RUN_ID on
#: the train-remote binding, CAIRN_PARENT_RUN on the dataflow/SSH path, #989).
_current_run: ContextVar[str] = ContextVar("cairn_run_id", default="")


def set_current_run(run_id: str):
    """Bind the ambient run id for this async context; returns the reset token."""
    return _current_run.set(run_id or "")


def current_run_id() -> str:
    """The run id LLM spans should carry: contextvar first, env chain second."""
    return (_current_run.get()
            or os.environ.get("CAIRN_RUN_ID", "").strip()
            or os.environ.get("CAIRN_PARENT_RUN", "").strip())

logger = logging.getLogger(__name__)

_tracer: Any = None


def init_tracing(service_name: str | None = None) -> None:
    """Initialize tracing if settings say so. Safe to call multiple times."""
    global _tracer

    if _tracer is not None:
        return

    from cairn.core.provider_settings import get_settings
    settings = get_settings()

    if not settings.tracing_enabled:
        return  # no exporter configured — stay no-op

    otlp_endpoint = settings.otel_exporter_otlp_endpoint
    console_export = settings.otel_console
    service_name = service_name or settings.otel_service_name

    try:
        from opentelemetry import trace
        from opentelemetry.sdk.resources import SERVICE_NAME, Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
    except ImportError:
        logger.warning("OpenTelemetry packages not installed — tracing disabled")
        return

    resource = Resource(attributes={SERVICE_NAME: service_name})
    provider = TracerProvider(resource=resource)

    if otlp_endpoint:
        try:
            from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
        except ImportError:
            logger.warning(
                "OTLP trace exporter not installed — tracing disabled; "
                "`pip install 'cairndev[telemetry]'` to enable it"
            )
            return
        provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=f"{otlp_endpoint}/v1/traces")))

    if console_export:
        provider.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))

    trace.set_tracer_provider(provider)
    _tracer = trace.get_tracer(service_name)

    # Auto-inject trace_id + span_id into every log record emitted inside a span.
    # Enables trace ↔ log correlation in Grafana/Tempo.
    try:
        from opentelemetry.instrumentation.logging import LoggingInstrumentor
        LoggingInstrumentor().instrument(set_logging_format=False)
    except ImportError:
        logger.warning("opentelemetry-instrumentation-logging not installed — log correlation disabled")

    logger.info("OpenTelemetry tracing initialized (otlp=%s, console=%s)", bool(otlp_endpoint), console_export)


# ─── OpenInference attribute mapping (#476, ADR-0059 §D4) ────────────
#
# Same spans, extra attributes — NOT a new exporter/pipeline. Any OTLP
# consumer that understands OpenInference semconv (Phoenix/Arize and other
# OpenInference-aware backends) can render these spans without cairn writing
# per-vendor integration code; consumers that don't recognize the extra
# attributes simply ignore them (existing GenAI-semconv attrs are untouched).
#
# Span-name → openinference.span.kind. Inferred from the *static* span name
# used at each call site (all are literal strings today: "llm.complete",
# "cairn.run", "cairn.operator") so no call-site changes are needed to get
# the kind mapping — this stays entirely inside telemetry.py.
_OPENINFERENCE_KIND_BY_SPAN_NAME: dict[str, str] = {
    "llm.complete": "LLM",
    "cairn.run": "CHAIN",
    "cairn.operator": "CHAIN",
}


def _openinference_kind(span_name: str) -> str | None:
    """Best-effort ``openinference.span.kind`` for a span name.

    Exact matches use the table above. Anything shaped like an MCP tool span
    (``mcp.<server>.<tool>``, per the module docstring) maps to ``TOOL`` even
    though no call site emits that span name yet (#476: ``record_mcp`` has no
    live caller — see ``core/obs/base.py``). Unknown names map to ``None`` so
    we never guess wrong."""
    if span_name in _OPENINFERENCE_KIND_BY_SPAN_NAME:
        return _OPENINFERENCE_KIND_BY_SPAN_NAME[span_name]
    if span_name.startswith("mcp."):
        return "TOOL"
    return None


def _openinference_attrs(name: str, attributes: dict[str, Any]) -> dict[str, Any]:
    """Map existing GenAI/cairn span attributes onto OpenInference semantic
    conventions, additively (#476). Pure attribute translation — reads values
    already present in ``attributes``, writes new keys alongside them. Never
    removes or overwrites the original GenAI-semconv attrs."""
    out: dict[str, Any] = {}
    kind = _openinference_kind(name)
    if kind is not None:
        out["openinference.span.kind"] = kind

    # input/output value — reuse the (already gated, #247) GenAI content
    # attrs when present; never re-derive or re-gate content here.
    if "gen_ai.prompt" in attributes:
        out["input.value"] = attributes["gen_ai.prompt"]
        out["input.mime_type"] = "application/json"
    if "gen_ai.completion" in attributes:
        out["output.value"] = attributes["gen_ai.completion"]
        out["output.mime_type"] = "text/plain"

    model = attributes.get("gen_ai.request.model") or attributes.get("llm.model")
    if model is not None:
        out["llm.model_name"] = model
    if "gen_ai.usage.input_tokens" in attributes:
        out["llm.token_count.prompt"] = attributes["gen_ai.usage.input_tokens"]
    if "gen_ai.usage.output_tokens" in attributes:
        out["llm.token_count.completion"] = attributes["gen_ai.usage.output_tokens"]

    if kind == "TOOL":
        tool_name = attributes.get("tool") or attributes.get("operator")
        if tool_name is not None:
            out["tool.name"] = tool_name

    return out


def set_span_attr_mirrored(s: Any, key: str, value: Any, *, openinference_key: str | None = None) -> None:
    """Set ``key`` on an open span, optionally mirroring it to an OpenInference
    attribute name too (#476). Additive, same-span mirroring only — no new
    exporter/pipeline. For attrs known only *after* the span has already
    started (e.g. completion content / usage, set post-hoc in ``client.py``),
    since :func:`span`'s own OpenInference mapping only sees the attrs dict
    supplied at entry. No-op if ``s`` is ``None`` (tracing inactive) or
    ``value`` is ``None``."""
    if s is None or value is None:
        return
    v = value if isinstance(value, (str, int, float, bool)) else str(value)
    s.set_attribute(key, v)
    if openinference_key:
        s.set_attribute(openinference_key, v)


@contextmanager
def span(name: str, attributes: dict[str, Any] | None = None):
    """Start a span. No-op if tracing isn't initialized."""
    if _tracer is None:
        yield None
        return

    with _tracer.start_as_current_span(name) as s:
        if attributes:
            for k, v in attributes.items():
                if v is not None:
                    s.set_attribute(k, v if isinstance(v, (str, int, float, bool)) else str(v))
            # OpenInference attrs are derived from the same `attributes` dict
            # passed in above — additive only, behind the same tracing-enabled
            # guard (we're already inside "if _tracer is not None").
            for k, v in _openinference_attrs(name, attributes).items():
                if v is not None:
                    s.set_attribute(k, v if isinstance(v, (str, int, float, bool)) else str(v))
        yield s


def current_trace_id() -> str:
    """The 32-hex trace_id of the current OTel span, or ``""`` when tracing is
    off / there is no active recording span (#248).

    Persisted on the ``Run`` at dispatch so the console can deep-link the run's
    trace in Langfuse (``{LANGFUSE_BASE}/trace/{trace_id}``). Never raises — a
    missing/invalid span context degrades to the empty string, so the capture is
    a pure no-op when no exporter is configured. The zero trace_id (all-zero span
    context = no active span) is treated as absent."""
    if _tracer is None:
        return ""
    try:
        from opentelemetry import trace  # base dep when telemetry extra is installed
        ctx = trace.get_current_span().get_span_context()
        if not ctx or not ctx.trace_id:  # 0 == invalid/no-span
            return ""
        return format(ctx.trace_id, "032x")
    except Exception:  # noqa: BLE001 — telemetry must never break a run
        return ""


def traced(name: str, attrs_fn: Callable[..., dict] | None = None):
    """Decorator that wraps an async function in a span."""

    def decorator(fn):
        @functools.wraps(fn)
        async def wrapper(*args, **kwargs):
            attrs = attrs_fn(*args, **kwargs) if attrs_fn else {}
            with span(name, attrs):
                return await fn(*args, **kwargs)

        return wrapper

    return decorator


# ─── Trace content-capture gate (#247) ───────────────────────────────

_CONTENT_MODES = {"off", "redacted", "raw"}


def tracing_active() -> bool:
    """Whether a tracer is initialized (spans actually emit)."""
    return _tracer is not None


def trace_content_mode() -> str:
    """Content-capture policy for the LLM prompt/completion span attrs (#247):

      off      — metadata only (model/tokens/cost); never export content.
      redacted — export content through the bound redactor (DEFAULT).
      raw      — export content verbatim (explicit opt-in; pre-#247 behavior).

    Env ``CAIRN_TRACE_CONTENT``; an unknown value falls back to ``redacted``.
    """
    v = (getenv("CAIRN_TRACE_CONTENT") or "redacted").strip().lower()
    return v if v in _CONTENT_MODES else "redacted"


async def trace_content(text: str) -> Optional[str]:
    """The value to set on a content span attr (``gen_ai.prompt`` /
    ``gen_ai.completion``), or ``None`` to OMIT the attr, per
    :func:`trace_content_mode` (#247).

    ``redacted`` is **fail-closed**: content is emitted only through an ACTIVE
    redactor (``redact.text`` bound — e.g. the ``secure`` profile). With no
    redactor bound the attr is omitted rather than leak un-redacted PII, so the
    default posture never exports raw prompt/completion to the trace backend.
    Bind a redactor for scrubbed content, or set ``CAIRN_TRACE_CONTENT=raw`` to
    opt into verbatim capture.
    """
    if not text:
        return None
    mode = trace_content_mode()
    if mode == "off":
        return None
    if mode == "raw":
        return text
    # redacted — fail closed when nothing will actually scrub the content.
    from cairn.core.obs.redact import get_redactor, redactor_active  # noqa: PLC0415
    if not redactor_active():
        return None
    res = await get_redactor().redact_text(text)
    return res.text
