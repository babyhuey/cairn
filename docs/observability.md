# Observability

cairn emits standard **OpenTelemetry** signals; it does **not** bundle a
tracing backend. You bring your own — point the engine at any OTLP endpoint, or
run the optional collector. Pick the tier that fits your deployment.

## What the engine emits

When tracing is enabled, every run produces OTLP spans:

- `agent.<name>.handle` — the top-level invocation
- `agent.<name>.<node>` — each graph node
- `llm.complete` — every model call (through the governed gateway)
- `mcp.<server>.<tool>` — every MCP tool call

Logs carry the trace/span id for correlation.

## Four tiers — least to most infrastructure

### 1. Off (default)
Leave `OTEL_EXPORTER_OTLP_ENDPOINT` unset. No spans are exported, zero overhead.

### 2. Console (local debug)
```
OTEL_CONSOLE=1
```
Spans print to the process log. No endpoint, no backend — handy while developing.

### 3. Direct OTLP to a managed backend — recommended for most deployments
The engine speaks OTLP directly, so **no collector is needed**. The OTLP exporter ships in the `telemetry` extra: install `cairndev[telemetry]` before setting the endpoint (without it the engine logs a warning and runs with tracing off). In your `.env`:
```
OTEL_EXPORTER_OTLP_ENDPOINT=https://<your-otlp-endpoint>
OTEL_EXPORTER_OTLP_HEADERS=authorization=<token>
```
Works with Grafana Cloud, Honeycomb, Axiom, Dash0 — any OTLP-native backend.
Your endpoint + token are **your** config: they live in `.env` (or a secret
store), never in the repo.

### 4. Collector — advanced
Run the optional collector for fan-out to multiple backends, tail sampling,
redaction, an on-disk buffer when the backend is down, or a backend that isn't
OTLP-native (e.g. AWS X-Ray):
```
docker compose -f docker-compose.yml --profile otel up -d
```
…and set `OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4318` in `.env`.
Configure exporters in `deploy/otel-collector.yaml` (commented examples for
Grafana Cloud / Jaeger / AWS X-Ray are included).

## Repo vs. your deployment

- **Repo ships:** the OTLP mechanism (in the engine) and the collector as an
  *opt-in example* with commented exporter templates. No backend, no credentials.
- **You provide:** the backend of your choice and its credentials, in `.env` or
  a secret store.

Same principle as Postgres, Temporal, and MLflow — backing services are the
deployment's choice; the engine ships the mechanism.
