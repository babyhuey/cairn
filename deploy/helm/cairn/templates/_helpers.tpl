{{/*
Names, labels, the Postgres DSN, and the render-time validation gate.

The validation here is the chart's launch door (ADR-0099's shape, one layer
down): a configuration that would deploy something broken must REFUSE to
render, not render something that fails at 3am. Every `fail` below names the
exact value to change.
*/}}

{{- define "cairn.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "cairn.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "cairn.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "cairn.labels" -}}
helm.sh/chart: {{ include "cairn.chart" . }}
{{ include "cairn.selectorLabels" . }}
app.kubernetes.io/version: {{ include "cairn.imageTag" . | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: cairn
{{- end -}}

{{- define "cairn.selectorLabels" -}}
app.kubernetes.io/name: {{ include "cairn.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "cairn.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "cairn.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{- define "cairn.imageTag" -}}
{{- default .Chart.AppVersion .Values.image.tag -}}
{{- end -}}

{{- define "cairn.image" -}}
{{- printf "%s:%s" .Values.image.repository (include "cairn.imageTag" .) -}}
{{- end -}}

{{/* The Secret every workload takes its env overrides from. */}}
{{- define "cairn.secretName" -}}
{{- if .Values.secrets.existingSecret -}}
{{- .Values.secrets.existingSecret -}}
{{- else -}}
{{- printf "%s-secrets" (include "cairn.fullname" .) -}}
{{- end -}}
{{- end -}}

{{- define "cairn.configMapName" -}}
{{- printf "%s-config" (include "cairn.fullname" .) -}}
{{- end -}}

{{- define "cairn.postgresName" -}}
{{- printf "%s-postgres" (include "cairn.fullname" .) -}}
{{- end -}}

{{/*
The one DSN every store uses (run store · LangGraph checkpointer · DBOS system
schema). Compose derives it from POSTGRES_PASSWORD; the chart derives it from
the bundled StatefulSet or takes the external one verbatim.
*/}}
{{- define "cairn.dsn" -}}
{{- if .Values.postgres.enabled -}}
{{- printf "postgresql://%s:%s@%s:%v/%s" .Values.postgres.user .Values.postgres.password (include "cairn.postgresName" .) .Values.postgres.port .Values.postgres.database -}}
{{- else -}}
{{- .Values.postgres.external.dsn -}}
{{- end -}}
{{- end -}}

{{- define "cairn.temporalAddress" -}}
{{- if .Values.profiles.temporal.address -}}
{{- .Values.profiles.temporal.address -}}
{{- else -}}
{{- printf "%s-temporal:7233" (include "cairn.fullname" .) -}}
{{- end -}}
{{- end -}}

{{- define "cairn.natsUrl" -}}
{{- printf "nats://%s-nats:4222" (include "cairn.fullname" .) -}}
{{- end -}}

{{- define "cairn.mlflowUri" -}}
{{- if .Values.profiles.mlflow.trackingUri -}}
{{- .Values.profiles.mlflow.trackingUri -}}
{{- else if .Values.profiles.mlflow.enabled -}}
{{- printf "http://%s-mlflow:5000" (include "cairn.fullname" .) -}}
{{- end -}}
{{- end -}}

{{/*
One DSN-family key (STORAGE / CHECKPOINTER_DSN / DBOS_DSN) — ADR-0122, #2429. Until
1.0: both names with no existingSecret; only the OBS_ name with one, so the Secret's
key wins in either spelling (configmap.yaml says why). Called as
`include "cairn.dsnEnv" (list . "STORAGE")`.
*/}}
{{- define "cairn.dsnEnv" -}}
{{- $root := index . 0 -}}
{{- $name := index . 1 -}}
{{- $dsn := include "cairn.dsn" $root | quote -}}
{{- if not $root.Values.secrets.existingSecret -}}
CAIRN_{{ $name }}: {{ $dsn }}
{{ end -}}
OBS_{{ $name }}: {{ $dsn }}
{{- end -}}

{{/* envFrom: ConfigMap first, Secret LAST so a secret key wins on collision. */}}
{{- define "cairn.envFrom" -}}
- configMapRef:
    name: {{ include "cairn.configMapName" . }}
- secretRef:
    name: {{ include "cairn.secretName" . }}
{{- end -}}

{{/*
── the door ─────────────────────────────────────────────────────────────
Called once from the ConfigMap (which always renders). Each branch is a
deployment that has actually gone wrong somewhere, on compose or on a box.
*/}}
{{- define "cairn.validate" -}}

{{- /* Postgres: bundled XOR external. Both = two databases, silently one used. */ -}}
{{- if and .Values.postgres.enabled .Values.postgres.external.dsn -}}
{{- fail "postgres.enabled=true AND postgres.external.dsn is set — pick one. The bundled StatefulSet and an external DSN are mutually exclusive; set postgres.enabled=false to use the external DSN." -}}
{{- end -}}
{{- if and (not .Values.postgres.enabled) (not .Values.postgres.external.dsn) -}}
{{- fail "no database: postgres.enabled=false and postgres.external.dsn is empty. Set one of them — cairn's run store, LangGraph checkpointer and DBOS system schema all live in Postgres." -}}
{{- end -}}
{{- if and .Values.postgres.enabled (not .Values.postgres.password) -}}
{{- fail "postgres.enabled=true but postgres.password is empty — generate one (openssl rand -hex 16)." -}}
{{- end -}}

{{- /* The canonical doctor rule (#390), now enforced before anything starts. */ -}}
{{- if and (eq .Values.runtime "temporal") (not .Values.profiles.temporal.enabled) -}}
{{- fail "runtime=temporal but profiles.temporal.enabled=false — the Temporal service and the worker would never start, so every submitted run would hang. Enable profiles.temporal, or set runtime=dbos." -}}
{{- end -}}
{{- if and .Values.profiles.temporal.enabled (ne .Values.runtime "temporal") -}}
{{- fail (printf "profiles.temporal.enabled=true but runtime=%s — the server would not submit to Temporal, leaving an idle cluster. Set runtime=temporal, or disable the profile." .Values.runtime) -}}
{{- end -}}

{{- /* #390/#532/#1847 — a durable runtime on a memory checkpointer replays the
       whole graph on recovery, charging every LLM call twice. */ -}}
{{- if and (ne .Values.runtime "inprocess") (eq .Values.checkpointer "memory") -}}
{{- fail (printf "runtime=%s with checkpointer=memory — recovery re-runs the whole graph from zero (every operator and LLM call charged again) because the LangGraph checkpoint died with the process. Set checkpointer=postgres." .Values.runtime) -}}
{{- end -}}

{{- /* Ingress needs a host; it is this chart's CADDY_DOMAIN. */ -}}
{{- if and .Values.ingress.enabled (not .Values.ingress.host) -}}
{{- fail "ingress.enabled=true but ingress.host is empty — set the public hostname (the chart's equivalent of CADDY_DOMAIN)." -}}
{{- end -}}

{{- /* #2296 — every deployment names its profile; the engine refuses to start without one. */ -}}
{{- if not (trim (toString (default "" .Values.env.profile))) -}}
{{- fail "env.profile is empty — every deployment names its profile (CAIRN_PROFILE); cairn-server and cairn-worker refuse to start without one. Use /app/.venv/lib/python3.13/site-packages/cairn/profiles/secure.toml (or prod-infisical.toml) or your own — see docs/runbooks/deployment-profile.md." -}}
{{- end -}}

{{- /* Secrets: present here, or promised by an existing Secret. */ -}}
{{- if not .Values.secrets.existingSecret -}}
{{- if not .Values.secrets.obsJwtSecret -}}
{{- fail "secrets.obsJwtSecret is empty and secrets.existingSecret is unset — auth is ON in prod (issue #2) and the engine refuses to start without a JWT secret." -}}
{{- end -}}
{{- if not .Values.secrets.obsApiPasswordHash -}}
{{- fail "secrets.obsApiPasswordHash is empty and secrets.existingSecret is unset — generate it with `cairn remote hash-password`." -}}
{{- end -}}
{{- end -}}

{{- /* #2073 — a PROVISIONING engine with ephemeral state leaks money. SkyPilot
       keeps its cluster state in ~/.sky; without a volume behind it a pod
       restart forgets every machine it launched, and `sky down` can no longer
       reach them. Refuse rather than rent something nothing can return. */ -}}
{{- if and (eq ((index .Values.env.extra "CAIRN_ML_ENGINE") | default (index .Values.env.extra "OBS_ML_ENGINE") | default "") "skypilot") (not .Values.server.persistence.enabled) -}}
{{- fail "CAIRN_ML_ENGINE=skypilot with server.persistence.enabled=false — SkyPilot keeps its cluster state in ~/.sky, so a pod restart would forget every machine it provisioned and `sky down` could not terminate them. They would keep billing. Enable server.persistence, or select an engine that does not provision." -}}
{{- end -}}

{{- /* The pull worker (`cairn worker --pull`, #565) has no default command. */ -}}
{{- if and .Values.pullWorker.enabled (not .Values.pullWorker.command) -}}
{{- fail "pullWorker.enabled=true but pullWorker.command is empty — the chart has no default command for the pull worker. Supply pullWorker.command explicitly, e.g. [cairn, worker, --pull, --engine-url, https://<host>]." -}}
{{- end -}}

{{- /* Delegated profiles: refuse loudly, never pretend. */ -}}
{{- if (index .Values.profiles "observability").enabled -}}
{{- fail "profiles.observability is not implemented by this chart — Prometheus and Grafana on Kubernetes are prometheus-community/kube-prometheus-stack. Install that chart and point observability.otlp.endpoint at your collector." -}}
{{- end -}}
{{- if (index .Values.profiles "otel").enabled -}}
{{- fail "profiles.otel is not implemented by this chart — use open-telemetry/opentelemetry-collector (deploy/otel-collector.yaml is a working config to start from) and set observability.otlp.endpoint." -}}
{{- end -}}
{{- if (index .Values.profiles "infisical").enabled -}}
{{- fail "profiles.infisical is not implemented by this chart — use infisical/infisical-standalone, then bind the engine to it with env.profile (CAIRN_PROFILE, #408)." -}}
{{- end -}}
{{- if (index .Values.profiles "llm-gateway").enabled -}}
{{- fail "profiles.llm-gateway is not implemented by this chart — use berriai/litellm-helm and set env.extra.LLM_GATEWAY_URL (#182)." -}}
{{- end -}}
{{- if (index .Values.profiles "metaflow-service").enabled -}}
{{- fail "profiles.metaflow-service is not implemented by this chart — use outerbounds/metaflow-tools and set env.extra.CAIRN_METAFLOW_SERVICE_URL (#1365)." -}}
{{- end -}}
{{- if (index .Values.profiles "dedicated-temporal-db").enabled -}}
{{- fail "profiles.dedicated-temporal-db is not implemented by this chart — give Temporal its own database via the official temporal chart, or a managed Postgres." -}}
{{- end -}}

{{- end -}}
