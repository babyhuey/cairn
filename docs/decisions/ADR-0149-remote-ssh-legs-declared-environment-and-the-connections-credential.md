# ADR-0149 — Remote SSH legs: declared environment and the connection's credential

- **Status:** ACCEPTED 2026-10-23 by the owner ("fix pipeline first": item 1 of the GPU-run
  investigation).
- **Date:** 2026-10-23
- **Issue:** cairndev-sh/cairn#193.
- **Amends:** ADR-0043's cancel-credential line ("Slice 1 SSH-cancel falls back to the ambient
  `SSH_PRIVATE_KEY`"), for jobs submitted after this change. ADR-0043 itself is not edited.
- **Keeps:** ADR-0081 D3 (the box never holds control-plane credentials; rows and registry
  access stay control-side), ADR-0089 D3 (credentials reach a task only as declared
  connections), ADR-0104 D1 (venv and image modes), ADR-0148 (each workspace reads its own
  registration).

## Context

A `flow:` leg on the builtin engine with a `<kind>_target` runs `cairn-dataflow-run` on a
`compute.ssh` box through `remote/exec`, with the control plane elsewhere. Three things did not
work for a fine-tune leg on that path:

- The job got only the spec, the parent run, the live channel and `MLFLOW_*`. A trainer that
  mirrors checkpoints and publishes to S3 also reads `CAIRN_CHECKPOINT_BUCKET`,
  `CAIRN_CHECKPOINT_PREFIX` and `AWS_DEFAULT_REGION`, so publishing failed and mirroring was a
  silent no-op.
- A `dataset:` ref resolved to the location the version was registered at, often a
  control-plane path, even when the registry held a durable copy in the object store. The box
  could not read the path.
- Submit authenticated with the connection's `PRIVATE_KEY`. Cancel and the checkpoint pull-back
  run later without an operation context and reconnected with `SSH_PRIVATE_KEY` from the
  control plane's environment, swallowing any failure. With the documented setup the cancel
  could not authenticate, and the job kept running on a paid box.

## Decision

**D1 — The deployment declares the extra env names a remote job receives.** `remote_env`
(`[profile.backends]`, else `CAIRN_REMOTE_ENV`) lists NAMES, comma- or space-separated. Each one
set on the control plane is forwarded; nothing else of its environment crosses beyond the
engine's own set (spec, lineage, live channel, `MLFLOW_*`). A flow or a tenant cannot extend the
list. The SSH engine's `validate` refuses the leg before submit, naming the variable and never
its value, for:

- a name containing `KEY`, `SECRET`, `TOKEN` or `PASSWORD` (this covers `AWS_ACCESS_KEY_ID`,
  `AWS_SECRET_ACCESS_KEY` and `AWS_SESSION_TOKEN`): credentials stay on the box, as an instance
  role or the box's own environment;
- a name that is not an environment variable name;
- a name the engine sets itself for each submission (the spec, lineage and live-channel names).

**D2 — Values never appear on a command line.** In venv mode they are exported from the job's
`runner.sh`, which `remote/exec` makes owner-only (0600) before writing anything to it. In image
mode `runner.sh` runs `docker run -e NAME` with no value, so docker takes it from that file's
environment.

`MLFLOW_TRACKING_PASSWORD` is the one credential the engine forwards itself, with the other
`MLFLOW_*` names (#1235), never through `remote_env`. It is unchanged by this ADR, and it lands
only in the 0600 runner file.

**D3 — A remote leg reads dataset versions from their durable copy.** An engine whose job runs
on another machine declares `needs_durable_inputs = True` (the SSH engine does). For such an
engine, the leg executor resolves `dataset:` refs with `prefer_durable`, and each ref resolves to
the workspace's own `durable_uri` when there is one. Any other leg keeps the registered
location, except that a local path that no longer exists falls back to the durable copy. The SSH
engine refuses, in `validate`, a pinned version whose only location is a control-plane path
(`file://…#sha256=…` or `/…#sha256=…`), with how to give it a durable copy. A plain upstream
path with no pin is still uploaded (#1180).

**D4 — Cancel and checkpoint pull-back use the job's connection credential.** `remote/exec`
records the connection's NAME in the job handle at submit. Cancel and the pull-back resolve that
connection's `PRIVATE_KEY` through the same `as_ssh_params` mapping submit uses; the SSH engine
passes its own target. A named connection that does not resolve is reported, never replaced by
another key. A handle saved before this change names no connection and keeps the ambient
`SSH_PRIVATE_KEY`. A cancel or pull-back that cannot reach the box logs a warning with the host,
workdir, connection and error type, and no secret material; cancel stays best-effort.

## Consequences

- A deployment with a remote trainer sets e.g.
  `remote_env = "CAIRN_CHECKPOINT_BUCKET,CAIRN_CHECKPOINT_PREFIX,AWS_DEFAULT_REGION"`.
  Without it, nothing beyond the earlier set is forwarded.
- A co-located leg's dataset locations are unchanged.
- `needs_durable_inputs` is an optional engine attribute read with `getattr`. An engine that
  does not declare it is treated as co-located.
- Job handles from `remote/exec` gain a `connection` key, a name only.

## Follow-ups

- The SkyPilot engine also runs jobs elsewhere. It should declare `needs_durable_inputs` and
  refuse control-plane-only dataset versions in the same way.
- `cairn-extensions`: the `finetune-ssh` profile comment names `remote_env`,
  `CAIRN_DATAFLOW_REMOTE_CMD` and the wheel matching.
