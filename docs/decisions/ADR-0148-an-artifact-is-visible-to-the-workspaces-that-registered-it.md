# ADR-0148 — An artifact is visible to the workspaces that registered it

- **Status:** ACCEPTED 2026-10-23 by the owner (option 2a: an ownership table written on every
  registration, reads filtered through it).
- **Date:** 2026-10-23
- **Issue:** tracked privately (deferred from cairndev-sh/cairn#190).
- **Amends:** the `ArtifactStore` protocol (`register`, `get`, `find_by_uri`, `list_artifacts`
  and `lineage` take a workspace) and the registry schema (a new `artifact_owners` table).
- **Keeps:** ADR-0081 D2/D3 (content-addressed, immutable versions registered control-side from
  engine descriptors; identical bytes are one row), ADR-0088 H6 (checkpoint relay and resume),
  the dataset registry's by-name workspace scope.
- **Builds on:** ADR-0051 (workspace isolation), the 0.2.0 rule that auth on with no workspace in
  scope is refused.

## Context

An artifact's id is the hash of its bytes, and the registry answered any id, URI or listing for
the whole deployment. Dataset names were already per workspace, but an id read from a run, a log
or a guess returned another workspace's record: its URI, durable copy, stats, schema and lineage.
`find_by_uri` could also map a URI to another workspace's artifact while recording a leg's
parents. The registry keeps no workspace on the row, and it cannot: content addressing makes
identical bytes from two workspaces one row.

## Decision

**D1 — Ownership is a row per registering workspace.** `artifact_owners(artifact_id,
workspace_id)`, keyed by both. Every `register` adds the registering workspace's row, including a
registration of bytes that are already registered (by itself or by another workspace): the
`artifacts` row stays one, and each registering workspace owns it. The first producer's
provenance on the row is unchanged.

**D2 — Reads go through ownership.** `get`, `find_by_uri`, `list_artifacts` and `lineage` answer
only with artifacts the workspace owns; another workspace's reads as not found. A parent the
workspace does not own is left out of a record's `parents` and of the lineage tree; a parent that
is not a registered artifact (an external URI) stays. The dataset registry reads a version's
artifact in the version's workspace, and `register_dataset_version` refuses an artifact the
workspace does not own. `count` stays the deployment's row count.

**D3 — The workspace comes from the caller or the scope.** Every method takes `workspace_id=`
(keyword), else `scoped_workspace()`. With auth on and neither, it raises
`WorkspaceScopeRequired` before relaying bytes or writing a row. Core callers pass the run's
workspace: a leg's registration and URI-parent mapping (`register_leg_descriptors`), checkpoint
relay (the leg's run record) and resume lookup, and `registry_health` (`cairn doctor` checks the
process's workspace, `CAIRN_WORKSPACE` else `default`).

**D4 — Existing artifacts are backfilled once.** When `artifact_owners` is created on a database
that already holds artifacts, in the same transaction: `default` owns every artifact; so does the
workspace of the run that produced it, when that run is in the same database; so does every
workspace that registered a dataset version of it. The backfill runs only when the table is
created, so a later registration is never handed to `default`.

## Consequences

- A single-workspace deployment is unchanged: everything is registered and read in `default`.
- Another workspace's artifact reads exactly like an id nobody registered.
- Identical bytes registered by two workspaces are visible to both, each through its own
  ownership; neither learns that the other registered them.
- A process that registers artifacts must run this version: an older replica writing after the
  migration adds no owner row, and its artifacts are visible to no workspace. Upgrade every
  replica together.
- A caller outside any workspace scope (a deployment-admin route, a seed script) must pass
  `workspace_id=`, or with auth on the call is refused.
- An artifact registered before this change by a run in another database, with no dataset
  version, is owned by `default` only.
