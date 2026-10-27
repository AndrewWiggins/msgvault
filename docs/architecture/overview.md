---
last_edited: "2026-10-27"
title: Architecture Overview
description: How msgvault captures communications, preserves the archive, and serves people, search, and automation.
---

msgvault keeps communications and curated relationships in an archive you
operate. The daemon owns archive access and background work. The browser,
terminal, CLI, HTTP clients, and MCP tools use that daemon to read and maintain
the same data.

SQLite is the default database. Attachments live beside it as files or packed
blobs. Search and analytics use indexes derived from the archive. Optional
model processing adds semantic search, document extraction, and profile
inference without making a model provider the system of record.

## What works today

- Provider sync and local imports share message, conversation, participant,
  attachment, and source records.
- A people layer connects observed identifiers to durable curated profiles,
  organizations, employment, relationships, and contact activity.
- Keyword search and analytics read archived data. Optional processing uses
  separately configured embedding, extraction, inference, or enrichment services.
- The daemon serves the Web UI and versioned HTTP API, schedules work, and
  coordinates mutations. CLI, TUI, and MCP clients can use a local or remote daemon.
- SQLite archives have Parquet analytics, packed attachment storage, and
  verifiable backup repositories.

## Current boundaries

- Source APIs and export formats determine which history and media can be
  captured. An archived message does not imply that every attachment downloaded.
- PostgreSQL is opt-in for new archives. It has no built-in SQLite migration
  command and does not serve every cache-backed analytical view. See
  [PostgreSQL scope](postgresql.md#current-scope).
- Optional processing may contact external services. Consent and configuration
  are specific to each feature; ordinary keyword search needs neither.
- Profile briefs currently summarize supported chat and text inputs only.
  They do not cover email, meeting transcripts, documents, or the user's replies.

## Data flow

```mermaid
flowchart TD
    Sources[Provider APIs and local exports] --> Ingest[Sync and import workers]
    Clients[Web UI, TUI, CLI, HTTP and MCP clients] --> Daemon[Daemon API and scheduler]
    Daemon --> Ingest
    Ingest --> Archive[Archive database and attachment store]
    Daemon --> People[People curation and contact sync]
    People --> Archive
    Archive --> Indexes[Keyword, analytics and optional search indexes]
    Daemon --> Indexes
    Archive --> Optional[Opt-in extraction, embeddings and profile processing]
    Optional --> Providers[Configured local or hosted providers]
    Optional --> Indexes
    Optional --> Archive
```

Sync stores source identifiers, checkpoints, and raw evidence alongside parsed
records. Attachments are addressed by their SHA-256 content hash so occurrences
can share the same stored bytes. Repeated imports and syncs use source identity
to update or skip records according to that provider's rules.

On SQLite, analytics exports message metadata to Parquet. DuckDB queries those
files for grouping and drill-down without scanning message bodies. FTS5 indexes
message text for keyword search. Semantic search stores vectors in a separate
SQLite index; PostgreSQL uses its own full-text search and optional pgvector.
See [storage](storage.md) and [search ranking](search-ranking.md).

Opt-in [calendar control](../usage/calendar.md#control-events-unreleased) runs
through the daemon's serialized mutation gate. The daemon verifies source policy,
delegated grants, OAuth scope, and live calendar access before sending a provider
write. Successful changes use calendar sync's Store persistence path immediately,
without advancing sync cursors. Remote completion and archive completion are
reported separately so clients can reconcile a partial failure.

## Responsibilities

| Component | Owns | Main source locations |
|---|---|---|
| Daemon and scheduler | Database lifecycle, request routing, mutation coordination, scheduled jobs, and operation history | `cmd/msgvault/cmd/serve*`, `internal/api`, `internal/scheduler`, `internal/operations` |
| Clients | Interaction, output formatting, and requests to the selected daemon | `web`, `internal/tui`, `internal/mcp`, `internal/daemonclient`, `cmd/msgvault/cmd` |
| Ingestion | Source authorization, provider mapping, imports, checkpoints, and retry behavior | Provider packages in `internal/`, `internal/importer`, `internal/meetingarchive` |
| Store | Archive schema, transactions, messages, provenance, profiles, and durable job state | `internal/store`, `internal/sqldialect` |
| Attachment storage | Shared bytes, streaming reads, media policies, and packed/loose layouts | `internal/attachmentstore`, `internal/attachmentpolicy`, shared pack engine |
| Search and analytics | Search parsing, query engines, caches, vector generations, and document indexes | `internal/search`, `internal/query`, `internal/vector`, `internal/documentindex` |
| People | Observed identity links, curated facts, profiles, activity, consented maintenance, and contact sync | `internal/identityindex`, `internal/personfacts`, `internal/activity`, `internal/peoplesweep`, `internal/personenrichment`, `internal/carddav` |
| Preservation and removal | Backups, duplicate handling, deletion manifests, and explicit execution | `internal/backupapp`, `internal/dedup`, `internal/deletion` |

## Rules that matter across components

### The daemon coordinates archive mutations

Archive-access CLI commands discover or start the local daemon unless a remote
is configured. For ordinary archive commands, `--local` selects the local daemon;
it does not make the CLI open SQLite directly. Mutating jobs wait for the daemon's
writer coordination instead of competing through independent foreground database
connections. Some offline recovery commands require the daemon to be stopped. The
offline `migrate` command applies the candidate binary's main archive schema
migrations directly; `schema-version --database` reads its completion marker
without migrations or daemon startup. See the [CLI contracts](../cli-reference.md#schema-version). The
[daemon guide](../guides/daemon-migration.md) owns those lifecycle rules.

### Archive records and derived indexes have different lifetimes

The database and attachment bytes preserve source content and user curation.
Parquet caches and semantic indexes are derived and can be rebuilt. A cache
failure does not erase archived messages. Index generations track which
embedding model, policy, and scope produced vectors; incompatible generations
report stale state rather than silently serving a different search mode.

During startup, the API becomes reachable before analytics maintenance finishes.
With the automatic analytics engine, eligible queries can use live SQL until
DuckDB is ready. Cache-dependent routes can return a structured `503` with
readiness information. An explicitly selected DuckDB engine waits for its
required cache. See [API readiness](../api-server.md).

### Observed evidence and curated profiles remain distinct

A participant is an address, handle, or other identity seen in source data.
A durable person is a profile the user chooses to maintain. Identity bindings
connect them; matching display names alone do not merge people. Profile facts
retain evidence and resolution history. User pins and explicit merge or split
actions have their own contracts. See [people and profiles](../usage/people.md).

Identity review decisions bind to the inspected evidence and endpoint snapshot.
Changed evidence or a candidate collapse requires a new review. Optional manual
identity scoring creates suggestions and journals proposals; it does not
accept them. The daemon checks local blockers before sending eligible pairs
to the fixed provider and checks consent at each provider attempt. Consent
changes wait for requests already admitted by that daemon. Provider I/O holds
no archive writer gate or database write transaction; local mutations retain
the daemon's normal coordination. See the
[review and scoring contract](../api-server.md#identity-match-review-and-scoring).

### Each external operation has its own scope

Provider sync, identity discovery, CardDAV publication, remote image downloads,
remote deletion, and optional model processing can contact services. Keyword
search and ordinary archive reads operate on stored data. Profile inference,
external enrichment, document extraction, and embedding configuration have
distinct consent and scope rules; see [configuration](../configuration.md).

### Upstream deletion and local removal are separate

Staging writes a reviewable manifest for one exact source. Execution requires
explicit consent from the invoking client. It records source-deletion state
while retaining archived messages and attachment content. Local commands such
as `gc` and `delete-deduped` can remove archive data and have separate
confirmation and backup rules. See [deletion](../usage/deletion.md) and
[deduplication](../usage/deduplication.md).

### Gmail acquisition failures survive sync progress

Gmail fetch and ingestion failures remain acquisition debt until a later sync
acquires the raw MIME or records a deletion disposition. The append-only
`sync_run_items` journal identifies each occurrence by source and native Gmail
message ID. Replay reads all source runs, including failed and full runs;
finishing a newer run does not erase earlier failures.

| Journal outcome | Effect on automatic acquisition |
|---|---|
| Fetch or ingestion failure, including a historical `gmail_not_found` skip | Retry on a later eligible sync; a generic HTTP 404 is ambiguous. |
| `acquisition_resolved` | Acquired while the RAW remains present. Accidental RAW loss makes the debt retryable again. |
| `message_gone` | The fetch explicitly confirmed disappearance; stop replay unless later acquisition events reopen the debt. |
| `acquisition_source_deleted` | History or a complete, valid, unfiltered and unlimited presence inventory confirmed absence; stop replay unless later acquisition events reopen the debt. No archived message row is required. |
| `acquisition_local_deletion` | Explicit local destruction prevents automatic reacquisition of that source/native ID, even after later failures, resolutions or remote events. |

Deletion dispositions retain the original diagnostics. Local intent takes
precedence over every later outcome. Resolution is recorded only if the RAW
still exists and no local deletion has committed. Store checks and writes in
one transaction, using the existing identity locks on PostgreSQL and SQLite's
writer serialization to coordinate with acquisition and purge across processes.
Purge holds the exclusive identity gate, so it can delay other archive writes;
network fetches hold no such gate.

Ordinary sync may advance its cursor past a failed acquisition only after the
diagnostic is durable. Failure to record an outcome blocks the next checkpoint
or final cursor publication. Each eligible sync makes one replay attempt per
outstanding ID; there is no automatic retry ceiling. A physical deletion made
before these dispositions existed cannot be distinguished from accidental loss
and remains retryable if an old failure is recorded.

## Read further

- [Storage](storage.md): database tables, attachments, and rebuildable caches.
- [Search ranking](search-ranking.md): search-mode and backend contracts.
- [PostgreSQL](postgresql.md): setup and feature boundaries.
- [Backup repository format](backup-format.md): snapshot and restore contracts.
- [Development](../development.md): build, test, and contributor workflows.

This page describes implemented behavior. Historical designs and engineering
plans are kept in the repository's `docs/internal/` directory, outside public
navigation; their task lists are not a roadmap or proof of current behavior.
