# Changelog

All notable changes to this project will be documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## [2.0.0] — 2026-05-02

A four-PR principal-engineer review pass: critical correctness fixes
across the Redis and Postgres backends, an architectural split of the
monolithic `Storage` trait into five cohesive sub-traits, and a wave of
quality / ergonomics polish. Several bug fixes were silent production
failures (Redis workers never picked up jobs against the default
config; Postgres scheduler/retry/recovery never matched any rows). The
public surface change is large enough to warrant a major bump.

### Fixed (correctness — silent production bugs)

- **Redis `fetch_and_lock_job` is now functional.** The previous Lua
  script had five entangled defects that made it silently return
  `Ok(None)` for every call: ignored `_queues` filter, wrong key
  namespace (`qml:job:` vs `qml:jobs:`), wrong order
  (`ZRANGEBYSCORE` ascending vs `ZREVRANGEBYSCORE` descending),
  hardcoded `qml:` prefix, and counter math that decremented both
  `enqueued` *and* `awaiting_retry` regardless of source state. Plus a
  sixth surfaced during the fix — timestamps written as raw millis
  numbers where chrono's serde format expects RFC3339. Workers never
  picked up Redis-backed jobs; latent because integration tests skip
  without `REDIS_URL`. ([#23])
- **Postgres scheduler / retry / recovery now match rows.** Every
  time-predicate query was reading `state_data->>'enqueue_at'` at the
  top level, but `JobState` is externally tagged so the actual path
  is `state_data->'Scheduled'->>'enqueue_at'`. NULL extraction made
  every predicate evaluate false: `fetch_due_*`, `claim_due_*`,
  `requeue_stranded_jobs`, `reclaim_jobs_from_server`, and the
  scheduled/retry branches of `get_available_jobs` all silently
  returned nothing. ([#23])
- **Redis `update` is now atomic.** Previously a `get()` + `set()`
  pair plus 5 non-transactional index commands. Two concurrent
  updaters reading the same `old_state` both decremented the same
  counter (drift); a reader between SET and the index commands could
  see a job whose `state` field disagreed with its state-set
  membership and counter. Single Lua script now. ([#24])
- **Redis no longer races itself on final-state expiration.** Native
  Redis `EXPIRE` on Succeeded/Failed jobs raced the cross-backend
  `CleanupWorker` sweeping `expires_at`. When the native TTL won, the
  job blob disappeared but `qml:state:succeeded` / `qml:all` /
  `qml:counts` index entries leaked forever. Dropped the native
  EXPIRE; `CleanupWorker` is the single source of truth. ([#24])
- **Postgres `qml_locks` no longer accumulates indefinitely.** New
  `Storage::cleanup_expired_named_locks` sweeper, called by
  `CleanupWorker`. The takeover-on-acquire path only fires on
  contention; one-shot lock workloads previously leaked rows. ([#24])
- **Redis queue-filtered `fetch_and_lock_job` is now exact, with no
  cap.** Previously scanned at most 1024 candidates from a global
  ZSET and could miss eligible jobs when more than 1024 ineligible-
  queue jobs sat ahead. Per-queue `qml:available:<queue>` ZSETs are
  now maintained alongside the global one; the Lua script peeks at
  the top entry of each candidate ZSET and picks the global max.
  ([#24])

### Added

- **Atomic claim-and-transition primitives.** `JobStore::
  claim_due_scheduled_jobs` and `claim_due_retry_jobs` perform the
  `Scheduled → Enqueued` / `AwaitingRetry → Enqueued` transition
  inside the storage engine. Replaces the previous racy
  fetch+update flow in `JobScheduler`. ([#23])
- **Compare-and-swap `update`.** `MonitoringApi::update_if_state(job,
  expected: JobStateKind) -> Result<bool, StorageError>` writes only
  if the persisted state matches `expected`. The dashboard "retry"
  button is now stomp-safe — a slow second retry can no longer
  overwrite a `Processing` state that a worker already took on after
  the first retry. ([#23])
- **Dashboard graceful shutdown.** `DashboardServer` owns a
  `CancellationToken`; `axum::serve(...).with_graceful_shutdown` is
  wired in; the periodic-updates task returns a `JoinHandle` and
  exits on cancel. New public API: `shutdown()`, `shutdown_token()`,
  `run_until_cancelled(token)`. The periodic-task await is bounded
  by a 5-second timeout so a hung `get_server_statistics` against an
  unhealthy backend can't block the caller's shutdown sequence. ([#23])
- **Postgres hot-path partial indexes.** Three new indexes covering
  `fetch_and_lock_job` / `claim_due_*`'s actual access pattern, plus
  the time predicates on `state_data->'Scheduled'->>'enqueue_at'`
  and `state_data->'AwaitingRetry'->>'retry_at'`. Backed by a new
  `qml.parse_iso_utc(text) RETURNS timestamptz` IMMUTABLE helper
  (Postgres rejects `text::timestamptz` in expression indexes
  because that cast is STABLE). ([#24])
- **`metrics_skip_auth` opt-in.** Lets Prometheus scrapers reach
  `/metrics` without speaking Basic/Bearer. The `/metrics` route is
  mounted outside the auth middleware when set; every other route
  still demands credentials. ([#26])

### Changed (BREAKING)

- **`Storage` trait split into five sub-traits.** The previous
  monolithic 24-method trait is now an empty umbrella over `JobStore +
  JobLocker + RecurringStore + ServerRegistry + NamedLocks + Send +
  Sync`. Custom backends implementing `Storage` outside this crate
  must split their `impl Storage for X { ... }` into five separate
  `impl JobStore for X`, `impl JobLocker for X`, etc. blocks. The
  umbrella is then a one-liner `impl Storage for X {}`. New
  `qml_rs::storage::prelude` re-exports all six traits — `use
  qml_rs::storage::prelude::*` is the easiest way to bring the
  full method surface into scope. ([#25])
- **`StorageInstance` enum retired.** Replaced with a unit struct
  whose associated functions return `Arc<dyn Storage>` directly.
  Pattern matches on `StorageInstance::Memory(_)` etc. no longer
  compile; switch to direct `Arc::new(MemoryStorage::with_config(...))`
  construction or `StorageInstance::from_config`. Calls like
  `Arc::new(StorageInstance::memory())` need the redundant outer
  wrap removed — the constructors return `Arc<dyn Storage>` already.
  ([#25])
- **`MonitoringApi::list` signature.** Changed from
  `Option<&JobState>` to `Option<JobStateKind>`. Callers that built
  throwaway `JobState::enqueued("default")` etc. just to pick a
  variant must switch to the discriminant. ([#23])
- **`MonitoringApi::update_if_state` is a new required method.**
  Custom impls must add it. The three in-tree backends are
  updated. ([#23])
- **`StorageError` shorthand variants removed.** `ConnectionError`,
  `SerializationError`, `DeserializationError`, `OperationError` are
  gone; use `Connection`, `Serialization`, the new `Deserialization`,
  and `OperationFailed` instead. The `operation: String` field on
  `OperationFailed` was dropped — its few callers folded the
  operation name into `message`. ([#24])
- **`StorageError` shorthand variants gained `source` field.** Pattern
  matches like `StorageError::Serialization { message }` need
  `{ message, source, .. }` (or `..` alone) to be exhaustive. ([#23])
- **Postgres now requires the `qml.parse_iso_utc` helper function.**
  Auto-installed by `migrate()`. Existing 1.x deployments need to
  call `storage.migrate()` once after upgrading or run the new
  `CREATE INDEX IF NOT EXISTS` / `CREATE OR REPLACE FUNCTION`
  statements manually. ([#24])
- **Redis index layout changed.** `qml:available:<queue>` per-queue
  ZSETs are new alongside the global `qml:available`. Existing data
  in the global ZSET is still picked up by no-filter workers, but
  queue-scoped workers won't find old jobs until those jobs cycle
  through `update_job_indices` again (e.g. via a state transition or
  re-enqueue). For a clean cutover, `FLUSHDB` the QML keys or let
  pending jobs drain. ([#24])
- **`MonitoringApi::list` filters by `JobStateKind`.** See above.
  ([#23])
- **Dashboard retry path goes through CAS.** `DashboardService::
  retry_job` now returns the `Ok(true)` / `Ok(false)` from
  `update_if_state` directly. ([#23])
- **`MemoryConfig::auto_cleanup` and `cleanup_interval` are
  `#[deprecated]`.** They're no-ops the runtime never read; kept on
  the struct so old serialized configs still round-trip through
  serde. Will be removed in the next major. ([#26])
- **`MemoryConfig::with_auto_cleanup` and `with_cleanup_interval`
  builders are `#[deprecated]`.** Same story. ([#26])

### Removed

- **`StorageInstance` enum variants.** See above. ([#25])
- **`StorageError::ConnectionError` / `SerializationError` /
  `DeserializationError` / `OperationError` shorthand variants.**
  See above. ([#24])
- **350+ lines of `StorageInstance` `impl Storage for ...` dispatch
  boilerplate.** Net `mod.rs` shrank by ~640 lines despite the new
  sub-trait declarations. ([#25])

### Internal / quality

- **`Storage::cleanup_expired_named_locks` cross-backend method.**
  ([#24])
- **Worker error back-off jitter** (0–1500ms) on the worker loop.
  Plus explicit `MissedTickBehavior::Skip` on the polling interval
  so a slow worker doesn't burst-tick after recovery. ([#23])
- **`StorageError` source-chain plumbing across 50+ call sites.**
  Driver errors are now preserved on `Error::source()` rather than
  being stringified into `message` and dropped. ([#23])
- **`is_schema_error` simplified to SQLSTATE only.** Locale-
  dependent string matching (`"does not exist"` etc.) removed —
  fragile under any non-English `lc_messages`. ([#26])
- **Hand-rolled Base64 / constant-time-eq replaced with crates.**
  `base64 = "0.22"` and `subtle = "2.6"`, both gated on the
  `dashboard` feature. ([#26])
- **Direct `Processing → AwaitingRetry` state transition.**
  Eliminates a two-step `Processing → Failed → AwaitingRetry`
  dance with an intermediate state never reaching storage. The
  retry path is now one `set_state` call; the state-change hook
  still fires once with the original pre-retry state. ([#26])
- **`Settings::from_env` propagates errors instead of panicking.**
  Function signature was already `Result<...>`; replaced
  `.expect()` with `?`. ([#23])
- **Dashboard `attempts` field reports `job.attempt`.** Was
  hardcoded to `0` with a stale "not tracked" comment. ([#23])
- **Examples build under `--all-targets`.** Two examples had stale
  call shapes that compiled under `cargo build` but not under
  `cargo build --examples`. ([#23], [#25])

### New tests

- `tests/queue_filter_test.rs` — cross-backend queue isolation; new
  regression test for the post-1024-cap behavior. ([#23], [#24])
- `tests/claim_due_jobs_test.rs` — atomic claim-and-transition
  contract. ([#23])
- `tests/processing_recovery_test.rs` — Postgres round-trip after
  the JSON-path fix. ([#23])
- `tests/update_if_state_test.rs` — CAS contract including
  stomp-avoidance. ([#23])
- `tests/dashboard_shutdown_test.rs` — graceful shutdown +
  wedged-task abort timeout. ([#23])
- `tests/named_lock_sweeper_test.rs` — sweeper contract across
  Memory / Redis / Postgres. ([#24])
- New unit tests in `processing/scheduler.rs` and
  `dashboard/server.rs::metrics_route_tests`.

[#23]: https://github.com/queue-me-later/qml/pull/23
[#24]: https://github.com/queue-me-later/qml/pull/24
[#25]: https://github.com/queue-me-later/qml/pull/25
[#26]: https://github.com/queue-me-later/qml/pull/26

## [1.1.0] and earlier

See git history; this is the first release with a maintained changelog.
