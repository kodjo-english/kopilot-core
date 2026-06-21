---
name: kopilot-core
description: Maintain or extend kopilot-core — the async automation microframework powering all kopilot-* services. Use whenever modifying anything inside the kopilot-core repository: bus.py, db.py, scheduler.py, log.py, runtime.py, pyproject.toml, or __init__.py. Also use when releasing a new version (tagging, semver bumps), debugging the NATS lifecycle, the @nc.sub / @nc.reply decorator mechanism, the configure() injection pattern, the run() entrypoint, the SIGTERM/SIGINT shutdown sequence, the MySQL pool, or APScheduler integration. Trigger on requests to add features parked in the debt list (deadlock retry, JSON logging, hot reload, JetStream, queue groups, multi-instance). Also trigger on architecture questions about why core is structured this way, what stays in core vs ships as a plugin, or how teams consume the package. Do NOT trigger for individual kopilot-* consumer services (kopilot-telegram, kopilot-saga, kopilot-content, kopilot-stripe, kopilot-zoom, kopilot-meeting, kopilot-airtable, kopilot-ghl, kopilot-passe, kopilot-samcart, kopilot-testimonial, kopilot-entitlement, kopilot-ingestion, kopilot-api, kopilot-dashboard, etc.) — those are downstream codebases that depend on this package; they have their own context.
---

# kopilot-core

This is the kernel that every kopilot-* service installs as a pip dependency. It exposes a NATS bus, a MySQL pool, an APScheduler instance, a logging configurator, and a runtime that boots all of the above with structured-concurrency shutdown semantics.

Read this once in full before you write a line of code. When in doubt, re-read the relevant section. If the answer is not here, **stop and ask** — do not invent a convention.

---

## 1. What this is, what it isn't

This package was extracted from 14+ forked services that had each duplicated the same `common/` folder. The goal of the extraction was to stop patching nats logic in 14 places, while preserving the fork-level isolation teams already had at the runtime level. The package is a **kernel**, not an orchestrator and not a library.

It IS:

- a single-process kernel for fork-and-forget automation services
- the framework layer that all kopilot-* services depend on
- opinionated about its core choices: NATS as the bus, MySQL as the canonical DB, APScheduler for jobs, anyio as the concurrency runtime
- designed for **vertical certainty** (one process per service, do it well) — not horizontal scale

It IS NOT:

- a workflow orchestrator (no DAGs, no automatic retries, no state machines)
- a distributed system (one runtime, one process; multi-instance and queue groups are deliberately deferred)
- a web framework (no HTTP server; mount your own alongside if needed)
- a general-purpose utility library (it's the framework, not a kitchen sink)

The maintainer's working philosophy, which the codebase reflects:

- Gall's Law: "A complex system that works evolved from a simple system that worked."
- KISS over cleverness, anti-DRY where DRY would create invisible coupling
- No speculative engineering: don't build for step 10 when we're on step 1
- Observability is sacred: when something fails silently for 50 days, that's a moral failure of the framework, not a discovery problem

---

## 2. Architecture map

```
kopilot-core/
├── pyproject.toml
└── kopilot_core/
    ├── __init__.py        # public surface only — no logic
    ├── bus.py             # NATSServer class + nc singleton
    ├── db.py              # MySQL class + db alias
    ├── log.py             # configure_logging() helper
    ├── runtime.py         # Service class + run() entrypoint
    └── scheduler.py       # AsyncIOScheduler singleton (sch)
```

Module dependencies inside the package:

- `runtime.py` → imports `bus.nc` and `scheduler.sch` to compose them in the boot sequence
- `log.py` → no internal imports
- `db.py` → no internal imports
- `bus.py` → no internal imports
- `scheduler.py` → no internal imports

There are no other intra-package imports. This is intentional — modules stay independently understandable. A future maintainer should be able to read `bus.py` cover-to-cover without opening any other file in this package.

---

## 3. Public API surface

The entire public surface is re-exported from `kopilot_core/__init__.py`:

```python
from kopilot_core import nc, db, sch, run, configure_logging
```

That is the contract. Nothing else is public. Even if a name happens to be importable via `kopilot_core.bus.something`, do not treat it as part of the API.

### nc (NATSServer singleton)

Decorators:
- `@nc.sub(subject)` — register an async handler for a subject (fire-and-forget)
- `@nc.reply(subject)` — register an async responder for the request/reply pattern

Methods:
- `nc.configure(**cfg)` — inject NATS connection config; must be called before `run()`
- `nc.pub(subject, data: dict)` — publish a JSON message
- `nc.request(subject, data: dict, timeout=5)` — request/reply with timeout

Lifecycle callbacks (private, fired by nats-py):
- `_on_disconnected`, `_on_reconnected`, `_on_error`, `_on_closed` — all log to the `nats` logger

Logging contract:
- Every `pub` writes `PUB <subject> <bytes>b` at INFO
- Every received message writes `RCV <subject> <bytes>b` at INFO
- Every `request` writes `REQ-OUT` and the responder side writes `REQ`
- This is the bus boundary trace. Do not remove it.

### db (alias for MySQL class)

Sync methods:
- `db.execute_query(query, params=None, fetch_one=False)` — SELECT, returns dicts
- `db.execute_update(query, params=None)` — UPDATE, returns affected row count
- `db.execute_insert(query, params=None)` — INSERT, returns lastrowid
- `db.execute_many(query, params_list)` — batch operations

Async variants (prefix with `a`):
- `db.aexecute_query`, `db.aexecute_update`, `db.aexecute_insert`, `db.aexecute_many`

Each async method delegates to `to_thread.run_sync` and is bounded by an anyio Semaphore sized to `pool_size`. The semaphore is the backpressure mechanism — it prevents the event loop from hammering more concurrent DB calls than the pool can serve.

Configuration:
- `db.configure(**cfg)` — must be called before first DB access; rebuilds the semaphore at the configured pool_size

### sch (AsyncIOScheduler instance)

A bare APScheduler singleton. Teams do `sch.add_job(coro, trigger, ...)` at module import time inside their `schedules/` folder. The runtime calls `sch.start()` during boot.

### run()

The boot function. Composes everything. Blocks until SIGTERM or SIGINT. Returns when shutdown completes.

### configure_logging(components, log_dir, level="INFO", rotation_bytes=10MB, rotation_backups=5)

Wires per-component rotating log files plus a duplicated `errors.log` that captures all ERROR+ records across components. Idempotent (safe to call twice). See §4.5 for the duplication mechanic.

---

## 4. Design invariants — never violate

These rules are what hold the framework together. Breaking one breaks the model.

### 4.1 Configure-injection, not import-time config

`nc.configure()` and `db.configure()` are the ONLY ways config enters the framework. The package never imports from `common.config` or any team-level module. Teams own their config; the framework owns its plumbing. This is how core stays portable across 14 services with different config conventions.

If you find yourself adding `from somewhere import SOME_CFG` inside `kopilot_core/`, stop. That config belongs in the team's repo, passed through `.configure(**cfg)`.

### 4.2 Decorators populate at import time, drain at connect time

`@nc.sub("subject")` appends to `nc._handlers` when the workflow file is imported. The actual NATS subscription happens inside `nc.connect()`, which runs once during `run()`. This is why a team's `main.py` must import workflows **before** calling `run()`.

Do NOT change this contract to use auto-discovery, magic file scanning, or runtime decoration. Magic breaks at scale; explicit imports stay grep-able and debuggable.

### 4.3 NATS handlers always dispatch into the task group

Inside the subscriber wrapper, the actual handler call goes through `task_group.start_soon(handle_safely, ...)`. The wrapper itself returns immediately so NATS can keep flowing. The handler runs as a child task under anyio with full cancellation semantics.

The default-arg trick (`async def wrapper(msg, h=handler, subj=subject)`) inside `_register_handlers` is **load-bearing**. It binds the loop variables per iteration. Removing it causes every wrapper to capture only the last (subject, handler) pair. Keep it.

### 4.4 Graceful shutdown sequence is fixed

```
signal → Service.stop() → shutdown_event.set() → nc.serve unblocks → connection.close() → scheduler.shutdown(wait=True) → task group exits → run() returns
```

Do not reorder. The scheduler shuts down AFTER NATS so any final scheduled jobs that publish do so before the bus is gone. The `shutdown_event` is the single coordination point; don't introduce parallel signaling primitives.

### 4.5 Logging uses one shared errors_handler attached to multiple loggers

`configure_logging` creates one `RotatingFileHandler` for `errors.log` (level=ERROR) and attaches the same instance to root + every component logger. Each component sets `propagate=False` so records flow through exactly one logger's handler chain — no double-writes, but every ERROR record hits both the per-component file AND `errors.log`.

If you change this design, verify by hand that an ERROR on the `mysql` logger ends up in `mysql.log` once, `errors.log` once, console once. Three appearances. Not two, not four.

### 4.6 Singletons are module-level instances; do not instantiate per-anything

- `nc = NATSServer()` at module bottom of `bus.py`
- `sch = AsyncIOScheduler()` at module bottom of `scheduler.py`
- `db = MySQL` (the class itself, since all methods are classmethods)

These are imported and reused. Do not create new instances per-team, per-request, per-thread, per-anything. The reason is rate-limiter / pool-state / subscription-state survival.

### 4.7 Lifecycle callbacks log; they do not try to heal

`_on_disconnected`, `_on_reconnected`, `_on_error`, `_on_closed` log the event and return. They do NOT attempt reconnection (nats-py owns that), do NOT page anyone, do NOT raise. Visibility, not policy. If the framework starts having opinions about how to react to disconnects, that's a different layer.

---

## 5. Common pitfalls and the right answer

**Pitfall: closure-over-loop-variable in `_register_handlers`**
The inner `wrapper` and `handle_safely` use default-arg binding (`h=handler, subj=subject`) precisely because they're defined inside a `for` loop. Without the default args, all wrappers would close over the same final loop variables. This is a classic Python gotcha. The current code is correct. Don't "clean it up."

**Pitfall: importing `from common.config import X` in any kopilot_core module**
Old pattern, breaks the configure-injection invariant (§4.1). All cfg arrives via `.configure(**cfg)`.

**Pitfall: doing real work inside a NATS callback**
The wrapper runs in the NATS client's callback context. Any `await` there blocks the bus. ALWAYS `task_group.start_soon` the actual handler. Already enforced for `@nc.sub`; was a bug for `@nc.reply` historically and was fixed in v0.1.0 — responder wrappers also dispatch into the task group now.

**Pitfall: forgetting that `mysql.connector` is sync**
The MySQL pool is from `mysql-connector-python`, which is sync. The async methods bridge via `anyio.to_thread.run_sync`. Inside `execute_*`, you cannot await; you cannot use anyio primitives. If you need async-native MySQL, that's a separate proposal — likely a parallel `db_aiomysql.py` module, not a rewrite of `db.py`.

**Pitfall: adding `sch.add_job()` calls inside core**
Core never schedules anything. Schedules are team-defined inside the team's `schedules/` folder. Core just provides the `sch` singleton.

**Pitfall: `pyproject.toml` dependency creep**
Each dependency is a long-term commitment that 14+ services inherit. Adding one means every team's CI rebuilds and every team's dep tree grows. Push back hard on new dependencies. The current set (anyio, nats-py, apscheduler, mysql-connector-python, python-dotenv) is the floor, not a starting point.

---

## 6. How teams consume this package

A team's `main.py` after migration looks like this:

```python
import os
from kopilot_core import run, nc, db, configure_logging
from common.config import NATS_CFG, MYSQL_CFG

import workflows
import schedules

if __name__ == "__main__":
    configure_logging(
        components=["mysql", "nats", "scheduler", "telegram"],
        log_dir=os.environ["LOG_PATH"],
    )
    nc.configure(**NATS_CFG)
    db.configure(**MYSQL_CFG)
    run()
```

Order matters:

1. `configure_logging` first, so any import-time log calls land in the right files
2. `nc.configure` and `db.configure` next, so the registries have config before subscription/connection
3. `import workflows` and `import schedules` — these trigger `@nc.sub` and `sch.add_job` side effects that populate the registries
4. `run()` last — boots the service

A workflow file looks like:

```python
from kopilot_core import nc, db

@nc.sub("orders.created")
async def on_order_created(data):
    await db.aexecute_insert("INSERT INTO orders (id) VALUES (%s)", (data["id"],))
    await nc.pub("orders.indexed", {"id": data["id"]})
```

A schedule file looks like:

```python
from apscheduler.triggers.cron import CronTrigger
from kopilot_core import nc, sch

async def midnight_sync():
    await nc.pub("orders.sync", {})

sch.add_job(midnight_sync, CronTrigger(hour=0))
```

Teams keep third-party API wrappers (telegram, zoom, ghl, samcart, etc.) inside their own `services/` folder as singleton modules with their own rate limiters. These do NOT belong in core. If multiple teams need the same wrapper and copy-paste pain becomes real, extract into a separate package (e.g., `kopilot-telegram`) that depends on `kopilot-core`.

---

## 7. Tech debt — parked deliberately, not forgotten

These are KNOWN gaps. They were considered, intentionally not shipped in v0.1.0, and have a known shape if revisited.

### 7.1 No retry on transient DB errors

MySQL error 1213 (deadlock), 1205 (lock wait timeout), 2013 (lost connection) are transient and the MySQL docs explicitly recommend retry. The current `db.execute_*` methods do NOT retry — exceptions propagate to the workflow and the message is lost.

If asked to add retry:
- Whitelist of retryable error codes, not blanket retry on all `mysql.connector.Error`
- Exponential backoff with jitter (e.g., 50ms, 100ms, 200ms, 400ms)
- Configurable max attempts (default 3)
- Log every retry at WARNING with the error code so it's visible
- Apply to the **sync** methods (the async ones inherit it via to_thread)

### 7.2 No structured/JSON logging

The current `VERBOSE_FORMAT` is grep-friendly text. If a team starts shipping logs to Loki, ELK, or Datadog and needs JSON, swap the formatter in `log.py`. Keep the option configurable: `configure_logging(..., format="json")`. Don't make JSON the default until it's universally needed.

### 7.3 No queue handler for non-blocking log writes

Logging is synchronous. Under high contention (thousands of records per second), `RotatingFileHandler.emit` can become a bottleneck. The fix is a `QueueHandler` + `QueueListener` pattern from `logging.handlers`. Not needed today; revisit if latency profiles show logging in the hot path.

### 7.4 No hot-reload mechanism

See §8 below. This was prototyped, deliberately reverted.

### 7.5 No JetStream / durable subscriptions

Current bus is core NATS — fire-and-forget, at-most-once delivery. If a subscriber is down or a message is malformed, it's gone. JetStream would add at-least-once with replay. Real upgrade, but only justified when the team has workloads where dropping a single message is unacceptable AND idempotency at the handler level isn't enough.

### 7.6 No queue groups (multi-instance load balancing)

`@nc.sub` calls `subscribe(subject, cb=...)` with no `queue` argument. Two instances of the same service both receive every message — leading to duplicate processing. To fix: optional `queue_group` parameter on `@nc.sub`. Teams running multiple instances opt in.

### 7.7 No prometheus metrics

Could be a thin layer: counter on PUB/RCV per subject, histogram on db query duration. Don't add a `/metrics` HTTP endpoint to core — expose the registry, let the team mount it however they want.

---

## 8. The reload mechanism (deliberately not shipped)

A `systemctl reload` → SIGHUP → re-import workflow modules without killing in-flight handler tasks was prototyped during v0.1.0 development and **reverted** before release. The maintainer changed their mind on the trade-off.

If asked to revisit, the working design was:

1. Track active subscriptions in `Dict[subject, sub_object]` so we can `unsubscribe()` them
2. Refactor wrapper creation into `_make_handler_wrapper(handler, subject)` and `_make_responder_wrapper(handler, subject)` helpers — same code path used by both initial register and reload
3. Add a new module `kopilot_core/reload.py` with `reload_namespaces(prefixes: List[str])` that nukes matching entries from `sys.modules` and re-imports the package using `pkgutil.walk_packages`
4. In `runtime.py`, add SIGHUP to the signal receiver, add `Service.reload()` guarded by an `anyio.Lock`, and a `reload_modules` arg to `run()`
5. The reload sequence: `nc.clear_registry()` → `sch.remove_all_jobs()` → `reload_namespaces(...)` → `nc.reload_handlers()` (which calls `_sync_subscriptions()` to diff old vs new and add/remove/replace as needed)

Mental model:

- in-flight handler tasks hold OLD function references and continue to completion
- new messages on existing subjects route to NEW wrappers post-swap
- new messages on new subjects subscribe at sync time
- removed subjects get unsubscribed at sync time

Caveats to flag explicitly to the user before re-implementing:

- Cross-workflow `from x import y` references don't update on reload (Python's named-import limitation). Use `import x; x.y(...)` if cross-workflow refs need to follow reloads.
- Reloading the team's `services/` folder is a footgun — those modules contain singleton state (rate limiters, connection pools) that you do NOT want reset.
- Service unit needs `ExecReload=/bin/kill -HUP $MAINPID` to make `systemctl reload` actually do something.

The reason this was reverted: the maintainer judged that the operational simplicity of "restart the service" was greater than the value of zero-downtime reload, given that systemd `Restart=always` already handles the restart in <2 seconds and the worst case is a few in-flight handler tasks getting cancelled. Don't push back on this decision unless the user explicitly asks.

---

## 9. Release flow

```
# bump version in pyproject.toml
git commit -am "vX.Y.Z: <one-line summary>

- bullet of change 1
- bullet of change 2"
git tag -a vX.Y.Z -m "vX.Y.Z"
git push origin main
git push origin vX.Y.Z
```

semver:

- patch (X.Y.Z → X.Y.Z+1) — bug fixes, internal refactors, log message tweaks
- minor (X.Y.Z → X.Y+1.0) — additions to the public API, new optional features, new dependencies
- major (X+1.0.0) — breaking changes to the public API, removed features, signature changes

Teams pin `kopilot-core>=X.Y` in their `pyproject.toml`. They migrate to a new major on their own schedule. NEVER backport breaking changes to a minor version.

When releasing, write the commit message and tag annotation as if a future maintainer of a team service is reading them six months from now and trying to figure out what changed. Be specific about the public-API impact.

---

## 10. Adding new features — the bar to clear

Before adding anything to core, answer all four:

1. **Is this needed by more than one team?** If only one team needs it, it lives in their repo.
2. **Is this infrastructure?** Core is for the bus, the DB, the scheduler, the runtime, the logger. Business logic, integrations, and team-specific orchestration are NOT core concerns.
3. **Does it break any invariant in §4?** If yes, the answer is no — find a different shape.
4. **Can the public surface stay the same?** New methods on existing classes are fine. New top-level exports require justification.

If all four pass, propose the change as a thin slice (the maintainer works in onion-layer ping-pong, not full PRs). Get buy-in on the slice before writing it. Land it. Then layer the next slice.

Things that do NOT belong in core:

- HTTP routing / web servers
- Workflow orchestration (DAGs, retries, sagas — that's a different package)
- Team-specific business logic
- Database migrations / schema management (use a real migration tool in the team repo)
- Authentication / authorization (per-team concern)

---

## 11. Common requests and the right answer

**Q: "Add a Postgres backend to db.py."**
A: Don't modify `db.py`. The MySQL pool is the canonical store for kopilot. If a team genuinely needs Postgres alongside, propose `kopilot-postgres` as a separate package. More likely answer: push back — adding a second SQL backend doubles the surface area for a niche use case.

**Q: "Add Kafka / RabbitMQ / SQS support alongside NATS."**
A: Don't. The bus is NATS. If something else is needed, that's a separate package.

**Q: "Make the workflows hot-reload."**
A: See §8. This was deliberately reverted. Revisit only if explicitly asked, and present the trade-offs from §8 first.

**Q: "Add prometheus metrics."**
A: §7.7. Plausible thin layer. Counter on PUB/RCV per subject, histogram on db query duration. Expose the registry; do not add an HTTP `/metrics` endpoint to core.

**Q: "Add JSON logging."**
A: §7.2. Make it configurable in `configure_logging`. Don't make JSON the default. Verify the existing per-component file behavior still works with the JSON formatter.

**Q: "Add deadlock retry to db.py."**
A: §7.1. Real concern. Implement with whitelisted error codes, exponential backoff with jitter, configurable max attempts, WARNING-level log on each retry. Apply to sync methods so the async ones inherit via to_thread.

**Q: "Refactor `_register_handlers` — those nested closures look ugly."**
A: Don't. The default-arg trick is load-bearing (§4.3). The closure pattern was deliberately preserved through the v0.1.0 refactor. If you want to clean it up, use the `_make_handler_wrapper` shape from §8 — but only if you're also implementing reload, otherwise it's a refactor with no payoff.

**Q: "Make `nc.configure()` happen automatically from env vars."**
A: Don't. The configure-injection invariant (§4.1) is what keeps core portable. Reading env vars is the team's job.

**Q: "Add an `nc.subscribe(subject, queue_group=...)` method."**
A: §7.6. This is the right shape if the team is moving to multi-instance. Add `queue_group` as an optional parameter on `@nc.sub` and pass it through to `connection.subscribe()`. Default to None (current behavior). Document loudly that with `queue_group=None`, all instances receive all messages — that's the duplicate-processing trap.

**Q: "Add WebSocket support / GraphQL / a REST API."**
A: Out of scope for core. Mount a separate FastAPI/aiohttp server in the team's main.py if needed. Core is the automation kernel, not the front door.

---

## 12. File-level cheat sheet

When asked to do X, the relevant module is usually:

| Task | File |
| --- | --- |
| change subject registration / wrapper logic | bus.py |
| add / change a NATS lifecycle log | bus.py (`_on_*` methods) |
| change the PUB/RCV/REQ trace format | bus.py |
| change pool semantics, async bridging, semaphore | db.py |
| add / change a DB convenience method | db.py |
| add / change a log handler, format, level routing | log.py |
| change the boot sequence, signal handling | runtime.py |
| change the scheduler instance config | scheduler.py |
| add a new public name | __init__.py + the source module |
| bump dependency versions, add optional extras | pyproject.toml |

If the task touches more than two files, you're probably crossing a layer boundary. Stop and ask.

---

## 13. Repo conventions

- Single package: `kopilot_core` (underscore for the Python package, hyphen for the distribution name `kopilot-core`)
- Module names match concerns: nouns, no `utils.py`, no `helpers.py`
- Loggers use the component name: `logging.getLogger("nats")`, `logging.getLogger("mysql")`, etc., so `configure_logging` can route them by name
- Public surface defined exclusively in `__init__.py`
- No `__all__` gymnastics inside individual modules — the contract lives in `__init__.py`
- Type hints on public methods; internal helpers can be untyped if obvious
- Docstrings on `configure_logging` and `run()` (the entrypoints teams interact with directly); other internals are read-the-source

---

## 14. When in doubt

- The maintainer prefers KISS, ping-pong, terminal-style communication
- Prefer paraphrasing over duplicating code from elsewhere
- Prefer asking one sharp question over guessing
- Prefer doing one thin slice over writing a 10-point plan
- Prefer breaking a change into a slice the maintainer can verify in <5 minutes
- If a request would violate any §4 invariant, name the invariant and ask before proceeding

This package is small on purpose. Keep it that way.