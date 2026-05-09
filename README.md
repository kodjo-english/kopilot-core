# kopilot-core

Async automation microframework. NATS message bus, APScheduler, MySQL pool - composed with anyio structured concurrency. Designed for fork-and-forget services that handle scheduled jobs, webhook ingestion, and event-driven automation.

## Install

```
pip install kopilot-core
```

Or from a checkout:

```
pip install -e /path/to/kopilot-core
```

## Service layout

```
my-service/
├── main.py
├── workflows/
│   └── orders.py
└── schedules/
    └── daily.py
```

## main.py

```python
import os
from kopilot_core import run, nc, db, configure_logging

import workflows.orders
import schedules.daily

NATS_CFG  = {"servers": os.environ["NATS_URL"]}
MYSQL_CFG = {
    "host":     os.environ["MYSQL_HOST"],
    "user":     os.environ["MYSQL_USER"],
    "password": os.environ["MYSQL_PASSWORD"],
    "database": os.environ["MYSQL_DATABASE"],
    "pool_size": 16,
}

if __name__ == "__main__":
    configure_logging(
        components=["mysql", "nats", "scheduler"],
        log_dir=os.environ["LOG_PATH"],
    )
    nc.configure(**NATS_CFG)
    db.configure(**MYSQL_CFG)
    run()
```

## Workflow

```python
from kopilot_core import nc, db

@nc.sub("orders.created")
async def on_order_created(data):
    await db.aexecute_insert(
        "INSERT INTO orders (id, total) VALUES (%s, %s)",
        (data["id"], data["total"]),
    )
    await nc.pub("orders.indexed", {"id": data["id"]})
```

## Schedule

```python
from apscheduler.triggers.cron import CronTrigger
from kopilot_core import nc, sch

async def midnight_sync():
    await nc.pub("orders.sync", {})

sch.add_job(midnight_sync, CronTrigger(hour=0))
```

## Public API

- `nc` - NATS bus: `@nc.sub`, `@nc.reply`, `nc.pub`, `nc.request`
- `db` - MySQL: `execute_query`, `execute_update`, `execute_insert`, plus async variants prefixed `a`
- `sch` - APScheduler instance, use `sch.add_job(...)`
- `run()` - boots the service, blocks until SIGTERM/SIGINT
- `configure_logging(components, log_dir)` - per-component rotating log files plus a duplicated `errors.log`

## systemd

```
[Unit]
Description=My Kopilot Service
After=network.target

[Service]
Type=exec
WorkingDirectory=/opt/services/my-service
ExecStart=/opt/services/my-service/venv/bin/python main.py
Environment=LOG_PATH=/var/log/my-service/
EnvironmentFile=/opt/services/my-service/.env
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```