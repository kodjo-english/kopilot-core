"""Generic module runner. Usage: python -m kopilot_core.module <module_name>"""
import importlib
import os
import sys

from dotenv import load_dotenv

from kopilot_core import run, nc, db, configure_logging


def main():
    if len(sys.argv) < 2:
        print("Usage: python -m kopilot_core.module <module_name>", file=sys.stderr)
        sys.exit(2)

    module_name = sys.argv[1]
    load_dotenv()

    nats_cfg = {
        "servers": os.environ["NATS_URL"],
        "name": f"kopilot_{module_name}",
        "reconnect_time_wait": 2,
        "max_reconnect_attempts": 10,
    }
    mysql_cfg = {
        "host":     os.environ["MYSQL_HOST"],
        "user":     os.environ["MYSQL_USER"],
        "password": os.environ["MYSQL_PASSWORD"],
        "database": os.environ["MYSQL_DATABASE"],
        "pool_size": int(os.environ.get("MYSQL_POOL_SIZE", "16")),
    }

    configure_logging(
        components=["mysql", "nats", "apscheduler", module_name],
        log_dir=os.environ["LOG_PATH"],
    )
    nc.configure(**nats_cfg)
    db.configure(**mysql_cfg)

    importlib.import_module(f"modules.{module_name}.handlers")
    try:
        importlib.import_module(f"modules.{module_name}.schedules")
    except ImportError:
        pass  # module may not have schedules

    run()


if __name__ == "__main__":
    main()