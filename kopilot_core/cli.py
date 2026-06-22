import argparse
import json
import os
import sys
from datetime import date, datetime
from decimal import Decimal

import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv


def _json_default(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return str(obj)
    if isinstance(obj, bytes):
        return obj.decode("utf-8", errors="replace")
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


def query_main():
    load_dotenv()

    parser = argparse.ArgumentParser(
        prog="kopilot-query",
        description="Read-only MySQL query CLI for kopilot. JSON on stdout, errors on stderr.",
    )
    parser.add_argument("query", help="SQL query")
    parser.add_argument("--params", default="[]", help="JSON array of query parameters")
    parser.add_argument("--database", default=None, help="Override MYSQL_DATABASE env var")
    parser.add_argument("--pretty", action="store_true", help="Pretty-print JSON")
    args = parser.parse_args()

    cfg = {
        "host": os.environ.get("MYSQL_HOST"),
        "user": os.environ.get("MYSQL_RO_USER"),
        "password": os.environ.get("MYSQL_RO_PASSWORD"),
        "database": args.database or os.environ.get("MYSQL_DATABASE"),
    }

    missing = [k for k, v in cfg.items() if not v]
    if missing:
        print(
            f"Missing config: {', '.join(missing)}. "
            "Set MYSQL_HOST, MYSQL_RO_USER, MYSQL_RO_PASSWORD, MYSQL_DATABASE in .env",
            file=sys.stderr,
        )
        sys.exit(2)

    try:
        params = json.loads(args.params)
        if not isinstance(params, list):
            raise ValueError("--params must be a JSON array")
    except (json.JSONDecodeError, ValueError) as e:
        print(f"Invalid --params: {e}", file=sys.stderr)
        sys.exit(1)

    conn = None
    cursor = None
    try:
        conn = mysql.connector.connect(**cfg)
        cursor = conn.cursor(dictionary=True)
        cursor.execute(args.query, tuple(params))
        rows = cursor.fetchall()
    except Error as e:
        print(f"MySQL error: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()

    indent = 2 if args.pretty else None
    print(json.dumps(rows, default=_json_default, indent=indent))


def shell_main():
    parser = argparse.ArgumentParser(
        prog="kopilot-shell",
        description="Interactive shell with kopilot db preconfigured from .env",
    )
    parser.add_argument("--ro", action="store_true", help="Use MYSQL_RO_USER (read-only)")
    parser.add_argument("--verbose", "-v", action="store_true", help="DEBUG logging")
    parser.add_argument("--quiet", "-q", action="store_true", help="WARNING logging only")
    args = parser.parse_args()

    load_dotenv()

    user_var = "MYSQL_RO_USER" if args.ro else "MYSQL_USER"
    pass_var = "MYSQL_RO_PASSWORD" if args.ro else "MYSQL_PASSWORD"

    mysql_cfg = {
        "host":     os.environ.get("MYSQL_HOST"),
        "user":     os.environ.get(user_var),
        "password": os.environ.get(pass_var),
        "database": os.environ.get("MYSQL_DATABASE"),
        "pool_size": int(os.environ.get("MYSQL_POOL_SIZE", "5")),
    }

    missing = [k for k, v in mysql_cfg.items() if v is None]
    if missing:
        print(
            f"Missing config: {', '.join(missing)}. "
            f"Set MYSQL_HOST, {user_var}, {pass_var}, MYSQL_DATABASE in .env",
            file=sys.stderr,
        )
        sys.exit(2)

    from kopilot_core import db, nc, sch
    import logging

    db.configure(**mysql_cfg)
    nc.configure(
        servers=os.environ.get("NATS_URL"),
        name="kopilot_shell",
        reconnect_time_wait=2,
        max_reconnect_attempts=10,
    )

    level = logging.DEBUG if args.verbose else (logging.WARNING if args.quiet else logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(levelname)s %(asctime)s %(name)s %(message)s",
    )

    mode = "READ-ONLY" if args.ro else "READ-WRITE"
    banner = (
        f"\nkopilot-shell — {mysql_cfg['host']}/{mysql_cfg['database']} "
        f"as {mysql_cfg['user']} ({mode})\n"
        "db is ready. For the bus, first:  await nc.connect()  "
        "then nc.pub / nc.jpub / nc.request\n"
    )

    namespace = {"db": db, "nc": nc, "sch": sch}

    try:
        from IPython.terminal.embed import InteractiveShellEmbed
        shell = InteractiveShellEmbed(user_ns=namespace, banner1=banner, colors="Linux")
        shell.run_line_magic("autoawait", "asyncio")
        shell()
    except ImportError:
        import code
        print("IPython not installed; plain REPL (no top-level await).", file=sys.stderr)
        code.interact(local=namespace, banner=banner)

if __name__ == "__main__":
    query_main()