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


if __name__ == "__main__":
    query_main()