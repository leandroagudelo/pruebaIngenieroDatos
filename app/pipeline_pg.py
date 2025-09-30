"""Command line interface for the RAW→SILVER→GOLD CSV pipeline."""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import os
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Iterable, List, Sequence

import psycopg2
from psycopg2.extras import execute_values

DEFAULT_DSN = os.getenv(
    "DATABASE_URL", "postgresql://postgres:postgres@postgres:5432/postgres"
)
DEFAULT_SOURCE = Path(
    os.getenv("PIPELINE_SOURCE_CSV", "/opt/pipeline/data/raw/events.csv")
)

RAW_SCHEMA_DDL = """
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS raw.events_raw (
    ingestion_id BIGSERIAL PRIMARY KEY,
    source_file TEXT NOT NULL,
    event_id TEXT,
    event_type TEXT,
    user_id TEXT,
    event_timestamp TEXT,
    amount TEXT,
    loaded_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS silver.events (
    event_id TEXT PRIMARY KEY,
    event_type TEXT NOT NULL,
    user_id TEXT NOT NULL,
    event_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    amount NUMERIC(12,2) NOT NULL,
    source_file TEXT NOT NULL,
    loaded_at TIMESTAMP WITH TIME ZONE NOT NULL
);

CREATE TABLE IF NOT EXISTS gold.global_stats (
    snapshot_date DATE PRIMARY KEY,
    total_events BIGINT NOT NULL,
    total_users BIGINT NOT NULL,
    total_amount NUMERIC(14,2) NOT NULL
);

CREATE TABLE IF NOT EXISTS gold.metrics (
    snapshot_date DATE NOT NULL,
    event_type TEXT NOT NULL,
    events_count BIGINT NOT NULL,
    total_amount NUMERIC(14,2) NOT NULL,
    avg_amount NUMERIC(14,2) NOT NULL,
    PRIMARY KEY (snapshot_date, event_type)
);
"""


def get_connection(dsn: str):
    return psycopg2.connect(dsn)


def run_init(dsn: str) -> None:
    with get_connection(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(RAW_SCHEMA_DDL)
        conn.commit()
    print("Schemas and tables are ready.")


def detect_chunk_size(path: Path) -> int:
    file_size = path.stat().st_size
    if file_size == 0:
        return 100
    if file_size < 128 * 1024:
        return 200
    if file_size < 1 * 1024 * 1024:
        return 500
    if file_size < 10 * 1024 * 1024:
        return 1000
    return 5000


def normalize_timestamp(raw_value: str) -> dt.datetime:
    value = raw_value.strip()
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    return dt.datetime.fromisoformat(value)


def safe_decimal(raw_value: str) -> Decimal:
    try:
        return Decimal(raw_value)
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"Invalid numeric value: {raw_value}") from exc


def chunked(iterable: Iterable[dict], size: int) -> Iterable[List[dict]]:
    batch: List[dict] = []
    for row in iterable:
        batch.append(row)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def insert_raw(cur, rows: Sequence[dict], source_file: str) -> None:
    values = [
        (
            source_file,
            row.get("event_id"),
            row.get("event_type"),
            row.get("user_id"),
            row.get("event_timestamp"),
            row.get("amount"),
        )
        for row in rows
    ]
    execute_values(
        cur,
        """
        INSERT INTO raw.events_raw
            (source_file, event_id, event_type, user_id, event_timestamp, amount)
        VALUES %s
        """,
        values,
    )


def upsert_silver(cur, rows: Sequence[dict], source_file: str) -> int:
    cleaned = []
    now = dt.datetime.now(dt.timezone.utc)
    for row in rows:
        try:
            event_id = row["event_id"].strip()
            event_type = row["event_type"].strip()
            user_id = row["user_id"].strip()
            ts = normalize_timestamp(row["event_timestamp"])
            amount = safe_decimal(row["amount"])
        except (AttributeError, KeyError, ValueError) as exc:
            print(f"Skipping row due to parsing error: {exc}. Row={row}")
            continue
        cleaned.append(
            (
                event_id,
                event_type,
                user_id,
                ts,
                amount,
                source_file,
                now,
            )
        )
    if not cleaned:
        return 0
    execute_values(
        cur,
        """
        INSERT INTO silver.events
            (event_id, event_type, user_id, event_timestamp, amount, source_file, loaded_at)
        VALUES %s
        ON CONFLICT (event_id) DO UPDATE SET
            event_type = EXCLUDED.event_type,
            user_id = EXCLUDED.user_id,
            event_timestamp = EXCLUDED.event_timestamp,
            amount = EXCLUDED.amount,
            source_file = EXCLUDED.source_file,
            loaded_at = EXCLUDED.loaded_at
        """,
        cleaned,
    )
    return len(cleaned)


def refresh_gold(conn) -> None:
    snapshot_date = dt.date.today()
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM gold.global_stats WHERE snapshot_date = %s", (snapshot_date,)
        )
        cur.execute(
            """
            INSERT INTO gold.global_stats (snapshot_date, total_events, total_users, total_amount)
            SELECT %s AS snapshot_date,
                   COUNT(*) AS total_events,
                   COUNT(DISTINCT user_id) AS total_users,
                   COALESCE(SUM(amount), 0) AS total_amount
            FROM silver.events
            """,
            (snapshot_date,),
        )
        cur.execute(
            "DELETE FROM gold.metrics WHERE snapshot_date = %s", (snapshot_date,)
        )
        cur.execute(
            """
            INSERT INTO gold.metrics (snapshot_date, event_type, events_count, total_amount, avg_amount)
            SELECT %s AS snapshot_date,
                   event_type,
                   COUNT(*) AS events_count,
                   COALESCE(SUM(amount), 0) AS total_amount,
                   COALESCE(AVG(amount), 0) AS avg_amount
            FROM silver.events
            GROUP BY event_type
            ORDER BY event_type
            """,
            (snapshot_date,),
        )
    conn.commit()
    print(f"Gold layer refreshed for snapshot {snapshot_date}.")


def run_load(dsn: str, source: Path, stage: str, chunk_size: str | int) -> None:
    if stage == "gold":
        with get_connection(dsn) as conn:
            refresh_gold(conn)
        return
    if not source.exists():
        raise FileNotFoundError(f"Source file not found: {source}")
    if chunk_size == "auto":
        size = detect_chunk_size(source)
    else:
        size = int(chunk_size)
    print(f"Using chunk size: {size}")

    with get_connection(dsn) as conn:
        conn.autocommit = False
        with open(source, newline="") as handle:
            reader = csv.DictReader(handle)
            total_raw = 0
            total_silver = 0
            for batch in chunked(reader, size):
                with conn.cursor() as cur:
                    if stage in {"all", "raw", "silver"}:
                        insert_raw(cur, batch, source.name)
                        total_raw += len(batch)
                    if stage in {"all", "silver"}:
                        inserted = upsert_silver(cur, batch, source.name)
                        total_silver += inserted
                conn.commit()
            print(
                f"Load completed. Raw rows inserted: {total_raw}. Silver rows upserted: {total_silver}."
            )
        if stage == "all":
            refresh_gold(conn)


def run_check(dsn: str) -> None:
    with get_connection(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM raw.events_raw")
            raw_count = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM silver.events")
            silver_count = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM gold.global_stats")
            global_stats_count = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM gold.metrics")
            metrics_count = cur.fetchone()[0]
        print("Data quality report:")
        print(f"  RAW rows: {raw_count}")
        print(f"  SILVER rows: {silver_count}")
        print(f"  GOLD global_stats snapshots: {global_stats_count}")
        print(f"  GOLD metrics rows: {metrics_count}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="CSV→Postgres pipeline orchestrator with RAW→SILVER→GOLD architecture.",
    )
    parser.add_argument(
        "--dsn",
        default=DEFAULT_DSN,
        help="Database DSN, default from DATABASE_URL environment variable.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("init", help="Create schemas and tables if they do not exist.")

    load_parser = subparsers.add_parser(
        "load",
        help="Load the CSV file in micro-batches and update RAW/SILVER/GOLD layers.",
    )
    load_parser.add_argument(
        "--source",
        type=Path,
        default=DEFAULT_SOURCE,
        help="Path to the CSV source file.",
    )
    load_parser.add_argument(
        "--chunk-size",
        default="auto",
        help="Micro-batch size. Use 'auto' to infer a reasonable value.",
    )
    load_parser.add_argument(
        "--stage",
        choices=["raw", "silver", "gold", "all"],
        default="all",
        help="Limit the processing to a specific layer.",
    )

    subparsers.add_parser(
        "check", help="Run lightweight checks on RAW/SILVER/GOLD layer counts."
    )

    return parser


def main(argv: Sequence[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "init":
        run_init(args.dsn)
    elif args.command == "load":
        run_load(args.dsn, args.source, args.stage, args.chunk_size)
    elif args.command == "check":
        run_check(args.dsn)
    else:
        parser.error(f"Unknown command {args.command}")


if __name__ == "__main__":
    main()
