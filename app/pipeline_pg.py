"""Command line interface for the RAW→SILVER→GOLD CSV pipeline."""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import os
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from typing import List, Sequence

import psycopg2
from psycopg2.extras import execute_values

DEFAULT_DSN = os.getenv(
    "DATABASE_URL", "postgresql://postgres:postgres@postgres:5432/postgres"
)
DEFAULT_DATA_DIR = Path(os.getenv("PIPELINE_DATA_DIR", "/opt/pipeline/data/raw"))
DEFAULT_CHUNK_SIZE = int(os.getenv("PIPELINE_BATCH_SIZE", "5"))
EXPECTED_HEADER = ["timestamp", "price", "user_id"]
TWO_PLACES = Decimal("0.01")
ZERO = Decimal("0")
SQL_DIR = Path(__file__).resolve().parent / "sql"

BASE_DDL = """
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS raw.events_raw (
    id BIGSERIAL PRIMARY KEY,
    source_file TEXT NOT NULL,
    row_number INTEGER NOT NULL,
    timestamp_raw TEXT,
    price_raw TEXT,
    user_id_raw TEXT,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (source_file, row_number)
);

CREATE TABLE IF NOT EXISTS silver.events (
    raw_id BIGINT PRIMARY KEY,
    event_date DATE NOT NULL,
    price NUMERIC(18, 2) NOT NULL,
    user_id NUMERIC(18, 0) NOT NULL,
    dq_status TEXT NOT NULL,
    source_file TEXT NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""


def read_sql_files() -> List[str]:
    if not SQL_DIR.exists():
        return []
    statements: List[str] = []
    for sql_file in sorted(SQL_DIR.glob("*.sql")):
        content = sql_file.read_text(encoding="utf-8").strip()
        if content:
            statements.append(content)
    return statements


@dataclass
class MetricSummary:
    count: int
    total: Decimal
    min_price: Decimal | None
    max_price: Decimal | None

    def average(self) -> Decimal:
        if self.count == 0:
            return Decimal("0.00")
        return (self.total / Decimal(self.count)).quantize(
            TWO_PLACES, rounding=ROUND_HALF_UP
        )


def format_decimal(value: Decimal | None) -> str:
    if value is None:
        return "n/a"
    if not isinstance(value, Decimal):
        value = Decimal(value)
    return str(value.quantize(TWO_PLACES, rounding=ROUND_HALF_UP))


def print_metrics(title: str, metrics: MetricSummary) -> None:
    print(title)
    print(f"  count: {metrics.count}")
    print(f"  avg: {format_decimal(metrics.average())}")
    print(f"  min: {format_decimal(metrics.min_price)}")
    print(f"  max: {format_decimal(metrics.max_price)}")


def get_connection(dsn: str):
    return psycopg2.connect(dsn)


def resolve_chunk_size(chunk_size: str | int | None) -> int:
    if chunk_size is None:
        return DEFAULT_CHUNK_SIZE
    if isinstance(chunk_size, int):
        return max(1, chunk_size)
    chunk_size_str = str(chunk_size).strip().lower()
    if chunk_size_str == "auto":
        return DEFAULT_CHUNK_SIZE
    return max(1, int(chunk_size_str))


def parse_excludes(values: Sequence[str] | None) -> set[str]:
    if not values:
        return set()
    result: set[str] = set()
    for value in values:
        for part in value.split(","):
            part = part.strip()
            if part:
                result.add(part)
    return result


def ensure_global_stats_row(cur) -> None:
    cur.execute(
        """
        INSERT INTO gold.global_stats (
            id, total_count, total_sum, min_price, max_price, last_silver_id, updated_at
        )
        VALUES (1, 0, 0, NULL, NULL, 0, NOW())
        ON CONFLICT (id) DO NOTHING
        """
    )


def run_init(dsn: str) -> None:
    with get_connection(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(BASE_DDL)
            for statement in read_sql_files():
                cur.execute(statement)
            ensure_global_stats_row(cur)
        conn.commit()
    print("Schemas and tables are ready.")


def discover_files(data_dir: Path, pattern: str) -> List[Path]:
    if not data_dir.exists():
        raise FileNotFoundError(f"Data directory not found: {data_dir}")
    return sorted(p for p in data_dir.glob(pattern) if p.is_file())


def coerce_date(value: str | None) -> tuple[dt.date, bool]:
    default_date = dt.date(1970, 1, 1)
    if value is None:
        return default_date, True
    text = value.strip()
    if not text:
        return default_date, True
    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        parsed = dt.datetime.fromisoformat(text)
        return parsed.date(), False
    except ValueError:
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                parsed = dt.datetime.strptime(text, fmt)
                return parsed.date(), True
            except ValueError:
                continue
    return default_date, True


def coerce_price(value: str | None) -> tuple[Decimal, bool]:
    if value is None:
        return Decimal("0.00"), True
    text = value.strip()
    if not text:
        return Decimal("0.00"), True
    try:
        decimal_value = Decimal(text)
    except InvalidOperation:
        return Decimal("0.00"), True
    return decimal_value.quantize(TWO_PLACES, rounding=ROUND_HALF_UP), False


def coerce_user_id(value: str | None) -> tuple[Decimal, bool]:
    if value is None:
        return Decimal("0"), True
    text = value.strip()
    if not text:
        return Decimal("0"), True
    try:
        decimal_value = Decimal(text)
    except InvalidOperation:
        return Decimal("0"), True
    try:
        integral = int(decimal_value)
    except (ValueError, OverflowError):
        return Decimal("0"), True
    coerced = decimal_value != Decimal(integral)
    return Decimal(integral), coerced


def record_load_log(
    cur,
    *,
    layer: str,
    file_name: str | None,
    records: int,
    prices: Sequence[Decimal],
    chunk_size: int,
    status: str,
    details: str | None = None,
) -> None:
    min_price = min(prices) if prices else None
    max_price = max(prices) if prices else None
    avg_price = None
    total_sum = sum(prices, ZERO)
    if records > 0 and prices:
        avg_price = (total_sum / Decimal(records)).quantize(
            TWO_PLACES, rounding=ROUND_HALF_UP
        )
    if min_price is not None:
        min_price = min_price.quantize(TWO_PLACES, rounding=ROUND_HALF_UP)
    if max_price is not None:
        max_price = max_price.quantize(TWO_PLACES, rounding=ROUND_HALF_UP)
    cur.execute(
        """
        INSERT INTO gold.load_log (
            layer, file_name, records, min_price, avg_price, max_price, chunk_size, status, details
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            layer,
            file_name,
            records,
            min_price,
            avg_price,
            max_price,
            chunk_size,
            status,
            details,
        ),
    )


def insert_raw_chunk(cur, rows: Sequence[tuple]) -> List[tuple[int, int]]:
    return execute_values(
        cur,
        """
        INSERT INTO raw.events_raw (
            source_file, row_number, timestamp_raw, price_raw, user_id_raw
        )
        VALUES %s
        ON CONFLICT (source_file, row_number) DO NOTHING
        RETURNING id, row_number
        """,
        rows,
        fetch=True,
    )


def run_load_raw(
    dsn: str,
    *,
    data_dir: Path,
    pattern: str,
    exclude: set[str],
    chunk_size: int,
) -> MetricSummary:
    files = discover_files(data_dir, pattern)
    if exclude:
        files = [f for f in files if f.name not in exclude]
    if not files:
        print("[RAW] No hay archivos para procesar.")
        return MetricSummary(0, Decimal("0.00"), None, None)

    total_inserted = 0
    all_inserted_prices: List[Decimal] = []

    with get_connection(dsn) as conn:
        with conn.cursor() as cur:
            for file_path in files:
                print(f"[RAW] Procesando {file_path.name}")
                with open(file_path, newline="", encoding="utf-8") as handle:
                    reader = csv.reader(handle)
                    header = next(reader, None)
                    if header != EXPECTED_HEADER:
                        message = (
                            f"Encabezado inesperado ({header}); se omite el archivo {file_path.name}."
                        )
                        print(f"  → {message}")
                        record_load_log(
                            cur,
                            layer="raw",
                            file_name=file_path.name,
                            records=0,
                            prices=[],
                            chunk_size=chunk_size,
                            status="SKIPPED_BAD_HEADER",
                            details=message,
                        )
                        conn.commit()
                        continue

                    batch: List[tuple] = []
                    row_number = 0
                    pending_prices: dict[int, Decimal] = {}
                    file_inserted_prices: List[Decimal] = []
                    for row in reader:
                        row_number += 1
                        if len(row) != 3:
                            print(
                                f"  → Fila {row_number} ignorada (se esperaban 3 columnas y se recibieron {len(row)})."
                            )
                            continue
                        timestamp_raw, price_raw, user_id_raw = row
                        batch.append(
                            (
                                file_path.name,
                                row_number,
                                timestamp_raw,
                                price_raw,
                                user_id_raw,
                            )
                        )
                        coerced_price, _ = coerce_price(price_raw)
                        pending_prices[row_number] = coerced_price
                        if len(batch) >= chunk_size:
                            inserted = insert_raw_chunk(cur, batch)
                            for _, inserted_row_number in inserted:
                                price_value = pending_prices[inserted_row_number]
                                file_inserted_prices.append(price_value)
                                all_inserted_prices.append(price_value)
                            total_inserted += len(inserted)
                            batch = []
                    if batch:
                        inserted = insert_raw_chunk(cur, batch)
                        for _, inserted_row_number in inserted:
                            price_value = pending_prices[inserted_row_number]
                            file_inserted_prices.append(price_value)
                            all_inserted_prices.append(price_value)
                        total_inserted += len(inserted)
                    if file_inserted_prices:
                        status = "SUCCESS"
                    elif row_number > 0:
                        status = "NO_NEW_ROWS"
                    else:
                        status = "EMPTY_FILE"
                    print(
                        f"  → Filas leídas: {row_number}, insertadas en RAW: {len(file_inserted_prices)}"
                    )
                    record_load_log(
                        cur,
                        layer="raw",
                        file_name=file_path.name,
                        records=len(file_inserted_prices),
                        prices=file_inserted_prices,
                        chunk_size=chunk_size,
                        status=status,
                    )
                    conn.commit()
    total_sum = sum(all_inserted_prices, ZERO)
    min_price = min(all_inserted_prices) if all_inserted_prices else None
    max_price = max(all_inserted_prices) if all_inserted_prices else None
    summary = MetricSummary(total_inserted, total_sum, min_price, max_price)
    print_metrics("[RAW] Resumen", summary)
    return summary


def run_raw_to_silver(dsn: str, *, chunk_size: int) -> MetricSummary:
    inserted_prices: List[Decimal] = []
    coerced_rows = 0
    processed_rows = 0

    with get_connection(dsn) as conn:
        with conn.cursor() as cur:
            while True:
                cur.execute(
                    """
                    SELECT r.id, r.source_file, r.timestamp_raw, r.price_raw, r.user_id_raw
                    FROM raw.events_raw r
                    LEFT JOIN silver.events s ON s.raw_id = r.id
                    WHERE s.raw_id IS NULL
                    ORDER BY r.id
                    LIMIT %s
                    """,
                    (chunk_size,),
                )
                rows = cur.fetchall()
                if not rows:
                    break

                payload: List[tuple] = []
                for raw_id, source_file, ts_raw, price_raw, user_raw in rows:
                    event_date, date_coerced = coerce_date(ts_raw)
                    price, price_coerced = coerce_price(price_raw)
                    user_id, user_coerced = coerce_user_id(user_raw)
                    dq_status = "COERCED" if (date_coerced or price_coerced or user_coerced) else "OK"
                    if dq_status == "COERCED":
                        coerced_rows += 1
                    payload.append(
                        (
                            raw_id,
                            event_date,
                            price,
                            user_id,
                            dq_status,
                            source_file,
                        )
                    )
                    inserted_prices.append(price)
                    processed_rows += 1
                execute_values(
                    cur,
                    """
                    INSERT INTO silver.events (
                        raw_id, event_date, price, user_id, dq_status, source_file
                    )
                    VALUES %s
                    ON CONFLICT (raw_id) DO NOTHING
                    """,
                    payload,
                )
                conn.commit()
            record_load_log(
                cur,
                layer="silver",
                file_name="raw.events_raw",
                records=processed_rows,
                prices=inserted_prices,
                chunk_size=chunk_size,
                status="SUCCESS" if processed_rows else "NO_NEW_ROWS",
                details=f"Filas con coerción: {coerced_rows}",
            )
        conn.commit()

    total_sum = sum(inserted_prices, ZERO)
    min_price = min(inserted_prices) if inserted_prices else None
    max_price = max(inserted_prices) if inserted_prices else None
    summary = MetricSummary(processed_rows, total_sum, min_price, max_price)
    print_metrics("[SILVER] Resumen", summary)
    print(f"[SILVER] Filas con coerción: {coerced_rows}")
    return summary


def fetch_global_stats(cur) -> MetricSummary:
    cur.execute(
        """
        SELECT total_count, total_sum, min_price, max_price
        FROM gold.global_stats
        WHERE id = 1
        """
    )
    row = cur.fetchone()
    if not row:
        return MetricSummary(0, Decimal("0.00"), None, None)
    total_count, total_sum, min_price, max_price = row
    return MetricSummary(
        int(total_count or 0),
        Decimal(total_sum or 0),
        Decimal(min_price) if min_price is not None else None,
        Decimal(max_price) if max_price is not None else None,
    )


def run_silver_to_gold(dsn: str, *, chunk_size: int) -> MetricSummary:
    aggregated_prices: List[Decimal] = []
    processed = 0

    with get_connection(dsn) as conn:
        with conn.cursor() as cur:
            ensure_global_stats_row(cur)
            before = fetch_global_stats(cur)
            last_processed_id = 0
            while True:
                cur.execute(
                    """
                    SELECT raw_id, price
                    FROM silver.events
                    WHERE raw_id > (
                        SELECT last_silver_id FROM gold.global_stats WHERE id = 1
                    )
                    ORDER BY raw_id
                    LIMIT %s
                    """,
                    (chunk_size,),
                )
                rows = cur.fetchall()
                if not rows:
                    break
                raw_ids = [row[0] for row in rows]
                prices = [row[1] for row in rows]
                batch_count = len(rows)
                batch_sum = sum(prices, ZERO)
                batch_min = min(prices)
                batch_max = max(prices)
                processed += batch_count
                aggregated_prices.extend(prices)
                last_processed_id = raw_ids[-1]
                cur.execute(
                    """
                    UPDATE gold.global_stats
                    SET total_count = total_count + %s,
                        total_sum = total_sum + %s,
                        min_price = CASE
                            WHEN min_price IS NULL THEN %s
                            WHEN %s IS NULL THEN min_price
                            ELSE LEAST(min_price, %s)
                        END,
                        max_price = CASE
                            WHEN max_price IS NULL THEN %s
                            WHEN %s IS NULL THEN max_price
                            ELSE GREATEST(max_price, %s)
                        END,
                        last_silver_id = %s,
                        updated_at = NOW()
                    WHERE id = 1
                    """,
                    (
                        batch_count,
                        batch_sum,
                        batch_min,
                        batch_min,
                        batch_min,
                        batch_max,
                        batch_max,
                        batch_max,
                        last_processed_id,
                    ),
                )
                conn.commit()
            after = fetch_global_stats(cur)
            record_load_log(
                cur,
                layer="gold",
                file_name="silver.events",
                records=processed,
                prices=aggregated_prices,
                chunk_size=chunk_size,
                status="SUCCESS" if processed else "NO_NEW_ROWS",
                details=None,
            )
        conn.commit()

    summary = after
    print_metrics("[GOLD] Resumen", summary)
    if processed:
        delta_count = summary.count - before.count
        delta_min = None
        delta_max = None
        if summary.min_price is not None and before.min_price is not None:
            delta_min = summary.min_price - before.min_price
        elif summary.min_price is not None:
            delta_min = summary.min_price
        if summary.max_price is not None and before.max_price is not None:
            delta_max = summary.max_price - before.max_price
        elif summary.max_price is not None:
            delta_max = summary.max_price
        print("[GOLD] Variación respecto al estado anterior:")
        print(f"  Δcount: {delta_count}")
        print(f"  Δavg: {format_decimal(summary.average() - before.average())}")
        print(f"  Δmin: {format_decimal(delta_min)}")
        print(f"  Δmax: {format_decimal(delta_max)}")
    else:
        print("[GOLD] Sin cambios: no se encontraron filas nuevas en SILVER.")
    return summary


def run_check(dsn: str) -> None:
    with get_connection(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM raw.events_raw")
            raw_count = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM silver.events")
            silver_count = cur.fetchone()[0]
            ensure_global_stats_row(cur)
            summary = fetch_global_stats(cur)
    print("[RUNNING] Estado del pipeline")
    print(f"  RAW rows: {raw_count}")
    print(f"  SILVER rows: {silver_count}")
    print("[DB-AGG] GOLD global stats")
    print_metrics("  ↳ Métricas", summary)


def run_load(
    dsn: str,
    *,
    stage: str,
    data_dir: Path,
    pattern: str,
    exclude: set[str] | None,
    chunk_size: int,
) -> None:
    exclude = exclude or set()
    if stage in {"raw", "silver", "all"} and not exclude and pattern in {"*.csv", "*"}:
        exclude = {"validation.csv"}

    if stage in {"raw", "silver", "all"}:
        run_load_raw(
            dsn,
            data_dir=data_dir,
            pattern=pattern,
            exclude=exclude,
            chunk_size=chunk_size,
        )
    if stage in {"silver", "all"}:
        run_raw_to_silver(dsn, chunk_size=chunk_size)
    if stage in {"gold", "all"}:
        run_silver_to_gold(dsn, chunk_size=chunk_size)


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
        help="Load data files and advance through RAW, SILVER and GOLD stages.",
    )
    load_parser.add_argument(
        "--data-dir",
        type=Path,
        default=DEFAULT_DATA_DIR,
        help="Directory containing CSV files to ingest.",
    )
    load_parser.add_argument(
        "--pattern",
        default="*.csv",
        help="Glob pattern to select files (default: *.csv).",
    )
    load_parser.add_argument(
        "--exclude",
        action="append",
        default=None,
        help="File name to exclude (can be passed multiple times).",
    )
    load_parser.add_argument(
        "--chunk-size",
        default="auto",
        help="Micro-batch size. Use 'auto' (default) to respect PIPELINE_BATCH_SIZE.",
    )
    load_parser.add_argument(
        "--stage",
        choices=["raw", "silver", "gold", "all"],
        default="all",
        help="Limit the processing to a specific layer.",
    )

    load_raw_parser = subparsers.add_parser(
        "load-raw", help="Load CSV files into raw.events_raw without transformations."
    )
    load_raw_parser.add_argument(
        "--data-dir",
        type=Path,
        default=DEFAULT_DATA_DIR,
        help="Directory containing CSV files to ingest.",
    )
    load_raw_parser.add_argument(
        "--pattern",
        default="*.csv",
        help="Glob pattern to select files (default: *.csv).",
    )
    load_raw_parser.add_argument(
        "--exclude",
        action="append",
        default=None,
        help="File name to exclude (can be passed multiple times).",
    )
    load_raw_parser.add_argument(
        "--chunk-size",
        default="auto",
        help="Micro-batch size. Use 'auto' (default) to respect PIPELINE_BATCH_SIZE.",
    )

    raw_to_silver_parser = subparsers.add_parser(
        "raw-to-silver", help="Transform RAW records into silver.events applying DQ rules."
    )
    raw_to_silver_parser.add_argument(
        "--chunk-size",
        default="auto",
        help="Micro-batch size when extracting from RAW.",
    )

    silver_to_gold_parser = subparsers.add_parser(
        "silver-to-gold", help="Aggregate SILVER data into GOLD incremental metrics."
    )
    silver_to_gold_parser.add_argument(
        "--chunk-size",
        default="auto",
        help="Micro-batch size when reading SILVER events.",
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
        run_load(
            args.dsn,
            stage=args.stage,
            data_dir=args.data_dir,
            pattern=args.pattern,
            exclude=parse_excludes(args.exclude),
            chunk_size=resolve_chunk_size(args.chunk_size),
        )
    elif args.command == "load-raw":
        run_load_raw(
            args.dsn,
            data_dir=args.data_dir,
            pattern=args.pattern,
            exclude=parse_excludes(args.exclude),
            chunk_size=resolve_chunk_size(args.chunk_size),
        )
    elif args.command == "raw-to-silver":
        run_raw_to_silver(args.dsn, chunk_size=resolve_chunk_size(args.chunk_size))
    elif args.command == "silver-to-gold":
        run_silver_to_gold(args.dsn, chunk_size=resolve_chunk_size(args.chunk_size))
    elif args.command == "check":
        run_check(args.dsn)
    else:
        parser.error(f"Unknown command {args.command}")


if __name__ == "__main__":
    main()
