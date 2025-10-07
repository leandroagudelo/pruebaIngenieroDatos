#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Reporte HTML por lotes usando SOLO las columnas existentes en gold.load_log:
(layer, file_name, records, min_price, avg_price, max_price, chunk_size, status, details)

Incluye:
- Tabla de cargas por lote: Capa, Archivo, Lote, Registros, Min/Avg/Max, Chunk, Estado, Detalles.
- Sección VALIDATION: métricas antes (sin validation), solo validation, y después (total), con deltas.

Ejecución:
  python report_lotes.py --out /opt/pipeline/report_lotes.html
"""

import os
import html
import datetime as dt
from argparse import ArgumentParser
from decimal import Decimal, ROUND_HALF_UP

import psycopg2
from psycopg2.extras import DictCursor

DEFAULT_DSN = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@postgres:5432/postgres")
TWO = Decimal("0.01")

def fmt_dec(x):
    if x is None:
        return "n/a"
    if not isinstance(x, Decimal):
        x = Decimal(x)
    return str(x.quantize(TWO, rounding=ROUND_HALF_UP))

def fmt_int(x):
    return "0" if x is None else f"{int(x)}"

def get_conn(dsn: str):
    return psycopg2.connect(dsn)

def fetch_load_log(cur):
    """
    Trae filas SOLO con las columnas existentes y usa ctid para ordenar
    (aprox. orden de inserción).
    """
    cur.execute("""
        SELECT ctid AS _ord,
               layer, file_name, records,
               min_price, avg_price, max_price,
               chunk_size, status, COALESCE(details,'') AS details
        FROM gold.load_log
        ORDER BY _ord
    """)
    return cur.fetchall()

def fetch_metrics_silver(cur, only_validation=False, exclude_validation=False):
    base = "SELECT COUNT(*)::bigint, COALESCE(SUM(price),0)::numeric, MIN(price), MAX(price) FROM silver.events"
    where = []
    if only_validation:
        where.append("source_file = 'validation.csv'")
    if exclude_validation:
        where.append("source_file <> 'validation.csv'")
    sql = base + (" WHERE " + " AND ".join(where) if where else "")
    cur.execute(sql)
    c, s, mi, ma = cur.fetchone()
    avg = (Decimal(s) / Decimal(c)).quantize(TWO, rounding=ROUND_HALF_UP) if c and s is not None else None
    return {
        "count": int(c or 0),
        "sum": Decimal(s or 0),
        "min": Decimal(mi) if mi is not None else None,
        "max": Decimal(ma) if ma is not None else None,
        "avg": avg
    }

def html_header(title: str):
    return f"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="utf-8"/>
<title>{html.escape(title)}</title>
<style>
body {{ font-family: -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif; margin: 24px; }}
h1, h2, h3 {{ margin: 0.6rem 0; }}
table {{ border-collapse: collapse; width: 100%; }}
th, td {{ border: 1px solid #ddd; padding: 8px; font-size: 14px; }}
th {{ background: #f5f5f5; text-align: left; }}
tr:nth-child(even) {{ background: #fafafa; }}
.badge {{ display:inline-block; padding:2px 8px; border-radius:10px; background:#eee; font-size:12px; }}
.small {{ color:#666; font-size:12px; }}
.section {{ margin: 1.5rem 0; }}
code {{ background:#f3f3f3; padding:2px 6px; border-radius:4px; }}
</style>
</head>
<body>
<h1>{html.escape(title)}</h1>
<p class="small">Generado: {dt.datetime.now().isoformat(timespec="seconds")}</p>
"""

def html_footer():
    return "</body></html>"

def build_batches_table(rows):
    # Numerar lotes por (layer, file_name)
    counters = {}
    recs = []
    for r in rows:
        key = (r["layer"], r["file_name"])
        counters[key] = counters.get(key, 0) + 1
        recs.append({
            "layer": r["layer"],
            "file": r["file_name"] or "-",
            "batch": counters[key],
            "count": r["records"],
            "min": r["min_price"],
            "avg": r["avg_price"],
            "max": r["max_price"],
            "chunk": r["chunk_size"],
            "status": r["status"],
            "details": r["details"] or ""
        })

    out = ['<div class="section"><h2>Cargas por lote (gold.load_log)</h2>']
    out.append("<table>")
    out.append("<tr><th>Capa</th><th>Archivo</th><th>Lote</th><th>Registros</th><th>Min</th><th>Avg</th><th>Max</th><th>Chunk</th><th>Estado</th><th>Detalles</th></tr>")
    for x in recs:
        out.append(
            "<tr>"
            f"<td>{html.escape(str(x['layer']))}</td>"
            f"<td>{html.escape(str(x['file']))}</td>"
            f"<td>{fmt_int(x['batch'])}</td>"
            f"<td>{fmt_int(x['count'])}</td>"
            f"<td>{fmt_dec(x['min'])}</td>"
            f"<td>{fmt_dec(x['avg'])}</td>"
            f"<td>{fmt_dec(x['max'])}</td>"
            f"<td>{fmt_int(x['chunk'])}</td>"
            f"<td><span class='badge'>{html.escape(str(x['status']))}</span></td>"
            f"<td>{html.escape(str(x['details']))}</td>"
            "</tr>"
        )
    out.append("</table></div>")
    return "\n".join(out)

def build_validation_section(cur):
    before = fetch_metrics_silver(cur, exclude_validation=True)
    onlyv  = fetch_metrics_silver(cur, only_validation=True)
    after  = fetch_metrics_silver(cur)

    def row(label, m):
        return (
            f"<tr><td><b>{html.escape(label)}</b></td>"
            f"<td>{fmt_int(m['count'])}</td>"
            f"<td>{fmt_dec(m['avg'])}</td>"
            f"<td>{fmt_dec(m['min'])}</td>"
            f"<td>{fmt_dec(m['max'])}</td></tr>"
        )

    def diff(a, b):
        # a y b pueden ser Decimal o None
        if a is None or b is None:
            return "n/a"
        return fmt_dec(Decimal(a) - Decimal(b))

    delta_count = after["count"] - before["count"]
    delta_avg   = diff(after["avg"], before["avg"])
    delta_min   = diff(after["min"], before["min"])
    delta_max   = diff(after["max"], before["max"])

    out = ['<div class="section"><h2>Validación</h2>']
    out.append("<p>Comparación de métricas al cargar <code>validation.csv</code>.</p>")
    out.append("<table>")
    out.append("<tr><th>Conjunto</th><th>Count</th><th>Avg</th><th>Min</th><th>Max</th></tr>")
    out.append(row("Antes (sin validation)", before))
    out.append(row("Solo validation", onlyv))
    out.append(row("Después (total)", after))
    out.append("</table>")

    out.append("<div class='section'><h3>Deltas (Después - Antes)</h3>")
    out.append("<table>")
    out.append("<tr><th>Δcount</th><th>Δavg</th><th>Δmin</th><th>Δmax</th></tr>")
    out.append(f"<tr><td>{fmt_int(delta_count)}</td><td>{delta_avg}</td><td>{delta_min}</td><td>{delta_max}</td></tr>")
    out.append("</table></div>")

    out.append("</div>")
    return "\n".join(out)

def main():
    ap = ArgumentParser()
    ap.add_argument("--dsn", default=DEFAULT_DSN)
    ap.add_argument("--out", default="/opt/pipeline/report_lotes.html")
    args = ap.parse_args()

    with get_conn(args.dsn) as conn, conn.cursor(cursor_factory=DictCursor) as cur:
        rows = fetch_load_log(cur)
        html_doc = []
        html_doc.append(html_header("Reporte de Cargas por Lote y Validación"))
        html_doc.append(build_batches_table(rows))
        html_doc.append(build_validation_section(cur))
        html_doc.append(html_footer())
        with open(args.out, "w", encoding="utf-8") as f:
            f.write("\n".join(html_doc))
        print(f"[OK] Reporte generado en: {args.out}")

if __name__ == "__main__":
    main()
