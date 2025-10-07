# Pipeline CSV→Postgres RAW→SILVER→GOLD

Este proyecto crea un laboratorio de ingeniería de datos ejecutable con **Docker Compose** que implementa una arquitectura **RAW → SILVER → GOLD** alimentada desde archivos **CSV** y almacenada en **Postgres 16**.  
Incluye micro-lotes configurables, bitácora de cargas y un **reporte HTML** para auditoría.

---

## 🧩 Servicios

| Servicio   | Propósito                                                                             | Imagen                     |
| ---------- | ------------------------------------------------------------------------------------- | -------------------------- |
| `postgres` | Base de datos Postgres 16 con los esquemas `raw`, `silver` y `gold`.                  | `postgres:16`              |
| `app`      | CLI Python 3.11 para orquestar el pipeline (`app/pipeline_pg.py`).                    | `python:3.11-slim`         |
| `jupyter`  | Entorno interactivo `jupyter/minimal-notebook` (abre `notebooks/0_START_HERE.ipynb`). | `jupyter/minimal-notebook` |
| `pgadmin`  | Consola pgAdmin con servidor preconfigurado (`local-postgres`) (opcional).            | `dpage/pgadmin4`           |

> Montajes clave del `app`:  
> `./app → /opt/pipeline` y `./data → /opt/pipeline/data`.  
> Por defecto, los CSV se buscan en **`/opt/pipeline/data/raw`**.

---

## ✅ Requisitos previos

- Docker + Docker Compose (plugin `docker compose`)
- `make`

---

## ⚡ Ejecución “súper rápida”

```bash
make demo-lotes
```

Ejecuta fin-a-fin: init → cargue inicial → check → validation → check → gold → check → reporte HTML.

---

## 🔁 Reiniciar desde cero (dos opciones)

### Opción 1 — Reset lógico (mantiene contenedores/volúmenes)

```bash
make reset-soft         # Vacía RAW/SILVER/GOLD y reinicia métricas
make db-init-schemas    # (opcional) recrea/asegura objetos
```

### Opción 2 — Reset total (borra volúmenes Docker)

```bash
docker compose down --volumes --rmi local --remove-orphans
docker compose build --no-cache
docker compose up -d
```

---

## ▶️ Puesta en marcha típica

```bash
# 1) Arranque + objetos
make up
make db-init-schemas

# 2) Entrenamiento (carga todo menos validation.csv), ver estado
make db-migrate        # RAW→SILVER (excluye validation.csv)
make gold              # SILVER→GOLD (incremental)
make check

# 3) Validación (carga validation.csv) y ver cambios
make validation        # RAW→SILVER (solo validation.csv)
make gold              # Incrementa GOLD con lo nuevo
make check

# 4) Reporte HTML
make report-lotes
make open-report-chrome
```

> Para detener/limpiar:

```bash
make stop   # Detiene servicios
make down   # Detiene y elimina volúmenes
```

---

## 🧱 Arquitectura del pipeline

1. **RAW** (`raw.events_raw`)  
   Ingesta directa **sin transformación**. Guarda texto crudo con control de duplicados por `(source_file, row_number)` e idempotencia.

2. **SILVER** (`silver.events`)  
   Normaliza y tipa:

   - `timestamp → DATE` (si inválido → `1970-01-01`)
   - `price → NUMERIC(18,2)` (inválido/nulo → `0.00`)
   - `user_id → NUMERIC(18,0)` (inválido/nulo → `0`)  
     Marca `dq_status = 'OK' | 'COERCED'`.

3. **GOLD** (`gold.global_stats`, `gold.load_log`)  
   Mantiene **métricas incrementales** (no reescanea histórico):
   - `total_count`, `total_sum`, `min_price`, `max_price`, `last_silver_id`
   - El **promedio** se deriva como `total_sum / total_count`.  
     **Bitácora de cargas**: `gold.load_log` registra cada **lote** (`status='BATCH'`) y los **resúmenes** de fase/archivo.

```
CSV (/opt/pipeline/data/raw/*.csv)
        │  (validación de encabezado y micro-lotes)
        ▼
RAW     raw.events_raw
        │  (coerciones/validación → SILVER por lotes)
        ▼
SILVER  silver.events
        │  (agregación incremental por lotes → GOLD)
        ▼
GOLD    gold.global_stats  +  gold.load_log
```

---

## 🗃️ Esquema de BD (resumen)

### `raw.events_raw`

- `id BIGSERIAL PK`, `source_file TEXT`, `row_number INT` (UNIQUE con `source_file`)
- `timestamp_raw TEXT`, `price_raw TEXT`, `user_id_raw TEXT`, `loaded_at TIMESTAMPTZ`

### `silver.events`

- `raw_id BIGINT PK` (del registro en RAW)
- `event_date DATE`, `price NUMERIC(18,2)`, `user_id NUMERIC(18,0)`
- `dq_status TEXT`, `source_file TEXT`, `loaded_at TIMESTAMPTZ`

### `gold.global_stats`

- `id SMALLINT = 1`
- `total_count NUMERIC(38,0)`, `total_sum NUMERIC(38,2)`
- `min_price NUMERIC(18,2)`, `max_price NUMERIC(18,2)`
- `last_silver_id BIGINT`, `updated_at TIMESTAMPTZ`

### `gold.load_log`

- `layer TEXT` (`raw|silver|gold`)
- `file_name TEXT`
- `records BIGINT`
- `min_price NUMERIC(18,2)`, `avg_price NUMERIC(18,2)`, `max_price NUMERIC(18,2)`
- `chunk_size INT`
- `status TEXT` (`BATCH`, `SUCCESS`, `NO_NEW_ROWS`, `SKIPPED_BAD_HEADER`, …)
- `details TEXT` (p.ej. `batch=3`, `file_summary`, `phase_summary`, `coerced=12`)

---

## ⚙️ Micro-lotes y bitácora

- Tamaño de lote por defecto: **5** (configurable con `--chunk-size` o `PIPELINE_BATCH_SIZE`).
- Las 3 fases (**RAW**, **SILVER**, **GOLD**) procesan en `LIMIT chunk_size` y registran:
  - Una fila por **lote** (`status='BATCH'`, `details='batch=N'`).
  - Una fila de **resumen** por archivo/fase (`status='SUCCESS'|...`, `details='file_summary|phase_summary'`).

**Verificación rápida de lotes (SQL):**

```sql
SELECT layer, file_name, records, chunk_size, status, details
FROM gold.load_log
ORDER BY ctid;  -- orden aproximado de inserción
```

---

## 🖥️ CLI del pipeline (`app/pipeline_pg.py`)

```bash
# Inicialización (esquemas/tablas + fila id=1 en gold.global_stats)
docker compose exec app python pipeline_pg.py init

# Orquestado (RAW→SILVER→GOLD) — por defecto exclude validation.csv si pattern=*.csv
docker compose exec app python pipeline_pg.py load \
  --data-dir /opt/pipeline/data/raw \
  --stage all \
  --chunk-size 5

# Solo RAW (sin transformaciones)
docker compose exec app python pipeline_pg.py load-raw \
  --data-dir /opt/pipeline/data/raw \
  --exclude validation.csv

# Solo RAW→SILVER (entrenamiento)
docker compose exec app python pipeline_pg.py raw-to-silver --chunk-size 5

# Solo SILVER→GOLD (incremental)
docker compose exec app python pipeline_pg.py silver-to-gold --chunk-size 5

# Estado rápido
docker compose exec app python pipeline_pg.py check

# Reset lógico de datos/métricas
docker compose exec app python pipeline_pg.py reset
```

> **Encabezado obligatorio** en CSV: `timestamp,price,user_id`.  
> Archivos sin esa cabecera se **omiten** y se registran en `gold.load_log` como `SKIPPED_BAD_HEADER`.  
> **Ruta por defecto**: `/opt/pipeline/data/raw` (`PIPELINE_DATA_DIR`).

---

## 🧰 Makefile (targets útiles)

- `up` — build & up de servicios
- `stop` / `down` — detener / detener + borrar volúmenes
- `db-init-schemas` — `pipeline_pg.py init`
- `db-migrate` — entrenamiento RAW→SILVER (excluye `validation.csv`)
- `validation` — carga `validation.csv` RAW→SILVER
- `gold` — agrega incrementales a GOLD
- `check` — estado actual
- `report-lotes` — **genera HTML** `app/report_lotes.html`
- `open-report-chrome` — abre el HTML en Google Chrome (macOS)
- `reset-soft` — vacía datos y reinicia métricas
- `wipe` — borra volúmenes (reset total)
- `demo-lotes` — encadena todo fin-a-fin + reporte

---

## 📊 Reporte HTML de lotes y validación

El script `app/report_lotes.py` produce **`app/report_lotes.html`** con:

- **Cargas por lote** desde `gold.load_log`: Capa, Archivo, **Lote**, Registros, Min/Avg/Max, Chunk, Estado, Detalles.
- **Sección VALIDATION**:
  - **Antes** (excluye `validation.csv`)
  - **Solo validation**
  - **Después** (total)
  - **Deltas** (Después – Antes)

**Ejemplo:**

```bash
make report-lotes
make open-report-chrome
```

---

## 🔧 Configuración y variables

- **BD (DSN)**: `DATABASE_URL` (p.ej. `postgresql://postgres:postgres@postgres:5432/postgres`)
- **Datos**: `PIPELINE_DATA_DIR` (por defecto `/opt/pipeline/data/raw`)
- **Micro-lote**: `PIPELINE_BATCH_SIZE` (por defecto `5`)
- **Jupyter**: `JUPYTER_TOKEN`
- **pgAdmin**: `PGADMIN_DEFAULT_EMAIL`, `PGADMIN_DEFAULT_PASSWORD`

> Los CSV de la prueba deben estar en `data/raw/` y respetar el encabezado requerido.

---

## 📁 Estructura del repositorio

```
.
├── Makefile
├── README.md
├── app/
│   ├── Dockerfile
│   ├── pipeline_pg.py
│   ├── report_lotes.py
│   └── requirements.txt
├── data/
│   └── raw/
│       ├── 2012-01.csv
│       ├── 2012-02.csv
│       ├── 2012-03.csv
│       ├── 2012-04.csv
│       ├── 2012-05.csv
│       └── validation.csv
├── docker-compose.yml
├── notebooks/
│   └── 0_START_HERE.ipynb
└── pgadmin/
    └── servers.json
```

---

## 🛠️ Troubleshooting

- **Auth fallida a Postgres**  
  Asegura que `DATABASE_URL` coincide con tus `POSTGRES_*` y que el servicio `postgres` está arriba.

- **“No se encuentra /opt/pipeline/data/raw”**  
  Verifica el montaje en `docker-compose.yml`:  
  `app.volumes: - ./data:/opt/pipeline/data` y que los archivos existen.

- **CSV omitido**  
  Verifica la cabecera exacta: `timestamp,price,user_id`.

- **No veo lotes en el log**  
  Ejecuta con `--chunk-size 5` y consulta:
  ```sql
  SELECT layer, file_name, records, chunk_size, status, details
  FROM gold.load_log
  ORDER BY ctid;
  ```

---

**¡Listo!** Con esto tienes una ejecución reproducible, una arquitectura clara y evidencias de micro-lotes y validación, todo con pocos comandos.
