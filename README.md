# Pipeline CSV→Postgres RAW→SILVER→GOLD

Este proyecto crea un laboratorio de ingeniería de datos ejecutable con Docker Compose que implementa una arquitectura RAW→SILVER→GOLD alimentada desde archivos CSV y almacenada en Postgres 16.

## Servicios

| Servicio  | Propósito | Imagen |
|-----------|-----------|--------|
| `postgres` | Base de datos Postgres 16 con los esquemas `raw`, `silver` y `gold`. | `postgres:16` |
| `app` | CLI Python 3.11 para orquestar el pipeline (`pipeline_pg.py`). | `python:3.11-slim` |
| `jupyter` | Entorno interactivo basado en `jupyter/minimal-notebook` que abre el cuaderno `notebooks/0_START_HERE.ipynb`. | `jupyter/minimal-notebook` |
| `pgadmin` | Consola administrativa pgAdmin con un servidor preconfigurado (`local-postgres`). | `dpage/pgadmin4` |

## Requisitos previos

* Docker y Docker Compose plugin (`docker compose`).
* Make.

## Puesta en marcha rápida

```bash
make up              # Construye y levanta los contenedores
make db-init-schemas # Crea esquemas y tablas (incluye gold.global_stats y gold.load_log)
make db-migrate      # Ingesta los CSV de ./data/raw excepto validation.csv (RAW→SILVER)
make gold            # Consolida incrementos desde SILVER hacia GOLD
make report          # Ejecuta verificaciones rápidas (check)
```

Los datos de entrenamiento se encuentran en `data/raw/` y deben respetar el encabezado `timestamp,price,user_id`. El micro-batch por defecto procesa 5 filas a la vez (configurable con `--chunk-size` o la variable `PIPELINE_BATCH_SIZE`).

Para detener o limpiar el entorno:

```bash
make stop  # Detiene los servicios
make down  # Detiene y elimina volúmenes
```

## Arquitectura del pipeline

1. **RAW**: ingesta directa de cada archivo válido en `raw.events_raw`, conservando los valores originales pero con control de duplicados por `(source_file, row_number)`.
2. **SILVER**: normalización tipada en `silver.events`, coercionando nulos o valores inválidos a `0`, almacenando la fecha (`DATE`), el precio (`NUMERIC(18,2)`), el usuario (`NUMERIC(18,0)`) y una columna `dq_status` que marca las filas ajustadas.
3. **GOLD**: métricas globales incrementales en `gold.global_stats` (conteo total, suma, mínimos/máximos y último `raw_id` procesado) y bitácora de cargas en `gold.load_log`.

El comando `pipeline_pg.py load` orquesta los tres pasos en micro-batches de 5 filas (por defecto), omite `validation.csv` durante el entrenamiento inicial y actualiza las métricas sin reescanear el histórico completo. Tras cada ejecución imprime `count/avg/min/max` y, en GOLD, las variaciones respecto al estado previo.

### CLI del pipeline

Además del modo orquestado (`load`), el script expone comandos específicos para controlar cada capa:

```bash
# Inicialización
docker compose exec app python pipeline_pg.py init

# Entrenamiento (RAW→SILVER) desde ./data/raw excluyendo validation.csv
docker compose exec app python pipeline_pg.py load \
  --data-dir /workspace/data/raw --exclude validation.csv --stage silver --chunk-size 5

# Validación (vuelve a ejecutar RAW→SILVER→GOLD solo con validation.csv)
docker compose exec app python pipeline_pg.py load \
  --data-dir /workspace/data/raw --pattern "validation.csv" --chunk-size 5

# Comandos granulares
docker compose exec app python pipeline_pg.py load-raw --data-dir /workspace/data/raw --exclude validation.csv
docker compose exec app python pipeline_pg.py raw-to-silver --chunk-size 5
docker compose exec app python pipeline_pg.py silver-to-gold --chunk-size 5

# Reporte de estado
docker compose exec app python pipeline_pg.py check
```

`gold.load_log` registra cada ejecución con el archivo procesado, filas afectadas, métricas (min/avg/max), chunk utilizado y estado (`SUCCESS`, `NO_NEW_ROWS`, etc.), útil para auditorías posteriores.

## Configuración adicional

Las credenciales y parámetros de conexión viven en el archivo `.env` (versionado) y son reutilizados por todos los servicios de Docker Compose. Puedes ajustarlos según tus necesidades antes de ejecutar `make up`.

* **Jupyter**: disponible en [http://localhost:8888](http://localhost:8888) con token `tu_token_seguro`. Trabaja sobre el directorio `notebooks/` y comparte datos en `/home/jovyan/data`.
* **pgAdmin**: disponible en [http://localhost:8080](http://localhost:8080). Credenciales por defecto `admin@example.com` / `admin123`. El archivo `pgadmin/servers.json` registra el servidor `local-postgres` (host `postgres`).
* **Variables de entorno**: la aplicación usa `DATABASE_URL` y `PIPELINE_SOURCE_CSV` definidos en `docker-compose.yml`; `DATABASE_URL` se genera con los parámetros `PIPELINE_DB_*` declarados en `.env`.

## Estructura del repositorio

```
.
├── Makefile
├── README.md
├── app/
│   ├── Dockerfile
│   ├── pipeline_pg.py
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

¡Listo para ejecutar y extender! Ajusta el CSV de entrada o incorpora transformaciones adicionales según tus necesidades.
