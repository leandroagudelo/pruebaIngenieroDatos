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
make db-init-schemas # Crea los esquemas y tablas RAW/SILVER/GOLD
make db-migrate      # Carga el CSV en micro-batches hacia RAW y SILVER
make gold            # Actualiza las métricas de la capa GOLD
make report          # Ejecuta verificaciones de control
```

Los datos de ejemplo viven en `data/raw/events.csv`. El tamaño del micro-batch puede ajustarse con `--chunk-size`; el valor `auto` determina un tamaño adecuado según el peso del archivo.

Para detener o limpiar el entorno:

```bash
make stop  # Detiene los servicios
make down  # Detiene y elimina volúmenes
```

## Arquitectura del pipeline

1. **RAW**: carga directa del CSV en `raw.events_raw`, conservando el payload original.
2. **SILVER**: transformación tipada en `silver.events` (parseo de fechas, montos numéricos y deduplicación por `event_id`).
3. **GOLD**: agregaciones diarias en `gold.global_stats` y `gold.metrics`.

El comando `pipeline_pg.py load` ejecuta el pipeline en micro-batches usando `psycopg2` y `execute_values` para inserciones eficientes.

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
│   └── raw/events.csv
├── docker-compose.yml
├── notebooks/
│   └── 0_START_HERE.ipynb
└── pgadmin/
    └── servers.json
```

¡Listo para ejecutar y extender! Ajusta el CSV de entrada o incorpora transformaciones adicionales según tus necesidades.
