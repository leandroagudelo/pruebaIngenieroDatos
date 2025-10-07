COMPOSE ?= docker compose

# Ajusta esta ruta al interior del contenedor "app".
# Con tu compose actual, ./data (host) -> /opt/pipeline/data (contenedor)
DATA_DIR ?= /opt/pipeline/data/raw
BATCH    ?= 5

TRAIN_PATTERN    ?= *.csv
EXCLUDE_TRAIN    ?= validation.csv
VALIDATION_FILE  ?= validation.csv

.PHONY: up stop down db-init-schemas db-migrate validation gold check report demo

up:
	$(COMPOSE) up -d --build

stop:
	$(COMPOSE) stop

down:
	$(COMPOSE) down -v

# 1) Crear esquemas/tablas + fila inicial en GOLD
db-init-schemas:
	$(COMPOSE) run --rm app python pipeline_pg.py init

# 2) ENTRENAMIENTO: RAW -> SILVER (todos los *.csv excepto validation.csv)
#    Nota: --stage silver ejecuta RAW y luego SILVER; pasamos exclude explícito.
db-migrate:
	$(COMPOSE) run --rm app python pipeline_pg.py load \
		--stage silver \
		--data-dir $(DATA_DIR) \
		--pattern "$(TRAIN_PATTERN)" \
		--exclude $(EXCLUDE_TRAIN) \
		--chunk-size $(BATCH)

# 3) VALIDACIÓN: carga SOLO validation.csv (RAW -> SILVER)
validation:
	$(COMPOSE) run --rm app python pipeline_pg.py load \
		--stage silver \
		--data-dir $(DATA_DIR) \
		--pattern "$(VALIDATION_FILE)" \
		--chunk-size $(BATCH)

# 4) GOLD: agrega lo nuevo de SILVER (métricas incrementales)
gold:
	$(COMPOSE) run --rm app python pipeline_pg.py load \
		--stage gold \
		--chunk-size $(BATCH)

# 5) CHECK: imprime métricas actuales (RAW/SILVER + GOLD)
check:
	$(COMPOSE) run --rm app python pipeline_pg.py check

# 6) Reporte rápido = check (si tienes report_demo.py, crea mejor un report-html)
report: check

# 7) DEMO end-to-end: init -> train -> check -> validation -> check -> gold -> check
demo: up db-init-schemas db-migrate check validation check gold check
