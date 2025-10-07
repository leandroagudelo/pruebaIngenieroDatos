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

# Ruta de salida DENTRO del contenedor (mapea a ./app/report_lotes.html en tu host)
REPORT_OUT ?= /opt/pipeline/report.html

.PHONY: report open-report demo-lotes open-report-chrome

# Genera el reporte HTML de lotes + sección de validation
report:
	$(COMPOSE) run --rm app python report.py --out $(REPORT_OUT)
	@echo "✔ Reporte generado en: app/$(notdir $(REPORT_OUT))"

# (Opcional) Abrir el reporte (macOS). En Linux usar: xdg-open
open-report:
	@if [ -f app/$(notdir $(REPORT_OUT)) ]; then open app/$(notdir $(REPORT_OUT)); else echo "No se encontró app/$(notdir $(REPORT_OUT))"; fi


# 🔹 Abrir específicamente en Google Chrome (macOS)
open-report-chrome:
	@if [ -f app/$(notdir $(REPORT_OUT)) ]; then open -a "Google Chrome" "app/$(notdir $(REPORT_OUT))"; else echo "No se encontró app/$(notdir $(REPORT_OUT)). Ejecuta 'make report' primero."; fi
# Flujo completo + reporte:
#   init -> entrenamiento -> check -> validation -> check -> gold -> check -> reporte
demo-lotes: up reset-soft db-init-schemas db-migrate check validation check gold check report open-report-chrome

.PHONY: reset-soft wipe

# Limpieza lógica (deja esquemas/tablas, elimina datos y reinicia métricas)
reset-soft:
	$(COMPOSE) run --rm app python pipeline_pg.py reset

# Limpieza total (baja y borra volúmenes de Docker: perderás TODO en Postgres/pgAdmin)
wipe:
	$(COMPOSE) down -v
	@echo "Listo. Vuelve a levantar con: make up && make db-init-schemas"
