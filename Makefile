COMPOSE ?= docker compose

.PHONY: up db-init-schemas db-migrate gold report stop down

up:
	$(COMPOSE) up -d --build

stop:
	$(COMPOSE) stop

down:
	$(COMPOSE) down -v

db-init-schemas:
	$(COMPOSE) run --rm app python pipeline_pg.py init

db-migrate:
	$(COMPOSE) run --rm app python pipeline_pg.py load --stage silver --chunk-size auto

gold:
	$(COMPOSE) run --rm app python pipeline_pg.py load --stage gold --chunk-size auto

report:
	$(COMPOSE) run --rm app python pipeline_pg.py check
