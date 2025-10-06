#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
cd "$SCRIPT_DIR"

LOG_DIR="logs"
mkdir -p "$LOG_DIR"
RUN_TS=$(date +%Y%m%d_%H%M%S)
LOG_FILE="$LOG_DIR/prueba_${RUN_TS}.log"

step() {
  local title="$1"
  local now
  now=$(date +"%Y-%m-%d %H:%M:%S%z")
  printf '\n[%s] ===== %s =====\n' "$now" "$title" | tee -a "$LOG_FILE"
}

log() {
  printf '%s\n' "$1" | tee -a "$LOG_FILE"
}

fail() {
  local message="$1"
  printf 'ERROR: %s\n' "$message" | tee -a "$LOG_FILE" >&2
  exit 1
}

run_cmd() {
  local title="$1"
  shift
  local tmp_err
  tmp_err=$(mktemp)

  step "$title"
  if ! "$@" > >(tee -a "$LOG_FILE") 2> >(tee "$tmp_err" | tee -a "$LOG_FILE" >&2); then
    log "Comando fallido: $*"
    if [ -s "$tmp_err" ]; then
      log "Últimas líneas de error:"
      tail -n 20 "$tmp_err" | tee -a "$LOG_FILE" >&2
    fi
    rm -f "$tmp_err"
    exit 1
  fi
  rm -f "$tmp_err"
}

count_rows() {
  local file="$1"
  awk 'NR>1 {c++} END {print c+0}' "$file"
}

print_sample() {
  local file="$1"
  tail -n +2 "$file" | head -n 3
}

validate_files() {
  step "Validar archivos de entrenamiento"

  local raw_dir="./data/raw"
  if [ ! -d "$raw_dir" ]; then
    fail "No se encontró el directorio $raw_dir"
  fi

  mapfile -t training_files < <(find "$raw_dir" -maxdepth 1 -type f -name '2012-*.csv' | sort)
  if [ "${#training_files[@]}" -ne 5 ]; then
    fail "Se esperaban exactamente 5 archivos 2012-*.csv en $raw_dir y se encontraron ${#training_files[@]}"
  fi

  local validation_file="$raw_dir/validation.csv"
  if [ ! -f "$validation_file" ]; then
    fail "No se encontró el archivo de validación $validation_file"
  fi

  local header_expected='timestamp,price,user_id'
  local all_files=("${training_files[@]}" "$validation_file")

  for file in "${all_files[@]}"; do
    if [ ! -f "$file" ]; then
      fail "Archivo faltante: $file"
    fi

    local header
    IFS= read -r header < "$file"
    if [ "$header" != "$header_expected" ]; then
      fail "El encabezado de $file es '$header' y se esperaba '$header_expected'"
    fi

    local rows
    rows=$(count_rows "$file")
    local size_bytes
    size_bytes=$(stat -c%s "$file")

    log "Archivo: $file"
    log " - Filas (sin cabecera): $rows"
    log " - Tamaño (bytes): $size_bytes"
    log " - Muestra (3 filas):"
    print_sample "$file" | sed 's/^/   /' | tee -a "$LOG_FILE"
    log ""
  done
}

wait_for_postgres() {
  step "Esperar a que Postgres esté healthy"
  local retries=30
  local delay=2
  local attempt
  for ((attempt=1; attempt<=retries; attempt++)); do
    if docker compose exec postgres pg_isready -U "${POSTGRES_USER:-leandro}" -d "${POSTGRES_DB:-prueba_datos}" >/dev/null 2>&1; then
      log "Postgres listo en el intento $attempt"
      return 0
    fi
    log "Postgres no listo (intento $attempt/$retries). Reintentando en ${delay}s..."
    sleep "$delay"
  done
  fail "Postgres no alcanzó estado healthy tras $((retries*delay)) segundos"
}

main() {
  validate_files

  run_cmd "Levantar servicios (make up)" make up
  wait_for_postgres

  run_cmd "Inicializar esquemas" make db-init-schemas
  run_cmd "Migrar datos base" make db-migrate
  run_cmd "Pipeline init" docker compose exec app python pipeline_pg.py init
  run_cmd "Carga entrenamiento" docker compose exec app python pipeline_pg.py load --data-dir /workspace/data/raw --pattern "2012-*.csv" --exclude validation.csv --chunk-size auto
  run_cmd "Check post-entrenamiento" docker compose exec app python pipeline_pg.py check
  run_cmd "Carga validación" docker compose exec app python pipeline_pg.py load --data-dir /workspace/data/raw --pattern "validation.csv"
  run_cmd "Check final" docker compose exec app python pipeline_pg.py check
  run_cmd "Construir GOLD" make gold
  run_cmd "Generar reporte" make report

  if [ -f "app/report.html" ]; then
    step "Resumen final"
    log "Pipeline ejecutado exitosamente."
    log "Reporte HTML: app/report.html"
    log "Log completo: $LOG_FILE"
  else
    fail "No se generó app/report.html"
  fi
}

main "$@"
