#!/bin/bash
set -euo pipefail

export LANG=C.UTF-8
export LC_ALL=C.UTF-8

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="${PROJECT_DIR:-$SCRIPT_DIR}"
LOG_DIR="$PROJECT_DIR/logs"
TIMESTAMP="$(date +"%Y-%m-%d_%H-%M-%S")"
LOG_FILE="$LOG_DIR/scraper_$TIMESTAMP.log"

mkdir -p "$LOG_DIR"
exec > >(tee -a "$LOG_FILE") 2>&1

echo "🏓 Inicio del scraping ($(date +"%Y-%m-%d %H:%M:%S"))"
cd "$PROJECT_DIR"

# Cargar .env si existe
if [ -f "$PROJECT_DIR/.env" ]; then
  set -a
  # shellcheck disable=SC1091
  source "$PROJECT_DIR/.env"
  set +a
fi

# Detectar venv
VENV_DIR="${VENV_DIR:-}"
if [ -z "$VENV_DIR" ]; then
  if [ -d "$PROJECT_DIR/venv" ]; then
    VENV_DIR="$PROJECT_DIR/venv"
  elif [ -d "$PROJECT_DIR/.venv" ]; then
    VENV_DIR="$PROJECT_DIR/.venv"
  fi
fi
if [ -z "$VENV_DIR" ]; then
  echo "⚠️ Entorno virtual no encontrado en $PROJECT_DIR/{venv,.venv}" >&2
  exit 1
fi
# shellcheck disable=SC1091
source "$VENV_DIR/bin/activate"

echo "📊 Ejecutando scraping_final.py..."
python scraping_final.py

if [ -n "${SHOPIFY_ADMIN_TOKEN:-}" ] && [ -n "${SHOPIFY_DOMAIN:-}" ]; then
  echo "🚀 Subiendo CSVs a Shopify (Estrategia híbrida)..."

  EXTRACTION_DIR=$(ls -td "$PROJECT_DIR"/Extracción_* 2>/dev/null | head -1)
  if [ -z "$EXTRACTION_DIR" ]; then
    echo "⚠️ No se encontró directorio de extracción (Extracción_*)." >&2
    exit 1
  fi
  echo "📂 Usando directorio: $(basename "$EXTRACTION_DIR")"

  # 1) Pasada rápida sin imágenes (datos)
  echo ""
  echo "⚡ [1/2] Actualizando datos (precios, stock, variantes)..."
  START_TIME_DATA=$(date +%s)
  DURATION_DATA=0
  if python upload_shopify.py --skip-images --source-dir "$EXTRACTION_DIR"; then
    END_TIME_DATA=$(date +%s)
    DURATION_DATA=$((END_TIME_DATA - START_TIME_DATA))
    echo "✅ Datos actualizados en $((DURATION_DATA/60)) minutos."
  else
    echo "⚠️ Hubo errores en la actualización de datos." >&2
  fi

  # 2) Pasada de imágenes
  echo ""
  echo "🖼️  [2/2] Verificando y subiendo imágenes faltantes..."
  START_TIME_IMAGES=$(date +%s)
  if python upload_shopify.py --source-dir "$EXTRACTION_DIR"; then
    END_TIME_IMAGES=$(date +%s)
    DURATION_IMAGES=$((END_TIME_IMAGES - START_TIME_IMAGES))
    echo "✅ Imágenes actualizadas en $((DURATION_IMAGES/60)) minutos."

    TOTAL_MIN=$(( (DURATION_DATA + DURATION_IMAGES)/60 ))
    echo ""
    echo "🎉 Subida a Shopify completada (Datos + Imágenes)."
    echo "⏱️  Tiempo total: ${TOTAL_MIN} minutos (~$((TOTAL_MIN/60))h $((TOTAL_MIN%60))m)"
  else
    echo "⚠️ Falló la subida de algunas imágenes (ver failed_images_report.csv)." >&2
  fi

  # Reporte de imágenes fallidas
  if [ -f "$EXTRACTION_DIR/failed_images_report.csv" ]; then
    FAILED_COUNT=$(wc -l < "$EXTRACTION_DIR/failed_images_report.csv")
    FAILED_COUNT=$((FAILED_COUNT - 1))
    if [ "$FAILED_COUNT" -gt 0 ]; then
      echo ""
      echo "⚠️  $FAILED_COUNT imágenes fallaron (revisa $EXTRACTION_DIR/failed_images_report.csv)"
    fi
  fi
else
  echo "⚠️ Variables SHOPIFY_ADMIN_TOKEN o SHOPIFY_DOMAIN no definidas. Se omite subida a Shopify." >&2
fi

echo ""
echo "✅ Scraping completado ($(date +"%Y-%m-%d %H:%M:%S"))"