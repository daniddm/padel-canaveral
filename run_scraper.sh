#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="${PROJECT_DIR:-$SCRIPT_DIR}"
LOG_DIR="$PROJECT_DIR/logs"
TIMESTAMP="$(date +"%Y-%m-%d_%H-%M-%S")"
LOG_FILE="$LOG_DIR/scraper_$TIMESTAMP.log"

mkdir -p "$LOG_DIR"

exec > >(tee -a "$LOG_FILE") 2>&1

echo "ðŸ“ Inicio del scraping (`date +"%Y-%m-%d %H:%M:%S"`)"

cd "$PROJECT_DIR"

if [ -f "$PROJECT_DIR/.env" ]; then
    set -a
    # shellcheck disable=SC1091
    source "$PROJECT_DIR/.env"
    set +a
fi

VENV_DIR="${VENV_DIR:-}"
if [ -z "$VENV_DIR" ]; then
    if [ -d "$PROJECT_DIR/venv" ]; then
        VENV_DIR="$PROJECT_DIR/venv"
    elif [ -d "$PROJECT_DIR/.venv" ]; then
        VENV_DIR="$PROJECT_DIR/.venv"
    fi
fi

if [ -z "$VENV_DIR" ]; then
    echo "âš ï¸ Entorno virtual no encontrado en $PROJECT_DIR/{venv,.venv}" >&2
    exit 1
fi

# shellcheck disable=SC1091
source "$VENV_DIR/bin/activate"

echo "ðŸ“Š Ejecutando scraping_final.py..."
python scraping_final.py

if [ -n "${SHOPIFY_ADMIN_TOKEN:-}" ] && [ -n "${SHOPIFY_DOMAIN:-}" ]; then
    echo "ðŸš€ Subiendo CSVs a Shopify..."
    if python upload_shopify.py; then
        echo "ðŸ›’ Subida a Shopify completada."
    else
        echo "âš ï¸ FallÃ³ la subida a Shopify (ver log)." >&2
    fi
else
    echo "âš ï¸ Variables SHOPIFY_ADMIN_TOKEN o SHOPIFY_DOMAIN no definidas. Se omite subida a Shopify." >&2
fi

echo "âœ… Scraping completado (`date +"%Y-%m-%d %H:%M:%S"`)"