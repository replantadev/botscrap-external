#!/bin/bash
# Script de inicio para el Dashboard de BotScrap External

# Directorio del proyecto
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$DIR"

# Activar entorno virtual
source venv/bin/activate

# Modo de ejecución
MODE=${1:-"dev"}

if [ "$MODE" == "dev" ]; then
    echo "🚀 Iniciando en modo desarrollo..."
    python webapp.py
elif [ "$MODE" == "prod" ]; then
    echo "🚀 Iniciando en modo producción (gunicorn)..."
    gunicorn webapp:app \
        --bind 0.0.0.0:${FLASK_PORT:-5000} \
        --workers 2 \
        --threads 2 \
        --timeout 120 \
        --access-logfile logs/access.log \
        --error-logfile logs/error.log \
        --capture-output \
        --daemon
    echo "✅ Dashboard iniciado en background"
    echo "   Ver logs: tail -f logs/error.log"
else
    echo "Uso: ./start_dashboard.sh [dev|prod]"
fi
