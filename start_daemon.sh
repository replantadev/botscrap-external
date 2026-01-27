#!/bin/bash
# ============================================================================
# start_daemon.sh - Iniciar Multi-Bot Daemon de forma segura
# ============================================================================
# Este script garantiza que solo haya UNA instancia del daemon corriendo.
# Uso: ./start_daemon.sh [--force]
# ============================================================================

cd /var/www/vhosts/territoriodrasanvicr.com/b/

API_KEY="sk_d40a7992c1fa4a3774134d31658ff10cc93ce505d95a3ce7d37e8459759578e9"
PID_FILE="daemon.pid"
LOG_FILE="daemon.log"

# Función para verificar si el daemon está corriendo
check_daemon() {
    if [ -f "$PID_FILE" ]; then
        PID=$(cat "$PID_FILE")
        if ps -p "$PID" > /dev/null 2>&1; then
            return 0  # Corriendo
        fi
    fi
    return 1  # No está corriendo
}

# Función para matar daemon existente
kill_daemon() {
    if [ -f "$PID_FILE" ]; then
        PID=$(cat "$PID_FILE")
        echo "🛑 Matando daemon existente (PID: $PID)..."
        kill -TERM "$PID" 2>/dev/null
        sleep 2
        # Si sigue vivo, forzar
        if ps -p "$PID" > /dev/null 2>&1; then
            echo "⚠️ Forzando SIGKILL..."
            kill -9 "$PID" 2>/dev/null
            sleep 1
        fi
    fi
    # Limpiar archivos
    rm -f daemon.pid daemon.lock
}

# Procesar argumentos
FORCE=false
if [ "$1" == "--force" ] || [ "$1" == "-f" ]; then
    FORCE=true
fi

# Verificar si ya está corriendo
if check_daemon; then
    PID=$(cat "$PID_FILE")
    if [ "$FORCE" == "true" ]; then
        echo "⚠️ --force detectado, reiniciando daemon..."
        kill_daemon
    else
        echo "❌ El daemon ya está corriendo (PID: $PID)"
        echo "   Usa: ./start_daemon.sh --force  para reiniciar"
        echo "   O:   ./stop_daemon.sh           para detener"
        exit 1
    fi
fi

# Limpiar log corrupto si existe
if [ -f "$LOG_FILE" ]; then
    # Verificar si tiene caracteres nulos (corrupción)
    if grep -q $'\x00' "$LOG_FILE" 2>/dev/null; then
        echo "🧹 Limpiando log corrupto..."
        rm -f "$LOG_FILE"
    fi
fi

# Iniciar daemon
echo "🚀 Iniciando Multi-Bot Daemon..."
nohup ./venv/bin/python multi_bot_daemon.py --api-key "$API_KEY" > daemon.log 2>&1 &

# Esperar un momento y verificar
sleep 3

if check_daemon; then
    PID=$(cat "$PID_FILE")
    echo "✅ Daemon iniciado correctamente (PID: $PID)"
    echo ""
    echo "📋 Comandos útiles:"
    echo "   tail -f daemon.log     # Ver logs en tiempo real"
    echo "   ./stop_daemon.sh       # Detener daemon"
    echo "   ./status_daemon.sh     # Ver estado"
else
    echo "❌ Error: El daemon no inició correctamente"
    echo "   Revisa el log: cat daemon.log"
    exit 1
fi
