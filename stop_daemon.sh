#!/bin/bash
# ============================================================================
# stop_daemon.sh - Detener Multi-Bot Daemon de forma segura
# ============================================================================

cd /var/www/vhosts/territoriodrasanvicr.com/b/

PID_FILE="daemon.pid"

if [ -f "$PID_FILE" ]; then
    PID=$(cat "$PID_FILE")
    
    if ps -p "$PID" > /dev/null 2>&1; then
        echo "🛑 Deteniendo daemon (PID: $PID)..."
        kill -TERM "$PID"
        
        # Esperar hasta 10 segundos
        for i in {1..10}; do
            if ! ps -p "$PID" > /dev/null 2>&1; then
                echo "✅ Daemon detenido correctamente"
                rm -f daemon.pid daemon.lock
                exit 0
            fi
            sleep 1
        done
        
        # Forzar si no responde
        echo "⚠️ Daemon no responde, forzando SIGKILL..."
        kill -9 "$PID" 2>/dev/null
        sleep 1
        rm -f daemon.pid daemon.lock
        echo "✅ Daemon terminado forzosamente"
    else
        echo "⚠️ PID $PID no existe (daemon ya terminó)"
        rm -f daemon.pid daemon.lock
    fi
else
    # Buscar procesos huérfanos
    PIDS=$(pgrep -f "multi_bot_daemon.py" 2>/dev/null)
    if [ -n "$PIDS" ]; then
        echo "⚠️ Encontrados procesos huérfanos: $PIDS"
        echo "   Matando..."
        pkill -9 -f "multi_bot_daemon.py"
        rm -f daemon.pid daemon.lock
        echo "✅ Procesos huérfanos eliminados"
    else
        echo "ℹ️ No hay daemon corriendo"
    fi
fi
