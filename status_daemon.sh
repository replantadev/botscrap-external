#!/bin/bash
# ============================================================================
# status_daemon.sh - Ver estado del Multi-Bot Daemon
# ============================================================================

cd /var/www/vhosts/territoriodrasanvicr.com/b/

PID_FILE="daemon.pid"
LOG_FILE="daemon.log"

echo "========================================"
echo "  Multi-Bot Daemon Status"
echo "========================================"

# Verificar PID
if [ -f "$PID_FILE" ]; then
    PID=$(cat "$PID_FILE")
    if ps -p "$PID" > /dev/null 2>&1; then
        echo "✅ Estado: CORRIENDO"
        echo "   PID: $PID"
        
        # Tiempo de ejecución
        UPTIME=$(ps -p "$PID" -o etime= 2>/dev/null | tr -d ' ')
        echo "   Uptime: $UPTIME"
        
        # Memoria
        MEM=$(ps -p "$PID" -o rss= 2>/dev/null | tr -d ' ')
        MEM_MB=$((MEM / 1024))
        echo "   Memoria: ${MEM_MB}MB"
    else
        echo "❌ Estado: DETENIDO (PID stale)"
        echo "   PID file existe pero proceso no corre"
    fi
else
    # Buscar procesos sin PID file
    PIDS=$(pgrep -f "multi_bot_daemon.py" 2>/dev/null)
    if [ -n "$PIDS" ]; then
        echo "⚠️ Estado: CORRIENDO (sin PID file)"
        echo "   PIDs encontrados: $PIDS"
    else
        echo "⏹️ Estado: DETENIDO"
    fi
fi

echo ""
echo "========================================"
echo "  Últimas líneas del log"
echo "========================================"
if [ -f "$LOG_FILE" ]; then
    tail -10 "$LOG_FILE"
else
    echo "(No hay log)"
fi

echo ""
echo "========================================"
echo "  Procesos relacionados"
echo "========================================"
ps aux | grep -E "multi_bot|geographic_bot" | grep -v grep || echo "(Ninguno)"
