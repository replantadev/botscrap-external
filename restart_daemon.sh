#!/bin/bash
# restart_daemon.sh - Reinicia el daemon de forma segura

API_KEY="sk_d40a7992c1fa4a3774134d31658ff10cc93ce505d95a3ce7d37e8459759578e9"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DAEMON_SCRIPT="$SCRIPT_DIR/multi_bot_daemon.py"
PID_FILE="$SCRIPT_DIR/daemon.pid"
LOG_FILE="$SCRIPT_DIR/daemon.log"
VENV_PYTHON="$SCRIPT_DIR/venv/bin/python"

# Colores
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${YELLOW}🔄 Reiniciando Multi-Bot Daemon...${NC}"

# 1. Matar daemon existente si hay PID
if [ -f "$PID_FILE" ]; then
    OLD_PID=$(cat "$PID_FILE")
    if ps -p "$OLD_PID" > /dev/null 2>&1; then
        echo -e "${YELLOW}⏹️  Deteniendo daemon antiguo (PID: $OLD_PID)...${NC}"
        kill "$OLD_PID" 2>/dev/null
        sleep 2
        # Forzar si no murió
        if ps -p "$OLD_PID" > /dev/null 2>&1; then
            echo -e "${RED}⚠️  Forzando kill -9...${NC}"
            kill -9 "$OLD_PID" 2>/dev/null
            sleep 1
        fi
    fi
    rm -f "$PID_FILE"
fi

# 2. Matar cualquier otro proceso daemon huérfano
pkill -f "multi_bot_daemon.py" 2>/dev/null

# 3. Truncar log (limpia el archivo)
> "$LOG_FILE"

# 4. Iniciar nuevo daemon
# < /dev/null evita "nohup: no se tendrá en cuenta la entrada"
# >> append al log (ya truncado)
# 2>&1 redirige stderr a stdout
nohup "$VENV_PYTHON" "$DAEMON_SCRIPT" --api-key "$API_KEY" >> "$LOG_FILE" 2>&1 < /dev/null &

NEW_PID=$!
sleep 2

# 5. Verificar que arrancó
if ps -p "$NEW_PID" > /dev/null 2>&1; then
    echo -e "${GREEN}✅ Daemon iniciado con PID: $NEW_PID${NC}"
    echo "$NEW_PID" > "$PID_FILE"
    echo ""
    echo -e "${GREEN}📋 Últimas líneas del log:${NC}"
    tail -10 "$LOG_FILE"
else
    echo -e "${RED}❌ Error: El daemon no arrancó${NC}"
    echo -e "${RED}📋 Log de error:${NC}"
    cat "$LOG_FILE"
    exit 1
fi
