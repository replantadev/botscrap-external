#!/bin/bash
##############################################################################
# BotScrap Daemon - Script de Instalación para Producción
# Ejecutar como root: sudo bash setup_production.sh
##############################################################################

set -e  # Exit on error

echo "=========================================="
echo "BotScrap Daemon - Setup Producción"
echo "=========================================="
echo ""

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Variables
INSTALL_DIR="/var/www/vhosts/territoriodrasanvicr.com/b"
SERVICE_FILE="botscrap-daemon.service"
LOGROTATE_FILE="logrotate-botscrap.conf"
USER="replanta"

# Check if running as root
if [ "$EUID" -ne 0 ]; then 
   echo -e "${RED}❌ Este script debe ejecutarse como root${NC}"
   echo "   Usa: sudo bash setup_production.sh"
   exit 1
fi

# Check if directory exists
if [ ! -d "$INSTALL_DIR" ]; then
    echo -e "${RED}❌ Directorio no encontrado: $INSTALL_DIR${NC}"
    exit 1
fi

cd "$INSTALL_DIR"

echo -e "${YELLOW}1. Verificando archivos...${NC}"
if [ ! -f "$SERVICE_FILE" ]; then
    echo -e "${RED}❌ Archivo no encontrado: $SERVICE_FILE${NC}"
    exit 1
fi
echo -e "${GREEN}   ✓ Archivos encontrados${NC}"

echo ""
echo -e "${YELLOW}2. Deteniendo daemon actual...${NC}"
if [ -f daemon.pid ]; then
    OLD_PID=$(cat daemon.pid)
    if ps -p $OLD_PID > /dev/null 2>&1; then
        echo "   Matando proceso $OLD_PID..."
        kill $OLD_PID || true
        sleep 2
    fi
    rm -f daemon.pid daemon.lock
fi
echo -e "${GREEN}   ✓ Daemon detenido${NC}"

echo ""
echo -e "${YELLOW}3. Instalando systemd service...${NC}"
cp "$SERVICE_FILE" /etc/systemd/system/
chmod 644 /etc/systemd/system/"$SERVICE_FILE"
systemctl daemon-reload
echo -e "${GREEN}   ✓ Service instalado${NC}"

echo ""
echo -e "${YELLOW}4. Configurando logrotate...${NC}"
if [ -f "$LOGROTATE_FILE" ]; then
    cp "$LOGROTATE_FILE" /etc/logrotate.d/botscrap
    chmod 644 /etc/logrotate.d/botscrap
    echo -e "${GREEN}   ✓ Logrotate configurado${NC}"
else
    echo -e "${YELLOW}   ⚠ Archivo logrotate no encontrado (opcional)${NC}"
fi

echo ""
echo -e "${YELLOW}5. Ajustando permisos...${NC}"
chown -R "$USER":"$USER" "$INSTALL_DIR"
chmod +x "$INSTALL_DIR/multi_bot_daemon.py"
echo -e "${GREEN}   ✓ Permisos ajustados${NC}"

echo ""
echo -e "${YELLOW}6. Habilitando servicio...${NC}"
systemctl enable botscrap-daemon.service
echo -e "${GREEN}   ✓ Auto-start habilitado${NC}"

echo ""
echo -e "${YELLOW}7. Iniciando servicio...${NC}"
systemctl start botscrap-daemon.service
sleep 3
echo -e "${GREEN}   ✓ Servicio iniciado${NC}"

echo ""
echo "=========================================="
echo -e "${GREEN}✅ INSTALACIÓN COMPLETA${NC}"
echo "=========================================="
echo ""
echo "Comandos útiles:"
echo "  Estado:     systemctl status botscrap-daemon"
echo "  Logs:       journalctl -u botscrap-daemon -f"
echo "  Reiniciar:  systemctl restart botscrap-daemon"
echo "  Detener:    systemctl stop botscrap-daemon"
echo "  Deshabilitar: systemctl disable botscrap-daemon"
echo ""
echo "Logs del daemon:"
echo "  tail -f $INSTALL_DIR/daemon.log"
echo ""

# Show status
echo "Estado actual:"
systemctl status botscrap-daemon --no-pager || true

echo ""
echo -e "${GREEN}✓ Setup completado exitosamente${NC}"
