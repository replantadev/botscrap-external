#!/bin/bash
##############################################################################
# BotScrap Daemon - Instalación Manual Rápida
# Ejecutar: sudo bash quick_install.sh [usuario]
# Ejemplo: sudo bash quick_install.sh root
##############################################################################

set -e

# Obtener usuario desde argumento o detectar
USER=${1:-$(stat -c '%U' . 2>/dev/null || stat -f '%Su' . 2>/dev/null || echo "root")}

echo "=========================================="
echo "BotScrap Daemon - Quick Install"
echo "Usuario: $USER"
echo "=========================================="

# 1. Detener daemon actual
if [ -f daemon.pid ]; then
    PID=$(cat daemon.pid)
    kill $PID 2>/dev/null || true
    rm -f daemon.pid daemon.lock
fi

# 2. Instalar systemd service
cat > /etc/systemd/system/botscrap-daemon.service << EOF
[Unit]
Description=BotScrap Multi-Bot Daemon
After=network.target

[Service]
Type=simple
User=$USER
Group=$USER
WorkingDirectory=/var/www/vhosts/territoriodrasanvicr.com/b
Environment="PATH=/var/www/vhosts/territoriodrasanvicr.com/b/venv/bin:/usr/local/bin:/usr/bin:/bin"
Environment="STAFFKIT_URL=https://staff.replanta.dev"
ExecStart=/var/www/vhosts/territoriodrasanvicr.com/b/venv/bin/python multi_bot_daemon.py --api-key sk_d40a7992c1fa4a3774134d31658ff10cc93ce505d95a3ce7d37e8459759578e9

Restart=always
RestartSec=10
StartLimitInterval=200
StartLimitBurst=5

StandardOutput=append:/var/www/vhosts/territoriodrasanvicr.com/b/daemon.log
StandardError=append:/var/www/vhosts/territoriodrasanvicr.com/b/daemon.log

NoNewPrivileges=true
PrivateTmp=true

MemoryLimit=512M
CPUQuota=50%

[Install]
WantedBy=multi-user.target
EOF

chmod 644 /etc/systemd/system/botscrap-daemon.service

# 3. Instalar logrotate
cat > /etc/logrotate.d/botscrap << EOF
/var/www/vhosts/territoriodrasanvicr.com/b/daemon.log {
    daily
    missingok
    rotate 14
    compress
    delaycompress
    notifempty
    create 0640 $USER $USER
    sharedscripts
}

/var/www/vhosts/territoriodrasanvicr.com/b/logs/*.log {
    daily
    missingok
    rotate 7
    compress
    delaycompress
    notifempty
    create 0640 $USER $USER
}
EOF

chmod 644 /etc/logrotate.d/botscrap

# 4. Ajustar permisos
chown -R $USER:$USER /var/www/vhosts/territoriodrasanvicr.com/b
chmod +x /var/www/vhosts/territoriodrasanvicr.com/b/multi_bot_daemon.py

# 5. Habilitar y arrancar
systemctl daemon-reload
systemctl enable botscrap-daemon
systemctl start botscrap-daemon

echo ""
echo "✅ INSTALACIÓN COMPLETA"
echo ""
echo "Comandos útiles:"
echo "  Estado:   systemctl status botscrap-daemon"
echo "  Logs:     journalctl -u botscrap-daemon -f"
echo "  Reiniciar: systemctl restart botscrap-daemon"
echo ""

# Mostrar estado
sleep 2
systemctl status botscrap-daemon --no-pager || true
