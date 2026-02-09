# 🚀 Guía de Deployment a Producción - BotScrap External

## 📋 Checklist Pre-Deployment

- [x] Coordenadas UTF-8 corregidas (0 searches sin coords)
- [ ] Systemd service instalado
- [ ] Logrotate configurado
- [ ] Primer lead capturado y validado
- [ ] B2B filter removal confirmado

---

## 🔧 Instalación Rápida (5 minutos)

### 1️⃣ Subir archivos al VPS

```bash
# En tu máquina local
cd botscrap_external/
git add botscrap-daemon.service logrotate-botscrap.conf setup_production.sh
git commit -m "Add production setup files"
git push origin main
```

### 2️⃣ Actualizar en el VPS

```bash
# Conectar al VPS
ssh replanta@staff.replanta.dev

# Ir al directorio
cd /var/www/vhosts/territoriodrasanvicr.com/b/

# Actualizar código
git pull origin main

# Dar permisos de ejecución al script
chmod +x setup_production.sh

# Ejecutar instalación (como root)
sudo ./setup_production.sh
```

### 3️⃣ Verificar instalación

```bash
# Ver estado del servicio
systemctl status botscrap-daemon

# Ver logs en tiempo real
journalctl -u botscrap-daemon -f

# O ver logs del daemon directamente
tail -f /var/www/vhosts/territoriodrasanvicr.com/b/daemon.log
```

---

## ⚡ Instalación Manual (si prefieres control)

### Paso 1: Detener daemon actual

```bash
cd /var/www/vhosts/territoriodrasanvicr.com/b/

# Matar proceso actual
PID=$(cat daemon.pid)
kill $PID

# Limpiar archivos
rm -f daemon.pid daemon.lock
```

### Paso 2: Instalar systemd service

```bash
# Copiar archivo de servicio
sudo cp botscrap-daemon.service /etc/systemd/system/

# Recargar systemd
sudo systemctl daemon-reload

# Habilitar auto-start
sudo systemctl enable botscrap-daemon

# Iniciar servicio
sudo systemctl start botscrap-daemon
```

### Paso 3: Configurar logrotate

```bash
# Copiar configuración
sudo cp logrotate-botscrap.conf /etc/logrotate.d/botscrap

# Test manual (opcional)
sudo logrotate -f /etc/logrotate.d/botscrap
```

---

## 🔍 Monitoreo y Comandos Útiles

### Systemd Commands

```bash
# Estado del servicio
systemctl status botscrap-daemon

# Logs en tiempo real
journalctl -u botscrap-daemon -f

# Logs desde el último boot
journalctl -u botscrap-daemon -b

# Logs de las últimas 100 líneas
journalctl -u botscrap-daemon -n 100

# Reiniciar servicio
sudo systemctl restart botscrap-daemon

# Detener servicio
sudo systemctl stop botscrap-daemon

# Deshabilitar auto-start
sudo systemctl disable botscrap-daemon
```

### Log Files

```bash
# Daemon log (append mode)
tail -f /var/www/vhosts/territoriodrasanvicr.com/b/daemon.log

# Últimas 50 líneas
tail -n 50 /var/www/vhosts/territoriodrasanvicr.com/b/daemon.log

# Buscar errores
grep -i error /var/www/vhosts/territoriodrasanvicr.com/b/daemon.log

# Buscar ejecuciones exitosas
grep "✅" /var/www/vhosts/territoriodrasanvicr.com/b/daemon.log | tail -20
```

### Verificar Proceso

```bash
# Ver si está corriendo
ps aux | grep multi_bot_daemon

# Ver consumo de recursos
top -p $(pgrep -f multi_bot_daemon)

# Ver archivos abiertos
lsof -p $(pgrep -f multi_bot_daemon)
```

---

## 🧪 Testing Post-Deployment

### 1. Verificar que el servicio está corriendo

```bash
systemctl is-active botscrap-daemon
# Expected: active

systemctl is-enabled botscrap-daemon
# Expected: enabled
```

### 2. Monitorear primera ejecución

```bash
# Ver logs en tiempo real
journalctl -u botscrap-daemon -f

# Esperar a ver línea como:
# [INFO] 🔄 [Floristerías Argentina] Ready to run
# [INFO] ▶️ [Floristerías Argentina] Starting run #316
# [INFO] ✅ [Floristerías Argentina] Run #316 done: X saved, Y duplicates
```

### 3. Verificar en base de datos

```sql
-- Ver últimas ejecuciones
SELECT bot_name, status, leads_saved, leads_duplicates, created_at 
FROM bot_runs 
WHERE bot_id = 5 
ORDER BY created_at DESC 
LIMIT 10;

-- Ver leads capturados
SELECT name, email, phone, city, website, created_at 
FROM contacts 
WHERE list_id = 14 
ORDER BY created_at DESC 
LIMIT 20;
```

### 4. Validar B2B filter removal

```sql
-- Buscar floristerías capturadas (prueba de que el filtro fue removido)
SELECT name, website, business_type 
FROM contacts 
WHERE list_id = 14 
  AND (name LIKE '%flori%' OR name LIKE '%herbo%' OR name LIKE '%yoga%')
LIMIT 10;
```

---

## 🔥 Troubleshooting

### El servicio no inicia

```bash
# Ver logs detallados
journalctl -u botscrap-daemon -xe

# Verificar permisos
ls -la /var/www/vhosts/territoriodrasanvicr.com/b/multi_bot_daemon.py

# Verificar que el usuario existe
id replanta

# Test manual
cd /var/www/vhosts/territoriodrasanvicr.com/b/
./venv/bin/python multi_bot_daemon.py --api-key sk_d40a...
```

### El daemon no ejecuta bots

```bash
# Verificar configuración del bot
curl -H "Authorization: Bearer sk_d40a..." \
  https://staff.replanta.dev/api/v2/external-bot?id=5

# Ver condiciones should_run
# Verificar: is_enabled=1, is_paused=0, run_hours, interval_minutes
```

### Los logs crecen mucho

```bash
# Ver tamaño de logs
du -h /var/www/vhosts/territoriodrasanvicr.com/b/*.log

# Rotar manualmente
sudo logrotate -f /etc/logrotate.d/botscrap

# Verificar cron de logrotate
sudo cat /etc/cron.daily/logrotate
```

### Reinicio después de crash

Systemd reiniciará automáticamente el daemon si crashea (configurado con `Restart=always`).

```bash
# Ver reinicios
systemctl show botscrap-daemon | grep Restart

# Ver cuántas veces se reinició
journalctl -u botscrap-daemon | grep -i "started"
```

---

## 🎯 Validación Final

**Checklist de Producción Estable:**

- [ ] ✅ Servicio `botscrap-daemon` activo y enabled
- [ ] ✅ Daemon ejecuta bots automáticamente (cada 60 min)
- [ ] ✅ Logs rotan correctamente (no crecen sin límite)
- [ ] ✅ Auto-restart funciona después de crash
- [ ] ✅ Bot Geographic captura leads
- [ ] ✅ Floristerías aparecen en resultados (B2B filter removed)
- [ ] ✅ Emails enriquecidos correctamente
- [ ] ✅ Deduplicación funciona (google_place_id)

**Si todos los checks están OK**: ✅ **Sistema en PRODUCCIÓN ESTABLE**

---

## 📊 Próximos Pasos (Opcional - Fase 2)

1. **Telegram Notifications** (30 min)
   - Crear bot de Telegram
   - Configurar webhook en StaffKit
   - Test de alertas

2. **Dashboard Web** (1 hora)
   - Exponer Flask app (webapp.py)
   - Configurar Nginx reverse proxy
   - Agregar autenticación básica

3. **Health Monitoring** (1 hora)
   - Script cron que verifica daemon cada 5 min
   - Alerta si daemon muerto >10 min
   - Alerta si 0 leads >2 horas

4. **Backup Automático** (30 min)
   - Backup diario de search_queue
   - Backup semanal de contacts (list_id=14)
   - Retention de 30 días

---

## 📞 Soporte

Si encuentras problemas:
1. Revisar logs: `journalctl -u botscrap-daemon -e`
2. Verificar estado: `systemctl status botscrap-daemon`
3. Ver configuración bot: API `/api/v2/external-bot?id=5`

**Archivos importantes:**
- Service: `/etc/systemd/system/botscrap-daemon.service`
- Logrotate: `/etc/logrotate.d/botscrap`
- Daemon: `/var/www/vhosts/territoriodrasanvicr.com/b/multi_bot_daemon.py`
- Logs: `/var/www/vhosts/territoriodrasanvicr.com/b/daemon.log`
