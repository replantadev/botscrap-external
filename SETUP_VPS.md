# 🚀 Guía de Instalación - BotScrap External

## Paso 1: Preparar el Repositorio Git

En tu máquina local (donde tienes el código):

```bash
cd c:\Users\programacion2\Documents\CODE\botscrap_external

# Inicializar Git
git init
git add .
git commit -m "Initial commit - BotScrap External"

# Crear repo en GitHub/GitLab y conectar
git remote add origin https://github.com/TU_USUARIO/botscrap-external.git
git branch -M main
git push -u origin main
```

---

## Paso 2: Preparar el VPS

Conectar al VPS por SSH:

```bash
ssh usuario@tu-vps-ip
```

### 2.1 Instalar dependencias del sistema

```bash
# Actualizar sistema
sudo apt update && sudo apt upgrade -y

# Instalar Python y Git
sudo apt install -y python3 python3-pip python3-venv git

# Verificar versiones
python3 --version  # Debe ser 3.9+
git --version
```

---

## Paso 3: Clonar el Repositorio

```bash
# Ir a home o donde quieras instalarlo
cd ~

# Clonar
git clone https://github.com/TU_USUARIO/botscrap-external.git
cd botscrap-external
```

---

## Paso 4: Configurar Entorno Python

```bash
# Crear entorno virtual
python3 -m venv venv

# Activar entorno
source venv/bin/activate

# Instalar dependencias
pip install --upgrade pip
pip install -r requirements.txt
```

---

## Paso 5: Configurar Variables de Entorno

```bash
# Copiar plantilla
cp .env.example .env

# Editar configuración
nano .env
```

**Contenido del `.env`:**

```env
# === StaffKit API ===
STAFFKIT_URL=https://tu-staffkit.com
STAFFKIT_API_KEY=tu_api_key_aqui
STAFFKIT_LIST_ID=1

# === Dashboard ===
ADMIN_USER=admin
ADMIN_PASS=tu_password_seguro_aqui
SECRET_KEY=genera_una_clave_larga_aleatoria
ACCESS_PATH=/panel

# === Opcionales ===
# GOOGLE_API_KEY=tu_google_api_key
# TELEGRAM_BOT_TOKEN=tu_bot_token
# TELEGRAM_CHAT_ID=tu_chat_id

# === Límites ===
MAX_LEADS_PER_RUN=50
DAILY_LIMIT=100
```

**Generar SECRET_KEY:**
```bash
python3 -c "import secrets; print(secrets.token_hex(32))"
```

---

## Paso 6: Probar que Funciona

```bash
# Activar entorno si no está activo
source venv/bin/activate

# Probar conexión a StaffKit
python3 test_connection.py

# Ejecutar dashboard en modo desarrollo
python3 webapp.py
```

Si todo va bien, verás:
```
 * Running on http://0.0.0.0:5000
```

Accede desde tu navegador: `http://tu-vps-ip:5000/panel`

---

## Paso 7: Configurar como Servicio (Producción)

### 7.1 Crear archivo de servicio

```bash
sudo nano /etc/systemd/system/botscrap-dashboard.service
```

**Contenido:**

```ini
[Unit]
Description=BotScrap External Dashboard
After=network.target

[Service]
Type=simple
User=TU_USUARIO
WorkingDirectory=/home/TU_USUARIO/botscrap-external
Environment="PATH=/home/TU_USUARIO/botscrap-external/venv/bin"
ExecStart=/home/TU_USUARIO/botscrap-external/venv/bin/gunicorn --workers 2 --bind 0.0.0.0:5000 webapp:app
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

> ⚠️ Cambia `TU_USUARIO` por tu usuario real del VPS

### 7.2 Activar y arrancar servicio

```bash
# Recargar systemd
sudo systemctl daemon-reload

# Habilitar para que arranque con el sistema
sudo systemctl enable botscrap-dashboard

# Iniciar
sudo systemctl start botscrap-dashboard

# Verificar estado
sudo systemctl status botscrap-dashboard
```

---

## Paso 8: Configurar Nginx (Opcional - Recomendado)

Si quieres usar un dominio y HTTPS:

```bash
sudo apt install -y nginx certbot python3-certbot-nginx
```

### 8.1 Crear config de Nginx

```bash
sudo nano /etc/nginx/sites-available/botscrap
```

**Contenido:**

```nginx
server {
    listen 80;
    server_name tu-dominio.com;

    location / {
        # Mostrar 404 falso en la raíz
        return 404;
    }

    location /panel {
        proxy_pass http://127.0.0.1:5000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }

    location /panel/ {
        proxy_pass http://127.0.0.1:5000/panel/;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }
}
```

### 8.2 Activar sitio

```bash
sudo ln -s /etc/nginx/sites-available/botscrap /etc/nginx/sites-enabled/
sudo nginx -t
sudo systemctl reload nginx
```

### 8.3 SSL con Let's Encrypt

```bash
sudo certbot --nginx -d tu-dominio.com
```

---

## Paso 9: Verificar Todo

```bash
# Estado del servicio
sudo systemctl status botscrap-dashboard

# Ver logs en tiempo real
sudo journalctl -u botscrap-dashboard -f

# Ver logs de la aplicación
tail -f ~/botscrap-external/logs/*.log
```

---

## 🔄 Actualizar el Bot (Después de hacer push)

### Opción A: Desde el Panel Web
1. Ve a `https://tu-dominio.com/panel`
2. Clic en "🔄 Updates" 
3. Si hay actualizaciones → "Aplicar actualización"
4. Reiniciar cuando te pregunte

### Opción B: Manual por SSH
```bash
cd ~/botscrap-external
git pull origin main
sudo systemctl restart botscrap-dashboard
```

---

## 📋 Resumen de Comandos Útiles

| Acción | Comando |
|--------|---------|
| Ver estado | `sudo systemctl status botscrap-dashboard` |
| Reiniciar | `sudo systemctl restart botscrap-dashboard` |
| Parar | `sudo systemctl stop botscrap-dashboard` |
| Ver logs | `sudo journalctl -u botscrap-dashboard -f` |
| Actualizar | `cd ~/botscrap-external && git pull && sudo systemctl restart botscrap-dashboard` |

---

## 🔐 Seguridad Recomendada

1. **Firewall:**
   ```bash
   sudo ufw allow 22
   sudo ufw allow 80
   sudo ufw allow 443
   sudo ufw enable
   ```

2. **Cambiar ACCESS_PATH** a algo difícil de adivinar:
   ```
   ACCESS_PATH=/secreto-xyz-123
   ```

3. **Password seguro** para ADMIN_PASS

4. **Usar HTTPS** siempre en producción

---

## ❓ Troubleshooting

### El servicio no arranca
```bash
# Ver error detallado
sudo journalctl -u botscrap-dashboard -n 50 --no-pager
```

### Error de permisos
```bash
sudo chown -R TU_USUARIO:TU_USUARIO ~/botscrap-external
```

### Puerto ocupado
```bash
sudo lsof -i :5000
# Si hay algo, matarlo o cambiar puerto
```

### Git pull falla por cambios locales
```bash
cd ~/botscrap-external
git stash
git pull origin main
git stash pop  # opcional, para recuperar cambios
```
