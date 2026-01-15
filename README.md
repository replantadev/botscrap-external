# 🤖 BotScrap External - Lead Generation Bot para VPS Externo

Bot de generación de leads que se ejecuta en un VPS externo y envía datos a StaffKit via API.

## 🎯 Características

- **3 tipos de bots:**
  - 🎯 **Direct Bot**: Búsqueda directa en Google de sitios WordPress
  - 😤 **Resentment Hunter**: Caza leads frustrados en Trustpilot/HostAdvice
  - 📡 **Social Signals**: Monitorea redes sociales buscando intención de compra

- **🖥️ Panel de Control Web:**
  - Dashboard visual para controlar los bots
  - Iniciar/detener bots con un click
  - Logs en tiempo real
  - Estado de conexión con StaffKit

- **🔐 Seguridad:**
  - Autenticación usuario/contraseña
  - URL de acceso oculta (configurable)
  - La raíz muestra 404 falso

- **Integración completa con StaffKit:**
  - Verificación de duplicados antes de guardar
  - Guardado de leads directo via API
  - Progreso en tiempo real
  - Notificaciones Telegram

## 📦 Instalación Rápida

```bash
# 1. Clonar o copiar archivos
git clone https://github.com/tu-repo/botscrap-external.git
cd botscrap_external

# 2. Crear entorno virtual
python3 -m venv venv
source venv/bin/activate  # Linux/Mac

# 3. Instalar dependencias
pip install -r requirements.txt

# 4. Configurar
cp .env.example .env
nano .env  # Editar con tus credenciales

# 5. Probar conexión
python test_connection.py

# 6. Iniciar dashboard
python webapp.py
```

## 🖥️ Panel de Control Web

### Acceder al Panel

```bash
# Desarrollo
python webapp.py
# Abre: http://localhost:5000/panel

# Producción con gunicorn
gunicorn webapp:app --bind 0.0.0.0:5000 --workers 2
```

### URL Oculta

El panel está oculto por defecto. Solo es accesible desde la URL configurada en `ACCESS_PATH`:

```bash
# En .env
ACCESS_PATH=/mi-panel-secreto

# Acceder: http://tu-vps.com:5000/mi-panel-secreto
# La raíz (/) muestra un 404 falso
```

### Credenciales

```bash
# En .env
ADMIN_USER=admin
ADMIN_PASS=tu-password-super-seguro
```

## ⚙️ Configuración Completa (.env)

```bash
# === STAFFKIT (OBLIGATORIO) ===
STAFFKIT_URL=https://staff.replanta.dev
STAFFKIT_API_KEY=sk_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
STAFFKIT_LIST_ID=1

# === GOOGLE APIs (para Direct Bot) ===
GOOGLE_API_KEY=AIzaSyXXXXXXXXXXXXXXXXXXXXXX
CX_ID=0123456789abcdef0

# === TELEGRAM (opcional) ===
TELEGRAM_TOKEN=123456789:ABCdefGHI...
TELEGRAM_CHAT_ID=123456789

# === WEB DASHBOARD ===
FLASK_PORT=5000
FLASK_SECRET_KEY=clave-secreta-larga-y-aleatoria
ADMIN_USER=admin
ADMIN_PASS=password-seguro
ACCESS_PATH=/panel

# === LIMITES ===
MAX_LEADS_PER_RUN=10
DAILY_LIMIT=50
```

## 🚀 Uso CLI

### Ejecutar Bot Directo
```bash
python run_bot.py direct --query "agencia marketing wordpress madrid" --limit 10
```

### Ejecutar Resentment Hunter
```bash
python run_bot.py resentment --hosting hostinger --limit 20
python run_bot.py resentment --all-hostings --limit 50
```

### Ejecutar Social Signals
```bash
python run_bot.py social --sources reddit,twitter --limit 15
```

## 🔧 Producción

### Systemd Service

```bash
# Copiar archivo de servicio
sudo cp botscrap-dashboard.service /etc/systemd/system/

# Editar con tus rutas
sudo nano /etc/systemd/system/botscrap-dashboard.service

# Activar
sudo systemctl daemon-reload
sudo systemctl enable botscrap-dashboard
sudo systemctl start botscrap-dashboard

# Ver logs
sudo journalctl -u botscrap-dashboard -f
```

### Nginx (opcional, para dominio)

```bash
# Copiar configuración
sudo cp nginx.conf.example /etc/nginx/sites-available/botscrap

# Editar dominio
sudo nano /etc/nginx/sites-available/botscrap

# Activar
sudo ln -s /etc/nginx/sites-available/botscrap /etc/nginx/sites-enabled/
sudo nginx -t
sudo systemctl reload nginx
```

### Cron (ejecución programada)

```bash
# Editar crontab
crontab -e

# Ejecutar cada 6 horas
0 */6 * * * cd /home/user/botscrap_external && ./venv/bin/python run_bot.py direct --query "wordpress agency" --limit 10 >> logs/cron.log 2>&1

# Resentment hunter diario a las 3am
0 3 * * * cd /home/user/botscrap_external && ./venv/bin/python run_bot.py resentment --all-hostings --limit 30 >> logs/cron.log 2>&1
```

## 📊 Flujo de Datos

```
┌─────────────────────────────────────────────────────────┐
│                    VPS EXTERNO                          │
│  ┌─────────────────────────────────────────────────┐   │
│  │           BotScrap External                      │   │
│  │  ┌─────────┐ ┌─────────┐ ┌─────────────────┐   │   │
│  │  │ Direct  │ │Resentmt │ │ Social Signals  │   │   │
│  │  │  Bot    │ │ Hunter  │ │     Bot         │   │   │
│  │  └────┬────┘ └────┬────┘ └───────┬─────────┘   │   │
│  │       │           │              │              │   │
│  │       └───────────┴──────────────┘              │   │
│  │                   │                              │   │
│  │           ┌───────▼───────┐                     │   │
│  │           │ StaffKit API  │                     │   │
│  │           │    Client     │                     │   │
│  │           └───────┬───────┘                     │   │
│  └───────────────────┼─────────────────────────────┘   │
│                      │                                  │
└──────────────────────┼──────────────────────────────────┘
                       │ HTTPS
                       ▼
┌──────────────────────────────────────────────────────────┐
│                 STAFFKIT SERVER                          │
│  ┌─────────────────────────────────────────────────────┐│
│  │              /api/bots.php                          ││
│  │  • check-duplicate  → Verificar si existe          ││
│  │  • save_lead       → Guardar nuevo lead            ││
│  │  • update_progress → Reportar progreso             ││
│  │  • complete        → Finalizar ejecución           ││
│  └─────────────────────────────────────────────────────┘│
│                         │                                │
│                         ▼                                │
│  ┌─────────────────────────────────────────────────────┐│
│  │               MySQL Database                        ││
│  │  • list_members (leads)                            ││
│  │  • bot_runs (ejecuciones)                          ││
│  │  • bots (configuración)                            ││
│  └─────────────────────────────────────────────────────┘│
└──────────────────────────────────────────────────────────┘
```

## 📁 Estructura del Proyecto

```
botscrap_external/
├── .env.example          # Plantilla de configuración
├── .gitignore
├── README.md
├── requirements.txt
│
├── config.py             # Configuración centralizada
├── staffkit_client.py    # Cliente API para StaffKit
├── run_bot.py            # CLI principal
├── test_connection.py    # Test de conexión
│
├── bots/
│   ├── __init__.py
│   ├── base_bot.py       # Clase base para todos los bots
│   ├── direct_bot.py     # Bot de búsqueda directa
│   ├── resentment_bot.py # Bot de reviews negativas
│   └── social_bot.py     # Bot de redes sociales
│
├── scrapers/
│   ├── __init__.py
│   ├── trustpilot.py     # Scraper Trustpilot
│   ├── hostadvice.py     # Scraper HostAdvice
│   └── google_search.py  # Búsqueda Google
│
├── utils/
│   ├── __init__.py
│   ├── telegram.py       # Notificaciones Telegram
│   ├── wordpress.py      # Detección WordPress
│   └── rate_limiter.py   # Control de rate limits
│
├── data/                 # Estado persistente local
│   └── .gitkeep
│
└── logs/                 # Logs de ejecución
    └── .gitkeep
```

## 🔐 Seguridad

- Las credenciales van en `.env` (nunca en el código)
- `.env` está en `.gitignore`
- Comunicación con StaffKit via HTTPS
- Token de API en header Authorization

## 🛠️ Troubleshooting

### Error de conexión a StaffKit
```bash
python test_connection.py
# Verificar URL y API key en .env
```

### Bloqueo por Trustpilot/Google
```bash
# Aumentar delays en config.py
SCRAPER_DELAY_MIN = 5
SCRAPER_DELAY_MAX = 15
```

### Ver logs
```bash
tail -f logs/bot.log
```

## 📝 Licencia

Uso interno - Replanta.net
