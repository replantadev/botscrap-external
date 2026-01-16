# 🤖 BotScrap External - Lead Generation Bot para VPS Externo

Bot de generación de leads que se ejecuta en un VPS externo y envía datos a StaffKit via API.

## 🎯 Características

- **3 tipos de bots:**
  - 🎯 **Direct Bot**: Búsqueda directa en Google con validación completa
  - 😤 **Resentment Hunter**: Caza leads frustrados en Trustpilot/HostAdvice
  - 📡 **Social Signals**: Monitorea redes sociales buscando intención de compra

- **🤖 Worker Autónomo 24/7 (Fase 2):**
  - ⏰ **Scheduler**: Programación con cron o intervalos
  - 📋 **Job Queue**: Cola de trabajos con prioridades
  - 💓 **Health Monitor**: Monitoreo y recovery automático
  - 📊 **Métricas**: Estadísticas de ejecución
  - 🔔 **Notificaciones**: Alertas por Telegram
  - 🛡️ **Rate Limiter**: Control de límites por API
  - 💾 **Persistencia**: Estado en SQLite

- **✨ Validación y Enriquecimiento (Fase A):**
  - 🔍 Detección de CMS (WordPress, Joomla, otros)
  - ⚡ PageSpeed check (API o fallback rápido)
  - 🌿 Detección de perfil ecológico
  - 📍 Detección de ubicación (ES, CO, MX)
  - 🏢 Detección de tipo de organización y sector
  - 📧 Multi-email enrichment con priorización
  - 🎯 Cálculo de prioridad de leads

- **🖥️ Panel de Control Web:**
  - Dashboard visual para controlar los bots
  - **Panel Worker**: Control del worker autónomo
  - Cola de jobs en tiempo real
  - Schedules programados
  - Historial de ejecuciones
  - Health checks y rate limits
  - Iniciar/detener bots con un click
  - **Filtros avanzados**: CMS, velocidad, eco-only
  - Logs en tiempo real
  - Selector de lista destino
  - Sistema de actualizaciones Git integrado

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

# === FILTROS DE VALIDACIÓN (Fase A) ===
CMS_FILTER=wordpress          # all, wordpress, joomla
MIN_SPEED_SCORE=0            # Score mínimo PageSpeed
MAX_SPEED_SCORE=80           # Captar webs con score menor (lentas)
ECO_VERDE_ONLY=false         # Solo empresas con perfil ecológico
SKIP_PAGESPEED_API=true      # Usar fallback rápido en vez de API
```

## 🔍 Filtros de Validación (Fase A)

El Direct Bot ahora incluye validación y enriquecimiento completo:

### Filtro CMS
- `wordpress`: Solo sitios WordPress (default)
- `joomla`: Solo sitios Joomla
- `all`: Cualquier CMS

### Filtro de Velocidad
Captura webs lentas que necesitan optimización:
- `MAX_SPEED_SCORE=80`: Captar webs con score menor a 80
- `MAX_SPEED_SCORE=60`: Solo webs muy lentas

### Filtro Ecológico
- `ECO_VERDE_ONLY=true`: Solo empresas con keywords ecológicas en su web

### Uso desde CLI
```bash
# Filtrar por CMS
python run_bot.py direct -q "agencia madrid" --cms joomla

# Captar solo webs muy lentas
python run_bot.py direct -q "agencia madrid" --max-speed 60

# Solo empresas ecológicas
python run_bot.py direct -q "agencia sostenible" --eco-only
```

### Desde Dashboard
Los filtros están disponibles en el panel de control del Direct Bot:
- Selector de CMS
- Selector de velocidad máxima
- Checkbox "Solo eco"

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

## 🤖 Worker Autónomo 24/7 (Fase 2)

El worker autónomo permite ejecutar los bots de forma programada sin intervención manual.

### Componentes

| Componente | Descripción |
|------------|-------------|
| **StateManager** | Persistencia de estado en SQLite |
| **JobQueue** | Cola de trabajos con prioridades (HOT → LOW) |
| **Scheduler** | Programación con expresiones cron o intervalos |
| **WorkerManager** | Ejecutor de jobs en background |
| **HealthMonitor** | Watchdog con recovery automático |
| **RateLimiter** | Control de límites por API |
| **Notifier** | Alertas por Telegram |
| **MetricsCollector** | Estadísticas de ejecución |

### Uso CLI

```bash
# Iniciar worker autónomo
python run_bot.py worker

# Solo probar configuración
python run_bot.py worker --test

# Ver estado del sistema
python run_bot.py status

# Añadir job manual a la cola
python run_bot.py queue direct --priority 1
```

### Dashboard Web

Accede a `/panel/worker` para:
- ▶️ Iniciar/pausar/detener worker
- 📋 Ver cola de jobs
- ⏰ Gestionar schedules
- 📊 Ver historial de ejecuciones
- 💓 Health checks en tiempo real
- 📈 Rate limits de APIs

### Schedules por Defecto

| Schedule | Bot | Cron | Descripción |
|----------|-----|------|-------------|
| direct_morning | Direct | 0 9 * * 1-5 | Lun-Vie 9:00 |
| direct_afternoon | Direct | 0 15 * * 1-5 | Lun-Vie 15:00 |
| resentment_daily | Resentment | 0 10 * * 1-5 | Lun-Vie 10:00 |
| social_weekly | Social | 0 11 * * 1 | Lunes 11:00 |

### Configuración Worker

```bash
# === WORKER AUTÓNOMO ===
WORKER_ENABLED=true
WORKER_POLL_INTERVAL=10
WORKER_HEARTBEAT_INTERVAL=30

# === SCHEDULER ===
SCHEDULER_ENABLED=true

# === HEALTH MONITOR ===
HEALTH_CHECK_INTERVAL=60
HEARTBEAT_TIMEOUT=120
MAX_RECOVERY_ATTEMPTS=3
```

### Systemd Service (Worker)

```bash
# Copiar archivo de servicio
sudo cp botscrap-worker.service /etc/systemd/system/

# Activar
sudo systemctl daemon-reload
sudo systemctl enable botscrap-worker
sudo systemctl start botscrap-worker

# Ver logs
sudo journalctl -u botscrap-worker -f
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
├── webapp.py             # Dashboard web Flask
│
├── orchestrator.py       # Orquestador del worker
├── worker_daemon.py      # Entry point para systemd
│
├── core/                 # 🤖 Worker Autónomo (Fase 2)
│   ├── __init__.py
│   ├── state_manager.py  # Persistencia SQLite
│   ├── rate_limiter.py   # Control rate limits
│   ├── job_queue.py      # Cola de trabajos
│   ├── scheduler.py      # APScheduler integration
│   ├── worker.py         # Ejecutor de jobs
│   ├── health_monitor.py # Watchdog y recovery
│   ├── notifier.py       # Telegram notifications
│   └── metrics.py        # Métricas y stats
│
├── bots/
│   ├── __init__.py
│   ├── base_bot.py       # Clase base para todos los bots
│   ├── direct_bot.py     # Bot de búsqueda directa
│   ├── resentment_bot.py # Bot de reviews negativas
│   └── social_bot.py     # Bot de redes sociales
│
├── utils/
│   ├── __init__.py
│   ├── lead_validator.py # Validación y enriquecimiento (Fase A)
│   ├── email_enricher.py # Multi-email enrichment
│   ├── telegram.py       # Notificaciones Telegram
│   └── wordpress.py      # Detección WordPress
│
├── templates/            # Templates HTML
│   ├── dashboard.html
│   ├── worker.html       # Dashboard worker autónomo
│   ├── login.html
│   └── logs.html
│
├── data/                 # Estado persistente local
│   ├── worker_state.db   # SQLite state manager
│   ├── job_queue.db      # SQLite job queue
│   └── scheduler.db      # APScheduler jobs
│
└── logs/                 # Logs de ejecución
    └── worker_daemon.log
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
