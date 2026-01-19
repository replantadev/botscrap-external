#!/usr/bin/env python3
"""
Configuración centralizada para BotScrap External
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Cargar .env
load_dotenv(override=True)

# Directorios
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / 'data'
LOGS_DIR = BASE_DIR / 'logs'

# Crear directorios si no existen
DATA_DIR.mkdir(exist_ok=True)
LOGS_DIR.mkdir(exist_ok=True)

# === STAFFKIT ===
STAFFKIT_URL = os.getenv('STAFFKIT_URL', '').rstrip('/')
STAFFKIT_API_KEY = os.getenv('STAFFKIT_API_KEY', '')
STAFFKIT_LIST_ID = int(os.getenv('STAFFKIT_LIST_ID', '1'))  # Lista por defecto
STAFFKIT_TIMEOUT = int(os.getenv('STAFFKIT_TIMEOUT', '20'))
STAFFKIT_RETRIES = int(os.getenv('STAFFKIT_RETRIES', '3'))

# Listas específicas por bot (si no se especifican, usan STAFFKIT_LIST_ID)
DIRECT_LIST_ID = int(os.getenv('DIRECT_LIST_ID', os.getenv('STAFFKIT_LIST_ID', '1')))
RESENTMENT_LIST_ID = int(os.getenv('RESENTMENT_LIST_ID', os.getenv('STAFFKIT_LIST_ID', '1')))
SOCIAL_LIST_ID = int(os.getenv('SOCIAL_LIST_ID', os.getenv('STAFFKIT_LIST_ID', '1')))

# === GOOGLE APIs ===
GOOGLE_API_KEY = os.getenv('GOOGLE_API_KEY', '')
CX_ID = os.getenv('CX_ID', '')

# === TELEGRAM ===
TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN', '')
TELEGRAM_BOT_TOKEN = TELEGRAM_TOKEN  # Alias
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID', '')

# === HUNTER.IO ===
HUNTER_KEY = os.getenv('HUNTER_KEY', '')

# === DATAFORSEO ===
DATAFORSEO_CREDENTIALS = os.getenv('DATAFORSEO_CREDENTIALS', '')

# === TWITTER/X API ===
TWITTER_API_KEY = os.getenv('TWITTER_API_KEY', '')
TWITTER_API_SECRET = os.getenv('TWITTER_API_SECRET', '')
TWITTER_BEARER_TOKEN = os.getenv('TWITTER_BEARER_TOKEN', '')

# === KEYWORDS SOCIAL BOT ===
# Keywords separadas por comas para cada categoría
SOCIAL_KEYWORDS_HOSTING = os.getenv('SOCIAL_KEYWORDS_HOSTING', 'hosting lento,mi hosting,problemas hosting,cambiar hosting,hosting malo,hosting caro')
SOCIAL_KEYWORDS_MIGRATION = os.getenv('SOCIAL_KEYWORDS_MIGRATION', 'migrar wordpress,cambiar hosting,mover web,nuevo hosting,hosting alternativa')
SOCIAL_KEYWORDS_ECO = os.getenv('SOCIAL_KEYWORDS_ECO', 'hosting ecológico,hosting verde,hosting sostenible,green hosting,carbono neutral')
SOCIAL_KEYWORDS_WORDPRESS = os.getenv('SOCIAL_KEYWORDS_WORDPRESS', 'ayuda wordpress,problema wordpress,wordpress lento,optimizar wordpress')
SOCIAL_KEYWORDS_EXCLUDE = os.getenv('SOCIAL_KEYWORDS_EXCLUDE', 'vps,servidor dedicado,kubernetes,docker,aws,azure,google cloud,devops')

# === LIMITES ===
MAX_LEADS_PER_RUN = int(os.getenv('MAX_LEADS_PER_RUN', '10'))
DAILY_LIMIT = int(os.getenv('DAILY_LIMIT', '50'))

# Límites diarios por bot (si no se especifican, usan DAILY_LIMIT)
DIRECT_DAILY_LIMIT = int(os.getenv('DIRECT_DAILY_LIMIT', os.getenv('DAILY_LIMIT', '50')))
RESENTMENT_DAILY_LIMIT = int(os.getenv('RESENTMENT_DAILY_LIMIT', os.getenv('DAILY_LIMIT', '50')))
SOCIAL_DAILY_LIMIT = int(os.getenv('SOCIAL_DAILY_LIMIT', os.getenv('DAILY_LIMIT', '50')))

# === WORKER AUTÓNOMO ===
WORKER_ENABLED = os.getenv('WORKER_ENABLED', 'true').lower() == 'true'
WORKER_POLL_INTERVAL = int(os.getenv('WORKER_POLL_INTERVAL', '10'))  # segundos
WORKER_HEARTBEAT_INTERVAL = int(os.getenv('WORKER_HEARTBEAT_INTERVAL', '30'))  # segundos

# === SCHEDULES ===
SCHEDULER_ENABLED = os.getenv('SCHEDULER_ENABLED', 'true').lower() == 'true'

# Auto-retry: Re-ejecutar bots hasta alcanzar objetivo diario
AUTO_RETRY_ENABLED = os.getenv('AUTO_RETRY_ENABLED', 'true').lower() == 'true'
AUTO_RETRY_INTERVAL = int(os.getenv('AUTO_RETRY_INTERVAL', '120'))  # minutos entre re-intentos
AUTO_RETRY_MAX_HOUR = int(os.getenv('AUTO_RETRY_MAX_HOUR', '20'))  # no ejecutar después de esta hora

# === HEALTH MONITOR ===
HEALTH_CHECK_INTERVAL = int(os.getenv('HEALTH_CHECK_INTERVAL', '60'))  # segundos
HEARTBEAT_TIMEOUT = int(os.getenv('HEARTBEAT_TIMEOUT', '120'))  # segundos sin heartbeat = problema
MAX_RECOVERY_ATTEMPTS = int(os.getenv('MAX_RECOVERY_ATTEMPTS', '3'))

# === LOGGING ===
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')

# === FILTROS DE VALIDACIÓN ===
# CMS Filter: 'all', 'wordpress', 'joomla'
CMS_FILTER = os.getenv('CMS_FILTER', 'wordpress')
# Speed Score: para captar webs lentas (0-100)
MIN_SPEED_SCORE = int(os.getenv('MIN_SPEED_SCORE', '0'))
MAX_SPEED_SCORE = int(os.getenv('MAX_SPEED_SCORE', '80'))  # Captar webs con score < 80
# Solo perfiles ecológicos
ECO_VERDE_ONLY = os.getenv('ECO_VERDE_ONLY', 'false').lower() == 'true'
# Usar fallback de velocidad en vez de PageSpeed API (más rápido)
SKIP_PAGESPEED_API = os.getenv('SKIP_PAGESPEED_API', 'true').lower() == 'true'

# === SCRAPING ===
SCRAPER_DELAY_MIN = float(os.getenv('SCRAPER_DELAY_MIN', '3'))
SCRAPER_DELAY_MAX = float(os.getenv('SCRAPER_DELAY_MAX', '8'))
HTTP_TIMEOUT = int(os.getenv('HTTP_TIMEOUT', '30'))

# === BOT INFO ===
BOT_NAME = os.getenv('BOT_NAME', 'BotScrap External')
BOT_ID = os.getenv('BOT_ID', '')

# === COMPETIDORES (para Resentment Hunter) ===
COMPETITOR_HOSTINGS = {
    'hostinger': {
        'trustpilot': 'hostinger.com',
        'hostadvice': 'hostinger',
        'name': 'Hostinger'
    },
    'ionos': {
        'trustpilot': 'ionos.es',
        'hostadvice': 'ionos',
        'name': 'IONOS'
    },
    'godaddy': {
        'trustpilot': 'godaddy.com',
        'hostadvice': 'godaddy',
        'name': 'GoDaddy'
    },
    'bluehost': {
        'trustpilot': 'bluehost.com',
        'hostadvice': 'bluehost',
        'name': 'Bluehost'
    },
    'siteground': {
        'trustpilot': 'siteground.es',
        'hostadvice': 'siteground',
        'name': 'SiteGround'
    },
    'hostgator': {
        'trustpilot': 'hostgator.com',
        'hostadvice': 'hostgator',
        'name': 'HostGator'
    },
    'namecheap': {
        'trustpilot': 'namecheap.com',
        'hostadvice': 'namecheap',
        'name': 'Namecheap'
    },
    'webempresa': {
        'trustpilot': 'webempresa.com',
        'hostadvice': 'webempresa',
        'name': 'Webempresa'
    },
    'dinahosting': {
        'trustpilot': 'dinahosting.com',
        'hostadvice': 'dinahosting',
        'name': 'Dinahosting'
    },
    'cdmon': {
        'trustpilot': 'cdmon.com',
        'hostadvice': 'cdmon',
        'name': 'CDmon'
    },
    'arsys': {
        'trustpilot': 'arsys.es',
        'hostadvice': 'arsys',
        'name': 'Arsys'
    },
    'raiolanetworks': {
        'trustpilot': 'raiolanetworks.es',
        'hostadvice': 'raiola-networks',
        'name': 'Raiola Networks'
    },
    'ovh': {
        'trustpilot': 'ovhcloud.com',
        'hostadvice': 'ovh',
        'name': 'OVH'
    },
    'donweb': {
        'trustpilot': 'donweb.com',
        'hostadvice': 'donweb',
        'name': 'DonWeb'
    },
}

# === KEYWORDS RESENTIMIENTO ===
RESENTMENT_KEYWORDS_ES = [
    'caído', 'caidas', 'downtime', 'error 500', 'error 503',
    'lento', 'muy lento', 'no carga', 'timeout',
    'perdí datos', 'backup falló', 'sin backup',
    'hackeado', 'malware',
    'soporte horrible', 'no responden', 'tardaron días',
    'soporte inútil', 'incompetentes',
    'subieron precio', 'renovación cara', 'estafa',
    'cobro doble', 'sin avisar', 'robo',
    'me voy', 'cambiar hosting', 'busco alternativa',
    'recomienden hosting', 'migrar',
    'nunca más', 'no contraten', 'eviten',
]

RESENTMENT_KEYWORDS_EN = [
    'terrible support', 'worst hosting', 'avoid',
    'migrating away', 'leaving', 'looking for alternative',
    'scam', 'hidden fees', 'downtime issues',
    'slow', 'unreliable', 'nightmare',
    'canceling', 'switching', 'never again',
]

# === DOMINIOS A EXCLUIR ===
SOCIAL_MEDIA_DOMAINS = {
    'facebook.com', 'fb.com', 'instagram.com', 'twitter.com', 'x.com',
    'linkedin.com', 'tiktok.com', 'youtube.com', 'pinterest.com',
    'yelp.com', 'tripadvisor.com', 'google.com', 'trustpilot.com',
    'amazon.com', 'ebay.com', 'etsy.com', 'mercadolibre.com',
    'wordpress.com', 'wix.com', 'weebly.com', 'medium.com',
    'blogspot.com', 'tumblr.com', 'linktr.ee', 'bit.ly',
}


def get_daily_limit(bot_type: str) -> int:
    """Obtener límite diario específico para un tipo de bot"""
    limits = {
        'direct': DIRECT_DAILY_LIMIT,
        'resentment': RESENTMENT_DAILY_LIMIT,
        'social': SOCIAL_DAILY_LIMIT,
    }
    return limits.get(bot_type, DAILY_LIMIT)


def get_list_id(bot_type: str) -> int:
    """Obtener lista específica para un tipo de bot"""
    lists = {
        'direct': DIRECT_LIST_ID,
        'resentment': RESENTMENT_LIST_ID,
        'social': SOCIAL_LIST_ID,
    }
    return lists.get(bot_type, STAFFKIT_LIST_ID)


def validate_config() -> dict:
    """Valida la configuración y retorna errores si los hay"""
    errors = []
    warnings = []
    
    # Obligatorios
    if not STAFFKIT_URL:
        errors.append("STAFFKIT_URL no configurado")
    if not STAFFKIT_API_KEY:
        errors.append("STAFFKIT_API_KEY no configurado")
    
    # Opcionales pero recomendados
    if not GOOGLE_API_KEY:
        warnings.append("GOOGLE_API_KEY no configurado (Direct Bot no funcionará)")
    if not CX_ID:
        warnings.append("CX_ID no configurado (Direct Bot no funcionará)")
    if not TELEGRAM_TOKEN:
        warnings.append("TELEGRAM_TOKEN no configurado (sin notificaciones)")
    
    return {
        'valid': len(errors) == 0,
        'errors': errors,
        'warnings': warnings
    }
