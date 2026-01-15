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
STAFFKIT_LIST_ID = int(os.getenv('STAFFKIT_LIST_ID', '1'))
STAFFKIT_TIMEOUT = int(os.getenv('STAFFKIT_TIMEOUT', '20'))
STAFFKIT_RETRIES = int(os.getenv('STAFFKIT_RETRIES', '3'))

# === GOOGLE APIs ===
GOOGLE_API_KEY = os.getenv('GOOGLE_API_KEY', '')
CX_ID = os.getenv('CX_ID', '')

# === TELEGRAM ===
TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN', '')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID', '')

# === HUNTER.IO ===
HUNTER_KEY = os.getenv('HUNTER_KEY', '')

# === DATAFORSEO ===
DATAFORSEO_CREDENTIALS = os.getenv('DATAFORSEO_CREDENTIALS', '')

# === LIMITES ===
MAX_LEADS_PER_RUN = int(os.getenv('MAX_LEADS_PER_RUN', '10'))
DAILY_LIMIT = int(os.getenv('DAILY_LIMIT', '50'))

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
