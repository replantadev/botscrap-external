#!/usr/bin/env python3
"""
Geographic Crawler Bot
======================
Barre un país por sector usando cola de búsquedas en StaffKit.

Características:
- Procesa cola de búsquedas geográficas
- Usa DataForSEO (económico) o Google Places
- Paginación inteligente (continúa donde quedó)
- Deduplicación antes de insertar
- Rate limiting configurable
- Modo económico: para cuando encuentra pocos resultados

Uso:
    python geographic_bot.py --bot-id 5 --api-key "sk_xxx"
    python geographic_bot.py --bot-id 5 --api-key "sk_xxx" --searches-per-run 10
"""

import argparse
import base64
import json
import logging
import re
import sys
import time
from datetime import datetime
from typing import Dict, List, Optional
from urllib.parse import urlparse

import requests

# ============================================================================
# CONFIGURACIÓN
# ============================================================================

STAFFKIT_URL = 'https://staff.replanta.dev'

# DataForSEO credentials (se obtienen de config del bot)
DATAFORSEO_LOGIN = None
DATAFORSEO_PASSWORD = None

# Rate limiting
DEFAULT_DELAY_BETWEEN_SEARCHES = 30  # segundos entre búsquedas
DEFAULT_DELAY_BETWEEN_PAGES = 5      # segundos entre páginas
DEFAULT_SEARCHES_PER_RUN = 20        # búsquedas por ejecución

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


# ============================================================================
# STAFFKIT API CLIENT
# ============================================================================

class StaffKitClient:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.headers = {
            'Authorization': f'Bearer {api_key}',
            'Content-Type': 'application/json'
        }
    
    def get_next_search(self, bot_id: int) -> Optional[Dict]:
        """Obtener siguiente búsqueda de la cola"""
        try:
            resp = requests.get(
                f"{STAFFKIT_URL}/api/v2/geographic.php",
                params={'action': 'get_next_search', 'bot_id': bot_id},
                headers=self.headers,
                timeout=10
            )
            data = resp.json()
            if data.get('success'):
                return data.get('search')
        except Exception as e:
            logger.error(f"Error getting next search: {e}")
        return None
    
    def update_search_progress(self, search_id: int, current_page: int, 
                                leads_added: int, leads_duplicates: int,
                                results_found: int, api_cost: float):
        """Actualizar progreso de búsqueda"""
        try:
            requests.post(
                f"{STAFFKIT_URL}/api/v2/geographic.php",
                json={
                    'action': 'update_search_progress',
                    'search_id': search_id,
                    'current_page': current_page,
                    'leads_added': leads_added,
                    'leads_duplicates': leads_duplicates,
                    'results_found': results_found,
                    'api_cost': api_cost
                },
                headers=self.headers,
                timeout=10
            )
        except Exception as e:
            logger.error(f"Error updating progress: {e}")
    
    def complete_search(self, search_id: int, status: str = 'completed', error: str = None):
        """Marcar búsqueda como completada"""
        try:
            requests.post(
                f"{STAFFKIT_URL}/api/v2/geographic.php",
                json={
                    'action': 'complete_search',
                    'search_id': search_id,
                    'status': status,
                    'error': error
                },
                headers=self.headers,
                timeout=10
            )
        except Exception as e:
            logger.error(f"Error completing search: {e}")
    
    def check_duplicates_batch(self, list_id: int, domains: List[str]) -> Dict[str, bool]:
        """Verificar qué dominios ya existen en la lista"""
        try:
            resp = requests.post(
                f"{STAFFKIT_URL}/api/bots.php",
                json={
                    'action': 'check_duplicates_batch',
                    'list_id': list_id,
                    'domains': domains
                },
                headers=self.headers,
                timeout=15
            )
            data = resp.json()
            if data.get('success'):
                return data.get('duplicates', {})
            else:
                # Log error pero continuar (no bloquear el bot)
                error_msg = data.get('error', 'Unknown error')
                logger.warning(f"check_duplicates_batch failed: {error_msg}")
                # Retornar vacío = asumir que no hay duplicados (se verificará en save_lead)
                return {}
        except Exception as e:
            logger.error(f"Error checking duplicates: {e}")
        return {}
    
    def save_lead(self, list_id: int, lead: Dict) -> Dict:
        """Guardar lead en StaffKit"""
        try:
            resp = requests.post(
                f"{STAFFKIT_URL}/api/bots.php",
                json={
                    'action': 'save_lead',
                    'list_id': list_id,
                    'lead_data': json.dumps(lead)
                },
                headers=self.headers,
                timeout=10
            )
            return resp.json()
        except Exception as e:
            logger.error(f"Error saving lead: {e}")
            return {'success': False, 'error': str(e)}
    
    def regenerate_queue(self, bot_id: int, bot_config: Dict) -> bool:
        """
        Regenerar cola de búsquedas cuando se vacía.
        Crea nuevas búsquedas basadas en la config del bot.
        """
        try:
            keyword = bot_config.get('config_geo_keyword', '')
            country = bot_config.get('config_geo_country', 'MX')
            list_id = bot_config.get('target_list_id', 0)
            max_cities = int(bot_config.get('config_geo_max_cities', 50) or 50)
            max_pages = int(bot_config.get('config_geo_max_pages', 3) or 3)
            
            if not keyword or not list_id:
                logger.warning("No se puede regenerar cola: falta keyword o list_id")
                return False
            
            logger.info(f"🔄 Regenerando cola: keyword='{keyword}', country={country}, max_cities={max_cities}")
            
            resp = requests.post(
                f"{STAFFKIT_URL}/api/v2/geographic.php",
                json={
                    'action': 'generate_plan',
                    'bot_id': bot_id,
                    'list_id': list_id,
                    'keyword': keyword,
                    'country': country,
                    'max_cities': max_cities,
                    'max_pages': max_pages
                },
                headers=self.headers,
                timeout=30
            )
            
            data = resp.json()
            if data.get('success'):
                added = data.get('searches_added', 0)
                logger.info(f"✅ Cola regenerada: {added} nuevas búsquedas")
                return added > 0
            else:
                logger.error(f"Error regenerando cola: {data.get('error', 'Unknown')}")
                return False
                
        except Exception as e:
            logger.error(f"Error regenerando cola: {e}")
            return False

    def get_bot_config(self, bot_id: int) -> Dict:
        """Obtener configuración del bot"""
        try:
            resp = requests.get(
                f"{STAFFKIT_URL}/api/v2/external-bot",
                params={'id': bot_id},
                headers=self.headers,
                timeout=10
            )
            if resp.ok:
                data = resp.json()
                return data.get('bot', {})
        except Exception as e:
            logger.error(f"Error getting bot config: {e}")
        return {}


# ============================================================================
# DATAFORSEO API CLIENT
# ============================================================================

class DataForSEOClient:
    """Cliente para DataForSEO Google Maps API"""
    
    BASE_URL = "https://api.dataforseo.com/v3"
    COST_PER_TASK = 0.002  # $0.002 por tarea
    
    def __init__(self, login: str, password: str):
        self.auth = base64.b64encode(f"{login}:{password}".encode()).decode()
        self.headers = {
            'Authorization': f'Basic {self.auth}',
            'Content-Type': 'application/json'
        }
    
    # Mapeo de códigos de país a nombres válidos de DataForSEO
    COUNTRY_MAP = {
        'MX': 'Mexico',
        'ES': 'Spain',
        'CO': 'Colombia',
        'AR': 'Argentina',
        'CL': 'Chile',
        'PE': 'Peru',
        'US': 'United States',
        'BR': 'Brazil',
    }
    
    def search_maps(self, keyword: str, location: str, country: str = 'MX',
                    depth: int = 20, offset: int = 0) -> Dict:
        """
        Buscar en Google Maps via DataForSEO
        
        Args:
            keyword: Término de búsqueda (ej: "floristerías")
            location: Ciudad/zona (ej: "Ciudad de México, CDMX")
            country: Código país (ej: "MX")
            depth: Resultados por página (max 100)
            offset: Offset para paginación
        
        Returns:
            {'results': [...], 'total': N}
        """
        # DataForSEO solo acepta país como location_name, no ciudades
        # La ciudad va incluida en el keyword
        country_name = self.COUNTRY_MAP.get(country, 'Mexico')
        
        # Keyword incluye la ubicación completa para búsqueda geolocalizada
        full_keyword = f"{keyword} en {location}"
        
        payload = [{
            "keyword": full_keyword,
            "location_name": country_name,
            "language_code": "es",
            "depth": depth,
            "offset": offset
        }]
        
        logger.debug(f"DataForSEO request: keyword='{full_keyword}', location='{country_name}'")
        
        try:
            resp = requests.post(
                f"{self.BASE_URL}/serp/google/maps/live/advanced",
                json=payload,
                headers=self.headers,
                timeout=60
            )
            
            data = resp.json()
            
            if data.get('status_code') != 20000:
                logger.error(f"DataForSEO error: {data.get('status_message')}")
                return {'results': [], 'total': 0, 'error': data.get('status_message')}
            
            tasks = data.get('tasks', [])
            if not tasks:
                return {'results': [], 'total': 0}
            
            task = tasks[0]
            result = task.get('result', [{}])[0] if task.get('result') else {}
            items = result.get('items', [])
            total = result.get('items_count', 0)
            
            # Procesar resultados
            results = []
            for item in items:
                if item.get('type') != 'maps_search':
                    continue
                
                business = {
                    'title': item.get('title', ''),
                    'address': item.get('address', ''),
                    'phone': item.get('phone', ''),
                    'website': item.get('url', '') or item.get('domain', ''),
                    'rating': item.get('rating', {}).get('value'),
                    'reviews': item.get('rating', {}).get('votes_count', 0),
                    'category': item.get('category', ''),
                    'place_id': item.get('place_id', ''),
                    'latitude': item.get('gps_coordinates', {}).get('latitude'),
                    'longitude': item.get('gps_coordinates', {}).get('longitude')
                }
                
                # Solo incluir si tiene datos útiles
                if business['title'] and (business['website'] or business['phone']):
                    results.append(business)
            
            return {
                'results': results,
                'total': total,
                'cost': self.COST_PER_TASK
            }
            
        except Exception as e:
            logger.error(f"DataForSEO API error: {e}")
            return {'results': [], 'total': 0, 'error': str(e)}


# ============================================================================
# UTILIDADES
# ============================================================================

def extract_domain(url: str) -> str:
    """Extraer dominio de URL"""
    if not url:
        return ''
    try:
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        # Quitar www
        if domain.startswith('www.'):
            domain = domain[4:]
        return domain
    except:
        return ''


def clean_phone(phone: str) -> str:
    """Limpiar teléfono"""
    if not phone:
        return ''
    # Quitar caracteres no numéricos excepto +
    cleaned = re.sub(r'[^\d+]', '', phone)
    return cleaned


# ============================================================================
# GEOGRAPHIC BOT
# ============================================================================

class GeographicBot:
    """Bot que procesa cola de búsquedas geográficas"""
    
    def __init__(self, bot_id: int, api_key: str, config: Dict = None):
        self.bot_id = bot_id
        self.staffkit = StaffKitClient(api_key)
        self.config = config or {}
        
        # Configurar DataForSEO
        dataforseo_login = self.config.get('config_dataforseo_login', '')
        dataforseo_password = self.config.get('config_dataforseo_password', '')
        
        if dataforseo_login and dataforseo_password:
            self.dataforseo = DataForSEOClient(dataforseo_login, dataforseo_password)
        else:
            self.dataforseo = None
            logger.warning("DataForSEO credentials not configured")
        
        # Rate limiting
        self.delay_between_searches = int(self.config.get('config_delay_searches', DEFAULT_DELAY_BETWEEN_SEARCHES))
        self.delay_between_pages = int(self.config.get('config_delay_pages', DEFAULT_DELAY_BETWEEN_PAGES))
        
        # Stats
        self.stats = {
            'searches_processed': 0,
            'total_leads': 0,
            'total_duplicates': 0,
            'total_cost': 0.0
        }
    
    def process_search(self, search: Dict) -> Dict:
        """
        Procesar una búsqueda de la cola
        
        Returns:
            {'success': bool, 'leads_added': N, 'leads_duplicates': N}
        """
        search_id = search['id']
        list_id = search['list_id']
        keyword = search['keyword']
        location = search['location']
        country = search['country']
        current_page = int(search['current_page'])
        max_pages = int(search['max_pages'])
        results_per_page = int(search.get('results_per_page', 20))
        api_source = search.get('api_source', 'dataforseo')
        
        logger.info(f"Procesando: '{keyword}' en {location} (página {current_page + 1}/{max_pages})")
        
        result = {
            'success': True,
            'leads_added': 0,
            'leads_duplicates': 0,
            'pages_processed': 0
        }
        
        # Procesar páginas restantes
        for page in range(current_page, max_pages):
            offset = page * results_per_page
            
            # Buscar en DataForSEO
            if api_source == 'dataforseo' and self.dataforseo:
                search_result = self.dataforseo.search_maps(
                    keyword=keyword,
                    location=location,
                    country=country,
                    depth=results_per_page,
                    offset=offset
                )
            else:
                logger.error(f"API source '{api_source}' not supported or not configured")
                self.staffkit.complete_search(search_id, 'failed', f"API '{api_source}' not configured")
                result['success'] = False
                return result
            
            if search_result.get('error'):
                logger.error(f"Search error: {search_result['error']}")
                self.staffkit.complete_search(search_id, 'failed', search_result['error'])
                result['success'] = False
                return result
            
            businesses = search_result.get('results', [])
            total_results = search_result.get('total', 0)
            api_cost = search_result.get('cost', 0)
            
            logger.info(f"  Página {page + 1}: {len(businesses)} resultados (total: {total_results})")
            
            if not businesses:
                # No más resultados, completar
                logger.info(f"  No hay más resultados, completando búsqueda")
                break
            
            # Deduplicar por dominio
            domains = [extract_domain(b.get('website', '')) for b in businesses]
            domains = [d for d in domains if d]  # Filtrar vacíos
            
            existing = {}
            if domains:
                existing = self.staffkit.check_duplicates_batch(list_id, domains)
            
            # Insertar leads nuevos
            page_leads = 0
            page_dupes = 0
            
            for business in businesses:
                domain = extract_domain(business.get('website', ''))
                
                # Skip si ya existe o no tiene website
                if not domain:
                    continue
                if existing.get(domain, False):
                    page_dupes += 1
                    continue
                
                # Preparar lead
                lead = {
                    'company': business.get('title', ''),
                    'website': business.get('website', ''),
                    'phone': clean_phone(business.get('phone', '')),
                    'city': location.split(',')[0].strip(),
                    'country': country,
                    'source': f'Geographic: {keyword}',
                    'notes': f"Rating: {business.get('rating', 'N/A')} | Reviews: {business.get('reviews', 0)} | {business.get('category', '')}"
                }
                
                # Guardar
                save_result = self.staffkit.save_lead(list_id, lead)
                if save_result.get('success'):
                    if save_result.get('status') == 'duplicate':
                        page_dupes += 1
                    else:
                        page_leads += 1
                        # Marcar dominio como existente para evitar duplicados en misma página
                        existing[domain] = True
            
            result['leads_added'] += page_leads
            result['leads_duplicates'] += page_dupes
            result['pages_processed'] += 1
            
            # Actualizar progreso
            self.staffkit.update_search_progress(
                search_id=search_id,
                current_page=page + 1,
                leads_added=page_leads,
                leads_duplicates=page_dupes,
                results_found=total_results,
                api_cost=api_cost
            )
            
            logger.info(f"  → {page_leads} leads nuevos, {page_dupes} duplicados")
            
            # Parar si ya no hay más resultados
            if len(businesses) < results_per_page:
                break
            
            # Esperar entre páginas
            if page < max_pages - 1:
                time.sleep(self.delay_between_pages)
        
        # Marcar como completada
        self.staffkit.complete_search(search_id, 'completed')
        logger.info(f"✅ Búsqueda completada: {result['leads_added']} leads, {result['leads_duplicates']} duplicados")
        
        return result
    
    def run(self, max_searches: int = None):
        """
        Ejecutar bot: procesar búsquedas de la cola
        
        Args:
            max_searches: Máximo de búsquedas a procesar (None = infinito)
        """
        max_searches = max_searches or DEFAULT_SEARCHES_PER_RUN
        
        logger.info("=" * 60)
        logger.info(f"Geographic Crawler Bot (ID: {self.bot_id})")
        logger.info(f"Max búsquedas: {max_searches}")
        logger.info(f"Delay entre búsquedas: {self.delay_between_searches}s")
        logger.info("=" * 60)
        
        searches_done = 0
        queue_regenerated = False  # Solo regenerar una vez por ejecución
        
        while searches_done < max_searches:
            # Obtener siguiente búsqueda
            search = self.staffkit.get_next_search(self.bot_id)
            
            if not search:
                # Cola vacía - intentar regenerar si no lo hicimos ya
                if not queue_regenerated:
                    logger.info("Cola vacía - intentando regenerar...")
                    bot_config = self.staffkit.get_bot_config(self.bot_id)
                    if self.staffkit.regenerate_queue(self.bot_id, bot_config):
                        queue_regenerated = True
                        continue  # Reintentar obtener búsqueda
                    else:
                        logger.info("No se pudo regenerar la cola o ya está completa")
                
                logger.info("No hay más búsquedas pendientes")
                break
            
            # Procesar
            result = self.process_search(search)
            
            if result['success']:
                searches_done += 1
                self.stats['searches_processed'] += 1
                self.stats['total_leads'] += result['leads_added']
                self.stats['total_duplicates'] += result['leads_duplicates']
            
            # Esperar entre búsquedas
            if searches_done < max_searches:
                logger.info(f"Esperando {self.delay_between_searches}s antes de siguiente búsqueda...")
                time.sleep(self.delay_between_searches)
        
        # Resumen - formato parseble por daemon
        logger.info("=" * 60)
        logger.info("RESUMEN")
        logger.info(f"  Búsquedas procesadas: {self.stats['searches_processed']}")
        logger.info(f"  Leads totales: {self.stats['total_leads']}")
        logger.info(f"  Duplicados: {self.stats['total_duplicates']}")
        logger.info("=" * 60)
        
        # Líneas con formato estándar para que el daemon las parsee
        logger.info(f"STATS:leads_found:{self.stats['total_leads']}")
        logger.info(f"STATS:leads_saved:{self.stats['total_leads']}")
        logger.info(f"STATS:leads_duplicates:{self.stats['total_duplicates']}")
        logger.info(f"STATS:searches_done:{self.stats['searches_processed']}")
        
        # Indicar si la cola quedó vacía
        if self.stats['searches_processed'] == 0:
            logger.info("STATS:queue_empty:true")
        
        return self.stats


# ============================================================================
# MAIN
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description='Geographic Crawler Bot')
    parser.add_argument('--bot-id', type=int, required=True, help='ID del bot en StaffKit')
    parser.add_argument('--api-key', required=True, help='StaffKit API Key')
    parser.add_argument('--searches-per-run', type=int, default=DEFAULT_SEARCHES_PER_RUN,
                        help=f'Búsquedas por ejecución (default: {DEFAULT_SEARCHES_PER_RUN})')
    parser.add_argument('--dry-run', action='store_true', help='Solo mostrar, no ejecutar')
    
    args = parser.parse_args()
    
    # Obtener configuración del bot
    client = StaffKitClient(args.api_key)
    config = client.get_bot_config(args.bot_id)
    
    if not config:
        logger.error(f"No se encontró configuración para bot {args.bot_id}")
        sys.exit(1)
    
    logger.info(f"Bot: {config.get('name', 'Unknown')}")
    
    if args.dry_run:
        logger.info("[DRY RUN] Solo mostraría búsquedas pendientes")
        search = client.get_next_search(args.bot_id)
        if search:
            logger.info(f"Siguiente: '{search['keyword']}' en {search['location']}")
        else:
            logger.info("No hay búsquedas pendientes")
        return
    
    # Ejecutar bot
    bot = GeographicBot(args.bot_id, args.api_key, config)
    stats = bot.run(max_searches=args.searches_per_run)
    
    # Exit code basado en resultados
    if stats['searches_processed'] == 0:
        sys.exit(0)  # No había trabajo, ok
    else:
        sys.exit(0)


if __name__ == '__main__':
    main()
