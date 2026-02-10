#!/usr/bin/env python3
"""
Geographic Crawler Bot v2.0 - Barre países enteros por sector/keyword
Usa DataForSEO Maps API + Email Scraping integrado + Hunter.io fallback

Flujo:
1. DataForSEO Maps → obtiene negocios con website
2. Scraping inteligente → sitemap.xml + páginas de contacto
3. Hunter.io → fallback si scraping no encuentra email
4. Guarda lead COMPLETO con email
"""

import argparse
import requests
import json
import time
import sys
import os
import re
from datetime import datetime
from typing import Optional, Dict, List, Any, Tuple
from urllib.parse import urlparse
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET

# Configuración
STAFFKIT_URL = os.getenv('STAFFKIT_URL', 'https://staff.replanta.dev')

# Emails a ignorar
IGNORE_EMAILS = [
    'example', 'test', 'domain', 'wixpress', 'sentry', 'localhost',
    'noreply', 'no-reply', 'donotreply', 'bounce', 'mailer-daemon',
    'wordpress', 'webmaster@wordpress', 'privacy@', 'abuse@',
    'wix.com', 'godaddy', 'hostinger'
]

# URLs de contacto comunes
CONTACT_PATHS = [
    '/contacto', '/contact', '/contactanos', '/contact-us',
    '/sobre-nosotros', '/about', '/about-us', '/nosotros',
    '/quienes-somos', '/empresa', '/company'
]

# Mapeo de códigos de país ISO a nombres completos para DataForSEO
COUNTRY_NAMES = {
    'AR': 'Argentina',
    'MX': 'Mexico',
    'ES': 'Spain',
    'CO': 'Colombia',
    'CL': 'Chile',
    'PE': 'Peru',
    'US': 'United States',
    'BR': 'Brazil'
}

class GeographicBot:
    def __init__(self, bot_id: int, api_token: str, 
                 dataforseo_login: str = None, dataforseo_password: str = None,
                 searches_per_run: int = 5, delay_between_searches: float = 2.0,
                 delay_between_pages: float = 1.0, verbose: bool = False):
        
        self.bot_id = bot_id
        self.api_token = api_token
        self.searches_per_run = searches_per_run
        self.delay_between_searches = delay_between_searches
        self.delay_between_pages = delay_between_pages
        self.verbose = verbose
        
        # Obtener credenciales de DataForSEO desde integraciones si no se pasaron
        if not dataforseo_login or not dataforseo_password:
            credentials = self._get_dataforseo_credentials()
            if credentials:
                self.dataforseo_login = credentials.get('login', '')
                self.dataforseo_password = credentials.get('password', '')
                self.log(f"✓ DataForSEO: login={self.dataforseo_login[:4]}***", 'INFO')
            else:
                # Fallback a valores pasados como argumentos si la API falla
                self.dataforseo_login = dataforseo_login or ''
                self.dataforseo_password = dataforseo_password or ''
                if not self.dataforseo_login:
                    self.log("⚠️ DataForSEO NO configurado - el bot no puede buscar", 'ERROR')
        else:
            self.dataforseo_login = dataforseo_login
            self.dataforseo_password = dataforseo_password
        
        # Obtener Hunter.io API key desde integraciones
        self.hunter_key = self._get_hunter_key()
        if self.hunter_key:
            self.log(f"✓ Hunter.io configurado (key: {self.hunter_key[:8]}...)", 'INFO')
        else:
            self.log("⚠️ Hunter.io NO configurado - solo scraping", 'WARNING')
        
        # Session con timeouts razonables para scraping
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        # Stats de esta ejecución
        self.stats = {
            'searches_processed': 0,
            'leads_found': 0,
            'leads_new': 0,
            'leads_with_email': 0,
            'emails_scraped': 0,
            'emails_hunter': 0,
            'api_cost': 0.0,
            'errors': []
        }
        
    def _get_dataforseo_credentials(self) -> Optional[dict]:
        """Obtiene credenciales de DataForSEO desde StaffKit Integraciones"""
        url = f"{STAFFKIT_URL}/api/v2/integrations.php/dataforseo"
        headers = {
            'Authorization': f'Bearer {self.api_token}',
            'Content-Type': 'application/json'
        }
        
        try:
            self.log(f"Obteniendo credenciales DataForSEO de {url}", 'DEBUG')
            response = requests.get(url, headers=headers, timeout=10)
            self.log(f"Respuesta: HTTP {response.status_code}", 'DEBUG')
            
            if response.status_code == 200:
                data = response.json()
                self.log(f"JSON: enabled={data.get('enabled')}, login={bool(data.get('login'))}", 'DEBUG')
                
                if data.get('enabled') and data.get('login') and data.get('password'):
                    self.log(f"✓ Credenciales DataForSEO obtenidas desde Integraciones", 'INFO')
                    return data
                else:
                    self.log(f"DataForSEO en Integraciones: enabled={data.get('enabled')}, login={bool(data.get('login'))}, password={bool(data.get('password'))}", 'WARNING')
            else:
                self.log(f"Error HTTP {response.status_code}: {response.text[:200]}", 'ERROR')
        except Exception as e:
            self.log(f"Error obteniendo credenciales de Integraciones: {e}", 'ERROR')
        
        return None
    
    def _get_hunter_key(self) -> Optional[str]:
        """Obtiene API key de Hunter.io desde StaffKit Integraciones"""
        url = f"{STAFFKIT_URL}/api/v2/integrations.php/hunter"
        headers = {
            'Authorization': f'Bearer {self.api_token}',
            'Content-Type': 'application/json'
        }
        
        try:
            response = requests.get(url, headers=headers, timeout=10)
            if response.status_code == 200:
                data = response.json()
                if data.get('enabled') and data.get('api_key'):
                    return data.get('api_key')
        except Exception as e:
            self.debug(f"Error obteniendo Hunter key: {e}")
        
        return None
        
    def log(self, msg: str, level: str = 'INFO'):
        """Log con timestamp"""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"[{timestamp}] [{level}] {msg}")
        
    def debug(self, msg: str):
        """Log solo si verbose"""
        if self.verbose:
            self.log(msg, 'DEBUG')
            
    def api_call(self, endpoint: str, method: str = 'GET', data: dict = None) -> Optional[dict]:
        """Llamada a la API de StaffKit"""
        url = f"{STAFFKIT_URL}/api/v2/geographic.php?action={endpoint}"
        headers = {
            'Authorization': f'Bearer {self.api_token}',
            'Content-Type': 'application/json'
        }
        
        try:
            if method == 'GET':
                response = requests.get(url, headers=headers, params=data, timeout=30)
            else:
                response = requests.post(url, headers=headers, json=data, timeout=30)
                
            if response.status_code == 200:
                return response.json()
            else:
                self.log(f"API error {response.status_code}: {response.text}", 'ERROR')
                return None
        except Exception as e:
            self.log(f"API exception: {str(e)}", 'ERROR')
            return None
            
    def get_next_search(self) -> Optional[dict]:
        """Obtiene la siguiente búsqueda pendiente"""
        result = self.api_call('get_next_search', 'POST', {'bot_id': self.bot_id})
        if result and result.get('success') and result.get('search'):
            return result['search']
        return None
        
    def update_search_progress(self, search_id: int, next_page: int, 
                                leads_found: int, leads_new: int) -> bool:
        """Actualiza progreso de una búsqueda (paginación)"""
        result = self.api_call('update_search_progress', 'POST', {
            'search_id': search_id,
            'next_page_token': str(next_page),
            'leads_found': leads_found,
            'leads_new': leads_new
        })
        return result and result.get('success', False)
        
    def complete_search(self, search_id: int, leads_found: int, 
                        leads_new: int, api_cost: float) -> bool:
        """Marca una búsqueda como completada"""
        result = self.api_call('complete_search', 'POST', {
            'search_id': search_id,
            'leads_found': leads_found,
            'leads_new': leads_new,
            'api_cost': api_cost
        })
        return result and result.get('success', False)
        
    def search_dataforseo_maps(self, keyword: str, location: str, country_code: str = 'AR',
                                latitude: float = None, longitude: float = None,
                                language_code: str = 'es', max_pages: int = 3) -> List[dict]:
        """
        Busca en DataForSEO Maps API
        Retorna lista de negocios encontrados
        """
        all_results = []
        
        # DataForSEO Maps API endpoint
        url = "https://api.dataforseo.com/v3/serp/google/maps/live/advanced"
        
        for page in range(max_pages):
            self.debug(f"DataForSEO página {page + 1}/{max_pages} para '{keyword}' en {location}")
            
            # Construir payload con location_coordinate para Maps API
            # Maps API requiere coordenadas, no nombres de lugar
            if not latitude or not longitude:
                self.log(f"ERROR: Sin coordenadas para '{location}'. Saltando búsqueda.", 'ERROR')
                break
            
            location_coordinate = f"{latitude},{longitude}"
            
            payload = [{
                "keyword": keyword,
                "location_coordinate": location_coordinate,
                "language_code": language_code,
                "device": "desktop",
                "os": "windows",
                "depth": 20  # resultados por página
            }]
            
            # DEBUG: Ver payload real enviado
            self.log(f"[DEBUG] Payload enviado: {json.dumps(payload, indent=2)}", 'INFO')
            
            try:
                response = requests.post(
                    url,
                    auth=(self.dataforseo_login, self.dataforseo_password),
                    json=payload,
                    timeout=60
                )
                
                if response.status_code != 200:
                    self.log(f"DataForSEO HTTP error {response.status_code}: {response.text[:200]}", 'ERROR')
                    break
                    
                data = response.json()
                
                # DEBUG: Ver respuesta completa de DataForSEO
                self.log(f"[DEBUG] Respuesta DataForSEO: {json.dumps(data, indent=2)[:500]}", 'INFO')
                
                # Contar costo
                if 'cost' in data:
                    self.stats['api_cost'] += data['cost']
                    
                # Extraer resultados
                tasks = data.get('tasks', [])
                if not tasks:
                    self.log(f"DataForSEO: No tasks in response for '{keyword}' in {location}", 'WARNING')
                    break
                    
                task = tasks[0]
                status_code = task.get('status_code')
                if status_code != 20000:
                    self.log(f"DataForSEO task error ({status_code}): {task.get('status_message')}", 'ERROR')
                    break
                    
                results = task.get('result', [])
                if not results:
                    self.log(f"DataForSEO: Empty results for '{keyword}' in {location}", 'WARNING')
                    break
                    
                items = results[0].get('items', [])
                if not items:
                    self.log(f"DataForSEO: No items found for '{keyword}' in {location} (page {page + 1})", 'INFO')
                    break
                    
                # Procesar items
                for item in items:
                    if item.get('type') == 'maps_search':
                        business = self._parse_maps_result(item)
                        if business:
                            all_results.append(business)
                            
                self.debug(f"Encontrados {len(items)} negocios en página {page + 1}")
                
                # Delay entre páginas
                if page < max_pages - 1:
                    time.sleep(self.delay_between_pages)
                    
            except Exception as e:
                self.log(f"DataForSEO exception: {str(e)}", 'ERROR')
                self.stats['errors'].append(str(e))
                break
                
        return all_results
        
    def _parse_maps_result(self, item: dict) -> Optional[dict]:
        """Parsea un resultado de Maps a formato de lead"""
        try:
            # Extraer datos básicos
            title = item.get('title', '')
            if not title:
                return None
            
            category = item.get('category', '')
                
            address_info = item.get('address_info', {}) or {}
            
            return {
                'company': title,
                'address': item.get('address', ''),
                'phone': item.get('phone', ''),
                'website': item.get('url', '') or item.get('domain', ''),
                'city': address_info.get('city', ''),
                'region': address_info.get('region', ''),
                'country': address_info.get('country_code', ''),
                'postal_code': address_info.get('zip', ''),
                'category': category,
                'rating': item.get('rating', {}).get('value') if item.get('rating') else None,
                'reviews_count': item.get('rating', {}).get('votes_count') if item.get('rating') else None,
                'latitude': item.get('latitude'),
                'longitude': item.get('longitude'),
                'place_id': item.get('place_id', ''),
                'cid': item.get('cid', ''),
            }
        except Exception as e:
            self.debug(f"Error parsing result: {e}")
            return None
            
    def add_leads_to_staffkit(self, search: dict, leads: List[dict]) -> int:
        """
        Añade leads al sistema StaffKit CON email enriquecido
        Retorna número de leads nuevos añadidos
        """
        if not leads:
            return 0
            
        # Obtener list_id del bot
        list_id = search.get('list_id')
        if not list_id:
            self.log("No list_id en búsqueda", 'ERROR')
            return 0
            
        new_count = 0
        
        for lead in leads:
            website = lead.get('website', '')
            company = lead.get('company', '')
            
            # ========== ENRIQUECIMIENTO DE EMAIL ==========
            email = ''
            email_source = 'none'
            
            if website:
                self.debug(f"  📧 Enriqueciendo {company} ({website})")
                
                # 1. Scraping inteligente (sitemap + contacto + homepage)
                email, email_source = self._scrape_email(website)
                
                if email:
                    self.stats['emails_scraped'] += 1
                    self.log(f"    ✓ Scraped: {email}", 'INFO')
                else:
                    # 2. Hunter.io fallback
                    if self.hunter_key:
                        email, email_source = self._hunter_search(website)
                        if email:
                            self.stats['emails_hunter'] += 1
                            self.log(f"    ✓ Hunter: {email}", 'INFO')
                        else:
                            self.debug(f"    ✗ Sin email para {website}")
            
            if email:
                self.stats['leads_with_email'] += 1
            
            # Preparar datos para StaffKit
            lead_data = {
                'list_id': list_id,
                'empresa': company,
                'email': email,  # Email enriquecido
                'email_source': email_source,
                'website': website,
                'telefono': lead.get('phone', ''),
                'direccion': lead.get('address', ''),
                'ciudad': lead.get('city', ''),
                'region': lead.get('region', ''),
                'pais': lead.get('country', ''),
                'codigo_postal': lead.get('postal_code', ''),
                'categoria': lead.get('category', ''),
                'rating': lead.get('rating'),
                'reviews': lead.get('reviews_count'),
                'latitud': lead.get('latitude'),
                'longitud': lead.get('longitude'),
                'place_id': lead.get('place_id', ''),
                'fuente': 'geographic_crawler',
                'notas': f"Keyword: {search.get('keyword')}, Ciudad: {search.get('location')}"
            }
            
            # Llamar API para añadir lead
            result = self._add_lead(lead_data)
            if result and result.get('is_new', False):
                new_count += 1
                
        return new_count
    
    # ========== EMAIL SCRAPING METHODS ==========
    
    def _scrape_email(self, website: str) -> Tuple[str, str]:
        """
        Scraping inteligente de email.
        Retorna (email, source) o ('', 'none')
        """
        domain = self._extract_domain(website)
        if not domain:
            return '', 'none'
        
        base_url = f"https://{domain}"
        
        # 1. Intentar sitemap.xml para encontrar URLs de contacto
        contact_urls = self._find_contact_urls_sitemap(base_url)
        
        # 2. Si no hay sitemap, buscar links en homepage
        if not contact_urls:
            contact_urls = self._find_contact_links_homepage(base_url)
        
        # 3. Fallback: URLs estándar
        if not contact_urls:
            contact_urls = [f"{base_url}{path}" for path in CONTACT_PATHS[:5]]
        
        # 4. Scrapear URLs encontradas
        all_emails = []
        
        # Primero la homepage
        emails = self._extract_emails_from_url(base_url)
        all_emails.extend([(e, 'homepage') for e in emails])
        
        # Luego las páginas de contacto
        for url in contact_urls[:5]:  # Limitar a 5 URLs
            emails = self._extract_emails_from_url(url)
            all_emails.extend([(e, 'contacto') for e in emails])
        
        # 5. Filtrar y priorizar
        if all_emails:
            # Priorizar emails personales o de ventas
            for email, source in all_emails:
                if self._is_priority_email(email):
                    return email, source
            # Si no hay prioritarios, devolver el primero
            return all_emails[0]
        
        return '', 'none'
    
    def _extract_domain(self, website: str) -> str:
        """Extraer dominio limpio"""
        try:
            if not website.startswith(('http://', 'https://')):
                website = 'https://' + website
            parsed = urlparse(website)
            domain = parsed.netloc.lower()
            if domain.startswith('www.'):
                domain = domain[4:]
            return domain
        except:
            return ''
    
    def _find_contact_urls_sitemap(self, base_url: str) -> List[str]:
        """Buscar URLs de contacto en sitemap.xml"""
        urls = []
        try:
            sitemap_url = f"{base_url}/sitemap.xml"
            response = self.session.get(sitemap_url, timeout=(3, 8))
            
            if response.status_code == 200 and 'xml' in response.headers.get('Content-Type', ''):
                root = ET.fromstring(response.content)
                
                # Namespaces comunes de sitemap
                ns = {'sm': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
                
                for loc in root.findall('.//sm:loc', ns) or root.findall('.//loc'):
                    url = loc.text.lower() if loc.text else ''
                    if any(kw in url for kw in ['contact', 'contacto', 'about', 'nosotros', 'empresa']):
                        urls.append(loc.text)
                        
        except Exception as e:
            self.debug(f"Sitemap error: {e}")
        
        return urls[:5]
    
    def _find_contact_links_homepage(self, base_url: str) -> List[str]:
        """Buscar links de contacto en homepage"""
        urls = []
        try:
            response = self.session.get(base_url, timeout=(3, 8))
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                
                for a in soup.find_all('a', href=True):
                    href = a.get('href', '')
                    text = a.get_text().lower()
                    
                    # Buscar links de contacto
                    if any(kw in text for kw in ['contacto', 'contact', 'nosotros', 'about']):
                        if href.startswith('/'):
                            urls.append(f"{base_url}{href}")
                        elif href.startswith('http'):
                            urls.append(href)
        except:
            pass
        
        return urls[:5]
    
    def _extract_emails_from_url(self, url: str) -> List[str]:
        """Extraer emails de una URL"""
        emails = []
        try:
            response = self.session.get(url, timeout=(3, 8), allow_redirects=True)
            if response.status_code == 200:
                # Regex para emails
                pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
                found = re.findall(pattern, response.text)
                
                # Filtrar válidos
                for email in found:
                    email = email.lower()
                    if self._is_valid_email(email):
                        emails.append(email)
                        
        except Exception as e:
            self.debug(f"Scrape error {url}: {e}")
        
        return list(set(emails))[:5]  # Únicos, máximo 5
    
    def _is_valid_email(self, email: str) -> bool:
        """Validar que el email es real y útil"""
        email = email.lower()
        
        # Ignorar emails de sistema/spam
        for ignore in IGNORE_EMAILS:
            if ignore in email:
                return False
        
        # Debe tener formato válido
        if not re.match(r'^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$', email):
            return False
        
        # Ignorar emails muy largos (spam)
        if len(email) > 50:
            return False
        
        return True
    
    def _is_priority_email(self, email: str) -> bool:
        """Verificar si es un email prioritario (personal o ventas)"""
        local = email.split('@')[0].lower()
        
        # Emails personales (nombre.apellido)
        if re.match(r'^[a-z]+\.[a-z]+$', local):
            return True
        
        # Emails de ventas/comercial
        if any(kw in local for kw in ['ventas', 'comercial', 'sales', 'info']):
            return True
        
        return False
    
    def _hunter_search(self, website: str) -> Tuple[str, str]:
        """Buscar email con Hunter.io API"""
        domain = self._extract_domain(website)
        if not domain or not self.hunter_key:
            return '', 'none'
        
        try:
            url = f"https://api.hunter.io/v2/domain-search?domain={domain}&api_key={self.hunter_key}"
            response = self.session.get(url, timeout=(5, 15))
            data = response.json()
            
            if data.get('data', {}).get('emails'):
                for item in data['data']['emails']:
                    email = item.get('value', '').lower()
                    if email and self._is_valid_email(email):
                        return email, 'hunter'
                        
        except Exception as e:
            self.debug(f"Hunter error: {e}")
        
        return '', 'none'
        
    def _add_lead(self, lead_data: dict) -> Optional[dict]:
        """Añade un lead individual a StaffKit"""
        url = f"{STAFFKIT_URL}/api/bots.php"
        headers = {
            'Authorization': f'Bearer {self.api_token}',
            'Content-Type': 'application/json'
        }
        
        payload = {
            'action': 'add_lead_geographic',
            **lead_data
        }
        
        try:
            response = requests.post(url, headers=headers, json=payload, timeout=30)
            self.debug(f"API response {response.status_code}: {response.text[:200]}")
            if response.status_code == 200:
                result = response.json()
                if not result.get('success', True):
                    self.debug(f"API error: {result.get('error', 'unknown')}")
                return result
            else:
                self.log(f"API HTTP error {response.status_code}: {response.text[:100]}", 'ERROR')
        except Exception as e:
            self.debug(f"Error adding lead: {e}")
            
        return None
        
    def process_search(self, search: dict) -> bool:
        """Procesa una búsqueda individual"""
        search_id = search['id']
        keyword = search['keyword']
        location = search['location']
        country_code = search.get('country', 'AR')
        latitude = search.get('latitude')
        longitude = search.get('longitude')
        max_pages = int(search.get('max_pages', 3))
        
        self.log(f"Procesando: '{keyword}' en {location}")
        
        # Buscar en DataForSEO
        leads = self.search_dataforseo_maps(keyword, location, country_code, 
                                            latitude=latitude, longitude=longitude,
                                            max_pages=max_pages)
        
        leads_found = len(leads)
        self.log(f"Encontrados {leads_found} negocios")
        
        # Añadir a StaffKit
        leads_new = self.add_leads_to_staffkit(search, leads)
        self.log(f"Nuevos leads añadidos: {leads_new}")
        
        # Actualizar stats
        self.stats['leads_found'] += leads_found
        self.stats['leads_new'] += leads_new
        self.stats['searches_processed'] += 1
        
        # Marcar como completada
        # Estimar costo: $0.002 por búsqueda/página
        estimated_cost = max_pages * 0.002
        self.complete_search(search_id, leads_found, leads_new, estimated_cost)
        
        return True
        
    def run(self):
        """Ejecuta el bot procesando N búsquedas"""
        self.log(f"Iniciando Geographic Bot #{self.bot_id}")
        self.log(f"Búsquedas por ejecución: {self.searches_per_run}")
        
        searches_done = 0
        
        while searches_done < self.searches_per_run:
            # Obtener siguiente búsqueda
            search = self.get_next_search()
            
            if not search:
                self.log("No hay más búsquedas pendientes")
                break
                
            # Procesar
            try:
                self.process_search(search)
                searches_done += 1
            except Exception as e:
                self.log(f"Error procesando búsqueda: {e}", 'ERROR')
                self.stats['errors'].append(str(e))
                
            # Delay entre búsquedas
            if searches_done < self.searches_per_run:
                self.debug(f"Esperando {self.delay_between_searches}s...")
                time.sleep(self.delay_between_searches)
                
        # Resumen final
        self.log("=" * 50)
        self.log("RESUMEN DE EJECUCIÓN")
        self.log(f"Búsquedas procesadas: {self.stats['searches_processed']}")
        self.log(f"Leads encontrados: {self.stats['leads_found']}")
        self.log(f"Leads nuevos: {self.stats['leads_new']}")
        self.log(f"📧 Leads con email: {self.stats['leads_with_email']}")
        self.log(f"   - Emails scrapeados: {self.stats['emails_scraped']}")
        self.log(f"   - Emails Hunter.io: {self.stats['emails_hunter']}")
        self.log(f"Costo API estimado: ${self.stats['api_cost']:.4f}")
        if self.stats['errors']:
            self.log(f"Errores: {len(self.stats['errors'])}")
        self.log("=" * 50)
        
        # CRITICAL: Output STATS lines for daemon parsing (MUST be at end, clean format)
        # Daemon parses these with STATS:key:value pattern
        print(f"STATS:leads_found:{self.stats['leads_found']}")
        print(f"STATS:leads_saved:{self.stats['leads_new']}")
        print(f"STATS:leads_duplicates:{self.stats['leads_found'] - self.stats['leads_new']}")
        print(f"STATS:leads_with_email:{self.stats['leads_with_email']}")
        print(f"STATS:emails_scraped:{self.stats['emails_scraped']}")
        print(f"STATS:emails_hunter:{self.stats['emails_hunter']}")
        print(f"STATS:searches_done:{self.stats['searches_processed']}")
        
        return self.stats


def main():
    parser = argparse.ArgumentParser(description='Geographic Crawler Bot')
    parser.add_argument('--bot-id', type=int, required=True, help='ID del bot')
    parser.add_argument('--api-key', type=str, required=True, help='Token de API StaffKit')
    parser.add_argument('--dataforseo-login', type=str, 
                       default=None,
                       help='Login DataForSEO (opcional, se obtiene de Integraciones si no se especifica)')
    parser.add_argument('--dataforseo-password', type=str,
                       default=None,
                       help='Password DataForSEO (opcional, se obtiene de Integraciones si no se especifica)')
    parser.add_argument('--searches-per-run', type=int, default=5, help='Búsquedas por ejecución')
    parser.add_argument('--delay-searches', type=float, default=2.0, help='Delay entre búsquedas (segundos)')
    parser.add_argument('--delay-pages', type=float, default=1.0, help='Delay entre páginas (segundos)')
    parser.add_argument('--verbose', action='store_true', help='Modo verbose')
    
    args = parser.parse_args()
    
    bot = GeographicBot(
        bot_id=args.bot_id,
        api_token=args.api_key,
        dataforseo_login=args.dataforseo_login,
        dataforseo_password=args.dataforseo_password,
        searches_per_run=args.searches_per_run,
        delay_between_searches=args.delay_searches,
        delay_between_pages=args.delay_pages,
        verbose=args.verbose
    )
    
    stats = bot.run()
    
    # Exit code basado en errores
    sys.exit(1 if stats['errors'] else 0)


if __name__ == '__main__':
    main()
