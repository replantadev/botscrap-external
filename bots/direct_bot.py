#!/usr/bin/env python3
"""
Direct Bot - Búsqueda directa de leads en Google
Versión mejorada con validación y enriquecimiento completo
"""

import re
import time
import random
import logging
import warnings
from typing import Dict, List, Optional
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup
import urllib3

# Silenciar warnings de SSL (normal en scraping)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.filterwarnings('ignore', message='Unverified HTTPS request')

from config import (
    GOOGLE_API_KEY, CX_ID, 
    SCRAPER_DELAY_MIN, SCRAPER_DELAY_MAX,
    HTTP_TIMEOUT, SOCIAL_MEDIA_DOMAINS,
    MAX_LEADS_PER_RUN,
    # Nuevos filtros
    CMS_FILTER, MIN_SPEED_SCORE, MAX_SPEED_SCORE,
    ECO_VERDE_ONLY, SKIP_PAGESPEED_API,
    # Lista específica
    DIRECT_LIST_ID
)
from .base_bot import BaseBot
from utils.lead_validator import LeadValidator
from utils.email_enricher import EmailEnricher

logger = logging.getLogger(__name__)

# Mapeo de países a TODAS sus ciudades principales
COUNTRY_CITIES = {
    'ES': [
        'Madrid', 'Barcelona', 'Valencia', 'Sevilla', 'Málaga', 'Bilbao',
        'Zaragoza', 'Alicante', 'Murcia', 'Palma de Mallorca', 'Las Palmas',
        'Valladolid', 'Córdoba', 'Vigo', 'Gijón', 'Granada', 'A Coruña',
        'Vitoria-Gasteiz', 'Santander', 'San Sebastián', 'Pamplona', 'Oviedo',
        'Logroño', 'Toledo', 'Salamanca', 'Burgos', 'León', 'Cáceres', 'Badajoz'
    ],
    'MX': [
        'Ciudad de México', 'Guadalajara', 'Monterrey', 'Puebla', 'Tijuana',
        'León', 'Juárez', 'Torreón', 'Querétaro', 'San Luis Potosí',
        'Mérida', 'Mexicali', 'Aguascalientes', 'Hermosillo', 'Saltillo',
        'Morelia', 'Culiacán', 'Chihuahua', 'Cancún', 'Acapulco', 'Toluca',
        'Veracruz', 'Oaxaca', 'Tampico', 'Durango', 'Mazatlán', 'Tuxtla Gutiérrez'
    ],
    'CO': [
        'Bogotá', 'Medellín', 'Cali', 'Barranquilla', 'Cartagena',
        'Bucaramanga', 'Pereira', 'Santa Marta', 'Cúcuta', 'Ibagué',
        'Manizales', 'Villavicencio', 'Pasto', 'Neiva', 'Armenia',
        'Montería', 'Valledupar', 'Popayán', 'Sincelejo', 'Tunja'
    ],
    'AR': [
        'Buenos Aires', 'Córdoba', 'Rosario', 'Mendoza', 'Tucumán',
        'La Plata', 'Mar del Plata', 'Salta', 'Santa Fe', 'San Juan',
        'Resistencia', 'Neuquén', 'Corrientes', 'Bahía Blanca', 'Posadas',
        'Paraná', 'San Miguel de Tucumán', 'Formosa', 'San Luis', 'La Rioja'
    ],
    'CL': [
        'Santiago', 'Valparaíso', 'Concepción', 'Viña del Mar', 'Antofagasta',
        'La Serena', 'Temuco', 'Rancagua', 'Talca', 'Arica', 'Iquique',
        'Puerto Montt', 'Chillán', 'Osorno', 'Coquimbo', 'Valdivia', 'Punta Arenas'
    ],
    'PE': [
        'Lima', 'Arequipa', 'Trujillo', 'Cusco', 'Chiclayo', 'Piura',
        'Iquitos', 'Huancayo', 'Tacna', 'Chimbote', 'Pucallpa', 'Ayacucho',
        'Juliaca', 'Cajamarca', 'Sullana', 'Ica', 'Huánuco', 'Tarapoto'
    ],
    'EC': [
        'Quito', 'Guayaquil', 'Cuenca', 'Santo Domingo', 'Ambato',
        'Machala', 'Durán', 'Manta', 'Portoviejo', 'Loja', 'Riobamba',
        'Esmeraldas', 'Ibarra', 'Quevedo', 'Milagro', 'Latacunga'
    ],
    'VE': [
        'Caracas', 'Maracaibo', 'Valencia', 'Barquisimeto', 'Maracay',
        'Ciudad Guayana', 'Barcelona', 'Maturín', 'Petare', 'Turmero',
        'Cumaná', 'Barinas', 'Ciudad Bolívar', 'Mérida', 'San Cristóbal'
    ],
    'UY': [
        'Montevideo', 'Salto', 'Paysandú', 'Las Piedras', 'Rivera',
        'Maldonado', 'Tacuarembó', 'Melo', 'Mercedes', 'Artigas', 'Punta del Este'
    ],
    'BO': [
        'Santa Cruz de la Sierra', 'La Paz', 'Cochabamba', 'Sucre', 'Oruro',
        'Tarija', 'Potosí', 'Sacaba', 'Quillacollo', 'Montero', 'Trinidad'
    ],
    'PY': [
        'Asunción', 'Ciudad del Este', 'San Lorenzo', 'Luque', 'Capiatá',
        'Lambaré', 'Fernando de la Mora', 'Encarnación', 'Pedro Juan Caballero'
    ],
    'CR': [
        'San José', 'Limón', 'Alajuela', 'Heredia', 'Puntarenas',
        'Cartago', 'Liberia', 'Paraíso', 'Desamparados', 'San Carlos'
    ],
    'PA': [
        'Ciudad de Panamá', 'San Miguelito', 'Colón', 'David', 'La Chorrera',
        'Penonomé', 'Santiago', 'Chitré', 'Aguadulce'
    ],
    'GT': [
        'Ciudad de Guatemala', 'Quetzaltenango', 'Escuintla', 'Villa Nueva',
        'Mixco', 'Cobán', 'Petapa', 'San Juan Sacatepéquez', 'Chimaltenango'
    ],
    'US': [
        'New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix',
        'Philadelphia', 'San Antonio', 'San Diego', 'Dallas', 'Austin',
        'San Jose', 'San Francisco', 'Seattle', 'Denver', 'Boston',
        'Miami', 'Atlanta', 'Las Vegas', 'Portland', 'Orlando'
    ],
    'UK': [
        'London', 'Birmingham', 'Manchester', 'Leeds', 'Glasgow',
        'Liverpool', 'Newcastle', 'Sheffield', 'Bristol', 'Edinburgh',
        'Cardiff', 'Belfast', 'Nottingham', 'Southampton', 'Leicester'
    ],
}


class DirectBot(BaseBot):
    """Bot de búsqueda directa en Google con validación completa"""
    
    def __init__(self, dry_run: bool = False, config: Dict = None):
        super().__init__(dry_run=dry_run)
        
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        # === API Cost Tracking ===
        self.api_calls = {
            'custom_search': 0,
            'maps': 0,
            'places_details': 0,
            'pagespeed': 0
        }
        self.api_pricing = {
            'custom_search': 5.00,
            'maps': 32.00,
            'places_details': 17.00,
            'pagespeed': 0.00
        }
    
    def track_api_call(self, api_name: str, count: int = 1):
        """Track an API call"""
        if api_name in self.api_calls:
            self.api_calls[api_name] += count
    
    def get_estimated_cost(self) -> float:
        """Calculate estimated cost in USD"""
        total = 0.0
        for api_name, calls in self.api_calls.items():
            price_per_1000 = self.api_pricing.get(api_name, 0)
            total += (calls / 1000) * price_per_1000
        return round(total, 4)
    
    def get_api_stats(self) -> dict:
        """Get API usage statistics"""
        return {
            'api_calls_maps': self.api_calls.get('maps', 0),
            'api_calls_places': self.api_calls.get('places_details', 0),
            'api_calls_pagespeed': self.api_calls.get('pagespeed', 0),
            'api_calls_custom_search': self.api_calls.get('custom_search', 0),
            'estimated_cost_usd': self.get_estimated_cost()
        }
        
        # Configuración de filtros (override desde parámetros o usar defaults)
        config = config or {}
        self.validator_config = {
            'cms_filter': config.get('cms_filter', CMS_FILTER),
            'min_speed_score': config.get('min_speed_score', MIN_SPEED_SCORE),
            'max_speed_score': config.get('max_speed_score', MAX_SPEED_SCORE),
            'eco_verde_only': config.get('eco_verde_only', ECO_VERDE_ONLY),
            'skip_pagespeed_api': config.get('skip_pagespeed_api', SKIP_PAGESPEED_API),
            'google_api_key': GOOGLE_API_KEY,
        }
        
        # Configuración de país(es) - soporta múltiples
        self.countries = config.get('countries', [config.get('country', 'ES')])
        if isinstance(self.countries, str):
            self.countries = [c.strip() for c in self.countries.split(',')]
        self.country = self.countries[0] if self.countries else 'ES'
        self.cities = COUNTRY_CITIES.get(self.country, COUNTRY_CITIES['ES'])
        
        # Inicializar validador y enriquecedor
        self.validator = LeadValidator(session=self.session, config=self.validator_config)
        self.email_enricher = EmailEnricher(session=self.session)
        
        # Lista específica para Direct Bot
        self.list_id = DIRECT_LIST_ID
    
    def _parse_keywords(self, query: str) -> List[str]:
        """Parsear keywords separadas por coma o líneas nuevas"""
        # Separar por coma y/o salto de línea
        keywords = []
        for line in query.replace('\r', '').split('\n'):
            for kw in line.split(','):
                kw = kw.strip()
                if kw:
                    keywords.append(kw)
        return keywords if keywords else [query]
    
    def run(self, query: str, max_leads: int = None, list_id: int = None, 
            country: str = None, countries: List[str] = None) -> Dict:
        """
        Ejecutar búsqueda directa EXHAUSTIVA
        
        Procesa CADA keyword × CADA país × TODAS las ciudades
        
        Args:
            query: Keywords de búsqueda (separadas por coma o líneas)
            max_leads: Máximo de leads totales
            list_id: ID de lista destino
            country: Código de país principal (ES, MX, CO, AR, etc.)
            countries: Lista de países a procesar
        """
        max_leads = max_leads or MAX_LEADS_PER_RUN
        if list_id:
            self.list_id = list_id
        
        # Configurar países
        if countries:
            self.countries = countries if isinstance(countries, list) else [c.strip() for c in countries.split(',')]
        elif country:
            self.countries = [country]
        
        # Parsear keywords
        keywords = self._parse_keywords(query)
        
        logger.info(f"🎯 Direct Bot - Búsqueda EXHAUSTIVA")
        logger.info(f"   Keywords: {len(keywords)} - {', '.join(keywords[:5])}{'...' if len(keywords) > 5 else ''}")
        logger.info(f"   Países: {len(self.countries)} - {', '.join(self.countries)}")
        logger.info(f"   Límite total: {max_leads} leads")
        
        total_leads = 0
        all_urls = []
        
        # Iterar por cada keyword
        for kw_idx, keyword in enumerate(keywords, 1):
            if total_leads >= max_leads:
                logger.info(f"🎯 Límite alcanzado ({max_leads}), deteniendo")
                break
            
            logger.info(f"\n📌 Keyword {kw_idx}/{len(keywords)}: '{keyword}'")
            
            # Iterar por cada país
            for country_code in self.countries:
                if total_leads >= max_leads:
                    break
                
                cities = COUNTRY_CITIES.get(country_code, COUNTRY_CITIES.get('ES', ['']))
                logger.info(f"  🌍 País: {country_code} ({len(cities)} ciudades)")
                
                # Buscar en TODAS las ciudades del país
                for city_idx, city in enumerate(cities, 1):
                    if total_leads >= max_leads:
                        break
                    
                    # Query: keyword + ciudad
                    search_query = f"{keyword} {city}" if city else keyword
                    logger.debug(f"    📍 [{city_idx}/{len(cities)}] {city}")
                    
                    # Buscar con paginación completa
                    city_urls = self._search_google_exhaustive(search_query, max_results=100)
                    
                    if city_urls:
                        new_urls = [u for u in city_urls if u not in all_urls]
                        all_urls.extend(new_urls)
                        logger.debug(f"      → {len(new_urls)} URLs nuevas")
                    
                    # Pequeña pausa entre ciudades
                    time.sleep(random.uniform(0.3, 0.8))
                
                # Búsqueda general con código de país
                general_query = f"{keyword} {country_code}"
                general_urls = self._search_google_exhaustive(general_query, max_results=50)
                new_general = [u for u in general_urls if u not in all_urls]
                all_urls.extend(new_general)
                
            # Procesar URLs acumuladas para esta keyword
            if all_urls:
                leads_this_batch = self._process_urls(
                    all_urls, 
                    max_leads - total_leads,
                    keyword
                )
                total_leads += leads_this_batch
                all_urls = []  # Reset para siguiente keyword
                logger.info(f"  ✅ Keyword '{keyword}': {leads_this_batch} leads guardados")
        
        logger.info(f"\n🏁 Búsqueda EXHAUSTIVA completada: {total_leads} leads totales")
        
        # Log API cost summary
        cost = self.get_estimated_cost()
        if cost > 0 or self.api_calls['custom_search'] > 0:
            logger.info(f"💰 API Usage: CustomSearch={self.api_calls['custom_search']} calls, Cost=${cost:.4f}")
        
        # Merge API stats into result
        stats = self.get_stats()
        stats['api_stats'] = self.get_api_stats()
        return stats
    
    def _search_google_exhaustive(self, query: str, max_results: int = 100) -> List[str]:
        """Buscar en Google agotando toda la paginación disponible"""
        
        if not GOOGLE_API_KEY or not CX_ID:
            logger.error("Google API Key o CX ID no configurados")
            return []
        
        urls = []
        start_index = 1
        
        # Google Custom Search permite hasta 100 resultados (10 páginas de 10)
        while len(urls) < max_results and start_index <= 91:
            try:
                self.track_api_call('custom_search')  # Track API call
                response = requests.get(
                    'https://www.googleapis.com/customsearch/v1',
                    params={
                        'key': GOOGLE_API_KEY,
                        'cx': CX_ID,
                        'q': query,
                        'start': start_index,
                        'num': 10
                    },
                    timeout=HTTP_TIMEOUT
                )
                
                if response.status_code == 200:
                    data = response.json()
                    items = data.get('items', [])
                    
                    if not items:
                        break  # No más resultados
                    
                    for item in items:
                        url = item.get('link')
                        if url and url not in urls:
                            urls.append(url)
                    
                    # Si hay menos de 10 resultados, no hay más páginas
                    if len(items) < 10:
                        break
                        
                elif response.status_code == 429:
                    logger.warning("Rate limit de Google API, esperando...")
                    time.sleep(5)
                    continue
                else:
                    logger.warning(f"Google API error: {response.status_code}")
                    break
                
                start_index += 10
                time.sleep(0.3)  # Pausa entre páginas
                
            except Exception as e:
                logger.error(f"Error en búsqueda: {e}")
                break
        
        return urls
    
    def _process_urls(self, urls: List[str], max_leads: int, keyword: str = '') -> int:
        """Procesar lista de URLs y guardar leads válidos"""
        
        # Filtrar dominios de redes sociales
        urls = self._filter_urls(urls)
        
        # Deduplificar
        urls = list(dict.fromkeys(urls))
        
        if not urls:
            return 0
        
        logger.info(f"📊 {len(urls)} URLs únicas para procesar")
        
        # Verificar duplicados en batch con StaffKit
        domains = [self._extract_domain(u) for u in urls]
        duplicates = self.check_duplicates_batch(domains)
        
        urls_to_process = [
            url for url in urls 
            if not duplicates.get(self._extract_domain(url), False)
        ]
        
        logger.info(f"📊 {len(urls_to_process)} URLs nuevas (no duplicadas)")
        
        # Procesar cada URL
        leads_saved = 0
        urls_attempted = 0
        
        for url in urls_to_process[:max_leads * 3]:  # Intentar más URLs para conseguir max_leads
            if leads_saved >= max_leads:
                break
            
            urls_attempted += 1
            
            try:
                logger.info(f"🔍 [{urls_attempted}] Analizando: {url[:60]}...")
                lead = self._analyze_url(url)
                
                if lead:
                    # Añadir keyword si está disponible
                    if keyword:
                        lead['notas'] = f"Keyword: {keyword} | " + lead.get('notas', '')
                    
                    result = self.save_lead(lead)
                    
                    if result.get('success'):
                        if result.get('status') != 'duplicate':
                            leads_saved += 1
                            logger.info(f"✅ Lead #{leads_saved}: {lead.get('web')}")
                        else:
                            logger.info(f"⏭️ Duplicado: {lead.get('web')}")
                else:
                    logger.debug(f"⊘ No válido: {url[:50]}")
                
                # Delay entre requests
                time.sleep(random.uniform(SCRAPER_DELAY_MIN, SCRAPER_DELAY_MAX))
                
            except Exception as e:
                logger.error(f"Error procesando {url}: {e}")
        
        return leads_saved
    
    # Método legacy para compatibilidad
    def _search_google(self, query: str, num_results: int = 30) -> List[str]:
        """Buscar en Google Custom Search API (alias de _search_google_exhaustive)"""
        return self._search_google_exhaustive(query, max_results=num_results)
    
    def _filter_urls(self, urls: List[str]) -> List[str]:
        """Filtrar URLs de redes sociales y plataformas"""
        filtered = []
        
        for url in urls:
            domain = self._extract_domain(url)
            
            # Excluir redes sociales
            if any(social in domain for social in SOCIAL_MEDIA_DOMAINS):
                continue
            
            # Excluir subdominios de plataformas gratuitas
            if domain.endswith('.wordpress.com') or domain.endswith('.blogspot.com'):
                continue
            
            filtered.append(url)
        
        return filtered
    
    def _extract_domain(self, url: str) -> str:
        """Extraer dominio de URL"""
        try:
            parsed = urlparse(url)
            domain = parsed.netloc.lower()
            # Quitar www.
            if domain.startswith('www.'):
                domain = domain[4:]
            return domain
        except:
            return url
    
    def _analyze_url(self, url: str) -> Optional[Dict]:
        """
        Analizar una URL y extraer información del lead con validación completa
        """
        try:
            response = self.session.get(url, timeout=HTTP_TIMEOUT, verify=False)
            
            if response.status_code != 200:
                logger.debug(f"HTTP {response.status_code}: {url[:50]}")
                return None
            
            html_content = response.text
            soup = BeautifulSoup(html_content, 'html.parser')
            domain = self._extract_domain(url)
            
            # Crear lead básico
            basic_lead = {
                'web': domain,
                'website': url,
                'empresa': self._extract_company_name(soup, domain),
                'notas': 'Encontrado via Direct Bot - Query search',
            }
            
            # Validar y enriquecer con el validador completo
            logger.debug(f"Validando {domain}...")
            enriched_lead = self.validator.validate_and_enrich(basic_lead, html_content)
            
            if not enriched_lead:
                logger.info(f"  ⊘ No pasó validación: {domain}")
                return None
            
            logger.info(f"  ✓ Validado: {domain}")
            
            # Enriquecer emails
            email_result = self.email_enricher.enrich_emails(url, enriched_lead.get('empresa', ''))
            enriched_lead['email'] = email_result.get('email_principal', '')
            enriched_lead['emails_adicionales'] = '|'.join(email_result.get('emails_adicionales', []))
            enriched_lead['email_tipo'] = email_result.get('email_tipo', 'unknown')
            enriched_lead['email_confianza'] = email_result.get('confianza', 0)
            
            # Buscar teléfono
            enriched_lead['telefono'] = self._extract_phone(html_content)
            
            # Buscar LinkedIn
            linkedin = self.validator.find_linkedin(html_content)
            if linkedin:
                enriched_lead['linkedin'] = linkedin
            
            # Marcar que necesita enriquecimiento adicional si no hay email de calidad
            enriched_lead['needs_email_enrichment'] = email_result.get('confianza', 0) < 50
            
            return enriched_lead
            
        except Exception as e:
            logger.debug(f"Error analizando {url}: {e}")
            return None
    
    def _detect_wordpress(self, html: str, soup: BeautifulSoup) -> bool:
        """Detectar si es WordPress (método legacy, usa validador para filtrado)"""
        cms = self.validator.quick_cms_check('', html)
        return cms == 'wordpress'
    
    def _extract_company_name(self, soup: BeautifulSoup, domain: str) -> str:
        """Extraer nombre de empresa"""
        # Intentar desde title
        title = soup.find('title')
        if title:
            name = title.get_text().split('|')[0].split('-')[0].strip()
            if name and len(name) < 100:
                return name
        
        # Fallback: capitalizar dominio
        return domain.split('.')[0].title()
    
    def _extract_phone(self, html: str) -> str:
        """Extraer teléfono del HTML"""
        patterns = [
            r'\+?[\d\s\-\(\)]{9,15}',
            r'(?:tel|phone|móvil|celular)[:\s]*([+\d\s\-\(\)]{9,15})',
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, html, re.IGNORECASE)
            if matches:
                phone = matches[0] if isinstance(matches[0], str) else matches[0]
                phone = re.sub(r'[^\d+]', '', phone)
                if len(phone) >= 9:
                    return phone
        
        return ''
