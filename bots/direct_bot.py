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

# Mapeo de países a ciudades
COUNTRY_CITIES = {
    'ES': ['Madrid', 'Barcelona', 'Valencia', 'Sevilla', 'Málaga', 'Bilbao'],
    'MX': ['Ciudad de México', 'Guadalajara', 'Monterrey', 'Puebla', 'Tijuana'],
    'CO': ['Bogotá', 'Medellín', 'Cali', 'Barranquilla', 'Cartagena'],
    'AR': ['Buenos Aires', 'Córdoba', 'Rosario', 'Mendoza'],
    'CL': ['Santiago', 'Valparaíso', 'Concepción'],
    'PE': ['Lima', 'Arequipa', 'Trujillo', 'Cusco'],
    'US': ['New York', 'Los Angeles', 'Miami', 'Houston', 'Chicago'],
    'UK': ['London', 'Manchester', 'Birmingham', 'Leeds'],
}


class DirectBot(BaseBot):
    """Bot de búsqueda directa en Google con validación completa"""
    
    def __init__(self, dry_run: bool = False, config: Dict = None):
        super().__init__(dry_run=dry_run)
        
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
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
        
        # Configuración de país
        self.country = config.get('country', 'ES')
        self.cities = COUNTRY_CITIES.get(self.country, COUNTRY_CITIES['ES'])
        
        # Inicializar validador y enriquecedor
        self.validator = LeadValidator(session=self.session, config=self.validator_config)
        self.email_enricher = EmailEnricher(session=self.session)
        
        # Lista específica para Direct Bot
        self.list_id = DIRECT_LIST_ID
    
    def run(self, query: str, max_leads: int = None, list_id: int = None, country: str = None) -> Dict:
        """
        Ejecutar búsqueda directa
        
        Args:
            query: Query de búsqueda (ej: "restaurante italiano")
            max_leads: Máximo de leads
            list_id: ID de lista destino
            country: Código de país (ES, MX, CO, AR, etc.)
        """
        max_leads = max_leads or MAX_LEADS_PER_RUN
        if list_id:
            self.list_id = list_id
        if country:
            self.country = country
            self.cities = COUNTRY_CITIES.get(country, COUNTRY_CITIES['ES'])
        
        logger.info(f"🎯 Direct Bot - Query: {query}, País: {self.country}")
        
        # 1. Buscar en Google con ciudades del país
        all_urls = []
        urls_per_city = max(5, (max_leads * 3) // len(self.cities[:3]))
        
        for city in self.cities[:3]:  # Top 3 ciudades del país
            city_query = f"{query} {city}"
            city_urls = self._search_google(city_query, num_results=urls_per_city)
            all_urls.extend(city_urls)
            logger.debug(f"  📍 {city}: {len(city_urls)} URLs")
            time.sleep(random.uniform(0.5, 1.5))
        
        # También búsqueda general con país
        general_query = f"{query} {self.country}"
        general_urls = self._search_google(general_query, num_results=max_leads)
        all_urls.extend(general_urls)
        
        # Deduplificar
        urls = list(dict.fromkeys(all_urls))
        
        if not urls:
            logger.warning("No se encontraron URLs")
            return self.get_stats()
        
        logger.info(f"📊 Encontradas {len(urls)} URLs")
        
        # 2. Filtrar dominios de redes sociales
        urls = self._filter_urls(urls)
        logger.info(f"📊 {len(urls)} URLs después de filtrar")
        
        # 3. Verificar duplicados en batch
        domains = [self._extract_domain(u) for u in urls]
        duplicates = self.check_duplicates_batch(domains)
        
        urls_to_process = [
            url for url in urls 
            if not duplicates.get(self._extract_domain(url), False)
        ]
        
        logger.info(f"📊 {len(urls_to_process)} URLs nuevas para procesar")
        
        # 4. Procesar cada URL
        leads_processed = 0
        
        for url in urls_to_process[:max_leads]:
            if leads_processed >= max_leads:
                break
            
            try:
                lead = self._analyze_url(url)
                
                if lead:
                    result = self.save_lead(lead)
                    
                    if result.get('success') and result.get('status') != 'duplicate':
                        leads_processed += 1
                        logger.info(f"✅ Lead #{leads_processed}: {lead.get('web')}")
                
                # Delay entre requests
                time.sleep(random.uniform(SCRAPER_DELAY_MIN, SCRAPER_DELAY_MAX))
                
            except Exception as e:
                logger.error(f"Error procesando {url}: {e}")
        
        return self.get_stats()
    
    def _search_google(self, query: str, num_results: int = 30) -> List[str]:
        """Buscar en Google Custom Search API"""
        
        if not GOOGLE_API_KEY or not CX_ID:
            logger.error("Google API Key o CX ID no configurados")
            return []
        
        urls = []
        start_index = 1
        
        while len(urls) < num_results and start_index <= 91:
            try:
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
                    
                    for item in items:
                        url = item.get('link')
                        if url:
                            urls.append(url)
                    
                    if not items:
                        break
                else:
                    logger.warning(f"Google API error: {response.status_code}")
                    break
                
                start_index += 10
                time.sleep(0.5)
                
            except Exception as e:
                logger.error(f"Error en búsqueda: {e}")
                break
        
        return urls
    
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
            enriched_lead = self.validator.validate_and_enrich(basic_lead, html_content)
            
            if not enriched_lead:
                logger.debug(f"Lead no pasó validación: {domain}")
                return None
            
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
