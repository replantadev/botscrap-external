#!/usr/bin/env python3
"""
Direct Bot - Búsqueda directa de leads en Google
"""

import re
import time
import random
import logging
from typing import Dict, List, Optional
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup

from config import (
    GOOGLE_API_KEY, CX_ID, 
    SCRAPER_DELAY_MIN, SCRAPER_DELAY_MAX,
    HTTP_TIMEOUT, SOCIAL_MEDIA_DOMAINS,
    MAX_LEADS_PER_RUN
)
from .base_bot import BaseBot

logger = logging.getLogger(__name__)


class DirectBot(BaseBot):
    """Bot de búsqueda directa en Google"""
    
    def __init__(self, dry_run: bool = False):
        super().__init__(dry_run=dry_run)
        
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
    
    def run(self, query: str, max_leads: int = None, list_id: int = None) -> Dict:
        """
        Ejecutar búsqueda directa
        
        Args:
            query: Query de búsqueda
            max_leads: Máximo de leads
            list_id: ID de lista destino
        """
        max_leads = max_leads or MAX_LEADS_PER_RUN
        if list_id:
            self.list_id = list_id
        
        logger.info(f"🎯 Direct Bot - Query: {query}")
        
        # 1. Buscar en Google
        urls = self._search_google(query, num_results=max_leads * 3)
        
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
        Analizar una URL y extraer información del lead
        """
        try:
            response = self.session.get(url, timeout=HTTP_TIMEOUT, verify=False)
            
            if response.status_code != 200:
                return None
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Detectar WordPress
            is_wordpress = self._detect_wordpress(response.text, soup)
            
            if not is_wordpress:
                logger.debug(f"No es WordPress: {url}")
                return None
            
            # Extraer información
            domain = self._extract_domain(url)
            
            lead = {
                'web': domain,
                'website': url,
                'empresa': self._extract_company_name(soup, domain),
                'email': self._extract_email(response.text),
                'telefono': self._extract_phone(response.text),
                'wp_version': self._detect_wp_version(response.text),
                'needs_email_enrichment': True,
                'notas': f'Encontrado via Direct Bot - Query search',
            }
            
            return lead
            
        except Exception as e:
            logger.debug(f"Error analizando {url}: {e}")
            return None
    
    def _detect_wordpress(self, html: str, soup: BeautifulSoup) -> bool:
        """Detectar si es WordPress"""
        indicators = [
            'wp-content',
            'wp-includes',
            'wordpress',
            '/wp-json/',
            'wp-emoji',
        ]
        
        html_lower = html.lower()
        
        for indicator in indicators:
            if indicator in html_lower:
                return True
        
        # Check meta generator
        generator = soup.find('meta', attrs={'name': 'generator'})
        if generator and 'wordpress' in generator.get('content', '').lower():
            return True
        
        return False
    
    def _detect_wp_version(self, html: str) -> str:
        """Detectar versión de WordPress"""
        patterns = [
            r'WordPress\s*([\d.]+)',
            r'ver=([\d.]+)',
            r'wp-includes.*?\?ver=([\d.]+)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, html, re.IGNORECASE)
            if match:
                return match.group(1)
        
        return ''
    
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
    
    def _extract_email(self, html: str) -> str:
        """Extraer email del HTML"""
        # Patrón de email
        pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
        
        emails = re.findall(pattern, html)
        
        # Filtrar emails genéricos
        excluded = ['example.com', 'domain.com', 'email.com', 'yoursite', 'wordpress']
        
        for email in emails:
            email_lower = email.lower()
            if not any(ex in email_lower for ex in excluded):
                return email
        
        return ''
    
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
                # Limpiar
                phone = re.sub(r'[^\d+]', '', phone)
                if len(phone) >= 9:
                    return phone
        
        return ''
