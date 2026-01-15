#!/usr/bin/env python3
"""
Resentment Bot - Busca clientes frustrados en reviews
"""

import re
import time
import random
import logging
from typing import Dict, List, Optional
from dataclasses import dataclass, asdict

import requests
from bs4 import BeautifulSoup

from config import (
    COMPETITOR_HOSTINGS,
    RESENTMENT_KEYWORDS_ES, RESENTMENT_KEYWORDS_EN,
    SCRAPER_DELAY_MIN, SCRAPER_DELAY_MAX,
    HTTP_TIMEOUT, MAX_LEADS_PER_RUN
)
from .base_bot import BaseBot

logger = logging.getLogger(__name__)

# User agents para rotar
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
]


@dataclass
class ResentmentLead:
    """Lead extraído de una review negativa"""
    reviewer_name: str
    review_url: str
    review_date: str
    source: str
    rating: int
    title: str
    content: str
    hosting_mentioned: str
    resentment_score: int
    resentment_keywords_found: List[str]
    migration_intent: bool
    website_mentioned: Optional[str] = None
    email: Optional[str] = None
    needs_enrichment: bool = True


class ResentmentBot(BaseBot):
    """Bot para buscar clientes frustrados en reviews"""
    
    def __init__(self, dry_run: bool = False):
        super().__init__(dry_run=dry_run)
        
        self.session = requests.Session()
        
        # Keywords de migración (alta intención)
        self.migration_keywords = [
            'me voy', 'cambiar hosting', 'migrar', 'busco alternativa',
            'recomienden', 'recomiendan', 'alternativas',
            'leaving', 'switching', 'moving', 'alternative',
        ]
    
    def run(self, hosting: str, max_leads: int = None, list_id: int = None) -> Dict:
        """
        Buscar leads frustrados de un hosting específico
        """
        max_leads = max_leads or MAX_LEADS_PER_RUN
        if list_id:
            self.list_id = list_id
        
        if hosting not in COMPETITOR_HOSTINGS:
            logger.error(f"Hosting '{hosting}' no conocido")
            return self.get_stats()
        
        hosting_info = COMPETITOR_HOSTINGS[hosting]
        logger.info(f"😤 Resentment Hunter - Buscando reviews de {hosting_info['name']}")
        
        # Scrape Trustpilot
        reviews = self._scrape_trustpilot(
            domain=hosting_info['trustpilot'],
            max_reviews=max_leads * 2
        )
        
        logger.info(f"📊 Encontradas {len(reviews)} reviews negativas")
        
        # Analizar y filtrar
        leads_saved = 0
        
        for review in reviews:
            if leads_saved >= max_leads:
                break
            
            lead = self._analyze_review(review, hosting_info['name'])
            
            if lead and lead.resentment_score >= 50:
                # Convertir a dict para guardar
                lead_dict = {
                    'web': lead.website_mentioned or '',
                    'email': lead.email or '',
                    'empresa': '',
                    'contacto': lead.reviewer_name,
                    'notas': f"Review {lead.rating}★ en {lead.source} sobre {lead.hosting_mentioned}. "
                             f"Score: {lead.resentment_score}. Keywords: {', '.join(lead.resentment_keywords_found[:3])}. "
                             f"Intención migración: {'Sí' if lead.migration_intent else 'No'}",
                    'prioridad': 'hot' if lead.migration_intent else 'alta',
                    'needs_email_enrichment': lead.needs_enrichment,
                }
                
                result = self.save_lead(lead_dict)
                
                if result.get('success') and result.get('status') != 'duplicate':
                    leads_saved += 1
                    logger.info(f"✅ Lead #{leads_saved}: Score {lead.resentment_score}, Intent: {lead.migration_intent}")
        
        return self.get_stats()
    
    def run_all(self, max_leads: int = None, list_id: int = None) -> Dict:
        """Buscar en todos los hostings conocidos"""
        max_leads = max_leads or MAX_LEADS_PER_RUN
        total_leads = 0
        leads_per_hosting = max(2, max_leads // len(COMPETITOR_HOSTINGS))
        
        for hosting in COMPETITOR_HOSTINGS:
            if total_leads >= max_leads:
                break
            
            logger.info(f"\n--- Procesando {hosting} ---")
            
            result = self.run(hosting=hosting, max_leads=leads_per_hosting, list_id=list_id)
            total_leads += result.get('leads_saved', 0)
            
            # Delay entre hostings
            time.sleep(random.uniform(5, 10))
        
        return self.get_stats()
    
    def _get_headers(self) -> Dict:
        """Headers con User-Agent rotado"""
        return {
            'User-Agent': random.choice(USER_AGENTS),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8',
            'Referer': 'https://www.trustpilot.com/',
        }
    
    def _scrape_trustpilot(self, domain: str, max_reviews: int = 40) -> List[Dict]:
        """Scrape reviews negativas de Trustpilot"""
        reviews = []
        page = 1
        max_pages = 5
        
        while len(reviews) < max_reviews and page <= max_pages:
            # URL con filtro de 1-2 estrellas
            url = f"https://www.trustpilot.com/review/{domain}?page={page}&stars=1&stars=2&sort=recency"
            
            try:
                if page > 1:
                    time.sleep(random.uniform(SCRAPER_DELAY_MIN, SCRAPER_DELAY_MAX))
                
                response = self.session.get(url, headers=self._get_headers(), timeout=HTTP_TIMEOUT)
                
                if response.status_code == 404:
                    logger.warning(f"Dominio no encontrado: {domain}")
                    break
                
                if response.status_code == 403:
                    logger.warning(f"Bloqueado por Trustpilot")
                    break
                
                page_reviews = self._parse_trustpilot_page(response.text, domain)
                
                if not page_reviews:
                    break
                
                reviews.extend(page_reviews)
                logger.info(f"  Página {page}: {len(page_reviews)} reviews")
                
                page += 1
                
            except Exception as e:
                logger.error(f"Error scrapeando: {e}")
                break
        
        return reviews[:max_reviews]
    
    def _parse_trustpilot_page(self, html: str, domain: str) -> List[Dict]:
        """Parsear página de Trustpilot"""
        soup = BeautifulSoup(html, 'html.parser')
        reviews = []
        
        # Buscar cards de review
        review_cards = soup.select('[data-service-review-card-paper]')
        
        if not review_cards:
            review_cards = soup.select('article[data-review-id]')
        
        for card in review_cards:
            try:
                # Rating
                rating_el = card.select_one('[data-rating]')
                rating = int(rating_el.get('data-rating', 3)) if rating_el else 3
                
                # Solo 1-2 estrellas
                if rating > 2:
                    continue
                
                # Autor
                author_el = card.select_one('[data-consumer-name]')
                author = author_el.get('data-consumer-name', 'Anónimo') if author_el else 'Anónimo'
                
                # Título
                title_el = card.select_one('h2')
                title = title_el.get_text(strip=True) if title_el else ''
                
                # Contenido
                content_el = card.select_one('[data-review-body]')
                content = content_el.get_text(strip=True) if content_el else ''
                
                # Fecha
                date_el = card.select_one('time')
                date = date_el.get('datetime', '')[:10] if date_el else ''
                
                # URL
                link_el = card.select_one('a[href*="/reviews/"]')
                review_url = f"https://www.trustpilot.com{link_el['href']}" if link_el else ''
                
                reviews.append({
                    'author': author,
                    'rating': rating,
                    'title': title,
                    'content': content,
                    'date': date,
                    'url': review_url,
                    'source': 'trustpilot',
                })
                
            except Exception as e:
                logger.debug(f"Error parseando review: {e}")
                continue
        
        return reviews
    
    def _analyze_review(self, review: Dict, hosting_name: str) -> Optional[ResentmentLead]:
        """Analizar una review y calcular score de resentimiento"""
        
        text = f"{review.get('title', '')} {review.get('content', '')}".lower()
        
        # Buscar keywords de resentimiento
        keywords_found = []
        
        for kw in RESENTMENT_KEYWORDS_ES + RESENTMENT_KEYWORDS_EN:
            if kw.lower() in text:
                keywords_found.append(kw)
        
        if not keywords_found:
            return None
        
        # Calcular score
        score = 0
        
        # Base por rating (1 estrella = 30pts, 2 estrellas = 15pts)
        rating = review.get('rating', 3)
        if rating == 1:
            score += 30
        elif rating == 2:
            score += 15
        
        # Puntos por keywords (5 pts cada una, max 40)
        score += min(len(keywords_found) * 5, 40)
        
        # Detectar intención de migración
        migration_intent = any(kw in text for kw in self.migration_keywords)
        if migration_intent:
            score += 30
        
        # Extraer website mencionado
        website = self._extract_website_from_text(text)
        
        # Extraer email
        email = self._extract_email_from_text(text)
        
        return ResentmentLead(
            reviewer_name=review.get('author', 'Anónimo'),
            review_url=review.get('url', ''),
            review_date=review.get('date', ''),
            source=review.get('source', 'trustpilot'),
            rating=rating,
            title=review.get('title', ''),
            content=review.get('content', ''),
            hosting_mentioned=hosting_name,
            resentment_score=min(score, 100),
            resentment_keywords_found=keywords_found,
            migration_intent=migration_intent,
            website_mentioned=website,
            email=email,
            needs_enrichment=not bool(email),
        )
    
    def _extract_website_from_text(self, text: str) -> Optional[str]:
        """Extraer URL/dominio del texto"""
        patterns = [
            r'(?:mi\s+)?(?:web|sitio|página)[:\s]+([a-z0-9][a-z0-9\-]*\.[a-z]{2,})',
            r'https?://([a-z0-9][a-z0-9\-\.]*\.[a-z]{2,})',
            r'www\.([a-z0-9][a-z0-9\-]*\.[a-z]{2,})',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return match.group(1)
        
        return None
    
    def _extract_email_from_text(self, text: str) -> Optional[str]:
        """Extraer email del texto"""
        pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
        match = re.search(pattern, text)
        return match.group(0) if match else None
