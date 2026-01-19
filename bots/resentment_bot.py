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
    HTTP_TIMEOUT, MAX_LEADS_PER_RUN, LOGS_DIR,
    RESENTMENT_LIST_ID
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
        
        # Lista específica para Resentment Bot
        self.list_id = RESENTMENT_LIST_ID
    
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
                
                logger.debug(f"Trustpilot response: {response.status_code} for {url}")
                
                if response.status_code == 404:
                    logger.warning(f"Dominio no encontrado: {domain}")
                    break
                
                if response.status_code == 403:
                    logger.warning(f"Bloqueado por Trustpilot (403)")
                    break
                
                if response.status_code != 200:
                    logger.warning(f"Error HTTP {response.status_code}")
                    break
                
                page_reviews = self._parse_trustpilot_page(response.text, domain)
                
                if not page_reviews:
                    # Debug: guardar HTML para análisis
                    if page == 1:
                        debug_file = LOGS_DIR / f"trustpilot_debug_{domain.replace('.', '_')}.html"
                        with open(debug_file, 'w', encoding='utf-8') as f:
                            f.write(response.text[:50000])  # Primeros 50KB
                        logger.warning(f"No se encontraron reviews. HTML guardado en {debug_file}")
                    break
                
                reviews.extend(page_reviews)
                logger.info(f"  Página {page}: {len(page_reviews)} reviews")
                
                page += 1
                
            except Exception as e:
                logger.error(f"Error scrapeando: {e}")
                break
        
        return reviews[:max_reviews]
    
    def _parse_trustpilot_page(self, html: str, domain: str) -> List[Dict]:
        """Parsear página de Trustpilot - Selectores actualizados 2026"""
        soup = BeautifulSoup(html, 'html.parser')
        reviews = []
        
        # Múltiples selectores para adaptarse a cambios de Trustpilot
        review_cards = (
            soup.select('[data-service-review-card-paper]') or
            soup.select('article[data-review-id]') or
            soup.select('div.styles_cardWrapper__LcCPA') or
            soup.select('[class*="reviewCard"]') or
            soup.select('article.review') or
            soup.select('[class*="paper_paper"]')  # Nuevo selector 2026
        )
        
        logger.debug(f"Encontradas {len(review_cards)} review cards")
        
        for card in review_cards:
            try:
                # === RATING - múltiples formas ===
                rating = 3  # default
                
                # Método 1: data-rating
                rating_el = card.select_one('[data-rating]')
                if rating_el:
                    rating = int(rating_el.get('data-rating', 3))
                else:
                    # Método 2: imagen de estrellas
                    star_img = card.select_one('img[alt*="star"], img[alt*="Rated"]')
                    if star_img:
                        alt = star_img.get('alt', '')
                        match = re.search(r'(\d)', alt)
                        if match:
                            rating = int(match.group(1))
                    else:
                        # Método 3: contar estrellas llenas
                        filled_stars = card.select('[class*="star"][class*="full"], [data-star-rating]')
                        if filled_stars:
                            rating = len(filled_stars)
                        else:
                            # Método 4: clase del rating
                            rating_class = card.select_one('[class*="rating"], [class*="stars"]')
                            if rating_class:
                                class_str = ' '.join(rating_class.get('class', []))
                                match = re.search(r'rating[_-]?(\d)|stars?[_-]?(\d)', class_str)
                                if match:
                                    rating = int(match.group(1) or match.group(2))
                
                # Solo 1-2 estrellas (reviews negativas)
                if rating > 2:
                    continue
                
                # === AUTOR - múltiples selectores ===
                author = 'Anónimo'
                author_selectors = [
                    '[data-consumer-name]',
                    '[class*="consumerName"]',
                    '[class*="consumer-information"] span',
                    '[class*="displayname"]',
                    'span[class*="typography_heading"]',
                    '.consumer-information__name',
                ]
                for sel in author_selectors:
                    author_el = card.select_one(sel)
                    if author_el:
                        if author_el.has_attr('data-consumer-name'):
                            author = author_el.get('data-consumer-name')
                        else:
                            author = author_el.get_text(strip=True)
                        if author and author != 'Anónimo':
                            break
                
                # === TÍTULO ===
                title = ''
                title_selectors = ['h2', '[class*="title"]', '[data-review-title]', 'h3']
                for sel in title_selectors:
                    title_el = card.select_one(sel)
                    if title_el:
                        title = title_el.get_text(strip=True)
                        if title:
                            break
                
                # === CONTENIDO ===
                content = ''
                content_selectors = [
                    '[data-review-body]',
                    '[class*="reviewContent"]',
                    '[class*="review-content"]',
                    'p[class*="typography_body"]',
                    '.review-content__text',
                ]
                for sel in content_selectors:
                    content_el = card.select_one(sel)
                    if content_el:
                        content = content_el.get_text(strip=True)
                        if content:
                            break
                
                # === FECHA ===
                date = ''
                date_el = card.select_one('time')
                if date_el:
                    date = date_el.get('datetime', '')[:10]
                else:
                    # Buscar en texto
                    date_selectors = ['[class*="date"]', '[data-service-review-date-of-experience]']
                    for sel in date_selectors:
                        date_el = card.select_one(sel)
                        if date_el:
                            date = date_el.get_text(strip=True)[:10]
                            break
                
                # === URL ===
                review_url = ''
                link_el = card.select_one('a[href*="/reviews/"]')
                if link_el:
                    href = link_el.get('href', '')
                    if href.startswith('/'):
                        review_url = f"https://www.trustpilot.com{href}"
                    else:
                        review_url = href
                
                # Solo agregar si tiene contenido útil
                if title or content:
                    reviews.append({
                        'author': author,
                        'rating': rating,
                        'title': title,
                        'content': content,
                        'date': date,
                        'url': review_url,
                        'source': 'trustpilot',
                    })
                    logger.debug(f"Review parsed: {author}, {rating}*, {title[:30]}...")
                
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
