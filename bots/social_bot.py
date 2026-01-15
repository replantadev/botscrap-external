#!/usr/bin/env python3
"""
Social Bot - Monitoreo de redes sociales para detectar intención de compra
"""

import re
import time
import random
import logging
from typing import Dict, List, Optional
from dataclasses import dataclass

import requests

from config import (
    SCRAPER_DELAY_MIN, SCRAPER_DELAY_MAX,
    HTTP_TIMEOUT, MAX_LEADS_PER_RUN
)
from .base_bot import BaseBot

logger = logging.getLogger(__name__)


@dataclass
class SocialLead:
    """Lead detectado en redes sociales"""
    author: str
    author_url: str
    post_url: str
    content: str
    source: str
    score: int
    keywords_found: List[str]
    website: Optional[str] = None
    email: Optional[str] = None


class SocialBot(BaseBot):
    """Bot para monitorear redes sociales buscando intención de compra"""
    
    def __init__(self, dry_run: bool = False):
        super().__init__(dry_run=dry_run)
        
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (compatible; ReplantaBot/1.0)'
        })
        
        # Keywords de búsqueda
        self.keywords = {
            'hosting_problem': [
                'hosting lento', 'mi hosting', 'problemas hosting',
                'cambiar hosting', 'hosting malo', 'hosting caro',
            ],
            'migration': [
                'migrar wordpress', 'cambiar hosting', 'mover web',
                'nuevo hosting', 'hosting alternativa',
            ],
            'eco_hosting': [
                'hosting ecológico', 'hosting verde', 'hosting sostenible',
                'green hosting', 'carbono neutral',
            ],
            'wordpress_help': [
                'ayuda wordpress', 'problema wordpress', 'wordpress lento',
                'optimizar wordpress',
            ],
        }
        
        # Keywords negativas (descartar)
        self.exclude_keywords = [
            'vps', 'servidor dedicado', 'kubernetes', 'docker',
            'aws', 'azure', 'google cloud', 'devops',
        ]
    
    def run(self, sources: List[str] = None, max_leads: int = None, list_id: int = None) -> Dict:
        """
        Buscar leads en redes sociales
        
        Args:
            sources: Lista de fuentes ['reddit', 'twitter']
            max_leads: Máximo de leads
            list_id: ID de lista destino
        """
        sources = sources or ['reddit']
        max_leads = max_leads or MAX_LEADS_PER_RUN
        if list_id:
            self.list_id = list_id
        
        logger.info(f"📡 Social Signals Bot - Fuentes: {sources}")
        
        all_leads = []
        
        for source in sources:
            if source == 'reddit':
                leads = self._search_reddit(max_results=max_leads)
                all_leads.extend(leads)
            elif source == 'twitter':
                logger.warning("Twitter scraping requiere API de pago")
            else:
                logger.warning(f"Fuente no soportada: {source}")
            
            time.sleep(2)
        
        logger.info(f"📊 Encontrados {len(all_leads)} posibles leads")
        
        # Filtrar y guardar
        leads_saved = 0
        
        for lead in all_leads:
            if leads_saved >= max_leads:
                break
            
            if lead.score >= 50:
                lead_dict = {
                    'web': lead.website or '',
                    'email': lead.email or '',
                    'contacto': lead.author,
                    'notas': f"Social lead de {lead.source}. Score: {lead.score}. "
                             f"Keywords: {', '.join(lead.keywords_found[:3])}. "
                             f"URL: {lead.post_url}",
                    'prioridad': 'hot' if lead.score >= 80 else 'alta',
                    'needs_email_enrichment': True,
                }
                
                result = self.save_lead(lead_dict)
                
                if result.get('success') and result.get('status') != 'duplicate':
                    leads_saved += 1
                    logger.info(f"✅ Social lead #{leads_saved}: Score {lead.score}")
        
        return self.get_stats()
    
    def _search_reddit(self, max_results: int = 20) -> List[SocialLead]:
        """Buscar en Reddit (via JSON endpoint público)"""
        leads = []
        
        # Subreddits relevantes
        subreddits = [
            'wordpress', 'webhosting', 'webdev',
            'smallbusiness', 'entrepreneur',
        ]
        
        for subreddit in subreddits:
            if len(leads) >= max_results:
                break
            
            try:
                # Buscar posts recientes
                url = f"https://www.reddit.com/r/{subreddit}/search.json"
                
                for keyword_group, keywords in self.keywords.items():
                    if len(leads) >= max_results:
                        break
                    
                    query = ' OR '.join(keywords[:3])
                    
                    response = self.session.get(
                        url,
                        params={
                            'q': query,
                            'sort': 'new',
                            'limit': 10,
                            't': 'month',
                            'restrict_sr': 'on',
                        },
                        headers={'User-Agent': 'BotScrapExternal/1.0'},
                        timeout=HTTP_TIMEOUT
                    )
                    
                    if response.status_code == 200:
                        data = response.json()
                        posts = data.get('data', {}).get('children', [])
                        
                        for post in posts:
                            post_data = post.get('data', {})
                            
                            lead = self._analyze_reddit_post(post_data)
                            if lead:
                                leads.append(lead)
                    
                    time.sleep(random.uniform(1, 3))
                
            except Exception as e:
                logger.error(f"Error buscando en r/{subreddit}: {e}")
            
            time.sleep(random.uniform(2, 4))
        
        return leads
    
    def _analyze_reddit_post(self, post: Dict) -> Optional[SocialLead]:
        """Analizar un post de Reddit"""
        title = post.get('title', '')
        selftext = post.get('selftext', '')
        text = f"{title} {selftext}".lower()
        
        # Verificar que no tenga keywords de exclusión
        if any(kw in text for kw in self.exclude_keywords):
            return None
        
        # Buscar keywords positivas
        keywords_found = []
        score = 0
        
        for group, keywords in self.keywords.items():
            for kw in keywords:
                if kw in text:
                    keywords_found.append(kw)
                    
                    if group == 'migration':
                        score += 25
                    elif group == 'hosting_problem':
                        score += 20
                    elif group == 'eco_hosting':
                        score += 15
                    else:
                        score += 10
        
        if not keywords_found or score < 30:
            return None
        
        # Extraer website si se menciona
        website = self._extract_website(text)
        
        return SocialLead(
            author=post.get('author', 'unknown'),
            author_url=f"https://reddit.com/u/{post.get('author', '')}",
            post_url=f"https://reddit.com{post.get('permalink', '')}",
            content=text[:500],
            source='reddit',
            score=min(score, 100),
            keywords_found=keywords_found,
            website=website,
        )
    
    def _extract_website(self, text: str) -> Optional[str]:
        """Extraer website del texto"""
        patterns = [
            r'https?://([a-z0-9][a-z0-9\-\.]*\.[a-z]{2,})',
            r'www\.([a-z0-9][a-z0-9\-]*\.[a-z]{2,})',
            r'(?:mi\s+)?(?:sitio|web|página)[:\s]+([a-z0-9][a-z0-9\-]*\.[a-z]{2,})',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                domain = match.group(1)
                # Excluir dominios comunes
                if domain not in ['reddit.com', 'imgur.com', 'youtube.com', 'google.com']:
                    return domain
        
        return None
