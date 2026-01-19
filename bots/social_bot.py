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
    HTTP_TIMEOUT, MAX_LEADS_PER_RUN,
    SOCIAL_LIST_ID, TWITTER_BEARER_TOKEN,
    SOCIAL_KEYWORDS_HOSTING, SOCIAL_KEYWORDS_MIGRATION,
    SOCIAL_KEYWORDS_ECO, SOCIAL_KEYWORDS_WORDPRESS,
    SOCIAL_KEYWORDS_EXCLUDE
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
        
        # Keywords de búsqueda (desde config)
        self.keywords = {
            'hosting_problem': [kw.strip() for kw in SOCIAL_KEYWORDS_HOSTING.split(',')],
            'migration': [kw.strip() for kw in SOCIAL_KEYWORDS_MIGRATION.split(',')],
            'eco_hosting': [kw.strip() for kw in SOCIAL_KEYWORDS_ECO.split(',')],
            'wordpress_help': [kw.strip() for kw in SOCIAL_KEYWORDS_WORDPRESS.split(',')],
        }
        
        # Keywords negativas (descartar)
        self.exclude_keywords = [kw.strip() for kw in SOCIAL_KEYWORDS_EXCLUDE.split(',')]
        
        # Lista específica para Social Bot
        self.list_id = SOCIAL_LIST_ID
    
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
                from config import TWITTER_BEARER_TOKEN
                if TWITTER_BEARER_TOKEN:
                    leads = self._search_twitter(max_results=max_leads)
                    all_leads.extend(leads)
                else:
                    logger.warning("Twitter API no configurada (falta TWITTER_BEARER_TOKEN)")
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
                    'web': lead.website or lead.post_url,  # URL del post si no hay website
                    'email': lead.email or '',
                    'empresa': lead.author,
                    'contacto': lead.author,
                    'notas': (
                        f"🔗 Post URL: {lead.post_url}\n"
                        f"👤 Autor: {lead.author}\n"
                        f"📱 Fuente: {lead.source}\n"
                        f"📊 Score interés: {lead.score}/100\n"
                        f"🔑 Keywords: {', '.join(lead.keywords_found[:5])}\n\n"
                        f"💬 Contenido:\n{lead.content[:500]}..."
                    ),
                    'prioridad': 'hot' if lead.score >= 80 else 'alta',
                    'needs_email_enrichment': True,
                    # Campos extra para investigación manual
                    'source_url': lead.post_url,
                    'source_type': f'{lead.source}_post',
                    'author_url': lead.author_url,
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
    
    def _search_twitter(self, keywords: List[str]) -> List[SocialLead]:
        """Buscar en Twitter API v2"""
        if not TWITTER_BEARER_TOKEN:
            logging.warning("⚠️ Twitter API no configurado (falta TWITTER_BEARER_TOKEN)")
            return []
        
        results = []
        
        try:
            import requests
            
            # Endpoint de búsqueda reciente (últimos 7 días)
            url = "https://api.twitter.com/2/tweets/search/recent"
            headers = {
                "Authorization": f"Bearer {TWITTER_BEARER_TOKEN}",
                "Content-Type": "application/json"
            }
            
            # Construir query con keywords
            query_parts = []
            for keyword in keywords[:3]:  # Limitar a 3 keywords para no exceder límite de API
                query_parts.append(keyword)
            
            query = " OR ".join(query_parts) + " -is:retweet lang:es"
            
            params = {
                "query": query,
                "max_results": 50,
                "tweet.fields": "created_at,public_metrics,author_id",
                "expansions": "author_id",
                "user.fields": "username,name"
            }
            
            response = requests.get(url, headers=headers, params=params, timeout=30)
            
            if response.status_code == 401:
                logging.error("❌ Twitter API: Token inválido o expirado")
                return []
            elif response.status_code == 429:
                logging.warning("⚠️ Twitter API: Rate limit alcanzado")
                return []
            elif response.status_code != 200:
                logging.error(f"❌ Twitter API error {response.status_code}: {response.text}")
                return []
            
            data = response.json()
            tweets = data.get('data', [])
            users = {u['id']: u for u in data.get('includes', {}).get('users', [])}
            
            logging.info(f"🐦 Twitter: {len(tweets)} tweets encontrados")
            
            for tweet in tweets:
                text = tweet.get('text', '')
                author_id = tweet.get('author_id')
                author_data = users.get(author_id, {})
                username = author_data.get('username', 'unknown')
                
                # Calcular score básico
                score = self._calculate_social_score(text, keywords)
                
                if score < 30:  # Umbral mínimo
                    continue
                
                # Identificar keywords encontradas
                keywords_found = [kw for kw in keywords if kw.lower() in text.lower()]
                
                # Extraer website si se menciona
                website = self._extract_website(text)
                
                lead = SocialLead(
                    author=author_data.get('name', username),
                    author_url=f"https://twitter.com/{username}",
                    post_url=f"https://twitter.com/{username}/status/{tweet['id']}",
                    content=text[:500],
                    source='twitter',
                    score=min(score, 100),
                    keywords_found=keywords_found,
                    website=website,
                )
                
                results.append(lead)
            
            logging.info(f"✅ Twitter: {len(results)} leads después de filtros")
            
        except requests.exceptions.Timeout:
            logging.error("⏱️ Twitter API: Timeout")
        except requests.exceptions.RequestException as e:
            logging.error(f"❌ Twitter API error de conexión: {e}")
        except Exception as e:
            logging.error(f"❌ Error procesando Twitter: {e}", exc_info=True)
        
        return results
