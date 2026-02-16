#!/usr/bin/env python3
"""
B Corp Directory Scraper Bot v1.0
=================================
Scrapea el directorio oficial de B Corp (bcorporation.net) y los importa a StaffKit.

Estrategia en 3 fases:
1. DISCOVERY: Usa Google CSE para encontrar URLs de perfiles B Corp por país
   Búsqueda: site:bcorporation.net/en-us/find-a-b-corp/company "Country"
2. SCRAPING: Visita cada perfil y extrae datos estructurados (nombre, web, sector, score)
3. IMPORT: Envía los leads a StaffKit vía API

Países objetivo: España + LATAM (Argentina, México, Colombia, Chile, Perú, Brasil,
                  Uruguay, Ecuador, Costa Rica, Panamá, Guatemala, Bolivia, Paraguay,
                  Rep. Dominicana, Honduras, El Salvador, Nicaragua)

Uso:
  python bcorp_scraper.py --api-key YOUR_KEY --list-id 5
  python bcorp_scraper.py --api-key KEY --list-id 5 --countries "Spain,Argentina,Mexico"
  python bcorp_scraper.py --api-key KEY --list-id 5 --dry-run  # Solo muestra, no importa
"""

import argparse
import requests
import json
import time
import sys
import os
import re
import random
from datetime import datetime
from typing import Optional, Dict, List, Set
from urllib.parse import urlparse, quote_plus
from concurrent.futures import ThreadPoolExecutor, as_completed

# ─── Config ───────────────────────────────────────────────────────────
STAFFKIT_URL = os.getenv('STAFFKIT_URL', 'https://staff.replanta.dev')
BCORP_BASE = 'https://www.bcorporation.net'
BCORP_DIR = f'{BCORP_BASE}/en-us/find-a-b-corp/company/'

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
]

# Países LATAM + España con sus nombres en inglés (como aparecen en B Corp)
# Incluye variantes de nombres y ciudades principales para maximizar búsqueda
TARGET_COUNTRIES = {
    'Spain': 'ES',
    'Argentina': 'AR',
    'Mexico': 'MX',
    'Colombia': 'CO',
    'Chile': 'CL',
    'Peru': 'PE',
    'Brazil': 'BR',
    'Uruguay': 'UY',
    'Ecuador': 'EC',
    'Costa Rica': 'CR',
    'Panama': 'PA',
    'Guatemala': 'GT',
    'Bolivia': 'BO',
    'Paraguay': 'PY',
    'Dominican Republic': 'DO',
    'Honduras': 'HN',
    'El Salvador': 'SV',
    'Nicaragua': 'NI',
    'Puerto Rico': 'PR',
    'Venezuela': 'VE',
}

# Variantes de búsqueda por país para maximizar descubrimiento
COUNTRY_SEARCH_VARIANTS = {
    'Spain': ['Spain', 'España', 'Madrid', 'Barcelona', 'Bilbao'],
    'Argentina': ['Argentina', 'Buenos Aires', 'Córdoba', 'Mendoza'],
    'Mexico': ['Mexico', 'México', 'Ciudad de México', 'Monterrey', 'Guadalajara'],
    'Colombia': ['Colombia', 'Bogotá', 'Bogota', 'Medellín', 'Medellin'],
    'Chile': ['Chile', 'Santiago', 'Valparaíso'],
    'Peru': ['Peru', 'Perú', 'Lima'],
    'Brazil': ['Brazil', 'Brasil', 'São Paulo', 'Sao Paulo', 'Rio de Janeiro'],
    'Uruguay': ['Uruguay', 'Montevideo'],
    'Ecuador': ['Ecuador', 'Quito', 'Guayaquil'],
    'Costa Rica': ['Costa Rica', 'San José'],
    'Panama': ['Panama', 'Panamá'],
    'Guatemala': ['Guatemala'],
    'Bolivia': ['Bolivia', 'La Paz'],
    'Paraguay': ['Paraguay', 'Asunción'],
    'Dominican Republic': ['Dominican Republic', 'Santo Domingo'],
    'Honduras': ['Honduras', 'Tegucigalpa'],
    'El Salvador': ['El Salvador', 'San Salvador'],
    'Nicaragua': ['Nicaragua', 'Managua'],
    'Puerto Rico': ['Puerto Rico', 'San Juan'],
    'Venezuela': ['Venezuela', 'Caracas'],
}


class BCorpScraper:
    """Scraper del directorio B Corp para LATAM y España"""

    def __init__(self, api_key: str, list_id: int,
                 google_api_key: str = '', google_cx: str = '',
                 countries: List[str] = None,
                 max_per_country: int = 200,
                 delay: float = 1.5,
                 dry_run: bool = False,
                 verbose: bool = False):
        
        self.api_key = api_key
        self.list_id = list_id
        self.google_api_key = google_api_key
        self.google_cx = google_cx
        self.countries = countries or list(TARGET_COUNTRIES.keys())
        self.max_per_country = max_per_country
        self.delay = delay
        self.dry_run = dry_run
        self.verbose = verbose
        
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': random.choice(USER_AGENTS),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9,es;q=0.8',
        })
        
        # Tracking
        self.seen_slugs: Set[str] = set()
        self.stats = {
            'countries_processed': 0,
            'profiles_found': 0,
            'profiles_scraped': 0,
            'profiles_errors': 0,
            'leads_imported': 0,
            'leads_duplicates': 0,
            'leads_errors': 0,
        }
        
    def log(self, msg: str, level: str = 'INFO'):
        ts = datetime.now().strftime('%H:%M:%S')
        print(f"[{ts}] [{level}] {msg}")
        
    def debug(self, msg: str):
        if self.verbose:
            self.log(msg, 'DEBUG')

    # ─── FASE 1: DISCOVERY via Google CSE ─────────────────────────────

    def discover_profiles_google(self, country: str) -> List[str]:
        """
        Usa Google Custom Search para encontrar perfiles B Corp de un país.
        Busca con variantes (nombre oficial, ciudades, etc.) para maximizar cobertura.
        Google CSE devuelve máx 100 resultados por query (10 páginas × 10).
        Retorna lista de URLs de perfiles.
        """
        if not self.google_api_key or not self.google_cx:
            self.log(f"⚠️  Sin Google CSE configurado, usando fallback directo", 'WARNING')
            return self.discover_profiles_direct(country)
        
        urls = []
        variants = COUNTRY_SEARCH_VARIANTS.get(country, [country])
        
        for variant in variants:
            if len(urls) >= self.max_per_country:
                break
                
            self.debug(f"  Buscando variante: '{variant}'")
            
            # Google CSE devuelve máx 10 resultados por petición, paginable hasta 100
            for start in range(1, 91, 10):  # max 10 pages = 100 results
                if len(urls) >= self.max_per_country:
                    break
                    
                query = f'site:bcorporation.net/en-us/find-a-b-corp/company/ "{variant}"'
                params = {
                    'key': self.google_api_key,
                    'cx': self.google_cx,
                    'q': query,
                    'start': start,
                    'num': 10,
                }
                
                try:
                    resp = self.session.get('https://www.googleapis.com/customsearch/v1', params=params, timeout=15)
                    if resp.status_code == 429:
                        self.log("  ⚠️  Google CSE rate limit, esperando 5s...", 'WARNING')
                        time.sleep(5)
                        continue
                    if resp.status_code != 200:
                        self.debug(f"  Google CSE HTTP {resp.status_code} en start={start}")
                        break
                        
                    data = resp.json()
                    
                    # Check for API errors
                    if 'error' in data:
                        self.log(f"  ⚠️  Google CSE error: {data['error'].get('message', '?')}", 'WARNING')
                        break
                        
                    items = data.get('items', [])
                    
                    if not items:
                        break
                        
                    new_in_batch = 0
                    for item in items:
                        link = item.get('link', '')
                        if '/find-a-b-corp/company/' in link:
                            slug = link.rstrip('/').split('/')[-1]
                            if slug and slug not in self.seen_slugs:
                                self.seen_slugs.add(slug)
                                urls.append(link)
                                new_in_batch += 1
                                
                    total = int(data.get('searchInformation', {}).get('totalResults', '0'))
                    self.debug(f"    start={start}: {new_in_batch} nuevos ({len(items)} resultados, total: {total})")
                    
                    if len(items) < 10:
                        break  # No hay más resultados
                        
                    time.sleep(0.3)  # Respetar rate limits
                    
                except Exception as e:
                    self.log(f"Error Google CSE: {e}", 'ERROR')
                    break
            
            time.sleep(0.5)  # Pausa entre variantes
                
        self.log(f"  🔍 Google CSE para '{country}': {len(urls)} perfiles descubiertos")
        return urls

    def discover_profiles_direct(self, country: str) -> List[str]:
        """
        Fallback: Scrapea directamente el directorio B Corp.
        - Intenta extraer links y datos de __NEXT_DATA__ JSON
        - Busca links a company pages en el HTML
        - También intenta la API interna si la descubre
        """
        urls = []
        
        # Intentar obtener __NEXT_DATA__ del directorio
        try:
            resp = self.session.get(
                f'{BCORP_BASE}/en-us/find-a-b-corp/',
                timeout=20
            )
            if resp.status_code == 200:
                # Buscar __NEXT_DATA__ JSON
                match = re.search(r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>', resp.text)
                if match:
                    try:
                        next_data = json.loads(match.group(1))
                        # Extraer props con data de empresas
                        page_props = next_data.get('props', {}).get('pageProps', {})
                        companies = page_props.get('companies', page_props.get('results', page_props.get('data', [])))
                        
                        if isinstance(companies, list):
                            for c in companies:
                                slug = c.get('slug', c.get('company_slug', ''))
                                hq = c.get('hq_country', c.get('country', ''))
                                if slug and (not country or country.lower() in hq.lower()):
                                    url = f'{BCORP_DIR}{slug}/'
                                    if slug not in self.seen_slugs:
                                        self.seen_slugs.add(slug)
                                        urls.append(url)
                            self.debug(f"  __NEXT_DATA__: {len(urls)} empresas de {country}")
                    except json.JSONDecodeError:
                        pass
                
                # Buscar API interna (buildId de Next.js)
                if not urls:
                    build_match = re.search(r'"buildId"\s*:\s*"([^"]+)"', resp.text)
                    if build_match:
                        build_id = build_match.group(1)
                        self.debug(f"  Next.js buildId: {build_id}")
                        # Intentar _next/data endpoint
                        try:
                            api_url = f'{BCORP_BASE}/_next/data/{build_id}/en-us/find-a-b-corp.json'
                            api_resp = self.session.get(api_url, timeout=15)
                            if api_resp.status_code == 200:
                                api_data = api_resp.json()
                                companies = api_data.get('pageProps', {}).get('companies', [])
                                if isinstance(companies, list):
                                    for c in companies:
                                        slug = c.get('slug', c.get('company_slug', ''))
                                        hq = c.get('hq_country', c.get('country', ''))
                                        if slug and (not country or country.lower() in hq.lower()):
                                            url = f'{BCORP_DIR}{slug}/'
                                            if slug not in self.seen_slugs:
                                                self.seen_slugs.add(slug)
                                                urls.append(url)
                                    self.debug(f"  _next/data API: {len(urls)} empresas")
                        except Exception:
                            pass
                    
                # Fallback final: extraer links del HTML (todos, sin filtro de país)
                if not urls:
                    found_links = re.findall(r'href="(/en-us/find-a-b-corp/company/[^"]+)"', resp.text)
                    for link in found_links:
                        slug = link.rstrip('/').split('/')[-1]
                        if slug and slug not in self.seen_slugs:
                            self.seen_slugs.add(slug)
                            urls.append(f'{BCORP_BASE}{link}')
                            
        except Exception as e:
            self.log(f"Error scraping directo: {e}", 'ERROR')
            
        self.log(f"  🔍 Scraping directo para '{country}': {len(urls)} perfiles")
        return urls

    # ─── FASE 2: SCRAPING de perfiles ────────────────────────────────

    def scrape_profile(self, url: str) -> Optional[Dict]:
        """
        Scrapea un perfil individual de B Corp.
        Extrae: nombre, sede, industria, sector, web, score, descripción.
        
        Usa 3 estrategias en cascada:
        1. __NEXT_DATA__ JSON embebido (más fiable)
        2. Regex sobre HTML semiestructurado
        3. Regex sobre texto plano (fallback)
        """
        try:
            time.sleep(self.delay * (0.5 + random.random()))
            
            # Rotar User-Agent
            self.session.headers['User-Agent'] = random.choice(USER_AGENTS)
            
            resp = self.session.get(url, timeout=20)
            if resp.status_code != 200:
                self.debug(f"HTTP {resp.status_code} en {url}")
                return None
                
            html = resp.text
            profile = {'source_url': url, 'source': 'bcorp_directory'}
            
            # ── Estrategia 1: __NEXT_DATA__ JSON ──
            next_match = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', html)
            if next_match:
                try:
                    nd = json.loads(next_match.group(1))
                    pp = nd.get('props', {}).get('pageProps', {})
                    company_data = pp.get('company', pp.get('companyData', pp))
                    
                    if isinstance(company_data, dict) and company_data.get('company_name'):
                        profile['company'] = company_data['company_name']
                        profile['country'] = company_data.get('hq_country', company_data.get('country', ''))
                        profile['city'] = company_data.get('hq_city', company_data.get('hq_state', ''))
                        profile['headquarters'] = f"{profile.get('city', '')}, {profile.get('country', '')}".strip(', ')
                        profile['industry'] = company_data.get('industry', '')
                        profile['sector'] = company_data.get('sector', '')
                        profile['website'] = company_data.get('website', '')
                        profile['description'] = (company_data.get('description', '') or '')[:500]
                        profile['certified_since'] = company_data.get('current_status', {}).get('certified_since', '')
                        
                        score = company_data.get('overall_score', company_data.get('b_impact_score', 0))
                        if score:
                            profile['bcorp_score'] = float(score)
                        
                        operates = company_data.get('operates_in', '')
                        if isinstance(operates, list):
                            operates = ', '.join(operates)
                        profile['operates_in'] = operates
                        
                        self.debug(f"  ✓ __NEXT_DATA__: {profile['company']}")
                        self.stats['profiles_scraped'] += 1
                        return profile
                except (json.JSONDecodeError, KeyError, TypeError):
                    pass
            
            # ── Estrategia 2: HTML con regex flexible ──
            # Nombre de la empresa
            title_match = re.search(r'<h1[^>]*>(.*?)</h1>', html, re.DOTALL)
            if title_match:
                profile['company'] = re.sub(r'<[^>]+>', '', title_match.group(1)).strip()
            else:
                title_match = re.search(r'<title>(.*?)(?:\s*[-|]|\s*<)', html)
                profile['company'] = title_match.group(1).strip() if title_match else ''
            
            if not profile.get('company'):
                return None
            
            # Strip HTML para texto plano como fallback
            text = re.sub(r'<[^>]+>', '\n', html)
            text = re.sub(r'\n{3,}', '\n\n', text)
            
            # Headquarters — varios patrones
            for pattern in [
                r'Headquarters\s*</?\w+[^>]*>\s*(?:<[^>]*>)*\s*([^<]+)',
                r'Headquarters\s*\n\s*([^\n]+)',
            ]:
                hq_match = re.search(pattern, html if '<' in pattern else text, re.DOTALL)
                if hq_match:
                    hq = hq_match.group(1).strip()
                    if hq and len(hq) < 100:
                        profile['headquarters'] = hq
                        parts = [p.strip() for p in hq.split(',')]
                        if len(parts) >= 2:
                            profile['city'] = parts[0]
                            profile['country'] = parts[-1]
                        elif parts:
                            profile['country'] = parts[0]
                        break
            
            # Industry
            for pattern in [
                r'Industry\s*</?\w+[^>]*>\s*(?:<[^>]*>)*\s*([^<]+)',
                r'Industry\s*\n\s*([^\n]+)',
            ]:
                m = re.search(pattern, html if '<' in pattern else text)
                if m and m.group(1).strip() and len(m.group(1).strip()) < 80:
                    profile['industry'] = m.group(1).strip()
                    break
                
            # Sector
            for pattern in [
                r'Sector\s*</?\w+[^>]*>\s*(?:<[^>]*>)*\s*([^<]+)',
                r'Sector\s*\n\s*([^\n]+)',
            ]:
                m = re.search(pattern, html if '<' in pattern else text)
                if m and m.group(1).strip() and len(m.group(1).strip()) < 80:
                    profile['sector'] = m.group(1).strip()
                    break
                
            # Website — buscar enlace junto al texto "Website"
            web_match = re.search(r'Website\s*(?:</?\w+[^>]*>\s*)*<a[^>]*href="(https?://[^"]+)"', html, re.DOTALL)
            if web_match:
                website = web_match.group(1)
                if 'bcorporation.net' not in website:
                    profile['website'] = website
            
            # Fallback website: buscar href que parezca web de empresa
            if not profile.get('website'):
                skip_domains = {
                    'bcorporation.net', 'facebook.com', 'linkedin.com', 'twitter.com',
                    'instagram.com', 'youtube.com', 'google.com', 'data.world',
                    'provoc.me', 'donate.bcorporation.net', 's3.amazonaws.com',
                    'medium.com', 'tiktok.com', 'x.com', 'thinkific.com',
                }
                ext_links = re.findall(r'href="(https?://[^"]+)"', html)
                for link in ext_links:
                    parsed = urlparse(link)
                    domain = parsed.netloc.lower().replace('www.', '')
                    if domain and not any(skip in domain for skip in skip_domains):
                        profile['website'] = link
                        break
            
            # B Impact Score
            score_match = re.search(
                r'(?:earned an overall score of|Overall B Impact Score[^0-9]{0,50}?)\s*(\d+\.?\d*)',
                html, re.IGNORECASE
            )
            if score_match:
                try:
                    profile['bcorp_score'] = float(score_match.group(1))
                except ValueError:
                    pass
                    
            # Certified Since
            for pattern in [
                r'Certified Since\s*</?\w+[^>]*>\s*(?:<[^>]*>)*\s*([^<]+)',
                r'Certified Since\s*\n\s*([^\n]+)',
            ]:
                m = re.search(pattern, html if '<' in pattern else text)
                if m and m.group(1).strip():
                    profile['certified_since'] = m.group(1).strip()
                    break
                    
            # Operates In
            ops_match = re.search(r'Operates In\s*(.*?)(?:Website|Certified|Industry|Sector|Overall|Standards)', text, re.DOTALL)
            if ops_match:
                ops = ops_match.group(1).strip()
                ops = re.sub(r'\s+', ' ', ops).strip()
                if ops and len(ops) < 200:
                    profile['operates_in'] = ops
            
            # Descripción — texto después de la sección de Website
            desc_match = re.search(
                r'(?:www\.[a-z0-9.-]+\.[a-z]+)\s*(.*?)(?:Overall B Impact Score|$)',
                text, re.DOTALL | re.IGNORECASE
            )
            if desc_match:
                desc = desc_match.group(1).strip()
                desc = re.sub(r'\s+', ' ', desc)
                if len(desc) > 30:
                    profile['description'] = desc[:500]
                    
            self.stats['profiles_scraped'] += 1
            return profile
            
        except Exception as e:
            self.stats['profiles_errors'] += 1
            self.debug(f"Error scraping {url}: {e}")
            return None

    # ─── FASE 3: IMPORT a StaffKit ──────────────────────────────────

    def import_to_staffkit(self, profile: Dict) -> bool:
        """Importa un perfil B Corp como lead en StaffKit"""
        if self.dry_run:
            self.log(f"  [DRY RUN] {profile.get('company', '?')} | {profile.get('website', 'sin web')} | {profile.get('country', '?')}")
            return True
        
        try:
            # Preparar notas con info B Corp
            notes_parts = [f"🏅 B Corp Certificada"]
            if profile.get('bcorp_score'):
                notes_parts.append(f"Score: {profile['bcorp_score']}")
            if profile.get('certified_since'):
                notes_parts.append(f"Desde: {profile['certified_since']}")
            if profile.get('operates_in'):
                notes_parts.append(f"Opera en: {profile['operates_in']}")
            if profile.get('description'):
                notes_parts.append(f"— {profile['description'][:300]}")
            notes_parts.append(f"Fuente: {profile.get('source_url', '')}")
                
            lead_data = {
                'email': '',  # Se rellenará con Opus después
                'name': '',
                'company': profile.get('company', ''),
                'website': profile.get('website', ''),
                'phone': '',
                'country': profile.get('country', ''),
                'city': profile.get('city', ''),
                'sector': profile.get('sector', profile.get('industry', '')),
                'notes': ' | '.join(notes_parts),
                'eco_profile': 'bcorp',
                'score': int(profile.get('bcorp_score', 0)),
            }

            response = requests.post(
                f"{STAFFKIT_URL}/api/bots.php",
                data={
                    'action': 'save_lead',
                    'list_id': self.list_id,
                    'bot_id': 0,
                    'run_id': 0,
                    'lead_data': json.dumps(lead_data),
                },
                headers={'Authorization': f'Bearer {self.api_key}'},
                timeout=20
            )
            
            if response.status_code == 200:
                result = response.json()
                if result.get('success'):
                    status = result.get('status', 'saved')
                    if status == 'duplicate':
                        self.stats['leads_duplicates'] += 1
                    else:
                        self.stats['leads_imported'] += 1
                    return True
                else:
                    self.debug(f"API error: {result.get('error', '?')}")
                    self.stats['leads_errors'] += 1
            else:
                self.debug(f"HTTP {response.status_code}")
                self.stats['leads_errors'] += 1
                
        except Exception as e:
            self.log(f"Error importando {profile.get('company')}: {e}", 'ERROR')
            self.stats['leads_errors'] += 1
            
        return False

    # ─── ORQUESTACIÓN ────────────────────────────────────────────────

    def run(self):
        """Ejecuta el scraper completo"""
        start = datetime.now()
        
        self.log("=" * 60)
        self.log("🏅 B CORP DIRECTORY SCRAPER v1.0")
        self.log(f"   Países: {', '.join(self.countries)}")
        self.log(f"   Lista destino: #{self.list_id}")
        self.log(f"   Máx por país: {self.max_per_country}")
        self.log(f"   Dry run: {'Sí' if self.dry_run else 'No'}")
        self.log("=" * 60)
        
        all_profiles = []
        
        # ── Fase 1: Discovery por país ──
        for country in self.countries:
            self.log(f"\n📍 Buscando B Corps en: {country}")
            
            urls = self.discover_profiles_google(country)
            self.stats['profiles_found'] += len(urls)
            self.stats['countries_processed'] += 1
            
            if not urls:
                self.log(f"  ⚠️  No se encontraron perfiles para {country}")
                continue
            
            # ── Fase 2: Scraping de perfiles ──
            self.log(f"  📄 Scrapeando {len(urls)} perfiles...")
            
            country_imported = 0
            for i, url in enumerate(urls):
                profile = self.scrape_profile(url)
                
                if profile and profile.get('company'):
                    # Verificar que el país coincide
                    # Flexible: acepta si el HQ contiene el país o variante
                    hq_country = profile.get('country', '').lower()
                    hq_full = profile.get('headquarters', '').lower()
                    
                    variants = COUNTRY_SEARCH_VARIANTS.get(country, [country])
                    country_match = any(
                        v.lower() in hq_country or v.lower() in hq_full
                        for v in variants
                    ) or not hq_country  # Si no tiene país, aceptar
                    
                    if country_match:
                        # Asegurar que tiene el país correcto
                        if not profile.get('country'):
                            profile['country'] = country
                            
                        all_profiles.append(profile)
                        
                        # ── Fase 3: Import ──
                        if self.import_to_staffkit(profile):
                            country_imported += 1
                        
                if (i + 1) % 10 == 0:
                    self.log(f"  ... {i+1}/{len(urls)} procesados")
                    
            self.log(f"  ✅ {country}: {len(urls)} perfiles → {country_imported} nuevos importados")
        
        # ── Guardar resultados a JSON ──
        if all_profiles:
            ts = datetime.now().strftime('%Y%m%d_%H%M%S')
            results_file = os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                f'bcorp_results_{ts}.json'
            )
            try:
                with open(results_file, 'w', encoding='utf-8') as f:
                    json.dump({
                        'scraped_at': datetime.now().isoformat(),
                        'countries': self.countries,
                        'total': len(all_profiles),
                        'profiles': all_profiles
                    }, f, ensure_ascii=False, indent=2)
                self.log(f"\n💾 Resultados guardados en: {results_file}")
            except Exception as e:
                self.debug(f"Error guardando JSON: {e}")
        
        # ── Resumen ──
        elapsed = (datetime.now() - start).total_seconds()
        
        self.log("\n" + "=" * 60)
        self.log("📊 RESUMEN FINAL")
        self.log(f"   Países procesados:   {self.stats['countries_processed']}")
        self.log(f"   Perfiles encontrados: {self.stats['profiles_found']}")
        self.log(f"   Perfiles scrapeados:  {self.stats['profiles_scraped']}")
        self.log(f"   Errores de scraping:  {self.stats['profiles_errors']}")
        self.log(f"   ─────────────────────────")
        self.log(f"   Leads importados:     {self.stats['leads_imported']}")
        self.log(f"   Leads duplicados:     {self.stats['leads_duplicates']}")
        self.log(f"   Errores de import:    {self.stats['leads_errors']}")
        self.log(f"   ─────────────────────────")
        self.log(f"   Tiempo total:         {elapsed:.0f}s ({elapsed/60:.1f} min)")
        self.log("=" * 60)
        
        self.log("\n💡 Siguiente paso: Ejecuta enriquecimiento 'Opus' en la lista para obtener emails y teléfonos.")
        
        # ── Emitir stats en formato daemon ──
        print(f"STATS:leads_found:{self.stats['profiles_found']}")
        print(f"STATS:leads_saved:{self.stats['leads_imported']}")
        print(f"STATS:leads_duplicates:{self.stats['leads_duplicates']}")
        print(f"STATS:errors:{self.stats['profiles_errors'] + self.stats['leads_errors']}")
        
        return self.stats


def fetch_bot_config(api_key: str, bot_id: int) -> dict:
    """Obtiene la configuración del bot desde StaffKit API"""
    try:
        resp = requests.get(
            f"{STAFFKIT_URL}/api/v2/external-bot",
            params={'id': bot_id},
            headers={'Authorization': f'Bearer {api_key}'},
            timeout=15
        )
        if resp.status_code == 200:
            data = resp.json()
            if data.get('success'):
                bots = data.get('bots', data.get('data', []))
                if isinstance(bots, list) and bots:
                    return bots[0]
                elif isinstance(bots, dict):
                    return bots
    except Exception as e:
        print(f"[WARNING] Could not fetch bot config: {e}")
    return {}


def report_run(api_key: str, bot_id: int, action: str, stats: dict = None):
    """Reporta inicio/fin de ejecución al daemon API"""
    try:
        payload = {
            'id': bot_id,
            'action': action,
        }
        if stats:
            payload['stats'] = json.dumps(stats)
        requests.post(
            f"{STAFFKIT_URL}/api/v2/external-bot",
            json=payload,
            headers={'Authorization': f'Bearer {api_key}'},
            timeout=10
        )
    except Exception:
        pass


def main():
    parser = argparse.ArgumentParser(
        description='B Corp Directory Scraper - Importa empresas B Corp de LATAM y España a StaffKit'
    )
    parser.add_argument('--api-key', required=True, help='API key de StaffKit')
    parser.add_argument('--bot-id', type=int, default=0, help='ID del bot externo (para modo daemon)')
    parser.add_argument('--list-id', type=int, default=0, help='ID de la lista destino en StaffKit')
    parser.add_argument('--google-api-key', default='', 
                        help='Google API Key para CSE (opcional, se obtiene de config si no se pasa)')
    parser.add_argument('--google-cx', default='',
                        help='Google Custom Search Engine ID (opcional)')
    parser.add_argument('--countries', default='',
                        help='Países separados por coma (default: todos LATAM + España)')
    parser.add_argument('--max-per-country', type=int, default=100,
                        help='Máximo de perfiles a buscar por país (default: 100)')
    parser.add_argument('--delay', type=float, default=2.0,
                        help='Delay entre peticiones en segundos (default: 2.0)')
    parser.add_argument('--dry-run', action='store_true',
                        help='Solo muestra resultados, no importa a StaffKit')
    parser.add_argument('--verbose', action='store_true', help='Modo verbose')
    
    args = parser.parse_args()
    
    list_id = args.list_id
    countries_str = args.countries
    max_per_country = args.max_per_country
    delay = args.delay
    
    # Si viene con --bot-id, obtener config del bot desde la API
    if args.bot_id:
        print(f"[INFO] Modo daemon — bot_id={args.bot_id}")
        bot_config = fetch_bot_config(args.api_key, args.bot_id)
        
        if bot_config:
            list_id = list_id or int(bot_config.get('target_list_id', 0) or 0)
            countries_str = countries_str or bot_config.get('config_bcorp_countries', '')
            max_per_country = int(bot_config.get('config_bcorp_max_per_country', max_per_country) or max_per_country)
            delay = float(bot_config.get('config_bcorp_delay', delay) or delay)
            print(f"[INFO] Config cargada: list_id={list_id}, countries={countries_str[:50]}..., max={max_per_country}")
        
        report_run(args.api_key, args.bot_id, 'start_run')
    
    if not list_id:
        print("[ERROR] Se requiere --list-id o un bot configurado con target_list_id")
        sys.exit(1)
    
    # Parsear países
    countries = None
    if countries_str:
        countries = [c.strip() for c in countries_str.split(',') if c.strip()]
    
    # Intentar obtener Google keys de config si no se pasaron
    google_key = args.google_api_key
    google_cx = args.google_cx
    
    if not google_key or not google_cx:
        config_paths = [
            os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data', 'config.json'),
            '/home/replanta/staff.replanta.dev/data/config.json',
        ]
        for cp in config_paths:
            if os.path.exists(cp):
                try:
                    with open(cp) as f:
                        config = json.load(f)
                    google_key = google_key or config.get('google', {}).get('api_key', '')
                    google_cx = google_cx or config.get('google', {}).get('cx_id', '')
                    if google_key:
                        print(f"[INFO] Google keys loaded from {cp}")
                        break
                except Exception:
                    pass
    
    scraper = BCorpScraper(
        api_key=args.api_key,
        list_id=list_id,
        google_api_key=google_key,
        google_cx=google_cx,
        countries=countries,
        max_per_country=max_per_country,
        delay=delay,
        dry_run=args.dry_run,
        verbose=args.verbose,
    )
    
    stats = scraper.run()
    
    # Reportar fin al daemon
    if args.bot_id:
        report_run(args.api_key, args.bot_id, 'end_run', stats)
    
    sys.exit(1 if stats['profiles_errors'] > stats['profiles_scraped'] else 0)


if __name__ == '__main__':
    main()
