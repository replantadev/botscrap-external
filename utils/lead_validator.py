#!/usr/bin/env python3
"""
Lead Validator - Módulo de validación y enriquecimiento de leads
Reciclado del botscrap original adaptado para botscrap_external
"""

import re
import time
import logging
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# Keywords ecológicas para detectar perfiles verdes
KEYWORDS_ECO = [
    'sostenible', 'sostenibilidad', 'ecológico', 'eco-friendly', 'verde',
    'bio', 'orgánico', 'reciclaje', 'reciclado', 'renovable', 'medioambiente',
    'medio ambiente', 'huella carbono', 'emisiones', 'sustainable', 'green',
    'organic', 'recycling', 'renewable', 'carbon footprint', 'net zero',
    'cero emisiones', 'energía limpia', 'clean energy', 'biodegradable'
]

# Sectores objetivo con keywords
SECTORES_OBJETIVO = {
    'ecommerce': {
        'keywords': ['tienda', 'shop', 'store', 'venta', 'comprar', 'cart', 'checkout', 'ecommerce']
    },
    'consultoria': {
        'keywords': ['consultor', 'consulting', 'asesor', 'advisory', 'servicios profesionales']
    },
    'marketing': {
        'keywords': ['marketing', 'publicidad', 'advertising', 'agencia', 'agency', 'digital']
    },
    'diseno_web': {
        'keywords': ['diseño web', 'web design', 'desarrollo web', 'web development', 'wordpress']
    },
    'inmobiliaria': {
        'keywords': ['inmobiliaria', 'real estate', 'propiedades', 'vivienda', 'alquiler']
    },
    'salud': {
        'keywords': ['clínica', 'médico', 'salud', 'health', 'dentista', 'fisioterapia']
    },
    'educacion': {
        'keywords': ['academia', 'formación', 'cursos', 'escuela', 'educación', 'training']
    },
    'turismo': {
        'keywords': ['hotel', 'turismo', 'viajes', 'travel', 'booking', 'vacaciones']
    },
    'restauracion': {
        'keywords': ['restaurante', 'bar', 'café', 'catering', 'food', 'gastronomía']
    },
}

# Ciudades para detección de ubicación
CITIES_ES = ['Madrid', 'Barcelona', 'Valencia', 'Sevilla', 'Bilbao', 'Málaga', 'Zaragoza', 
             'Murcia', 'Palma', 'Las Palmas', 'Alicante', 'Córdoba', 'Valladolid', 'Vigo',
             'Gijón', 'Granada', 'A Coruña', 'Vitoria', 'Elche', 'Oviedo', 'Santander']

CITIES_CO = ['Bogotá', 'Medellín', 'Cali', 'Barranquilla', 'Cartagena', 'Bucaramanga',
             'Pereira', 'Santa Marta', 'Ibagué', 'Cúcuta', 'Manizales', 'Villavicencio']

CITIES_MX = ['Ciudad de México', 'Guadalajara', 'Monterrey', 'Puebla', 'Tijuana',
             'León', 'Cancún', 'Mérida', 'Querétaro', 'Aguascalientes']


class LeadValidator:
    """Validador y enriquecedor de leads"""
    
    def __init__(self, session: requests.Session = None, config: Dict = None):
        """
        Args:
            session: Sesión HTTP compartida
            config: Configuración de filtros
        """
        self.session = session or self._create_session()
        self.config = config or {}
        
        # Filtros
        self.cms_filter = self.config.get('cms_filter', 'all')  # wordpress, joomla, all
        self.min_speed_score = self.config.get('min_speed_score', 0)
        self.max_speed_score = self.config.get('max_speed_score', 100)  # Para captar webs lentas
        self.eco_verde_only = self.config.get('eco_verde_only', False)
        self.skip_pagespeed_api = self.config.get('skip_pagespeed_api', True)
        
        # API Keys
        self.google_api_key = self.config.get('google_api_key', '')
    
    def _create_session(self) -> requests.Session:
        """Crear sesión HTTP con headers"""
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        return session
    
    def _ensure_http(self, url: str) -> str:
        """Asegurar que URL tiene scheme"""
        if not url.startswith(('http://', 'https://')):
            return f'https://{url}'
        return url
    
    def _extract_domain(self, url: str) -> str:
        """Extraer dominio de URL"""
        try:
            parsed = urlparse(self._ensure_http(url))
            domain = parsed.netloc.lower()
            if domain.startswith('www.'):
                domain = domain[4:]
            return domain
        except:
            return url
    
    def validate_and_enrich(self, lead: Dict, html_content: str = None) -> Optional[Dict]:
        """
        Validar y enriquecer un lead completo
        
        Args:
            lead: Dict con datos del lead
            html_content: HTML ya obtenido (para evitar request extra)
            
        Returns:
            Lead enriquecido o None si no pasa filtros
        """
        website = lead.get('web') or lead.get('website', '')
        if not website:
            return None
        
        domain = self._extract_domain(website)
        empresa = lead.get('empresa', domain.split('.')[0].title())
        
        logger.debug(f"Validando: {domain}")
        
        # Obtener HTML si no lo tenemos
        if not html_content:
            html_content = self._fetch_html(website)
        
        if not html_content:
            logger.debug(f"No se pudo obtener HTML de {domain}")
            return None
        
        # 1. Verificar CMS si hay filtro
        if self.cms_filter != 'all':
            cms = self.quick_cms_check(website, html_content)
            if cms != self.cms_filter:
                logger.debug(f"CMS no coincide: {cms} != {self.cms_filter}")
                return None
        
        # 2. Verificar velocidad
        speed_data = self.check_pagespeed(website)
        score = speed_data.get('score', 50)
        
        if score < self.min_speed_score or score > self.max_speed_score:
            logger.debug(f"Velocidad fuera de rango: {score} (min={self.min_speed_score}, max={self.max_speed_score})")
            return None
        
        # 3. Calcular CO2
        co2_data = self.calculate_co2(website, html_content)
        
        # 4. Detecciones
        eco_profile = self.detect_eco_profile(website, html_content)
        
        # Filtro eco si está activo
        if self.eco_verde_only and eco_profile != 'verde':
            logger.debug(f"No es perfil verde: {eco_profile}")
            return None
        
        org_type = self.detect_org_type(website, empresa, html_content)
        sector = self.detect_sector(empresa, html_content)
        location = self.detect_location(website, empresa)
        wp_tech = self.detect_wp_technologies(website, html_content)
        
        # 5. Construir lead enriquecido
        enriched = {
            **lead,
            'web': domain,
            'empresa': empresa,
            'puntuacion': score,
            'co2_visita': co2_data.get('co2_visita', 0),
            'emisiones_anuales': co2_data.get('emisiones_anuales', 0),
            'page_weight': co2_data.get('page_weight', 'N/A'),
            'tipo_org': org_type,
            'perfil_eco': eco_profile,
            'sector': sector,
            'ciudad': location.get('ciudad', ''),
            'pais': location.get('pais', ''),
            'wp_version': wp_tech.get('wp_version', ''),
            'plugins': wp_tech.get('plugins', ''),
            'hosting': wp_tech.get('hosting', ''),
        }
        
        # 6. Calcular prioridad
        enriched['prioridad'] = self.calculate_priority(enriched)
        
        return enriched
    
    def _fetch_html(self, website: str, timeout: int = 10) -> Optional[str]:
        """Obtener HTML de una URL"""
        try:
            url = self._ensure_http(website)
            response = self.session.get(url, timeout=timeout, verify=False)
            if response.status_code == 200:
                return response.text
        except Exception as e:
            logger.debug(f"Error fetching {website}: {e}")
        return None
    
    def quick_cms_check(self, website: str, html_content: str = None) -> str:
        """
        Detección rápida de CMS
        
        Returns:
            'wordpress', 'joomla', 'other', 'unknown'
        """
        try:
            if not html_content:
                html_content = self._fetch_html(website)
            
            if not html_content:
                return 'unknown'
            
            html = html_content.lower()[:15000]  # Solo primeros 15KB
            
            # WordPress indicators (más común)
            if any(x in html for x in ['wp-content', 'wp-includes', '/wp-json/']):
                return 'wordpress'
            
            # Joomla indicators
            if any(x in html for x in ['/media/jui/', '/components/com_', 'option=com_']):
                return 'joomla'
            
            # Check meta generator
            if 'name="generator"' in html:
                if 'wordpress' in html:
                    return 'wordpress'
                if 'joomla' in html:
                    return 'joomla'
                if any(x in html for x in ['drupal', 'prestashop', 'magento', 'shopify', 'wix', 'squarespace']):
                    return 'other'
            
            return 'unknown'
            
        except Exception as e:
            logger.debug(f"CMS check failed for {website}: {e}")
            return 'unknown'
    
    def check_pagespeed(self, website: str) -> Dict:
        """
        Verificar PageSpeed (API o fallback)
        
        Returns:
            {'score': int, 'lcp': int}
        """
        if self.skip_pagespeed_api or not self.google_api_key:
            return self._fallback_speed_check(website)
        
        try:
            url = "https://www.googleapis.com/pagespeedonline/v5/runPagespeed"
            params = {
                'url': self._ensure_http(website),
                'key': self.google_api_key,
                'category': 'performance',
                'strategy': 'mobile'
            }
            
            response = self.session.get(url, params=params, timeout=25)
            response.raise_for_status()
            data = response.json()
            
            lighthouse = data.get('lighthouseResult', {})
            audits = lighthouse.get('audits', {})
            
            score = int(lighthouse.get('categories', {}).get('performance', {}).get('score', 0) * 100)
            lcp_audit = audits.get('largest-contentful-paint', {})
            lcp = int(lcp_audit.get('numericValue', 0))
            
            return {'score': score, 'lcp': lcp}
            
        except Exception as e:
            logger.warning(f"PageSpeed API error for {website}: {e}, using fallback")
            return self._fallback_speed_check(website)
    
    def _fallback_speed_check(self, website: str) -> Dict:
        """Fallback de velocidad midiendo tiempo de respuesta"""
        try:
            url = self._ensure_http(website)
            start = time.perf_counter()
            response = self.session.get(url, timeout=10, verify=False)
            elapsed = (time.perf_counter() - start) * 1000
            
            # Scoring aproximado
            if elapsed < 1000:
                score = 95
            elif elapsed < 2000:
                score = 75
            elif elapsed < 3000:
                score = 60
            else:
                score = 40
            
            return {'score': score, 'lcp': int(elapsed)}
            
        except Exception as e:
            logger.debug(f"Fallback speed check failed: {e}")
            return {'score': 50, 'lcp': 4000}
    
    def calculate_co2(self, website: str, html_content: str = None) -> Dict:
        """
        Calcular emisiones de CO2 basado en tamaño de página
        
        Returns:
            {'co2_visita': float, 'emisiones_anuales': float, 'page_weight': str}
        """
        try:
            if html_content:
                size_kb = len(html_content.encode('utf-8')) / 1024
            else:
                url = self._ensure_http(website)
                response = self.session.get(url, timeout=5, verify=False)
                size_kb = len(response.content) / 1024
            
            size_mb = size_kb / 1024
            
            # Format page weight
            if size_mb >= 1:
                page_weight = f"{size_mb:.1f}MB"
            else:
                page_weight = f"{size_kb:.0f}KB"
            
            # CO2 per visit = size_kb * 0.0008 grams
            co2_visita = round(size_kb * 0.0008, 2)
            
            # Annual emissions (100,000 visits/year estimate)
            emisiones_anuales = round(co2_visita * 100000 / 1000, 2)  # in kg
            
            return {
                'co2_visita': co2_visita,
                'emisiones_anuales': emisiones_anuales,
                'page_weight': page_weight
            }
            
        except Exception as e:
            logger.debug(f"CO2 calculation failed: {e}")
            return {'co2_visita': 0.5, 'emisiones_anuales': 50.0, 'page_weight': 'N/A'}
    
    def detect_org_type(self, website: str, empresa: str, html_content: str = None) -> str:
        """
        Detectar tipo de organización
        
        Returns:
            'empresa', 'fundacion', 'asociacion', 'ong'
        """
        try:
            empresa_lower = empresa.lower()
            
            if any(x in empresa_lower for x in ['fundación', 'fundacion']):
                return 'fundacion'
            if any(x in empresa_lower for x in ['asociación', 'asociacion', 'asoc.']):
                return 'asociacion'
            if any(x in empresa_lower for x in ['ong', 'cooperativa', 'coop.']):
                return 'ong'
            
            return 'empresa'
            
        except Exception as e:
            logger.debug(f"Error detecting org type: {e}")
            return 'empresa'
    
    def detect_eco_profile(self, website: str, html_content: str = None) -> str:
        """
        Detectar perfil ecológico/sostenible
        
        Returns:
            'verde', 'neutro', 'sin_info'
        """
        try:
            if not html_content:
                html_content = self._fetch_html(website)
            
            if not html_content:
                return 'sin_info'
            
            content = html_content.lower()[:20000]
            
            # Contar keywords ecológicas
            eco_count = sum(1 for keyword in KEYWORDS_ECO if keyword.lower() in content)
            
            if eco_count >= 3:
                return 'verde'
            elif eco_count >= 1:
                return 'neutro'
            else:
                return 'sin_info'
                
        except Exception as e:
            logger.debug(f"Error detecting eco profile: {e}")
            return 'sin_info'
    
    def detect_sector(self, empresa: str, html_content: str = None) -> str:
        """
        Detectar sector del negocio
        
        Returns:
            Nombre del sector o 'otro'
        """
        try:
            # Buscar en nombre de empresa primero
            empresa_lower = empresa.lower()
            
            for sector, data in SECTORES_OBJETIVO.items():
                for keyword in data['keywords']:
                    if keyword.lower() in empresa_lower:
                        return sector
            
            # Buscar en HTML si tenemos
            if html_content:
                content = html_content.lower()[:10000]
                scores = {}
                
                for sector, data in SECTORES_OBJETIVO.items():
                    score = sum(1 for kw in data['keywords'] if kw.lower() in content)
                    if score > 0:
                        scores[sector] = score
                
                if scores:
                    return max(scores, key=scores.get)
            
            return 'otro'
            
        except Exception as e:
            logger.debug(f"Error detecting sector: {e}")
            return 'otro'
    
    def detect_location(self, website: str, empresa: str, direccion: str = '', notas: str = '') -> Dict[str, str]:
        """
        Detectar ciudad y país
        
        Returns:
            {'ciudad': str, 'pais': str}
        """
        try:
            # 1. Extraer de dirección
            if direccion:
                direccion_lower = direccion.lower()
                for ciudad in CITIES_ES:
                    if ciudad.lower() in direccion_lower:
                        return {'ciudad': ciudad, 'pais': 'ES'}
                for ciudad in CITIES_CO:
                    if ciudad.lower() in direccion_lower:
                        return {'ciudad': ciudad, 'pais': 'CO'}
                for ciudad in CITIES_MX:
                    if ciudad.lower() in direccion_lower:
                        return {'ciudad': ciudad, 'pais': 'MX'}
            
            # 2. Buscar en notas
            if notas and ' en ' in notas:
                parts = notas.split(' en ')
                if len(parts) > 1:
                    search_city = parts[-1].strip()
                    for ciudad in CITIES_ES + CITIES_CO + CITIES_MX:
                        if ciudad.lower() == search_city.lower():
                            pais = 'ES' if ciudad in CITIES_ES else ('CO' if ciudad in CITIES_CO else 'MX')
                            return {'ciudad': ciudad, 'pais': pais}
            
            # 3. Buscar en nombre de empresa
            empresa_lower = empresa.lower()
            for ciudad in CITIES_ES:
                if ciudad.lower() in empresa_lower:
                    return {'ciudad': ciudad, 'pais': 'ES'}
            for ciudad in CITIES_CO:
                if ciudad.lower() in empresa_lower:
                    return {'ciudad': ciudad, 'pais': 'CO'}
            
            # 4. Por extensión de dominio
            domain = self._extract_domain(website)
            if '.es' in domain:
                return {'ciudad': '', 'pais': 'ES'}
            elif '.co' in domain or '.com.co' in domain:
                return {'ciudad': '', 'pais': 'CO'}
            elif '.mx' in domain or '.com.mx' in domain:
                return {'ciudad': '', 'pais': 'MX'}
            
            return {'ciudad': '', 'pais': ''}
            
        except Exception as e:
            logger.debug(f"Error detecting location: {e}")
            return {'ciudad': '', 'pais': ''}
    
    def detect_wp_technologies(self, website: str, html_content: str = None) -> Dict[str, str]:
        """
        Detectar versión WordPress, plugins y hosting
        
        Returns:
            {'wp_version': str, 'plugins': str, 'hosting': str}
        """
        result = {'wp_version': '', 'plugins': '', 'hosting': ''}
        
        try:
            if not html_content:
                html_content = self._fetch_html(website)
            
            if not html_content:
                return result
            
            # Detectar versión WP
            patterns = [
                r'WordPress\s*([\d.]+)',
                r'ver=([\d.]+)',
                r'wp-includes.*?\?ver=([\d.]+)',
            ]
            
            for pattern in patterns:
                match = re.search(pattern, html_content, re.IGNORECASE)
                if match:
                    result['wp_version'] = match.group(1)
                    break
            
            # Detectar plugins comunes
            plugins_found = []
            plugin_indicators = {
                'elementor': 'Elementor',
                'wpbakery': 'WPBakery',
                'divi': 'Divi',
                'woocommerce': 'WooCommerce',
                'yoast': 'Yoast SEO',
                'contact-form-7': 'CF7',
                'wpml': 'WPML',
                'jetpack': 'Jetpack',
                'wordfence': 'Wordfence',
                'all-in-one-seo': 'AIOSEO',
                'rank-math': 'RankMath',
            }
            
            html_lower = html_content.lower()
            for indicator, name in plugin_indicators.items():
                if indicator in html_lower:
                    plugins_found.append(name)
            
            if plugins_found:
                result['plugins'] = ', '.join(plugins_found[:5])  # Max 5
            
            # Detectar hosting (heurísticas)
            hosting_indicators = {
                'siteground': 'SiteGround',
                'bluehost': 'Bluehost',
                'godaddy': 'GoDaddy',
                'hostinger': 'Hostinger',
                'ionos': 'IONOS',
                'ovh': 'OVH',
                'cloudflare': 'Cloudflare',
                'wpengine': 'WPEngine',
                'kinsta': 'Kinsta',
                'webempresa': 'Webempresa',
                'dinahosting': 'Dinahosting',
                'raiola': 'Raiola',
                'arsys': 'Arsys',
                'cdmon': 'CDmon',
                'donweb': 'DonWeb',
            }
            
            for indicator, name in hosting_indicators.items():
                if indicator in html_lower:
                    result['hosting'] = name
                    break
            
        except Exception as e:
            logger.debug(f"Error detecting WP tech: {e}")
        
        return result
    
    def calculate_priority(self, lead: Dict) -> str:
        """
        Calcular prioridad del lead basado en múltiples factores
        
        Returns:
            'alta', 'media', 'baja'
        """
        score = 0
        
        # Factor 1: PageSpeed (más lento = más prioridad para webs a mejorar)
        puntuacion = lead.get('puntuacion', 100)
        if puntuacion < 50:
            score += 30
        elif puntuacion < 70:
            score += 20
        elif puntuacion < 80:
            score += 10
        
        # Factor 2: Sector
        sector = lead.get('sector', 'otro')
        if sector in ['marketing', 'diseno_web']:
            score += 25
        elif sector == 'ecommerce':
            score += 20
        elif sector == 'consultoria':
            score += 15
        
        # Factor 3: Tipo organización
        tipo_org = lead.get('tipo_org', 'empresa')
        if tipo_org == 'empresa':
            score += 15
        elif tipo_org in ['ong', 'fundacion']:
            score += 5
        
        # Factor 4: Perfil ecológico
        perfil_eco = lead.get('perfil_eco', 'sin_info')
        if perfil_eco == 'verde':
            score += 10
        
        # Factor 5: CO2 alto (argumento de venta)
        emisiones = lead.get('emisiones_anuales', 0)
        if emisiones > 1000:
            score += 10
        elif emisiones > 500:
            score += 5
        
        # Factor 6: Datos de contacto completos
        if lead.get('email') and lead.get('contacto'):
            score += 10
        elif lead.get('email'):
            score += 5
        
        # Clasificación
        if score >= 70:
            return 'alta'
        elif score >= 50:
            return 'media'
        else:
            return 'baja'
    
    def find_email(self, website: str, html_content: str = None) -> str:
        """
        Buscar email en la página
        
        Returns:
            Email encontrado o string vacío
        """
        try:
            if not html_content:
                html_content = self._fetch_html(website)
            
            if not html_content:
                return ''
            
            # Buscar mailto primero
            mailto_match = re.search(r'mailto:([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})', html_content)
            if mailto_match:
                return mailto_match.group(1)
            
            # Regex en texto
            email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
            emails = re.findall(email_pattern, html_content)
            
            if emails:
                # Filtrar emails genéricos
                excluded = ['example', 'test', 'domain', 'wixpress', 'sentry', 'wordpress', 'noreply']
                filtered = [e for e in emails if not any(x in e.lower() for x in excluded)]
                if filtered:
                    return filtered[0]
            
            return ''
            
        except Exception as e:
            logger.debug(f"Error finding email: {e}")
            return ''
    
    def find_linkedin(self, html_content: str = None) -> str:
        """
        Buscar perfil LinkedIn en la página
        
        Returns:
            URL de LinkedIn o string vacío
        """
        try:
            if not html_content:
                return ''
            
            # Buscar links de LinkedIn
            linkedin_pattern = r'https?://(?:www\.)?linkedin\.com/(?:company|in)/[a-zA-Z0-9_-]+'
            matches = re.findall(linkedin_pattern, html_content)
            
            if matches:
                return matches[0]
            
            return ''
            
        except Exception as e:
            logger.debug(f"Error finding LinkedIn: {e}")
            return ''
