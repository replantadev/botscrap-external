#!/usr/bin/env python3
"""
Email Enricher - Sistema de múltiples emails por empresa con priorización
Reciclado del botscrap original para botscrap_external
"""

import re
import os
import logging
from typing import List, Dict, Optional, Tuple
from urllib.parse import urlparse

import requests

logger = logging.getLogger(__name__)

# Patrones de email comunes por prioridad
EMAIL_PATTERNS = {
    'comercial': ['ventas', 'comercial', 'sales', 'business'],
    'general': ['info', 'contacto', 'contact', 'hola', 'hello'],
    'soporte': ['soporte', 'support', 'ayuda', 'help'],
    'admin': ['admin', 'administracion', 'webmaster'],
    'rrhh': ['rrhh', 'hr', 'empleo', 'jobs', 'careers'],
}

# Prioridad de tipos de email (menor = mejor para ventas)
EMAIL_PRIORITY = {
    'personal': 1,      # maria@empresa.com
    'comercial': 2,     # ventas@empresa.com
    'general': 3,       # info@empresa.com
    'soporte': 4,       # soporte@empresa.com
    'admin': 5,         # admin@empresa.com
    'rrhh': 6,          # rrhh@empresa.com
    'unknown': 7,       # otros
}

# Emails a ignorar
IGNORE_EMAILS = [
    'example', 'test', 'domain', 'wixpress', 'sentry', 'localhost',
    'noreply', 'no-reply', 'donotreply', 'bounce', 'mailer-daemon',
    'wordpress', 'webmaster@wordpress', 'privacy@', 'abuse@'
]


class EmailEnricher:
    """Enriquecedor de emails para leads"""
    
    def __init__(self, session: requests.Session = None, apollo_key: str = None):
        self.session = session or self._create_session()
        self.apollo_key = apollo_key or os.getenv('APOLLO_KEY', '')
        self.timeout = (5, 10)
    
    def _create_session(self) -> requests.Session:
        """Crear sesión HTTP"""
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        return session
    
    def enrich_emails(self, website: str, empresa: str = '') -> Dict:
        """
        Buscar múltiples emails para un dominio.
        
        Returns:
            {
                'email_principal': 'mejor@email.com',
                'emails_adicionales': ['otro@email.com', 'info@email.com'],
                'email_tipo': 'personal',
                'confianza': 85,
                'todos_emails': [...]
            }
        """
        domain = self._extract_domain(website)
        if not domain:
            return self._empty_result()
        
        all_emails = []
        
        # 1. Scraping de la web principal
        web_emails = self._scrape_website_emails(website)
        all_emails.extend(web_emails)
        
        # 2. Scraping de página de contacto
        contact_emails = self._scrape_contact_page(website)
        all_emails.extend(contact_emails)
        
        # 3. Generar patrones comunes
        pattern_emails = self._generate_pattern_emails(domain)
        all_emails.extend(pattern_emails)
        
        # 4. Apollo.io Org Enrichment (si hay API key - obtener teléfono)
        phone = ''
        apollo_data = {}
        if self.apollo_key:
            phone, apollo_data = self._apollo_org_enrich(domain)
        
        # 5. Deduplicar y priorizar
        unique_emails = self._deduplicate_emails(all_emails)
        prioritized = self._prioritize_emails(unique_emails)
        
        # 6. Construir resultado (incluye phone de Apollo)
        return self._build_result(prioritized, phone=phone, apollo_data=apollo_data)
    
    def _extract_domain(self, website: str) -> str:
        """Extraer dominio de URL"""
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
    
    def _scrape_website_emails(self, website: str) -> List[Dict]:
        """Extraer emails de la página principal"""
        emails = []
        try:
            url = website if website.startswith('http') else f'https://{website}'
            response = self.session.get(url, timeout=self.timeout, verify=False)
            
            found = self._extract_emails_from_html(response.text)
            for email in found:
                emails.append({
                    'email': email.lower(),
                    'tipo': self._classify_email(email),
                    'prioridad': EMAIL_PRIORITY.get(self._classify_email(email), 7),
                    'fuente': 'web_principal',
                    'verificado': False
                })
                
        except Exception as e:
            logger.debug(f"Error scraping {website}: {e}")
        
        return emails
    
    def _scrape_contact_page(self, website: str) -> List[Dict]:
        """Buscar emails en páginas de contacto"""
        emails = []
        contact_paths = ['/contacto', '/contact', '/contactanos', '/contact-us', '/sobre-nosotros', '/about']
        
        base_url = website if website.startswith('http') else f'https://{website}'
        base_url = base_url.rstrip('/')
        
        for path in contact_paths[:3]:
            try:
                url = f"{base_url}{path}"
                response = self.session.get(url, timeout=self.timeout, allow_redirects=True, verify=False)
                
                if response.status_code == 200:
                    found = self._extract_emails_from_html(response.text)
                    for email in found:
                        emails.append({
                            'email': email.lower(),
                            'tipo': self._classify_email(email),
                            'prioridad': EMAIL_PRIORITY.get(self._classify_email(email), 7),
                            'fuente': f'web_{path.strip("/")}',
                            'verificado': False
                        })
            except:
                continue
        
        return emails
    
    def _generate_pattern_emails(self, domain: str) -> List[Dict]:
        """Generar emails basados en patrones comunes"""
        emails = []
        common_patterns = ['info', 'contacto', 'hola', 'ventas', 'comercial']
        
        for pattern in common_patterns:
            email = f"{pattern}@{domain}"
            emails.append({
                'email': email,
                'tipo': self._classify_email(email),
                'prioridad': EMAIL_PRIORITY.get(self._classify_email(email), 7) + 1,
                'fuente': 'pattern',
                'verificado': False
            })
        
        return emails
    
    def _apollo_org_enrich(self, domain: str) -> Tuple[str, dict]:
        """Enriquecer organización con Apollo.io (gratis) - obtiene teléfono, LinkedIn, etc."""
        if not domain or not self.apollo_key:
            return '', {}
        
        try:
            url = f"https://api.apollo.io/v1/organizations/enrich?domain={domain}"
            headers = {
                'Content-Type': 'application/json',
                'x-api-key': self.apollo_key
            }
            
            response = self.session.get(url, headers=headers, timeout=self.timeout, verify=False)
            data = response.json()
            
            org = data.get('organization', {})
            
            if not org or not org.get('name'):
                return '', {}
            
            result = {
                'name': org.get('name'),
                'phone': org.get('phone', ''),
                'linkedin_url': org.get('linkedin_url', ''),
                'industry': org.get('industry', ''),
                'city': org.get('city', ''),
                'country': org.get('country', ''),
                'facebook_url': org.get('facebook_url', ''),
            }
            
            return result.get('phone', ''), result
                        
        except Exception as e:
            logger.debug(f"Apollo org enrich error: {e}")
        
        return '', {}
    
    def _extract_emails_from_html(self, html: str) -> List[str]:
        """Extraer emails de HTML"""
        emails = []
        
        # mailto: links
        mailto_pattern = r'mailto:([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})'
        emails.extend(re.findall(mailto_pattern, html))
        
        # Emails en texto
        email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
        emails.extend(re.findall(email_pattern, html))
        
        # Filtrar y deduplicar
        valid_emails = []
        seen = set()
        for email in emails:
            email_lower = email.lower()
            if email_lower not in seen and self._is_valid_email(email):
                valid_emails.append(email)
                seen.add(email_lower)
        
        return valid_emails
    
    def _is_valid_email(self, email: str) -> bool:
        """Verificar si un email es válido"""
        email_lower = email.lower()
        
        if not re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', email):
            return False
        
        for ignore in IGNORE_EMAILS:
            if ignore in email_lower:
                return False
        
        return True
    
    def _classify_email(self, email: str) -> str:
        """Clasificar tipo de email"""
        local_part = email.split('@')[0].lower()
        
        # Personal (nombre.apellido)
        if re.match(r'^[a-z]+\.[a-z]+$', local_part):
            return 'personal'
        if re.match(r'^[a-z]{2,}$', local_part) and len(local_part) > 3:
            is_pattern = any(local_part in patterns for patterns in EMAIL_PATTERNS.values())
            if not is_pattern:
                return 'personal'
        
        # Por patrones
        for tipo, patterns in EMAIL_PATTERNS.items():
            if any(p in local_part for p in patterns):
                return tipo
        
        return 'unknown'
    
    def _deduplicate_emails(self, emails: List[Dict]) -> List[Dict]:
        """Eliminar duplicados"""
        seen = {}
        for email_data in emails:
            email = email_data['email'].lower()
            if email not in seen:
                seen[email] = email_data
            else:
                existing = seen[email]
                if email_data['fuente'] != 'pattern' and existing['fuente'] == 'pattern':
                    seen[email] = email_data
                elif email_data.get('verificado') and not existing.get('verificado'):
                    seen[email] = email_data
        
        return list(seen.values())
    
    def _prioritize_emails(self, emails: List[Dict]) -> List[Dict]:
        """Ordenar por prioridad"""
        def sort_key(e):
            verified_score = 0 if e.get('verificado') else 1
            source_score = 0 if e['fuente'] != 'pattern' else 1
            return (verified_score, e['prioridad'], source_score)
        
        return sorted(emails, key=sort_key)
    
    def _build_result(self, prioritized_emails: List[Dict], phone: str = '', apollo_data: dict = None) -> Dict:
        """Construir resultado final"""
        if not prioritized_emails and not phone:
            return self._empty_result()
        
        if prioritized_emails:
            principal = prioritized_emails[0]
            adicionales = [e['email'] for e in prioritized_emails[1:5]]
            
            # Calcular confianza
            confianza = 50
            if principal.get('verificado'):
                confianza += 30
            if principal['fuente'] != 'pattern':
                confianza += 15
            if principal['tipo'] == 'personal':
                confianza += 5
            
            return {
                'email_principal': principal['email'],
                'emails_adicionales': adicionales,
                'email_tipo': principal['tipo'],
                'confianza': min(confianza, 100),
                'todos_emails': prioritized_emails,
                'phone': phone,
                'apollo_data': apollo_data or {}
            }
        else:
            # Solo tenemos datos de Apollo (teléfono) sin emails
            return {
                'email_principal': '',
                'emails_adicionales': [],
                'email_tipo': 'unknown',
                'confianza': 0,
                'todos_emails': [],
                'phone': phone,
                'apollo_data': apollo_data or {}
            }
    
    def _empty_result(self) -> Dict:
        """Resultado vacío"""
        return {
            'email_principal': '',
            'emails_adicionales': [],
            'email_tipo': 'unknown',
            'confianza': 0,
            'todos_emails': [],
            'phone': '',
            'apollo_data': {}
        }


def format_emails_for_storage(email_result: Dict) -> Tuple[str, str, str, int]:
    """
    Formatear resultado para almacenamiento.
    Returns: (email, emails_adicionales_str, email_tipo, confianza)
    """
    return (
        email_result.get('email_principal', ''),
        '|'.join(email_result.get('emails_adicionales', [])),
        email_result.get('email_tipo', 'unknown'),
        email_result.get('confianza', 0)
    )
