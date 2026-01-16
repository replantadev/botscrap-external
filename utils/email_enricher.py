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
    
    def __init__(self, session: requests.Session = None, hunter_key: str = None):
        self.session = session or self._create_session()
        self.hunter_key = hunter_key or os.getenv('HUNTER_KEY', '')
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
        
        # 4. Hunter.io (si hay API key y pocos emails encontrados)
        if self.hunter_key and len([e for e in all_emails if e['fuente'] != 'pattern']) < 2:
            hunter_emails = self._hunter_search(domain)
            all_emails.extend(hunter_emails)
        
        # 5. Deduplicar y priorizar
        unique_emails = self._deduplicate_emails(all_emails)
        prioritized = self._prioritize_emails(unique_emails)
        
        # 6. Construir resultado
        return self._build_result(prioritized)
    
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
    
    def _hunter_search(self, domain: str) -> List[Dict]:
        """Buscar emails con Hunter.io API"""
        emails = []
        
        if not self.hunter_key:
            return emails
        
        try:
            url = f"https://api.hunter.io/v2/domain-search?domain={domain}&api_key={self.hunter_key}"
            response = self.session.get(url, timeout=self.timeout)
            data = response.json()
            
            if data.get('data', {}).get('emails'):
                for item in data['data']['emails'][:5]:
                    email = item.get('value', '')
                    if email and self._is_valid_email(email):
                        tipo = 'personal' if item.get('first_name') else self._classify_email(email)
                        emails.append({
                            'email': email.lower(),
                            'tipo': tipo,
                            'prioridad': EMAIL_PRIORITY.get(tipo, 7),
                            'fuente': 'hunter',
                            'verificado': item.get('verification', {}).get('status') == 'valid',
                            'nombre': f"{item.get('first_name', '')} {item.get('last_name', '')}".strip(),
                            'cargo': item.get('position', '')
                        })
                        
        except Exception as e:
            logger.debug(f"Hunter API error: {e}")
        
        return emails
    
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
    
    def _build_result(self, prioritized_emails: List[Dict]) -> Dict:
        """Construir resultado final"""
        if not prioritized_emails:
            return self._empty_result()
        
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
            'todos_emails': prioritized_emails
        }
    
    def _empty_result(self) -> Dict:
        """Resultado vacío"""
        return {
            'email_principal': '',
            'emails_adicionales': [],
            'email_tipo': 'unknown',
            'confianza': 0,
            'todos_emails': []
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
