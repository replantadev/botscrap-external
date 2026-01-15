#!/usr/bin/env python3
"""
StaffKit API Client
Cliente para comunicación con StaffKit desde VPS externo
"""

import os
import time
import json
import logging
from typing import List, Dict, Set, Optional
from urllib.parse import urlparse

import requests
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)


class StaffKitClient:
    """Cliente para comunicarse con la API de StaffKit"""
    
    def __init__(self, api_url: str = None, api_key: str = None):
        """
        Inicializar cliente
        
        Args:
            api_url: URL base de StaffKit (ej: https://staff.replanta.dev)
            api_key: API key para autenticación
        """
        self.api_url = (api_url or os.getenv('STAFFKIT_URL', '')).rstrip('/')
        self.api_key = api_key or os.getenv('STAFFKIT_API_KEY', '')
        self.timeout = int(os.getenv('STAFFKIT_TIMEOUT', '20'))
        self.max_retries = int(os.getenv('STAFFKIT_RETRIES', '3'))
        self.enabled = bool(self.api_url and self.api_key)
        
        if self.enabled:
            logger.info(f"✅ StaffKit client initialized: {self.api_url}")
        else:
            logger.warning("⚠️ StaffKit client disabled (no URL or API key)")
    
    def _headers(self) -> Dict:
        """Headers para las peticiones"""
        return {
            'Authorization': f'Bearer {self.api_key}',
            'Content-Type': 'application/json',
            'User-Agent': 'BotScrap-External/1.0'
        }
    
    def _form_headers(self) -> Dict:
        """Headers para form-data"""
        return {
            'Authorization': f'Bearer {self.api_key}',
            'User-Agent': 'BotScrap-External/1.0'
        }
    
    def test_connection(self) -> Dict:
        """
        Probar conexión con StaffKit
        
        Returns:
            Dict con estado de conexión
        """
        if not self.enabled:
            return {'success': False, 'error': 'Client not configured'}
        
        try:
            response = requests.get(
                f"{self.api_url}/api/bots.php",
                params={'action': 'ping'},
                headers=self._headers(),
                timeout=10
            )
            
            return {
                'success': response.status_code == 200,
                'status_code': response.status_code,
                'url': self.api_url
            }
            
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    def check_duplicate(self, domain: str) -> bool:
        """
        Verificar si un dominio ya existe en StaffKit
        
        Returns:
            True si es duplicado, False si es nuevo
        """
        if not self.enabled:
            return False
        
        try:
            response = requests.get(
                f"{self.api_url}/api/v2/check-duplicate",
                params={'domain': domain},
                headers=self._headers(),
                timeout=5
            )
            
            if response.status_code == 200:
                data = response.json()
                return data.get('exists', False)
            else:
                logger.warning(f"StaffKit API error: {response.status_code}")
                return False
                
        except Exception as e:
            logger.warning(f"StaffKit check failed: {e}")
            return False
    
    def check_duplicates_batch(self, domains: List[str]) -> Dict[str, bool]:
        """
        Verificar múltiples dominios en una sola petición
        
        Args:
            domains: Lista de dominios/URLs a verificar
            
        Returns:
            Dict con {domain: is_duplicate}
        """
        if not self.enabled or not domains:
            return {d: False for d in domains}
        
        try:
            response = requests.post(
                f"{self.api_url}/api/v2/check-duplicate",
                json={'domains': domains},
                headers=self._headers(),
                timeout=10
            )
            
            if response.status_code == 200:
                data = response.json()
                results = data.get('results', {})
                
                duplicates = data.get('duplicates_count', 0)
                new_count = data.get('new_count', 0)
                logger.info(f"📊 StaffKit check: {duplicates} duplicates, {new_count} new")
                
                return {
                    domain: results.get(domain, {}).get('exists', False)
                    for domain in domains
                }
            else:
                logger.warning(f"StaffKit batch API error: {response.status_code}")
                return {d: False for d in domains}
                
        except Exception as e:
            logger.warning(f"StaffKit batch check failed: {e}")
            return {d: False for d in domains}
    
    def save_lead(self, lead: Dict, list_id: int, bot_id: int = None, run_id: int = None) -> Dict:
        """
        Guardar un lead directamente en StaffKit
        
        Args:
            lead: Datos del lead
            list_id: ID de la lista destino
            bot_id: ID del bot (opcional)
            run_id: ID de la ejecución (opcional)
            
        Returns:
            Dict con resultado: {'success': bool, 'status': 'saved'|'duplicate'|'error', 'id': int}
        """
        if not self.enabled:
            return {'success': False, 'status': 'disabled'}
        
        # Mapear campos
        lead_data = {
            'email': lead.get('email', ''),
            'name': lead.get('contacto', lead.get('name', '')),
            'company': lead.get('empresa', lead.get('company', '')),
            'website': lead.get('web', lead.get('website', '')),
            'phone': lead.get('telefono', lead.get('phone', '')),
            'country': lead.get('pais', lead.get('country', '')),
            'city': lead.get('ciudad', lead.get('city', '')),
            'sector': lead.get('sector', ''),
            'org_type': lead.get('tipo_org', lead.get('org_type', '')),
            'eco_profile': lead.get('perfil_eco', lead.get('eco_profile', '')),
            'priority': lead.get('prioridad', lead.get('priority', '')),
            'score': lead.get('puntuacion', lead.get('score', 0)),
            'co2_visit': lead.get('co2_visita', lead.get('co2_visit')),
            'annual_emissions': lead.get('emisiones_anuales', lead.get('annual_emissions')),
            'linkedin': lead.get('linkedin', ''),
            'notes': lead.get('notas', lead.get('notes', '')),
            'wp_version': lead.get('wp_version', ''),
            'plugins': lead.get('plugins', ''),
            'hosting': lead.get('hosting', ''),
            'load_time': lead.get('load_time'),
            'page_weight': lead.get('page_weight'),
            'report_url': lead.get('report_url'),
            # Flags
            'needs_email_enrichment': lead.get('needs_email_enrichment', False),
        }
        
        # Retry logic
        last_error = None
        backoff = 0.5
        
        for attempt in range(1, self.max_retries + 1):
            try:
                response = requests.post(
                    f"{self.api_url}/api/bots.php",
                    data={
                        'action': 'save_lead',
                        'list_id': list_id,
                        'bot_id': bot_id or 0,
                        'run_id': run_id or 0,
                        'lead_data': json.dumps(lead_data)
                    },
                    headers=self._form_headers(),
                    timeout=self.timeout
                )
                
                if response.status_code == 200:
                    result = response.json()
                    if result.get('success'):
                        logger.info(f"✅ Lead saved: {lead_data.get('website')} ({result.get('status')})")
                        return result
                    else:
                        logger.warning(f"StaffKit save error: {result.get('error')}")
                        return {'success': False, 'status': 'error', 'error': result.get('error')}
                else:
                    if 500 <= response.status_code < 600 and attempt < self.max_retries:
                        logger.debug(f"HTTP {response.status_code}, retry {attempt}/{self.max_retries}...")
                        time.sleep(backoff)
                        backoff *= 2
                        continue
                    return {'success': False, 'status': 'error', 'error': f'HTTP {response.status_code}'}
                    
            except requests.Timeout as e:
                last_error = str(e)
                if attempt < self.max_retries:
                    logger.debug(f"Timeout, retry {attempt}/{self.max_retries}...")
                    time.sleep(backoff)
                    backoff *= 2
                    continue
                    
            except Exception as e:
                last_error = str(e)
                if attempt < self.max_retries:
                    time.sleep(backoff)
                    backoff *= 2
                    continue
        
        return {'success': False, 'status': 'error', 'error': last_error or 'unknown error'}
    
    def update_progress(self, run_id: int, leads_found: int = 0, leads_saved: int = 0,
                       leads_duplicates: int = 0, status: str = None, error: str = None,
                       current_action: str = None) -> bool:
        """
        Actualizar progreso de ejecución en StaffKit
        """
        if not self.enabled or not run_id:
            return False
        
        try:
            data = {
                'action': 'update_progress',
                'run_id': run_id,
                'leads_found': leads_found,
                'leads_saved': leads_saved,
                'leads_duplicates': leads_duplicates,
            }
            
            if status:
                data['status'] = status
            if error:
                data['last_error'] = error
            if current_action:
                data['current_action'] = current_action
            
            response = requests.post(
                f"{self.api_url}/api/bots.php",
                data=data,
                headers=self._form_headers(),
                timeout=10
            )
            
            return response.status_code == 200
            
        except Exception as e:
            logger.debug(f"Update progress error: {e}")
            return False
    
    def complete_run(self, run_id: int, leads_found: int = 0, leads_saved: int = 0,
                    leads_duplicates: int = 0, status: str = 'completed', error: str = None) -> bool:
        """
        Marcar ejecución como completada
        """
        if not self.enabled or not run_id:
            return False
        
        try:
            data = {
                'action': 'complete',
                'run_id': run_id,
                'leads_found': leads_found,
                'leads_saved': leads_saved,
                'leads_duplicates': leads_duplicates,
                'status': status,
            }
            
            if error:
                data['error'] = error
            
            response = requests.post(
                f"{self.api_url}/api/bots.php",
                data=data,
                headers=self._form_headers(),
                timeout=10
            )
            
            return response.status_code == 200
            
        except Exception as e:
            logger.debug(f"Complete run error: {e}")
            return False
    
    def send_telegram(self, message: str) -> bool:
        """
        Enviar mensaje via StaffKit (usa la config de Telegram de StaffKit)
        """
        if not self.enabled:
            return False
        
        try:
            response = requests.post(
                f"{self.api_url}/api/bots.php",
                data={
                    'action': 'send_telegram',
                    'message': message
                },
                headers=self._form_headers(),
                timeout=10
            )
            
            return response.status_code == 200 and response.json().get('sent', False)
            
        except Exception as e:
            logger.debug(f"Send telegram error: {e}")
            return False


def get_staffkit_client(config: Dict = None) -> StaffKitClient:
    """Factory para obtener cliente de StaffKit"""
    config = config or {}
    
    return StaffKitClient(
        api_url=config.get('staffkit_url'),
        api_key=config.get('staffkit_api_key')
    )
