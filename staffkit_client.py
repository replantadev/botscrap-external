#!/usr/bin/env python3
"""
StaffKit API Client
Cliente para comunicación con StaffKit desde VPS externo
"""

import os
import re
import time
import json
import logging
from typing import List, Dict, Set, Optional
from urllib.parse import urlparse

import requests
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)


def normalize_domain(url: str) -> str:
    """
    Normalizar URL/dominio para comparaciones consistentes.
    Quita protocolo, www., trailing slash y paths.
    
    Args:
        url: URL o dominio a normalizar
        
    Returns:
        Dominio normalizado (ej: 'example.com')
    """
    if not url:
        return ''
    
    url = url.lower().strip()
    # Quitar protocolo
    url = re.sub(r'^https?://', '', url)
    # Quitar www.
    url = re.sub(r'^www\.', '', url)
    # Quitar trailing slash
    url = url.rstrip('/')
    # Quedarse solo con el dominio (sin path)
    url = url.split('/')[0]
    # Quitar puerto si existe
    url = url.split(':')[0]
    
    return url


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
            return {'success': False, 'error': 'Client not configured', 'status': 'error'}
        
        try:
            response = requests.get(
                f"{self.api_url}/api/bots.php",
                params={'action': 'ping'},
                headers=self._headers(),
                timeout=10
            )
            
            success = response.status_code == 200
            return {
                'success': success,
                'status': 'ok' if success else 'error',
                'status_code': response.status_code,
                'url': self.api_url
            }
            
        except Exception as e:
            return {'success': False, 'status': 'error', 'error': str(e)}
    
    def check_connection(self) -> Dict:
        """Alias para test_connection (usado por health monitor)"""
        return self.test_connection()
    
    def get_lists(self) -> List[Dict]:
        """
        Obtener listas disponibles de StaffKit
        
        Returns:
            Lista de diccionarios con id y nombre de cada lista
        """
        if not self.enabled:
            return []
        
        try:
            response = requests.get(
                f"{self.api_url}/api/bots.php",
                params={'action': 'get_lists'},
                headers=self._headers(),
                timeout=10
            )
            
            if response.status_code == 200:
                data = response.json()
                if data.get('success') and 'lists' in data:
                    return data['lists']
            
            # Si falla, devolver lista vacía
            logger.warning(f"Could not fetch lists: {response.status_code}")
            return []
            
        except Exception as e:
            logger.warning(f"Error fetching lists: {e}")
            return []
    
    def check_duplicate(self, domain: str) -> bool:
        """
        Verificar si un dominio ya existe en StaffKit
        
        Args:
            domain: URL o dominio a verificar (se normaliza automáticamente)
        
        Returns:
            True si es duplicado, False si es nuevo
        """
        if not self.enabled:
            return False
        
        # Normalizar dominio antes de enviar
        domain = normalize_domain(domain)
        if not domain:
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
            domains: Lista de dominios/URLs a verificar (se normalizan automáticamente)
            
        Returns:
            Dict con {original_domain: is_duplicate}
        """
        if not self.enabled or not domains:
            return {d: False for d in domains}
        
        # Normalizar dominios y crear mapeo original -> normalizado
        normalized_map = {}  # {normalized: [originals]}
        original_to_normalized = {}  # {original: normalized}
        
        for domain in domains:
            normalized = normalize_domain(domain)
            if normalized:
                original_to_normalized[domain] = normalized
                if normalized not in normalized_map:
                    normalized_map[normalized] = []
                normalized_map[normalized].append(domain)
        
        if not normalized_map:
            return {d: False for d in domains}
        
        # Chunkar en lotes de 100 (límite del API)
        BATCH_SIZE = 100
        all_normalized = list(normalized_map.keys())
        all_results = {}
        total_duplicates = 0
        total_new = 0
        
        try:
            for i in range(0, len(all_normalized), BATCH_SIZE):
                chunk = all_normalized[i:i + BATCH_SIZE]
                
                response = requests.post(
                    f"{self.api_url}/api/v2/check-duplicate",
                    json={'domains': chunk},
                    headers=self._headers(),
                    timeout=15
                )
                
                if response.status_code == 200:
                    data = response.json()
                    chunk_results = data.get('results', {})
                    all_results.update(chunk_results)
                    
                    total_duplicates += data.get('duplicates_count', 0)
                    total_new += data.get('new_count', 0)
                else:
                    logger.warning(f"StaffKit batch API error: {response.status_code} (chunk {i//BATCH_SIZE + 1})")
            
            logger.info(f"📊 StaffKit check: {total_duplicates} duplicates, {total_new} new ({len(all_normalized)} domains in {(len(all_normalized)-1)//BATCH_SIZE + 1} batches)")
            
            # Mapear resultados de vuelta a dominios originales
            output = {}
            for original, normalized in original_to_normalized.items():
                output[original] = all_results.get(normalized, {}).get('exists', False)
            
            # Incluir dominios que no se pudieron normalizar
            for domain in domains:
                if domain not in output:
                    output[domain] = False
            
            return output
                
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
