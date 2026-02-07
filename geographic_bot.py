#!/usr/bin/env python3
"""
Geographic Crawler Bot - Barre países enteros por sector/keyword
Usa DataForSEO Maps API para búsquedas económicas ($0.002/búsqueda)
"""

import argparse
import requests
import json
import time
import sys
import os
from datetime import datetime
from typing import Optional, Dict, List, Any

# Configuración
STAFFKIT_URL = os.getenv('STAFFKIT_URL', 'https://staff.replanta.dev')

class GeographicBot:
    def __init__(self, bot_id: int, api_token: str, 
                 dataforseo_login: str = None, dataforseo_password: str = None,
                 searches_per_run: int = 5, delay_between_searches: float = 2.0,
                 delay_between_pages: float = 1.0, verbose: bool = False):
        
        self.bot_id = bot_id
        self.api_token = api_token
        self.searches_per_run = searches_per_run
        self.delay_between_searches = delay_between_searches
        self.delay_between_pages = delay_between_pages
        self.verbose = verbose
        
        # Obtener credenciales de DataForSEO desde integraciones si no se pasaron
        if not dataforseo_login or not dataforseo_password:
            credentials = self._get_dataforseo_credentials()
            if credentials:
                self.dataforseo_login = credentials.get('login', '')
                self.dataforseo_password = credentials.get('password', '')
            else:
                # Fallback a valores pasados como argumentos si la API falla
                self.dataforseo_login = dataforseo_login or ''
                self.dataforseo_password = dataforseo_password or ''
        else:
            self.dataforseo_login = dataforseo_login
            self.dataforseo_password = dataforseo_password
        
        # Stats de esta ejecución
        self.stats = {
            'searches_processed': 0,
            'leads_found': 0,
            'leads_new': 0,
            'api_cost': 0.0,
            'errors': []
        }
        
    def _get_dataforseo_credentials(self) -> Optional[dict]:
        """Obtiene credenciales de DataForSEO desde StaffKit Integraciones"""
        url = f"{STAFFKIT_URL}/api/v2/integrations.php/dataforseo"
        headers = {
            'Authorization': f'Bearer {self.api_token}',
            'Content-Type': 'application/json'
        }
        
        try:
            response = requests.get(url, headers=headers, timeout=10)
            if response.status_code == 200:
                data = response.json()
                if data.get('enabled') and data.get('login') and data.get('password'):
                    self.log(f"Credenciales DataForSEO obtenidas desde Integraciones", 'INFO')
                    return data
                else:
                    self.log(f"DataForSEO no configurado en Integraciones", 'WARNING')
            else:
                self.log(f"No se pudieron obtener credenciales de Integraciones (HTTP {response.status_code})", 'WARNING')
        except Exception as e:
            self.log(f"Error obteniendo credenciales de Integraciones: {e}", 'WARNING')
        
        return None
        
    def log(self, msg: str, level: str = 'INFO'):
        """Log con timestamp"""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"[{timestamp}] [{level}] {msg}")
        
    def debug(self, msg: str):
        """Log solo si verbose"""
        if self.verbose:
            self.log(msg, 'DEBUG')
            
    def api_call(self, endpoint: str, method: str = 'GET', data: dict = None) -> Optional[dict]:
        """Llamada a la API de StaffKit"""
        url = f"{STAFFKIT_URL}/api/v2/geographic.php?action={endpoint}"
        headers = {
            'Authorization': f'Bearer {self.api_token}',
            'Content-Type': 'application/json'
        }
        
        try:
            if method == 'GET':
                response = requests.get(url, headers=headers, params=data, timeout=30)
            else:
                response = requests.post(url, headers=headers, json=data, timeout=30)
                
            if response.status_code == 200:
                return response.json()
            else:
                self.log(f"API error {response.status_code}: {response.text}", 'ERROR')
                return None
        except Exception as e:
            self.log(f"API exception: {str(e)}", 'ERROR')
            return None
            
    def get_next_search(self) -> Optional[dict]:
        """Obtiene la siguiente búsqueda pendiente"""
        result = self.api_call('get_next_search', 'POST', {'bot_id': self.bot_id})
        if result and result.get('success') and result.get('search'):
            return result['search']
        return None
        
    def update_search_progress(self, search_id: int, next_page: int, 
                                leads_found: int, leads_new: int) -> bool:
        """Actualiza progreso de una búsqueda (paginación)"""
        result = self.api_call('update_search_progress', 'POST', {
            'search_id': search_id,
            'next_page_token': str(next_page),
            'leads_found': leads_found,
            'leads_new': leads_new
        })
        return result and result.get('success', False)
        
    def complete_search(self, search_id: int, leads_found: int, 
                        leads_new: int, api_cost: float) -> bool:
        """Marca una búsqueda como completada"""
        result = self.api_call('complete_search', 'POST', {
            'search_id': search_id,
            'leads_found': leads_found,
            'leads_new': leads_new,
            'api_cost': api_cost
        })
        return result and result.get('success', False)
        
    def search_dataforseo_maps(self, keyword: str, location: str, 
                                language_code: str = 'es', max_pages: int = 3) -> List[dict]:
        """
        Busca en DataForSEO Maps API
        Retorna lista de negocios encontrados
        """
        all_results = []
        
        # DataForSEO Maps API endpoint
        url = "https://api.dataforseo.com/v3/serp/google/maps/live/advanced"
        
        for page in range(max_pages):
            self.debug(f"DataForSEO página {page + 1}/{max_pages} para '{keyword}' en {location}")
            
            # Construir payload
            # Nota: Maps API no usa location_name, el keyword ya incluye la ubicación
            payload = [{
                "keyword": f"{keyword} {location}",
                "language_code": language_code,
                "device": "desktop",
                "os": "windows",
                "depth": 20  # resultados por página
            }]
            
            # DEBUG: Ver payload real enviado
            self.log(f"[DEBUG] Payload enviado: {json.dumps(payload, indent=2)}", 'INFO')
            
            try:
                response = requests.post(
                    url,
                    auth=(self.dataforseo_login, self.dataforseo_password),
                    json=payload,
                    timeout=60
                )
                
                if response.status_code != 200:
                    self.log(f"DataForSEO HTTP error {response.status_code}: {response.text[:200]}", 'ERROR')
                    break
                    
                data = response.json()
                
                # Contar costo
                if 'cost' in data:
                    self.stats['api_cost'] += data['cost']
                    
                # Extraer resultados
                tasks = data.get('tasks', [])
                if not tasks:
                    self.log(f"DataForSEO: No tasks in response for '{keyword}' in {location}", 'WARNING')
                    break
                    
                task = tasks[0]
                status_code = task.get('status_code')
                if status_code != 20000:
                    self.log(f"DataForSEO task error ({status_code}): {task.get('status_message')}", 'ERROR')
                    break
                    
                results = task.get('result', [])
                if not results:
                    self.log(f"DataForSEO: Empty results for '{keyword}' in {location}", 'WARNING')
                    break
                    
                items = results[0].get('items', [])
                if not items:
                    self.log(f"DataForSEO: No items found for '{keyword}' in {location} (page {page + 1})", 'INFO')
                    break
                    
                # Procesar items
                for item in items:
                    if item.get('type') == 'maps_search':
                        business = self._parse_maps_result(item)
                        if business:
                            all_results.append(business)
                            
                self.debug(f"Encontrados {len(items)} negocios en página {page + 1}")
                
                # Delay entre páginas
                if page < max_pages - 1:
                    time.sleep(self.delay_between_pages)
                    
            except Exception as e:
                self.log(f"DataForSEO exception: {str(e)}", 'ERROR')
                self.stats['errors'].append(str(e))
                break
                
        return all_results
        
    def _parse_maps_result(self, item: dict) -> Optional[dict]:
        """Parsea un resultado de Maps a formato de lead"""
        try:
            # Extraer datos básicos
            title = item.get('title', '')
            if not title:
                return None
                
            address_info = item.get('address_info', {}) or {}
            
            return {
                'company': title,
                'address': item.get('address', ''),
                'phone': item.get('phone', ''),
                'website': item.get('url', '') or item.get('domain', ''),
                'city': address_info.get('city', ''),
                'region': address_info.get('region', ''),
                'country': address_info.get('country_code', ''),
                'postal_code': address_info.get('zip', ''),
                'category': item.get('category', ''),
                'rating': item.get('rating', {}).get('value') if item.get('rating') else None,
                'reviews_count': item.get('rating', {}).get('votes_count') if item.get('rating') else None,
                'latitude': item.get('latitude'),
                'longitude': item.get('longitude'),
                'place_id': item.get('place_id', ''),
                'cid': item.get('cid', ''),
            }
        except Exception as e:
            self.debug(f"Error parsing result: {e}")
            return None
            
    def add_leads_to_staffkit(self, search: dict, leads: List[dict]) -> int:
        """
        Añade leads al sistema StaffKit
        Retorna número de leads nuevos añadidos
        """
        if not leads:
            return 0
            
        # Obtener list_id del bot
        list_id = search.get('list_id')
        if not list_id:
            self.log("No list_id en búsqueda", 'ERROR')
            return 0
            
        new_count = 0
        
        for lead in leads:
            # Preparar datos para StaffKit
            lead_data = {
                'list_id': list_id,
                'empresa': lead.get('company', ''),
                'website': lead.get('website', ''),
                'telefono': lead.get('phone', ''),
                'direccion': lead.get('address', ''),
                'ciudad': lead.get('city', ''),
                'region': lead.get('region', ''),
                'pais': lead.get('country', ''),
                'codigo_postal': lead.get('postal_code', ''),
                'categoria': lead.get('category', ''),
                'rating': lead.get('rating'),
                'reviews': lead.get('reviews_count'),
                'latitud': lead.get('latitude'),
                'longitud': lead.get('longitude'),
                'place_id': lead.get('place_id', ''),
                'fuente': 'geographic_crawler',
                'notas': f"Keyword: {search.get('keyword')}, Ciudad: {search.get('location')}"
            }
            
            # Llamar API para añadir lead
            result = self._add_lead(lead_data)
            if result and result.get('is_new', False):
                new_count += 1
                
        return new_count
        
    def _add_lead(self, lead_data: dict) -> Optional[dict]:
        """Añade un lead individual a StaffKit"""
        url = f"{STAFFKIT_URL}/api/bots.php"
        headers = {
            'Authorization': f'Bearer {self.api_token}',
            'Content-Type': 'application/json'
        }
        
        payload = {
            'action': 'add_lead_geographic',
            **lead_data
        }
        
        try:
            response = requests.post(url, headers=headers, json=payload, timeout=30)
            self.debug(f"API response {response.status_code}: {response.text[:200]}")
            if response.status_code == 200:
                result = response.json()
                if not result.get('success', True):
                    self.debug(f"API error: {result.get('error', 'unknown')}")
                return result
            else:
                self.log(f"API HTTP error {response.status_code}: {response.text[:100]}", 'ERROR')
        except Exception as e:
            self.debug(f"Error adding lead: {e}")
            
        return None
        
    def process_search(self, search: dict) -> bool:
        """Procesa una búsqueda individual"""
        search_id = search['id']
        keyword = search['keyword']
        location = search['location']
        max_pages = int(search.get('max_pages', 3))
        
        self.log(f"Procesando: '{keyword}' en {location}")
        
        # Buscar en DataForSEO
        leads = self.search_dataforseo_maps(keyword, location, max_pages=max_pages)
        
        leads_found = len(leads)
        self.log(f"Encontrados {leads_found} negocios")
        
        # Añadir a StaffKit
        leads_new = self.add_leads_to_staffkit(search, leads)
        self.log(f"Nuevos leads añadidos: {leads_new}")
        
        # Actualizar stats
        self.stats['leads_found'] += leads_found
        self.stats['leads_new'] += leads_new
        self.stats['searches_processed'] += 1
        
        # Marcar como completada
        # Estimar costo: $0.002 por búsqueda/página
        estimated_cost = max_pages * 0.002
        self.complete_search(search_id, leads_found, leads_new, estimated_cost)
        
        return True
        
    def run(self):
        """Ejecuta el bot procesando N búsquedas"""
        self.log(f"Iniciando Geographic Bot #{self.bot_id}")
        self.log(f"Búsquedas por ejecución: {self.searches_per_run}")
        
        searches_done = 0
        
        while searches_done < self.searches_per_run:
            # Obtener siguiente búsqueda
            search = self.get_next_search()
            
            if not search:
                self.log("No hay más búsquedas pendientes")
                break
                
            # Procesar
            try:
                self.process_search(search)
                searches_done += 1
            except Exception as e:
                self.log(f"Error procesando búsqueda: {e}", 'ERROR')
                self.stats['errors'].append(str(e))
                
            # Delay entre búsquedas
            if searches_done < self.searches_per_run:
                self.debug(f"Esperando {self.delay_between_searches}s...")
                time.sleep(self.delay_between_searches)
                
        # Resumen final
        self.log("=" * 50)
        self.log("RESUMEN DE EJECUCIÓN")
        self.log(f"Búsquedas procesadas: {self.stats['searches_processed']}")
        self.log(f"Leads encontrados: {self.stats['leads_found']}")
        self.log(f"Leads nuevos: {self.stats['leads_new']}")
        self.log(f"Costo API estimado: ${self.stats['api_cost']:.4f}")
        if self.stats['errors']:
            self.log(f"Errores: {len(self.stats['errors'])}")
        self.log("=" * 50)
        
        # CRITICAL: Output STATS lines for daemon parsing (MUST be at end, clean format)
        # Daemon parses these with STATS:key:value pattern
        print(f"STATS:leads_found:{self.stats['leads_found']}")
        print(f"STATS:leads_saved:{self.stats['leads_new']}")
        print(f"STATS:leads_duplicates:{self.stats['leads_found'] - self.stats['leads_new']}")
        print(f"STATS:searches_done:{self.stats['searches_processed']}")
        
        return self.stats


def main():
    parser = argparse.ArgumentParser(description='Geographic Crawler Bot')
    parser.add_argument('--bot-id', type=int, required=True, help='ID del bot')
    parser.add_argument('--api-key', type=str, required=True, help='Token de API StaffKit')
    parser.add_argument('--dataforseo-login', type=str, 
                       default=None,
                       help='Login DataForSEO (opcional, se obtiene de Integraciones si no se especifica)')
    parser.add_argument('--dataforseo-password', type=str,
                       default=None,
                       help='Password DataForSEO (opcional, se obtiene de Integraciones si no se especifica)')
    parser.add_argument('--searches-per-run', type=int, default=5, help='Búsquedas por ejecución')
    parser.add_argument('--delay-searches', type=float, default=2.0, help='Delay entre búsquedas (segundos)')
    parser.add_argument('--delay-pages', type=float, default=1.0, help='Delay entre páginas (segundos)')
    parser.add_argument('--verbose', action='store_true', help='Modo verbose')
    
    args = parser.parse_args()
    
    bot = GeographicBot(
        bot_id=args.bot_id,
        api_token=args.api_key,
        dataforseo_login=args.dataforseo_login,
        dataforseo_password=args.dataforseo_password,
        searches_per_run=args.searches_per_run,
        delay_between_searches=args.delay_searches,
        delay_between_pages=args.delay_pages,
        verbose=args.verbose
    )
    
    stats = bot.run()
    
    # Exit code basado en errores
    sys.exit(1 if stats['errors'] else 0)


if __name__ == '__main__':
    main()
