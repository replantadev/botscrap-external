#!/usr/bin/env python3
"""
Rate Limiter - Control de límites de APIs
"""

import time
import logging
from datetime import datetime, timedelta
from typing import Dict, Optional, Tuple
from collections import defaultdict
import threading
import json

logger = logging.getLogger(__name__)


class RateLimiter:
    """
    Control de rate limits para múltiples APIs.
    Implementa ventanas de tiempo sliding y backoff exponencial.
    """
    
    # Configuración de límites por defecto
    DEFAULT_LIMITS = {
        'google_search': {
            'requests': 100,
            'window': 'day',        # 100/día (free tier)
            'backoff_base': 60,     # 1 minuto base
        },
        'google_pagespeed': {
            'requests': 25000,
            'window': 'day',        # 25000/día
            'backoff_base': 1,
        },
        'trustpilot': {
            'requests': 100,
            'window': 'hour',       # ~100/hora (estimado)
            'backoff_base': 120,    # 2 minutos base
        },
        'reddit': {
            'requests': 60,
            'window': 'minute',     # 60/minuto
            'backoff_base': 10,
        },
        'staffkit': {
            'requests': 1000,
            'window': 'hour',       # Sin límite real, pero por seguridad
            'backoff_base': 1,
        },
        'hunter': {
            'requests': 25,
            'window': 'month',      # 25/mes (free tier)
            'backoff_base': 3600,
        },
    }
    
    WINDOW_SECONDS = {
        'minute': 60,
        'hour': 3600,
        'day': 86400,
        'month': 2592000,  # 30 días
    }
    
    def __init__(self, custom_limits: Dict = None):
        """
        Args:
            custom_limits: Override de límites por defecto
        """
        self.limits = {**self.DEFAULT_LIMITS}
        if custom_limits:
            for api, config in custom_limits.items():
                if api in self.limits:
                    self.limits[api].update(config)
                else:
                    self.limits[api] = config
        
        # Tracking de requests: api -> [(timestamp, count), ...]
        self._requests: Dict[str, list] = defaultdict(list)
        
        # Backoff state: api -> (backoff_until, backoff_count)
        self._backoff: Dict[str, Tuple[datetime, int]] = {}
        
        # Lock para thread safety
        self._lock = threading.Lock()
        
        # Persistencia (opcional)
        self._state_file = None
    
    def set_persistence(self, state_file: str):
        """Activar persistencia de estado"""
        self._state_file = state_file
        self._load_state()
    
    def _load_state(self):
        """Cargar estado desde archivo"""
        if not self._state_file:
            return
        try:
            from pathlib import Path
            if Path(self._state_file).exists():
                with open(self._state_file, 'r') as f:
                    data = json.load(f)
                    # Convertir timestamps
                    for api, requests in data.get('requests', {}).items():
                        self._requests[api] = [
                            (datetime.fromisoformat(ts), count) 
                            for ts, count in requests
                        ]
        except Exception as e:
            logger.warning(f"Error loading rate limiter state: {e}")
    
    def _save_state(self):
        """Guardar estado a archivo"""
        if not self._state_file:
            return
        try:
            data = {
                'requests': {
                    api: [(ts.isoformat(), count) for ts, count in reqs]
                    for api, reqs in self._requests.items()
                }
            }
            with open(self._state_file, 'w') as f:
                json.dump(data, f)
        except Exception as e:
            logger.warning(f"Error saving rate limiter state: {e}")
    
    def _clean_old_requests(self, api: str):
        """Limpiar requests antiguos fuera de la ventana"""
        if api not in self.limits:
            return
        
        window = self.limits[api]['window']
        window_seconds = self.WINDOW_SECONDS.get(window, 3600)
        cutoff = datetime.now() - timedelta(seconds=window_seconds)
        
        self._requests[api] = [
            (ts, count) for ts, count in self._requests[api]
            if ts > cutoff
        ]
    
    def _get_current_count(self, api: str) -> int:
        """Obtener número de requests en la ventana actual"""
        self._clean_old_requests(api)
        return sum(count for _, count in self._requests[api])
    
    def can_request(self, api: str) -> bool:
        """
        Verificar si se puede hacer un request.
        
        Returns:
            True si se puede hacer el request
        """
        if api not in self.limits:
            return True
        
        with self._lock:
            # Verificar backoff
            if api in self._backoff:
                backoff_until, _ = self._backoff[api]
                if datetime.now() < backoff_until:
                    return False
                else:
                    del self._backoff[api]
            
            # Verificar límite
            current = self._get_current_count(api)
            limit = self.limits[api]['requests']
            
            return current < limit
    
    def track_request(self, api: str, count: int = 1):
        """
        Registrar un request realizado.
        
        Args:
            api: Nombre de la API
            count: Número de requests (default 1)
        """
        with self._lock:
            self._requests[api].append((datetime.now(), count))
            self._save_state()
    
    def track_error(self, api: str, is_rate_limit: bool = False):
        """
        Registrar un error (para backoff).
        
        Args:
            api: Nombre de la API
            is_rate_limit: Si fue error de rate limit (429)
        """
        with self._lock:
            if api not in self.limits:
                return
            
            base = self.limits[api].get('backoff_base', 60)
            
            # Incrementar contador de backoff
            current_count = 0
            if api in self._backoff:
                _, current_count = self._backoff[api]
            
            current_count += 1
            
            # Calcular tiempo de backoff exponencial
            if is_rate_limit:
                backoff_seconds = base * (2 ** min(current_count, 6))
            else:
                backoff_seconds = base * current_count
            
            backoff_until = datetime.now() + timedelta(seconds=backoff_seconds)
            self._backoff[api] = (backoff_until, current_count)
            
            logger.warning(f"Rate limiter backoff for {api}: {backoff_seconds}s (attempt {current_count})")
    
    def reset_backoff(self, api: str):
        """Resetear backoff después de request exitoso"""
        with self._lock:
            if api in self._backoff:
                del self._backoff[api]
    
    def get_status(self, api: str) -> Dict:
        """
        Obtener estado del rate limiter para una API.
        
        Returns:
            {
                'current': int,
                'limit': int,
                'remaining': int,
                'percentage': float,
                'window': str,
                'resets_in': int (seconds),
                'in_backoff': bool,
                'backoff_until': datetime or None
            }
        """
        if api not in self.limits:
            return {'error': f'API {api} not configured'}
        
        with self._lock:
            config = self.limits[api]
            current = self._get_current_count(api)
            limit = config['requests']
            window = config['window']
            
            # Calcular cuando se resetea
            window_seconds = self.WINDOW_SECONDS.get(window, 3600)
            if self._requests[api]:
                oldest = min(ts for ts, _ in self._requests[api])
                resets_in = max(0, int((oldest + timedelta(seconds=window_seconds) - datetime.now()).total_seconds()))
            else:
                resets_in = 0
            
            # Estado de backoff
            in_backoff = False
            backoff_until = None
            if api in self._backoff:
                backoff_until, _ = self._backoff[api]
                in_backoff = datetime.now() < backoff_until
            
            return {
                'current': current,
                'limit': limit,
                'remaining': max(0, limit - current),
                'percentage': round((current / limit) * 100, 1) if limit > 0 else 0,
                'window': window,
                'resets_in': resets_in,
                'in_backoff': in_backoff,
                'backoff_until': backoff_until.isoformat() if backoff_until else None,
            }
    
    def get_all_status(self) -> Dict[str, Dict]:
        """Obtener estado de todas las APIs"""
        return {api: self.get_status(api) for api in self.limits}
    
    def wait_if_needed(self, api: str, max_wait: int = 300) -> bool:
        """
        Esperar si es necesario hasta poder hacer request.
        
        Args:
            api: Nombre de la API
            max_wait: Máximo tiempo de espera en segundos
            
        Returns:
            True si se puede proceder, False si timeout
        """
        start = datetime.now()
        
        while not self.can_request(api):
            elapsed = (datetime.now() - start).total_seconds()
            if elapsed > max_wait:
                logger.warning(f"Rate limiter timeout for {api} after {elapsed}s")
                return False
            
            # Calcular tiempo de espera
            status = self.get_status(api)
            if status.get('in_backoff'):
                backoff_until = datetime.fromisoformat(status['backoff_until'])
                wait = min(30, (backoff_until - datetime.now()).total_seconds())
            else:
                wait = min(30, status.get('resets_in', 60))
            
            wait = max(1, wait)
            logger.debug(f"Rate limiter waiting {wait}s for {api}")
            time.sleep(wait)
        
        return True
    
    def get_recommended_delay(self, api: str) -> float:
        """
        Obtener delay recomendado entre requests.
        
        Returns:
            Segundos de delay recomendado
        """
        if api not in self.limits:
            return 0
        
        status = self.get_status(api)
        percentage = status.get('percentage', 0)
        
        # Aumentar delay según uso
        if percentage > 90:
            return 10.0
        elif percentage > 75:
            return 5.0
        elif percentage > 50:
            return 2.0
        else:
            return 0.5
