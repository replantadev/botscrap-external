#!/usr/bin/env python3
"""
Metrics Collector - Recolección de métricas y estadísticas
"""

import logging
import json
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
from collections import defaultdict
import threading
import statistics

logger = logging.getLogger(__name__)


@dataclass
class MetricPoint:
    """Punto de métrica"""
    name: str
    value: float
    timestamp: str
    tags: Dict[str, str] = None
    
    def to_dict(self) -> Dict:
        return asdict(self)


class MetricsCollector:
    """
    Recolector de métricas del sistema.
    
    Métricas recolectadas:
    - Leads por bot, día, hora
    - Tiempos de ejecución
    - Tasas de éxito/error
    - Uso de rate limits
    - Performance del sistema
    """
    
    def __init__(self, state_manager):
        """
        Args:
            state_manager: StateManager instance para persistencia
        """
        self.state_manager = state_manager
        self._lock = threading.Lock()
        
        # Métricas en memoria (para agregaciones rápidas)
        self._counters: Dict[str, float] = defaultdict(float)
        self._gauges: Dict[str, float] = {}
        self._histograms: Dict[str, List[float]] = defaultdict(list)
        self._timeseries: Dict[str, List[MetricPoint]] = defaultdict(list)
    
    # === COUNTERS (acumulativos) ===
    
    def increment(self, name: str, value: float = 1, tags: Dict = None):
        """
        Incrementar contador.
        
        Args:
            name: Nombre de la métrica
            value: Valor a incrementar
            tags: Tags adicionales
        """
        with self._lock:
            key = self._make_key(name, tags)
            self._counters[key] += value
            
            # Guardar punto
            self._add_point(name, self._counters[key], tags)
    
    def get_counter(self, name: str, tags: Dict = None) -> float:
        """Obtener valor de contador"""
        key = self._make_key(name, tags)
        return self._counters.get(key, 0)
    
    # === GAUGES (valores instantáneos) ===
    
    def set_gauge(self, name: str, value: float, tags: Dict = None):
        """
        Establecer valor de gauge.
        
        Args:
            name: Nombre de la métrica
            value: Valor actual
            tags: Tags adicionales
        """
        with self._lock:
            key = self._make_key(name, tags)
            self._gauges[key] = value
            self._add_point(name, value, tags)
    
    def get_gauge(self, name: str, tags: Dict = None) -> Optional[float]:
        """Obtener valor de gauge"""
        key = self._make_key(name, tags)
        return self._gauges.get(key)
    
    # === HISTOGRAMS (distribuciones) ===
    
    def observe(self, name: str, value: float, tags: Dict = None):
        """
        Registrar observación para histograma.
        
        Args:
            name: Nombre de la métrica
            value: Valor observado
            tags: Tags adicionales
        """
        with self._lock:
            key = self._make_key(name, tags)
            self._histograms[key].append(value)
            
            # Limitar tamaño
            if len(self._histograms[key]) > 1000:
                self._histograms[key] = self._histograms[key][-500:]
            
            self._add_point(name, value, tags)
    
    def get_histogram_stats(self, name: str, tags: Dict = None) -> Dict:
        """
        Obtener estadísticas de histograma.
        
        Returns:
            {count, min, max, mean, median, p95, p99}
        """
        key = self._make_key(name, tags)
        values = self._histograms.get(key, [])
        
        if not values:
            return {}
        
        sorted_values = sorted(values)
        count = len(values)
        
        return {
            'count': count,
            'min': min(values),
            'max': max(values),
            'mean': statistics.mean(values),
            'median': statistics.median(values),
            'p95': sorted_values[int(count * 0.95)] if count > 20 else max(values),
            'p99': sorted_values[int(count * 0.99)] if count > 100 else max(values),
        }
    
    # === TIMERS ===
    
    def timer(self, name: str, tags: Dict = None):
        """
        Context manager para medir tiempo.
        
        Usage:
            with metrics.timer('bot_execution', {'bot': 'direct'}):
                run_bot()
        """
        return _Timer(self, name, tags)
    
    # === UTILIDADES ===
    
    def _make_key(self, name: str, tags: Dict = None) -> str:
        """Crear clave única para métrica"""
        if not tags:
            return name
        tag_str = ','.join(f"{k}={v}" for k, v in sorted(tags.items()))
        return f"{name}:{tag_str}"
    
    def _add_point(self, name: str, value: float, tags: Dict = None):
        """Añadir punto a timeseries"""
        point = MetricPoint(
            name=name,
            value=value,
            timestamp=datetime.now().isoformat(),
            tags=tags
        )
        
        key = self._make_key(name, tags)
        self._timeseries[key].append(point)
        
        # Limitar tamaño
        if len(self._timeseries[key]) > 500:
            self._timeseries[key] = self._timeseries[key][-250:]
    
    # === MÉTRICAS ESPECÍFICAS DEL BOT ===
    
    def record_run(self, bot_type: str, duration: float, leads_found: int, 
                   leads_saved: int, success: bool):
        """
        Registrar ejecución de bot.
        
        Args:
            bot_type: Tipo de bot
            duration: Duración en segundos
            leads_found: Leads encontrados
            leads_saved: Leads guardados
            success: Si fue exitoso
        """
        tags = {'bot': bot_type}
        
        # Counters
        self.increment('bot_runs_total', tags=tags)
        self.increment('leads_found_total', leads_found, tags=tags)
        self.increment('leads_saved_total', leads_saved, tags=tags)
        
        if success:
            self.increment('bot_runs_success', tags=tags)
        else:
            self.increment('bot_runs_failed', tags=tags)
        
        # Histogramas
        self.observe('bot_duration_seconds', duration, tags=tags)
        self.observe('leads_per_run', leads_saved, tags=tags)
        
        # Tasa de conversión
        if leads_found > 0:
            conversion = (leads_saved / leads_found) * 100
            self.observe('conversion_rate', conversion, tags=tags)
    
    def record_api_call(self, api: str, duration: float, success: bool, 
                        status_code: int = None):
        """
        Registrar llamada a API.
        
        Args:
            api: Nombre de la API
            duration: Duración en segundos
            success: Si fue exitosa
            status_code: Código HTTP (opcional)
        """
        tags = {'api': api}
        
        self.increment('api_calls_total', tags=tags)
        self.observe('api_duration_seconds', duration, tags=tags)
        
        if success:
            self.increment('api_calls_success', tags=tags)
        else:
            self.increment('api_calls_failed', tags=tags)
            if status_code:
                self.increment('api_errors_by_code', tags={**tags, 'code': str(status_code)})
    
    def record_lead(self, bot_type: str, sector: str = None, 
                    location: str = None, priority: int = None):
        """
        Registrar lead guardado.
        
        Args:
            bot_type: Tipo de bot
            sector: Sector del lead
            location: Ubicación
            priority: Prioridad
        """
        self.increment('leads_total', tags={'bot': bot_type})
        
        if sector:
            self.increment('leads_by_sector', tags={'sector': sector})
        if location:
            self.increment('leads_by_location', tags={'location': location})
        if priority:
            self.increment('leads_by_priority', tags={'priority': str(priority)})
    
    # === REPORTES ===
    
    def get_daily_stats(self, target_date: date = None) -> Dict:
        """Obtener estadísticas del día"""
        target_date = target_date or date.today()
        
        return {
            'date': target_date.isoformat(),
            'leads_saved': int(self.get_counter('leads_saved_total')),
            'runs_total': int(self.get_counter('bot_runs_total')),
            'runs_success': int(self.get_counter('bot_runs_success')),
            'runs_failed': int(self.get_counter('bot_runs_failed')),
            'by_bot': {
                bot: {
                    'leads': int(self.get_counter('leads_saved_total', {'bot': bot})),
                    'runs': int(self.get_counter('bot_runs_total', {'bot': bot})),
                    'duration_avg': self.get_histogram_stats('bot_duration_seconds', {'bot': bot}).get('mean', 0)
                }
                for bot in ['direct', 'resentment', 'social']
            }
        }
    
    def get_performance_stats(self) -> Dict:
        """Obtener estadísticas de performance"""
        return {
            'bot_duration': {
                bot: self.get_histogram_stats('bot_duration_seconds', {'bot': bot})
                for bot in ['direct', 'resentment', 'social']
            },
            'api_duration': {
                api: self.get_histogram_stats('api_duration_seconds', {'api': api})
                for api in ['google_search', 'staffkit', 'trustpilot']
            },
            'conversion_rate': {
                bot: self.get_histogram_stats('conversion_rate', {'bot': bot})
                for bot in ['direct', 'resentment', 'social']
            }
        }
    
    def get_timeseries(self, name: str, tags: Dict = None, 
                       hours: int = 24) -> List[Dict]:
        """
        Obtener timeseries para gráficos.
        
        Args:
            name: Nombre de la métrica
            tags: Tags de filtro
            hours: Horas hacia atrás
            
        Returns:
            Lista de puntos [{timestamp, value}, ...]
        """
        key = self._make_key(name, tags)
        points = self._timeseries.get(key, [])
        
        cutoff = datetime.now() - timedelta(hours=hours)
        
        return [
            {'timestamp': p.timestamp, 'value': p.value}
            for p in points
            if datetime.fromisoformat(p.timestamp) > cutoff
        ]
    
    def export_metrics(self) -> Dict:
        """Exportar todas las métricas"""
        return {
            'counters': dict(self._counters),
            'gauges': dict(self._gauges),
            'histograms': {
                k: self.get_histogram_stats(k.split(':')[0], 
                    dict(t.split('=') for t in k.split(':')[1].split(',')) if ':' in k else None)
                for k in self._histograms
            }
        }
    
    def reset_counters(self):
        """Resetear contadores (para inicio de día)"""
        with self._lock:
            # Guardar snapshot antes de resetear
            self.state_manager.log_event(
                'metrics_reset',
                None,
                'Daily metrics reset',
                {'snapshot': self.export_metrics()}
            )
            
            self._counters.clear()


class _Timer:
    """Context manager para medir tiempo"""
    
    def __init__(self, collector: MetricsCollector, name: str, tags: Dict = None):
        self.collector = collector
        self.name = name
        self.tags = tags
        self._start = None
    
    def __enter__(self):
        self._start = datetime.now()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        duration = (datetime.now() - self._start).total_seconds()
        self.collector.observe(self.name, duration, self.tags)
        return False
