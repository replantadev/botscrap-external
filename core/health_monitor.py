#!/usr/bin/env python3
"""
Health Monitor - Monitoreo y recuperación automática
"""

import logging
import threading
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Callable
import traceback

logger = logging.getLogger(__name__)


class HealthCheck:
    """Resultado de health check"""
    
    def __init__(self, name: str, healthy: bool, message: str = '', details: Dict = None):
        self.name = name
        self.healthy = healthy
        self.message = message
        self.details = details or {}
        self.timestamp = datetime.now()
    
    def to_dict(self) -> Dict:
        return {
            'name': self.name,
            'healthy': self.healthy,
            'message': self.message,
            'details': self.details,
            'timestamp': self.timestamp.isoformat(),
        }


class HealthMonitor:
    """
    Monitor de salud del sistema con recuperación automática.
    
    Funcionalidades:
    - Health checks periódicos
    - Detección de worker muerto (heartbeat)
    - Alertas por Telegram
    - Recovery automático
    """
    
    def __init__(self, state_manager, worker_manager, notifier=None):
        """
        Args:
            state_manager: StateManager instance
            worker_manager: WorkerManager instance
            notifier: Notifier instance (opcional)
        """
        self.state_manager = state_manager
        self.worker_manager = worker_manager
        self.notifier = notifier
        
        self._running = False
        self._monitor_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        
        # Config
        self.check_interval = 60  # segundos entre checks
        self.heartbeat_timeout = 120  # segundos sin heartbeat = problema
        self.max_recovery_attempts = 3
        
        # Estado
        self._recovery_attempts = 0
        self._last_healthy = None
        self._health_history: List[Dict] = []
        
        # Checks customizables
        self._custom_checks: Dict[str, Callable] = {}
    
    def register_check(self, name: str, check_func: Callable):
        """
        Registrar health check personalizado.
        
        Args:
            name: Nombre del check
            check_func: Función que retorna HealthCheck
        """
        self._custom_checks[name] = check_func
    
    def start(self):
        """Iniciar monitor"""
        if self._running:
            logger.warning("Health monitor already running")
            return
        
        self._running = True
        self._stop_event.clear()
        
        self._monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self._monitor_thread.start()
        
        logger.info("Health monitor started")
    
    def stop(self):
        """Detener monitor"""
        if not self._running:
            return
        
        self._running = False
        self._stop_event.set()
        
        if self._monitor_thread and self._monitor_thread.is_alive():
            self._monitor_thread.join(timeout=10)
        
        logger.info("Health monitor stopped")
    
    def _monitor_loop(self):
        """Loop principal de monitoreo"""
        while self._running:
            try:
                results = self.run_checks()
                self._process_results(results)
                
            except Exception as e:
                logger.error(f"Error in health monitor: {e}")
                traceback.print_exc()
            
            self._stop_event.wait(timeout=self.check_interval)
    
    def run_checks(self) -> List[HealthCheck]:
        """Ejecutar todos los health checks"""
        results = []
        
        # 1. Check heartbeat del worker
        results.append(self._check_heartbeat())
        
        # 2. Check estado del worker
        results.append(self._check_worker_status())
        
        # 3. Check conectividad a StaffKit
        results.append(self._check_staffkit())
        
        # 4. Check espacio en disco
        results.append(self._check_disk_space())
        
        # 5. Check rate limits
        results.append(self._check_rate_limits())
        
        # 6. Checks personalizados
        for name, check_func in self._custom_checks.items():
            try:
                result = check_func()
                results.append(result)
            except Exception as e:
                results.append(HealthCheck(name, False, f"Check failed: {e}"))
        
        return results
    
    def _check_heartbeat(self) -> HealthCheck:
        """Verificar heartbeat del worker"""
        last_hb = self.state_manager.get_last_heartbeat()
        
        if not last_hb:
            return HealthCheck(
                'heartbeat',
                False,
                'No heartbeat recorded',
                {'last_heartbeat': None}
            )
        
        age = (datetime.now() - last_hb).total_seconds()
        healthy = age < self.heartbeat_timeout
        
        return HealthCheck(
            'heartbeat',
            healthy,
            f"Last heartbeat {int(age)}s ago" + ("" if healthy else " (STALE)"),
            {'last_heartbeat': last_hb.isoformat(), 'age_seconds': int(age)}
        )
    
    def _check_worker_status(self) -> HealthCheck:
        """Verificar estado del worker"""
        status = self.state_manager.get_worker_status()
        worker_running = self.worker_manager.is_running()
        
        healthy = True
        message = f"Worker status: {status}"
        
        if status == 'running' and not worker_running:
            healthy = False
            message = "Worker claims running but thread not active"
        elif status == 'error':
            healthy = False
            message = "Worker in error state"
        
        return HealthCheck(
            'worker_status',
            healthy,
            message,
            {
                'status': status,
                'thread_active': worker_running,
                'paused': self.worker_manager.is_paused(),
                'current_job': self.worker_manager.get_current_job().id if self.worker_manager.get_current_job() else None
            }
        )
    
    def _check_staffkit(self) -> HealthCheck:
        """Verificar conexión a StaffKit API"""
        try:
            from staffkit_client import StaffKitClient
            import config
            
            client = StaffKitClient()
            result = client.check_connection()
            
            healthy = result.get('status') == 'ok'
            
            return HealthCheck(
                'staffkit_api',
                healthy,
                'StaffKit API ' + ('connected' if healthy else 'unreachable'),
                {'response': result}
            )
        except Exception as e:
            return HealthCheck(
                'staffkit_api',
                False,
                f'StaffKit check failed: {e}'
            )
    
    def _check_disk_space(self) -> HealthCheck:
        """Verificar espacio en disco"""
        try:
            import shutil
            from config import DATA_DIR
            
            total, used, free = shutil.disk_usage(DATA_DIR)
            free_percent = (free / total) * 100
            
            healthy = free_percent > 10  # Mínimo 10% libre
            
            return HealthCheck(
                'disk_space',
                healthy,
                f"{free_percent:.1f}% free ({free // (1024**3)}GB)",
                {
                    'total_gb': total // (1024**3),
                    'used_gb': used // (1024**3),
                    'free_gb': free // (1024**3),
                    'free_percent': round(free_percent, 1)
                }
            )
        except Exception as e:
            return HealthCheck('disk_space', True, f'Check skipped: {e}')
    
    def _check_rate_limits(self) -> HealthCheck:
        """Verificar estado de rate limits"""
        try:
            from core.rate_limiter import RateLimiter
            
            # Obtener rate limiter global si existe
            limiter = getattr(self, '_rate_limiter', None)
            if not limiter:
                return HealthCheck('rate_limits', True, 'No rate limiter configured')
            
            all_status = limiter.get_all_status()
            
            # Verificar si alguno está en backoff o muy alto
            issues = []
            for api, status in all_status.items():
                if status.get('in_backoff'):
                    issues.append(f"{api}: in backoff until {status.get('backoff_until')}")
                elif status.get('percentage', 0) > 90:
                    issues.append(f"{api}: {status['percentage']}% of limit")
            
            healthy = len(issues) == 0
            
            return HealthCheck(
                'rate_limits',
                healthy,
                'OK' if healthy else '; '.join(issues),
                all_status
            )
        except Exception as e:
            return HealthCheck('rate_limits', True, f'Check skipped: {e}')
    
    def _process_results(self, results: List[HealthCheck]):
        """Procesar resultados de health checks"""
        all_healthy = all(r.healthy for r in results)
        
        # Guardar en historial
        self._health_history.append({
            'timestamp': datetime.now().isoformat(),
            'healthy': all_healthy,
            'checks': [r.to_dict() for r in results]
        })
        
        # Mantener solo últimas 100 entradas
        if len(self._health_history) > 100:
            self._health_history = self._health_history[-100:]
        
        if all_healthy:
            self._last_healthy = datetime.now()
            self._recovery_attempts = 0
            logger.debug("Health check passed")
        else:
            # Identificar problemas
            issues = [r for r in results if not r.healthy]
            logger.warning(f"Health check failed: {[i.name for i in issues]}")
            
            # Intentar recovery
            self._attempt_recovery(issues)
            
            # Alertar
            self._alert_unhealthy(issues)
    
    def _attempt_recovery(self, issues: List[HealthCheck]):
        """Intentar recuperación automática"""
        if self._recovery_attempts >= self.max_recovery_attempts:
            logger.error("Max recovery attempts reached")
            self.state_manager.set_worker_status('error')
            
            if self.notifier:
                self.notifier.send_critical(
                    "🚨 Max recovery attempts reached. Worker stopped."
                )
            return
        
        self._recovery_attempts += 1
        logger.info(f"Attempting recovery ({self._recovery_attempts}/{self.max_recovery_attempts})")
        
        for issue in issues:
            if issue.name == 'heartbeat' or issue.name == 'worker_status':
                self._recover_worker()
                break
    
    def _recover_worker(self):
        """Intentar recuperar worker"""
        logger.info("Attempting worker recovery...")
        
        try:
            # Detener worker actual
            self.worker_manager.stop(timeout=10)
            time.sleep(2)
            
            # Reiniciar
            self.worker_manager.start()
            
            logger.info("Worker recovery attempted")
            
            self.state_manager.log_event(
                'recovery_attempted',
                None,
                f'Worker recovery attempt {self._recovery_attempts}'
            )
            
            if self.notifier:
                self.notifier.send_status(
                    f"🔄 Worker reiniciado (intento {self._recovery_attempts})"
                )
                
        except Exception as e:
            logger.error(f"Recovery failed: {e}")
    
    def _alert_unhealthy(self, issues: List[HealthCheck]):
        """Alertar sobre problemas de salud"""
        if not self.notifier:
            return
        
        messages = []
        for issue in issues:
            messages.append(f"❌ {issue.name}: {issue.message}")
        
        self.notifier.send_error(
            "Health Check",
            "\n".join(messages)
        )
    
    def get_health_status(self) -> Dict:
        """Obtener estado de salud actual"""
        results = self.run_checks()
        all_healthy = all(r.healthy for r in results)
        
        return {
            'healthy': all_healthy,
            'timestamp': datetime.now().isoformat(),
            'last_healthy': self._last_healthy.isoformat() if self._last_healthy else None,
            'recovery_attempts': self._recovery_attempts,
            'checks': [r.to_dict() for r in results]
        }
    
    def get_health_history(self, limit: int = 20) -> List[Dict]:
        """Obtener historial de health checks"""
        return self._health_history[-limit:]
