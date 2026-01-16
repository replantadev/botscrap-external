#!/usr/bin/env python3
"""
Orchestrator - Orquestador principal del worker autónomo 24/7
"""

import logging
import signal
import sys
import time
import threading
from datetime import datetime, timedelta
from typing import Dict, Optional

logger = logging.getLogger(__name__)


class Orchestrator:
    """
    Orquestador principal que coordina todos los componentes:
    - StateManager: Persistencia de estado
    - JobQueue: Cola de trabajos
    - BotScheduler: Programación de ejecuciones
    - WorkerManager: Ejecución de trabajos
    - HealthMonitor: Monitoreo y recovery
    - RateLimiter: Control de rate limits
    - Notifier: Notificaciones Telegram
    - MetricsCollector: Métricas y estadísticas
    """
    
    def __init__(self, config: Dict = None):
        """
        Args:
            config: Configuración override
        """
        self.config = config or {}
        self._components_initialized = False
        self._running = False
        
        # Componentes (se inicializan en setup())
        self.state_manager = None
        self.rate_limiter = None
        self.job_queue = None
        self.scheduler = None
        self.worker = None
        self.health_monitor = None
        self.notifier = None
        self.metrics = None
    
    def setup(self):
        """Inicializar todos los componentes"""
        if self._components_initialized:
            return
        
        logger.info("Setting up orchestrator components...")
        
        # 1. State Manager
        from core.state_manager import StateManager
        self.state_manager = StateManager()
        logger.info("✓ StateManager initialized")
        
        # 2. Rate Limiter
        from core.rate_limiter import RateLimiter
        from config import DATA_DIR
        self.rate_limiter = RateLimiter()
        self.rate_limiter.set_persistence(str(DATA_DIR / 'rate_limits.json'))
        logger.info("✓ RateLimiter initialized")
        
        # 3. Job Queue
        from core.job_queue import JobQueue
        self.job_queue = JobQueue()
        logger.info("✓ JobQueue initialized")
        
        # 4. Notifier
        from core.notifier import Notifier
        self.notifier = Notifier()
        if self.notifier.enabled:
            logger.info("✓ Notifier initialized (Telegram enabled)")
        else:
            logger.info("✓ Notifier initialized (Telegram disabled)")
        
        # 5. Worker Manager
        from core.worker import (
            WorkerManager, 
            create_direct_executor,
            create_resentment_executor,
            create_social_executor
        )
        self.worker = WorkerManager(
            job_queue=self.job_queue,
            state_manager=self.state_manager,
            rate_limiter=self.rate_limiter,
            notifier=self.notifier
        )
        
        # Registrar ejecutores
        self.worker.register_executor('direct', create_direct_executor(self.config))
        self.worker.register_executor('resentment', create_resentment_executor(self.config))
        self.worker.register_executor('social', create_social_executor(self.config))
        logger.info("✓ WorkerManager initialized with executors")
        
        # 6. Scheduler
        from core.scheduler import BotScheduler
        self.scheduler = BotScheduler(
            job_queue=self.job_queue,
            state_manager=self.state_manager,
            db_path=str(DATA_DIR / 'scheduler.db')
        )
        logger.info("✓ Scheduler initialized")
        
        # 7. Health Monitor
        from core.health_monitor import HealthMonitor
        self.health_monitor = HealthMonitor(
            state_manager=self.state_manager,
            worker_manager=self.worker,
            notifier=self.notifier
        )
        logger.info("✓ HealthMonitor initialized")
        
        # 8. Metrics
        from core.metrics import MetricsCollector
        self.metrics = MetricsCollector(self.state_manager)
        logger.info("✓ MetricsCollector initialized")
        
        self._components_initialized = True
        logger.info("All components initialized successfully")
    
    def start(self):
        """Iniciar el orquestador y todos los componentes"""
        if self._running:
            logger.warning("Orchestrator already running")
            return
        
        if not self._components_initialized:
            self.setup()
        
        logger.info("Starting orchestrator...")
        self._running = True
        
        # Configurar signal handlers
        self._setup_signals()
        
        # Iniciar componentes
        try:
            # 1. Worker
            self.worker.start()
            logger.info("Worker started")
            
            # 2. Scheduler
            self.scheduler.start()
            logger.info("Scheduler started")
            
            # 3. Health Monitor
            self.health_monitor.start()
            logger.info("Health monitor started")
            
            # Actualizar estado
            self.state_manager.set_worker_status('running')
            self.state_manager.log_event('orchestrator_started', None, 'Orchestrator started')
            
            # Notificar
            if self.notifier:
                self.notifier.send_status("🚀 Bot Worker iniciado y funcionando")
            
            logger.info("=" * 50)
            logger.info("ORCHESTRATOR RUNNING")
            logger.info("=" * 50)
            
        except Exception as e:
            logger.error(f"Error starting orchestrator: {e}")
            self.stop()
            raise
    
    def stop(self, reason: str = "manual"):
        """Detener el orquestador y todos los componentes"""
        if not self._running:
            return
        
        logger.info(f"Stopping orchestrator (reason: {reason})...")
        self._running = False
        
        # Detener componentes en orden inverso
        try:
            if self.health_monitor:
                self.health_monitor.stop()
                logger.info("Health monitor stopped")
            
            if self.scheduler:
                self.scheduler.stop()
                logger.info("Scheduler stopped")
            
            if self.worker:
                self.worker.stop()
                logger.info("Worker stopped")
            
            if self.notifier:
                self.notifier.stop()
                self.notifier.send_status(f"🛑 Bot Worker detenido ({reason})")
            
            # Actualizar estado
            if self.state_manager:
                self.state_manager.set_worker_status('stopped')
                self.state_manager.log_event('orchestrator_stopped', None, f'Reason: {reason}')
            
            logger.info("Orchestrator stopped successfully")
            
        except Exception as e:
            logger.error(f"Error stopping orchestrator: {e}")
    
    def pause(self):
        """Pausar el worker (mantiene scheduler)"""
        if self.worker:
            self.worker.pause()
    
    def resume(self):
        """Reanudar el worker"""
        if self.worker:
            self.worker.resume()
    
    def _setup_signals(self):
        """Configurar signal handlers para graceful shutdown"""
        def signal_handler(signum, frame):
            logger.info(f"Received signal {signum}")
            self.stop(reason=f"signal_{signum}")
            sys.exit(0)
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
    
    def run_forever(self):
        """Ejecutar el orquestador indefinidamente"""
        self.start()
        
        try:
            while self._running:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Keyboard interrupt received")
        finally:
            self.stop(reason="shutdown")
    
    # === API para dashboard ===
    
    def get_status(self) -> Dict:
        """Obtener estado completo del sistema"""
        return {
            'running': self._running,
            'worker': self.worker.get_status() if self.worker else None,
            'scheduler_running': self.scheduler.is_running() if self.scheduler else False,
            'health': self.health_monitor.get_health_status() if self.health_monitor else None,
            'stats': self.state_manager.get_stats_summary() if self.state_manager else None,
            'rate_limits': self.rate_limiter.get_all_status() if self.rate_limiter else None,
            'queue': {
                'pending': len(self.job_queue.get_pending()) if self.job_queue else 0,
                'running': len(self.job_queue.get_running()) if self.job_queue else 0,
            }
        }
    
    def add_job(self, bot_type: str, params: Dict = None, priority: int = 3) -> str:
        """Añadir job manual a la cola"""
        if not self.job_queue:
            raise RuntimeError("JobQueue not initialized")
        
        return self.job_queue.create(
            bot_type=bot_type,
            params=params or {},
            priority=priority,
            source='manual'
        )
    
    def get_schedules(self):
        """Obtener programación actual"""
        if self.scheduler:
            return self.scheduler.get_schedules()
        return []
    
    def update_schedule(self, schedule_id: str, **kwargs):
        """Actualizar un schedule"""
        if self.scheduler:
            return self.scheduler.update_schedule(schedule_id, **kwargs)
        return None
    
    def get_job_history(self, limit: int = 50):
        """Obtener historial de jobs"""
        if self.job_queue:
            return self.job_queue.get_history(limit)
        return []
    
    def get_metrics_summary(self):
        """Obtener resumen de métricas"""
        if self.metrics:
            return self.metrics.get_daily_stats()
        return {}


# Singleton global
_orchestrator: Optional[Orchestrator] = None


def get_orchestrator() -> Orchestrator:
    """Obtener instancia global del orquestador"""
    global _orchestrator
    if _orchestrator is None:
        _orchestrator = Orchestrator()
    return _orchestrator


def main():
    """Entry point para ejecución directa"""
    import argparse
    from config import LOG_LEVEL
    
    # Configurar logging
    logging.basicConfig(
        level=getattr(logging, LOG_LEVEL),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('logs/orchestrator.log')
        ]
    )
    
    parser = argparse.ArgumentParser(description='Bot Worker Orchestrator')
    parser.add_argument('--daemon', '-d', action='store_true', help='Run as daemon')
    parser.add_argument('--test', '-t', action='store_true', help='Test configuration')
    args = parser.parse_args()
    
    orchestrator = get_orchestrator()
    
    if args.test:
        print("Testing configuration...")
        orchestrator.setup()
        print("Configuration OK")
        
        # Test notifier
        if orchestrator.notifier.enabled:
            print("Testing Telegram connection...")
            if orchestrator.notifier.test_connection():
                print("Telegram OK")
            else:
                print("Telegram FAILED")
        
        return
    
    # Ejecutar
    orchestrator.run_forever()


if __name__ == '__main__':
    main()
