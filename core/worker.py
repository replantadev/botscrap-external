#!/usr/bin/env python3
"""
Worker Manager - Ejecutor de trabajos del bot
"""

import logging
import time
import threading
import traceback
from datetime import datetime, timedelta
from typing import Dict, Optional, Callable
import uuid

logger = logging.getLogger(__name__)


class WorkerManager:
    """
    Gestor de ejecución de trabajos.
    Procesa jobs de la cola y ejecuta los bots correspondientes.
    """
    
    def __init__(self, job_queue, state_manager, rate_limiter, notifier=None):
        """
        Args:
            job_queue: JobQueue instance
            state_manager: StateManager instance  
            rate_limiter: RateLimiter instance
            notifier: Notifier instance (opcional)
        """
        self.job_queue = job_queue
        self.state_manager = state_manager
        self.rate_limiter = rate_limiter
        self.notifier = notifier
        
        self._running = False
        self._paused = False
        self._worker_thread: Optional[threading.Thread] = None
        self._current_job = None
        self._stop_event = threading.Event()
        
        # Bot executors
        self._executors: Dict[str, Callable] = {}
        
        # Config
        self.poll_interval = 10  # segundos entre polls
        self.heartbeat_interval = 30  # segundos entre heartbeats
        self._last_heartbeat = None
    
    def register_executor(self, bot_type: str, executor: Callable):
        """
        Registrar ejecutor para un tipo de bot.
        
        Args:
            bot_type: Tipo de bot (direct, resentment, social)
            executor: Función que ejecuta el bot, signature: (params: Dict) -> Dict
        """
        self._executors[bot_type] = executor
        logger.info(f"Registered executor for {bot_type}")
    
    def start(self):
        """Iniciar worker en background thread"""
        if self._running:
            logger.warning("Worker already running")
            return
        
        self._running = True
        self._paused = False
        self._stop_event.clear()
        
        self._worker_thread = threading.Thread(target=self._worker_loop, daemon=True)
        self._worker_thread.start()
        
        self.state_manager.set_worker_status('running')
        logger.info("Worker started")
        
        if self.notifier:
            self.notifier.send_status("🚀 Worker iniciado")
    
    def stop(self, timeout: int = 30):
        """
        Detener worker.
        
        Args:
            timeout: Segundos a esperar para que termine el job actual
        """
        if not self._running:
            return
        
        logger.info("Stopping worker...")
        self._running = False
        self._stop_event.set()
        
        if self._worker_thread and self._worker_thread.is_alive():
            self._worker_thread.join(timeout=timeout)
        
        self.state_manager.set_worker_status('stopped')
        logger.info("Worker stopped")
        
        if self.notifier:
            self.notifier.send_status("🛑 Worker detenido")
    
    def pause(self):
        """Pausar worker (termina job actual y espera)"""
        if not self._running:
            return
        
        self._paused = True
        self.state_manager.set_worker_status('paused')
        logger.info("Worker paused")
        
        if self.notifier:
            self.notifier.send_status("⏸️ Worker pausado")
    
    def resume(self):
        """Reanudar worker pausado"""
        if not self._running or not self._paused:
            return
        
        self._paused = False
        self.state_manager.set_worker_status('running')
        logger.info("Worker resumed")
        
        if self.notifier:
            self.notifier.send_status("▶️ Worker reanudado")
    
    def is_running(self) -> bool:
        """Verificar si worker está corriendo"""
        return self._running and not self._paused
    
    def is_paused(self) -> bool:
        """Verificar si worker está pausado"""
        return self._paused
    
    def get_current_job(self):
        """Obtener job actual en ejecución"""
        return self._current_job
    
    def get_status(self) -> Dict:
        """Obtener estado del worker"""
        return {
            'running': self._running,
            'paused': self._paused,
            'status': self.state_manager.get_worker_status(),
            'current_job': self._current_job.to_dict() if self._current_job else None,
            'last_heartbeat': self.state_manager.get_last_heartbeat(),
            'executors': list(self._executors.keys()),
            'pending_jobs': len(self.job_queue.get_pending()),
        }
    
    def _worker_loop(self):
        """Loop principal del worker"""
        logger.info("Worker loop started")
        
        while self._running:
            try:
                # Heartbeat
                self._update_heartbeat()
                
                # Si está pausado, solo esperar
                if self._paused:
                    self._stop_event.wait(timeout=self.poll_interval)
                    continue
                
                # Obtener siguiente job
                job = self.job_queue.get_next()
                
                if job:
                    self._execute_job(job)
                else:
                    # No hay jobs, esperar
                    self._stop_event.wait(timeout=self.poll_interval)
                
            except Exception as e:
                logger.error(f"Error in worker loop: {e}")
                traceback.print_exc()
                
                self.state_manager.log_event('worker_error', None, str(e))
                
                # Esperar antes de reintentar
                self._stop_event.wait(timeout=30)
        
        logger.info("Worker loop ended")
    
    def _update_heartbeat(self):
        """Actualizar heartbeat"""
        now = datetime.now()
        if not self._last_heartbeat or (now - self._last_heartbeat).total_seconds() > self.heartbeat_interval:
            self.state_manager.update_heartbeat()
            self._last_heartbeat = now
    
    def _send_callback(self, callback_url: str, job_id: str, status: str, result: Dict):
        """Enviar callback a StaffKit AI Orchestrator"""
        try:
            import requests
            
            payload = {
                'job_id': job_id,
                'status': status,
                'result': result,
                'timestamp': datetime.now().isoformat()
            }
            
            response = requests.post(
                callback_url,
                json=payload,
                timeout=10
            )
            
            if response.status_code == 200:
                logger.info(f"✅ Callback enviado a {callback_url}")
            else:
                logger.warning(f"⚠️ Callback falló: {response.status_code}")
                
        except Exception as e:
            logger.error(f"❌ Error enviando callback: {e}")
    
    def _execute_job(self, job):
        """Ejecutar un job"""
        from core.job_queue import JobStatus
        
        logger.info(f"Executing job {job.id} ({job.bot_type})")
        self._current_job = job
        
        # Marcar como running
        self.job_queue.mark_running(job.id)
        
        # Crear run_id
        run_id = f"{job.bot_type}_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{job.id}"
        self.state_manager.start_run(run_id, job.bot_type, job.params)
        
        try:
            # Verificar límites
            from config import DAILY_LIMIT
            if not self.state_manager.can_run_today(job.bot_type, DAILY_LIMIT):
                raise Exception(f"Daily limit reached for {job.bot_type}")
            
            # Obtener ejecutor
            executor = self._executors.get(job.bot_type)
            if not executor:
                raise Exception(f"No executor registered for {job.bot_type}")
            
            # Ejecutar bot
            start_time = time.time()
            result = executor(job.params)
            duration = time.time() - start_time
            
            # Procesar resultado
            stats = {
                'leads_found': result.get('leads_found', 0),
                'leads_saved': result.get('leads_saved', 0),
                'leads_duplicates': result.get('duplicates', 0),
                'leads_filtered': result.get('filtered', 0),
                'duration': duration,
            }
            
            # Marcar completado
            self.job_queue.mark_completed(job.id, stats)
            self.state_manager.end_run(run_id, 'completed', stats)
            
            logger.info(f"Job {job.id} completed: {stats['leads_saved']} leads saved in {duration:.1f}s")
            
            # Callback a StaffKit si fue ordenado desde allí
            if job.metadata and job.metadata.get('callback_url'):
                self._send_callback(job.metadata['callback_url'], job.id, 'completed', stats)
            
            # Notificar si hay leads
            if self.notifier and stats['leads_saved'] > 0:
                self.notifier.send_leads_found(
                    bot_type=job.bot_type,
                    leads_count=stats['leads_saved'],
                    total_found=stats['leads_found']
                )
            
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Job {job.id} failed: {error_msg}")
            traceback.print_exc()
            
            # Marcar como fallido (con retry si corresponde)
            self.job_queue.mark_failed(job.id, error_msg, retry=True)
            self.state_manager.end_run(run_id, 'failed', {'errors': [error_msg]})
            
            # Notificar error
            if self.notifier:
                self.notifier.send_error(
                    bot_type=job.bot_type,
                    error=error_msg
                )
        
        finally:
            self._current_job = None
            
            # Pequeña pausa entre jobs
            time.sleep(2)


def create_direct_executor(config: Dict = None):
    """
    Crear ejecutor para Direct Bot.
    
    Returns:
        Función ejecutora
    """
    def executor(params: Dict) -> Dict:
        from bots.direct_bot import DirectBot
        from staffkit_client import StaffKitClient
        
        # Merge config con params
        bot_config = {**(config or {}), **params}
        
        bot = DirectBot(config=bot_config)
        client = StaffKitClient()
        
        # Ejecutar búsqueda
        query = params.get('query', config.get('DEFAULT_QUERY', 'empresas sostenibles España'))
        max_results = params.get('max_results', 50)
        
        leads = bot.search(query, max_results=max_results)
        
        # Enviar a StaffKit
        saved = 0
        duplicates = 0
        
        for lead in leads:
            result = client.submit_lead(lead, source='direct_bot')
            if result.get('status') == 'success':
                saved += 1
            elif result.get('status') == 'duplicate':
                duplicates += 1
        
        return {
            'leads_found': len(leads),
            'leads_saved': saved,
            'duplicates': duplicates,
            'filtered': len(leads) - saved - duplicates,
        }
    
    return executor


def create_resentment_executor(config: Dict = None):
    """
    Crear ejecutor para Resentment Bot.
    
    Returns:
        Función ejecutora
    """
    def executor(params: Dict) -> Dict:
        from bots.resentment_bot import ResentmentBot
        from staffkit_client import StaffKitClient
        
        bot_config = {**(config or {}), **params}
        
        bot = ResentmentBot(config=bot_config)
        client = StaffKitClient()
        
        # Ejecutar búsqueda
        platforms = params.get('platforms', ['trustpilot', 'reddit'])
        
        leads = []
        for platform in platforms:
            platform_leads = bot.search(platform)
            leads.extend(platform_leads)
        
        # Enviar a StaffKit
        saved = 0
        duplicates = 0
        
        for lead in leads:
            result = client.submit_lead(lead, source='resentment_bot')
            if result.get('status') == 'success':
                saved += 1
            elif result.get('status') == 'duplicate':
                duplicates += 1
        
        return {
            'leads_found': len(leads),
            'leads_saved': saved,
            'duplicates': duplicates,
            'filtered': len(leads) - saved - duplicates,
        }
    
    return executor


def create_social_executor(config: Dict = None):
    """
    Crear ejecutor para Social Bot.
    
    Returns:
        Función ejecutora
    """
    def executor(params: Dict) -> Dict:
        from bots.social_bot import SocialBot
        from staffkit_client import StaffKitClient
        
        bot_config = {**(config or {}), **params}
        
        bot = SocialBot(config=bot_config)
        client = StaffKitClient()
        
        # Ejecutar búsqueda
        leads = bot.search()
        
        # Enviar a StaffKit
        saved = 0
        duplicates = 0
        
        for lead in leads:
            result = client.submit_lead(lead, source='social_bot')
            if result.get('status') == 'success':
                saved += 1
            elif result.get('status') == 'duplicate':
                duplicates += 1
        
        return {
            'leads_found': len(leads),
            'leads_saved': saved,
            'duplicates': duplicates,
            'filtered': len(leads) - saved - duplicates,
        }
    
    return executor
