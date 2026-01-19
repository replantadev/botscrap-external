#!/usr/bin/env python3
"""
Scheduler - Programación de ejecuciones con APScheduler
"""

import logging
from datetime import datetime, time, timedelta
from typing import Dict, List, Optional, Callable
from dataclasses import dataclass
import json

logger = logging.getLogger(__name__)

try:
    from apscheduler.schedulers.background import BackgroundScheduler
    from apscheduler.triggers.cron import CronTrigger
    from apscheduler.triggers.interval import IntervalTrigger
    from apscheduler.jobstores.memory import MemoryJobStore
    HAS_APSCHEDULER = True
except ImportError:
    HAS_APSCHEDULER = False
    logger.warning("APScheduler not installed. Scheduler will run in basic mode.")


@dataclass
class Schedule:
    """Configuración de schedule"""
    id: str
    bot_type: str
    enabled: bool = True
    cron: str = None           # Expresión cron: "0 9 * * *" (9am diario)
    interval_hours: float = None  # O intervalo en horas
    params: Dict = None
    priority: int = 3
    description: str = ''
    last_run: str = None
    next_run: str = None


class BotScheduler:
    """
    Programador de bots usando APScheduler.
    Soporta expresiones cron e intervalos.
    """
    
    # Schedules por defecto
    DEFAULT_SCHEDULES = {
        'direct_morning': {
            'bot_type': 'direct',
            'cron': '0 9 * * 1-5',  # Lun-Vie 9:00
            'description': 'Direct Bot - Búsqueda matutina',
            'params': {'max_results': 50}
        },
        'direct_afternoon': {
            'bot_type': 'direct',
            'cron': '0 15 * * 1-5',  # Lun-Vie 15:00
            'description': 'Direct Bot - Búsqueda vespertina',
            'params': {'max_results': 30}
        },
        'resentment_daily': {
            'bot_type': 'resentment',
            'cron': '0 10 * * 1-5',  # Lun-Vie 10:00
            'description': 'Resentment Bot - Búsqueda diaria',
            'params': {'platforms': ['trustpilot', 'reddit']}
        },
        'social_weekly': {
            'bot_type': 'social',
            'cron': '0 11 * * 1',  # Lunes 11:00
            'description': 'Social Bot - Monitoreo semanal',
            'params': {}
        },
    }
    
    def __init__(self, job_queue, state_manager, db_path: str = None):
        """
        Args:
            job_queue: JobQueue instance
            state_manager: StateManager instance
            db_path: Path para persistencia de APScheduler
        """
        self.job_queue = job_queue
        self.state_manager = state_manager
        self.db_path = db_path
        
        self._schedules: Dict[str, Schedule] = {}
        self._scheduler = None
        self._callbacks: Dict[str, Callable] = {}
        
        self._load_schedules()
    
    def _load_schedules(self):
        """Cargar schedules desde state manager o usar defaults"""
        saved = self.state_manager.get_state('schedules')
        
        if saved:
            for sched_id, data in saved.items():
                self._schedules[sched_id] = Schedule(id=sched_id, **data)
        else:
            # Usar defaults
            for sched_id, config in self.DEFAULT_SCHEDULES.items():
                self._schedules[sched_id] = Schedule(
                    id=sched_id,
                    **config
                )
            self._save_schedules()
    
    def _save_schedules(self):
        """Guardar schedules"""
        data = {}
        for sched_id, sched in self._schedules.items():
            data[sched_id] = {
                'bot_type': sched.bot_type,
                'enabled': sched.enabled,
                'cron': sched.cron,
                'interval_hours': sched.interval_hours,
                'params': sched.params,
                'priority': sched.priority,
                'description': sched.description,
                'last_run': sched.last_run,
                'next_run': sched.next_run,
            }
        self.state_manager.set_state('schedules', data)
    
    def start(self):
        """Iniciar scheduler"""
        if not HAS_APSCHEDULER:
            logger.error("Cannot start scheduler: APScheduler not installed")
            return False
        
        if self._scheduler and self._scheduler.running:
            logger.warning("Scheduler already running")
            return True
        
        # Usar MemoryJobStore - nuestra persistencia es vía StateManager
        # No usamos SQLAlchemyJobStore porque los callbacks contienen objetos no serializables
        jobstores = {
            'default': MemoryJobStore()
        }
        
        self._scheduler = BackgroundScheduler(jobstores=jobstores)
        
        # Añadir jobs
        for sched_id, sched in self._schedules.items():
            if sched.enabled:
                self._add_scheduler_job(sched)
        
        # Job de mantenimiento cada hora
        self._scheduler.add_job(
            self._maintenance_job,
            IntervalTrigger(hours=1),
            id='maintenance',
            replace_existing=True
        )
        
        self._scheduler.start()
        logger.info("Scheduler started")
        
        self.state_manager.log_event('scheduler_started', None, 'Bot scheduler started')
        return True
    
    def stop(self):
        """Detener scheduler"""
        if self._scheduler and self._scheduler.running:
            self._scheduler.shutdown(wait=False)
            logger.info("Scheduler stopped")
            self.state_manager.log_event('scheduler_stopped', None, 'Bot scheduler stopped')
    
    def is_running(self) -> bool:
        """Verificar si scheduler está corriendo"""
        return self._scheduler is not None and self._scheduler.running
    
    def _add_scheduler_job(self, sched: Schedule):
        """Añadir job al APScheduler"""
        if not self._scheduler:
            return
        
        try:
            if sched.cron:
                trigger = CronTrigger.from_crontab(sched.cron)
            elif sched.interval_hours:
                trigger = IntervalTrigger(hours=sched.interval_hours)
            else:
                logger.warning(f"Schedule {sched.id} has no trigger configured")
                return
            
            self._scheduler.add_job(
                self._trigger_scheduled_job,
                trigger,
                args=[sched.id],
                id=sched.id,
                replace_existing=True,
                name=sched.description or sched.id
            )
            
            # Actualizar next_run
            job = self._scheduler.get_job(sched.id)
            if job and job.next_run_time:
                sched.next_run = job.next_run_time.isoformat()
                self._save_schedules()
            
            logger.info(f"Scheduled job added: {sched.id} -> {sched.cron or f'{sched.interval_hours}h'}")
            
        except Exception as e:
            logger.error(f"Error adding scheduled job {sched.id}: {e}")
    
    def _trigger_scheduled_job(self, schedule_id: str):
        """Ejecutar cuando el schedule se dispara"""
        sched = self._schedules.get(schedule_id)
        if not sched:
            return
        
        logger.info(f"Schedule triggered: {schedule_id}")
        
        # Verificar límites diarios con límite específico del bot
        from config import get_daily_limit, AUTO_RETRY_ENABLED, AUTO_RETRY_INTERVAL, AUTO_RETRY_MAX_HOUR
        
        daily_limit = get_daily_limit(sched.bot_type)
        leads_today = self.state_manager.get_leads_today(sched.bot_type)
        remaining = daily_limit - leads_today
        
        if remaining <= 0:
            logger.info(f"✅ Schedule {schedule_id} skipped: daily goal reached ({leads_today}/{daily_limit})")
            return
        
        logger.info(f"📊 {sched.bot_type}: {leads_today}/{daily_limit} leads hoy, faltan {remaining}")
        
        # Crear job en la cola
        job_id = self.job_queue.create(
            bot_type=sched.bot_type,
            params=sched.params or {},
            priority=sched.priority,
            source='scheduled'
        )
        
        # Actualizar último run
        sched.last_run = datetime.now().isoformat()
        
        # Actualizar next_run
        if self._scheduler:
            job = self._scheduler.get_job(schedule_id)
            if job and job.next_run_time:
                sched.next_run = job.next_run_time.isoformat()
        
        self._save_schedules()
        
        self.state_manager.log_event(
            'schedule_triggered', 
            sched.bot_type,
            f'Schedule {schedule_id} triggered (job {job_id})',
            {'schedule_id': schedule_id, 'job_id': job_id, 'leads_today': leads_today, 'goal': daily_limit}
        )
        
        # Auto-retry: Programar re-ejecución si está habilitado y es temprano
        if AUTO_RETRY_ENABLED and remaining > 0:
            current_hour = datetime.now().hour
            if current_hour < AUTO_RETRY_MAX_HOUR:
                self._schedule_retry(sched.bot_type, schedule_id, AUTO_RETRY_INTERVAL)
    
    def _schedule_retry(self, bot_type: str, schedule_id: str, minutes: int):
        """Programar re-ejecución automática"""
        retry_id = f"{schedule_id}_retry"
        
        # Eliminar retry anterior si existe
        if self._scheduler and self._scheduler.get_job(retry_id):
            self._scheduler.remove_job(retry_id)
        
        # Programar nuevo retry
        if self._scheduler:
            run_time = datetime.now() + timedelta(minutes=minutes)
            self._scheduler.add_job(
                self._trigger_scheduled_job,
                'date',
                run_date=run_time,
                args=[schedule_id],
                id=retry_id,
                replace_existing=True,
                name=f"Retry {schedule_id}"
            )
            logger.info(f"🔄 Auto-retry programado para {bot_type} en {minutes} minutos ({run_time.strftime('%H:%M')})")
    
    def _maintenance_job(self):
        """Job de mantenimiento periódico"""
        # Limpiar jobs antiguos
        self.job_queue.cleanup_old(days=7)
        
        # Limpiar jobs atascados
        self.job_queue.clear_stuck(timeout_minutes=60)
        
        logger.debug("Maintenance job completed")
    
    # === CRUD de Schedules ===
    
    def get_schedules(self) -> List[Schedule]:
        """Obtener todos los schedules"""
        return list(self._schedules.values())
    
    def get_schedule(self, schedule_id: str) -> Optional[Schedule]:
        """Obtener schedule por ID"""
        return self._schedules.get(schedule_id)
    
    def add_schedule(self, schedule_id: str, bot_type: str, cron: str = None,
                    interval_hours: float = None, params: Dict = None,
                    priority: int = 3, description: str = '') -> Schedule:
        """Añadir nuevo schedule"""
        sched = Schedule(
            id=schedule_id,
            bot_type=bot_type,
            cron=cron,
            interval_hours=interval_hours,
            params=params or {},
            priority=priority,
            description=description,
            enabled=True
        )
        
        self._schedules[schedule_id] = sched
        self._save_schedules()
        
        if self._scheduler and self._scheduler.running:
            self._add_scheduler_job(sched)
        
        return sched
    
    def update_schedule(self, schedule_id: str, **kwargs) -> Optional[Schedule]:
        """Actualizar schedule"""
        sched = self._schedules.get(schedule_id)
        if not sched:
            return None
        
        for key, value in kwargs.items():
            if hasattr(sched, key):
                setattr(sched, key, value)
        
        self._save_schedules()
        
        # Re-añadir al scheduler si está corriendo
        if self._scheduler and self._scheduler.running:
            try:
                self._scheduler.remove_job(schedule_id)
            except:
                pass
            
            if sched.enabled:
                self._add_scheduler_job(sched)
        
        return sched
    
    def enable_schedule(self, schedule_id: str):
        """Habilitar schedule"""
        return self.update_schedule(schedule_id, enabled=True)
    
    def disable_schedule(self, schedule_id: str):
        """Deshabilitar schedule"""
        sched = self.update_schedule(schedule_id, enabled=False)
        
        if self._scheduler and self._scheduler.running:
            try:
                self._scheduler.remove_job(schedule_id)
            except:
                pass
        
        return sched
    
    def delete_schedule(self, schedule_id: str) -> bool:
        """Eliminar schedule"""
        if schedule_id not in self._schedules:
            return False
        
        if self._scheduler and self._scheduler.running:
            try:
                self._scheduler.remove_job(schedule_id)
            except:
                pass
        
        del self._schedules[schedule_id]
        self._save_schedules()
        return True
    
    def run_now(self, schedule_id: str):
        """Ejecutar schedule inmediatamente"""
        sched = self._schedules.get(schedule_id)
        if not sched:
            return None
        
        return self.job_queue.create(
            bot_type=sched.bot_type,
            params=sched.params or {},
            priority=sched.priority,
            source='manual'
        )
    
    def get_upcoming(self, hours: int = 24) -> List[Dict]:
        """Obtener schedules que se ejecutarán en las próximas N horas"""
        upcoming = []
        cutoff = datetime.now() + timedelta(hours=hours)
        
        for sched in self._schedules.values():
            if sched.enabled and sched.next_run:
                next_run = datetime.fromisoformat(sched.next_run)
                if next_run <= cutoff:
                    upcoming.append({
                        'id': sched.id,
                        'bot_type': sched.bot_type,
                        'description': sched.description,
                        'next_run': sched.next_run,
                        'in_minutes': int((next_run - datetime.now()).total_seconds() / 60)
                    })
        
        return sorted(upcoming, key=lambda x: x['next_run'])
