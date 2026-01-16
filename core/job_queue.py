#!/usr/bin/env python3
"""
Job Queue - Cola de trabajos con prioridades y reintentos
"""

import sqlite3
import json
import logging
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
from enum import Enum
from contextlib import contextmanager
import threading

logger = logging.getLogger(__name__)


class JobStatus(Enum):
    PENDING = 'pending'
    RUNNING = 'running'
    COMPLETED = 'completed'
    FAILED = 'failed'
    CANCELLED = 'cancelled'
    RETRY = 'retry'


class JobPriority(Enum):
    HOT = 1         # Leads calientes, ejecutar ASAP
    HIGH = 2        # Alta prioridad
    NORMAL = 3      # Normal
    LOW = 4         # Background tasks
    

@dataclass
class Job:
    """Representa un trabajo en la cola"""
    id: str
    bot_type: str
    params: Dict[str, Any]
    priority: int = JobPriority.NORMAL.value
    status: str = JobStatus.PENDING.value
    source: str = 'manual'  # manual, scheduled, governance, retry
    created_at: str = None
    started_at: str = None
    completed_at: str = None
    retry_count: int = 0
    max_retries: int = 3
    error: str = None
    result: Dict = None
    scheduled_for: str = None  # Para jobs programados
    
    def __post_init__(self):
        if not self.id:
            self.id = str(uuid.uuid4())[:8]
        if not self.created_at:
            self.created_at = datetime.now().isoformat()
    
    def to_dict(self) -> Dict:
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'Job':
        return cls(**data)


class JobQueue:
    """Cola de trabajos persistente con SQLite"""
    
    def __init__(self, db_path: str = None):
        from config import DATA_DIR
        self.db_path = db_path or str(DATA_DIR / 'job_queue.db')
        self._lock = threading.Lock()
        self._init_db()
    
    def _init_db(self):
        """Inicializar base de datos"""
        with self._get_connection() as conn:
            conn.executescript('''
                CREATE TABLE IF NOT EXISTS jobs (
                    id TEXT PRIMARY KEY,
                    bot_type TEXT NOT NULL,
                    params TEXT,
                    priority INTEGER DEFAULT 3,
                    status TEXT DEFAULT 'pending',
                    source TEXT DEFAULT 'manual',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    started_at TIMESTAMP,
                    completed_at TIMESTAMP,
                    retry_count INTEGER DEFAULT 0,
                    max_retries INTEGER DEFAULT 3,
                    error TEXT,
                    result TEXT,
                    scheduled_for TIMESTAMP
                );
                
                CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);
                CREATE INDEX IF NOT EXISTS idx_jobs_priority ON jobs(priority);
                CREATE INDEX IF NOT EXISTS idx_jobs_scheduled ON jobs(scheduled_for);
                CREATE INDEX IF NOT EXISTS idx_jobs_bot ON jobs(bot_type);
            ''')
    
    @contextmanager
    def _get_connection(self):
        """Context manager para conexiones"""
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()
    
    def _row_to_job(self, row) -> Job:
        """Convertir row de SQLite a Job"""
        return Job(
            id=row['id'],
            bot_type=row['bot_type'],
            params=json.loads(row['params']) if row['params'] else {},
            priority=row['priority'],
            status=row['status'],
            source=row['source'],
            created_at=row['created_at'],
            started_at=row['started_at'],
            completed_at=row['completed_at'],
            retry_count=row['retry_count'],
            max_retries=row['max_retries'],
            error=row['error'],
            result=json.loads(row['result']) if row['result'] else None,
            scheduled_for=row['scheduled_for'],
        )
    
    def add(self, job: Job) -> str:
        """
        Añadir trabajo a la cola.
        
        Returns:
            ID del job
        """
        with self._lock:
            with self._get_connection() as conn:
                conn.execute('''
                    INSERT INTO jobs (id, bot_type, params, priority, status, source, 
                                     created_at, max_retries, scheduled_for)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    job.id,
                    job.bot_type,
                    json.dumps(job.params),
                    job.priority,
                    job.status,
                    job.source,
                    job.created_at,
                    job.max_retries,
                    job.scheduled_for,
                ))
                
                logger.info(f"Job added: {job.id} ({job.bot_type}) priority={job.priority}")
                return job.id
    
    def create(self, bot_type: str, params: Dict = None, priority: int = None, 
               source: str = 'manual', scheduled_for: datetime = None) -> str:
        """
        Crear y añadir un job.
        
        Args:
            bot_type: Tipo de bot (direct, resentment, social)
            params: Parámetros del bot
            priority: Prioridad (1-4)
            source: Origen (manual, scheduled, governance)
            scheduled_for: Fecha/hora programada
            
        Returns:
            ID del job
        """
        job = Job(
            id=str(uuid.uuid4())[:8],
            bot_type=bot_type,
            params=params or {},
            priority=priority or JobPriority.NORMAL.value,
            source=source,
            scheduled_for=scheduled_for.isoformat() if scheduled_for else None,
        )
        return self.add(job)
    
    def get_next(self) -> Optional[Job]:
        """
        Obtener siguiente job a ejecutar.
        Prioriza por: scheduled_for (si vencido), priority, created_at
        
        Returns:
            Job o None
        """
        with self._lock:
            with self._get_connection() as conn:
                now = datetime.now().isoformat()
                
                # Buscar job pendiente
                row = conn.execute('''
                    SELECT * FROM jobs
                    WHERE status IN ('pending', 'retry')
                    AND (scheduled_for IS NULL OR scheduled_for <= ?)
                    ORDER BY priority ASC, created_at ASC
                    LIMIT 1
                ''', (now,)).fetchone()
                
                if row:
                    return self._row_to_job(row)
                return None
    
    def get(self, job_id: str) -> Optional[Job]:
        """Obtener job por ID"""
        with self._get_connection() as conn:
            row = conn.execute('SELECT * FROM jobs WHERE id = ?', (job_id,)).fetchone()
            if row:
                return self._row_to_job(row)
            return None
    
    def update_status(self, job_id: str, status: JobStatus, error: str = None, result: Dict = None):
        """Actualizar estado de un job"""
        with self._lock:
            with self._get_connection() as conn:
                updates = ['status = ?']
                values = [status.value]
                
                if status == JobStatus.RUNNING:
                    updates.append('started_at = CURRENT_TIMESTAMP')
                elif status in (JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED):
                    updates.append('completed_at = CURRENT_TIMESTAMP')
                
                if error:
                    updates.append('error = ?')
                    values.append(error)
                
                if result:
                    updates.append('result = ?')
                    values.append(json.dumps(result))
                
                values.append(job_id)
                
                conn.execute(f'''
                    UPDATE jobs SET {', '.join(updates)}
                    WHERE id = ?
                ''', values)
                
                logger.debug(f"Job {job_id} status -> {status.value}")
    
    def mark_running(self, job_id: str):
        """Marcar job como en ejecución"""
        self.update_status(job_id, JobStatus.RUNNING)
    
    def mark_completed(self, job_id: str, result: Dict = None):
        """Marcar job como completado"""
        self.update_status(job_id, JobStatus.COMPLETED, result=result)
    
    def mark_failed(self, job_id: str, error: str, retry: bool = True):
        """
        Marcar job como fallido.
        Si retry=True y no ha excedido max_retries, se pone en cola de retry.
        """
        job = self.get(job_id)
        if not job:
            return
        
        with self._lock:
            with self._get_connection() as conn:
                if retry and job.retry_count < job.max_retries:
                    # Programar retry con backoff
                    backoff = 60 * (2 ** job.retry_count)  # 1min, 2min, 4min, ...
                    retry_at = datetime.now() + timedelta(seconds=backoff)
                    
                    conn.execute('''
                        UPDATE jobs SET
                            status = 'retry',
                            retry_count = retry_count + 1,
                            error = ?,
                            scheduled_for = ?
                        WHERE id = ?
                    ''', (error, retry_at.isoformat(), job_id))
                    
                    logger.info(f"Job {job_id} scheduled for retry in {backoff}s")
                else:
                    conn.execute('''
                        UPDATE jobs SET
                            status = 'failed',
                            error = ?,
                            completed_at = CURRENT_TIMESTAMP
                        WHERE id = ?
                    ''', (error, job_id))
                    
                    logger.warning(f"Job {job_id} failed permanently: {error}")
    
    def cancel(self, job_id: str) -> bool:
        """Cancelar un job pendiente"""
        with self._lock:
            with self._get_connection() as conn:
                result = conn.execute('''
                    UPDATE jobs SET status = 'cancelled', completed_at = CURRENT_TIMESTAMP
                    WHERE id = ? AND status IN ('pending', 'retry')
                ''', (job_id,))
                return result.rowcount > 0
    
    def get_pending(self, bot_type: str = None) -> List[Job]:
        """Obtener jobs pendientes"""
        with self._get_connection() as conn:
            if bot_type:
                rows = conn.execute('''
                    SELECT * FROM jobs
                    WHERE status IN ('pending', 'retry') AND bot_type = ?
                    ORDER BY priority ASC, created_at ASC
                ''', (bot_type,)).fetchall()
            else:
                rows = conn.execute('''
                    SELECT * FROM jobs
                    WHERE status IN ('pending', 'retry')
                    ORDER BY priority ASC, created_at ASC
                ''').fetchall()
            
            return [self._row_to_job(row) for row in rows]
    
    def get_running(self) -> List[Job]:
        """Obtener jobs en ejecución"""
        with self._get_connection() as conn:
            rows = conn.execute('''
                SELECT * FROM jobs WHERE status = 'running'
            ''').fetchall()
            return [self._row_to_job(row) for row in rows]
    
    def get_history(self, limit: int = 50, bot_type: str = None) -> List[Job]:
        """Obtener historial de jobs completados/fallidos"""
        with self._get_connection() as conn:
            if bot_type:
                rows = conn.execute('''
                    SELECT * FROM jobs
                    WHERE status IN ('completed', 'failed', 'cancelled') AND bot_type = ?
                    ORDER BY completed_at DESC LIMIT ?
                ''', (bot_type, limit)).fetchall()
            else:
                rows = conn.execute('''
                    SELECT * FROM jobs
                    WHERE status IN ('completed', 'failed', 'cancelled')
                    ORDER BY completed_at DESC LIMIT ?
                ''', (limit,)).fetchall()
            
            return [self._row_to_job(row) for row in rows]
    
    def get_stats(self) -> Dict:
        """Obtener estadísticas de la cola"""
        with self._get_connection() as conn:
            stats = {}
            
            # Conteos por estado
            for status in JobStatus:
                row = conn.execute(
                    'SELECT COUNT(*) as count FROM jobs WHERE status = ?',
                    (status.value,)
                ).fetchone()
                stats[status.value] = row['count'] if row else 0
            
            # Por bot type
            stats['by_bot'] = {}
            for bot in ['direct', 'resentment', 'social']:
                row = conn.execute('''
                    SELECT 
                        COUNT(*) FILTER (WHERE status = 'pending') as pending,
                        COUNT(*) FILTER (WHERE status = 'completed') as completed,
                        COUNT(*) FILTER (WHERE status = 'failed') as failed
                    FROM jobs WHERE bot_type = ?
                ''', (bot,)).fetchone()
                stats['by_bot'][bot] = dict(row) if row else {}
            
            return stats
    
    def cleanup_old(self, days: int = 7):
        """Limpiar jobs antiguos completados/fallidos"""
        with self._lock:
            with self._get_connection() as conn:
                cutoff = (datetime.now() - timedelta(days=days)).isoformat()
                result = conn.execute('''
                    DELETE FROM jobs
                    WHERE status IN ('completed', 'failed', 'cancelled')
                    AND completed_at < ?
                ''', (cutoff,))
                
                if result.rowcount > 0:
                    logger.info(f"Cleaned up {result.rowcount} old jobs")
    
    def clear_stuck(self, timeout_minutes: int = 60):
        """
        Limpiar jobs atascados (running por mucho tiempo).
        Los pone en estado de retry.
        """
        with self._lock:
            with self._get_connection() as conn:
                cutoff = (datetime.now() - timedelta(minutes=timeout_minutes)).isoformat()
                
                # Obtener jobs atascados
                rows = conn.execute('''
                    SELECT id FROM jobs
                    WHERE status = 'running' AND started_at < ?
                ''', (cutoff,)).fetchall()
                
                for row in rows:
                    job = self.get(row['id'])
                    if job and job.retry_count < job.max_retries:
                        conn.execute('''
                            UPDATE jobs SET
                                status = 'retry',
                                retry_count = retry_count + 1,
                                error = 'Timeout - job stuck'
                            WHERE id = ?
                        ''', (row['id'],))
                        logger.warning(f"Job {row['id']} marked as stuck, scheduling retry")
                    else:
                        conn.execute('''
                            UPDATE jobs SET
                                status = 'failed',
                                error = 'Timeout - max retries exceeded',
                                completed_at = CURRENT_TIMESTAMP
                            WHERE id = ?
                        ''', (row['id'],))
                        logger.error(f"Job {row['id']} failed due to timeout")
