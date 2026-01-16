#!/usr/bin/env python3
"""
State Manager - Persistencia de estado del worker autónomo
"""

import json
import sqlite3
import logging
from datetime import datetime, date
from pathlib import Path
from typing import Dict, Any, Optional, List
from contextlib import contextmanager
import threading

logger = logging.getLogger(__name__)


class StateManager:
    """Gestor de estado persistente para el worker"""
    
    def __init__(self, db_path: str = None):
        from config import DATA_DIR
        self.db_path = db_path or str(DATA_DIR / 'worker_state.db')
        self._lock = threading.Lock()
        self._init_db()
    
    def _init_db(self):
        """Inicializar base de datos"""
        with self._get_connection() as conn:
            conn.executescript('''
                -- Estado general del worker
                CREATE TABLE IF NOT EXISTS worker_state (
                    key TEXT PRIMARY KEY,
                    value TEXT,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                
                -- Contadores diarios
                CREATE TABLE IF NOT EXISTS daily_counters (
                    date TEXT,
                    bot_type TEXT,
                    counter_type TEXT,
                    value INTEGER DEFAULT 0,
                    PRIMARY KEY (date, bot_type, counter_type)
                );
                
                -- Historial de ejecuciones
                CREATE TABLE IF NOT EXISTS run_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT UNIQUE,
                    bot_type TEXT,
                    status TEXT,
                    started_at TIMESTAMP,
                    completed_at TIMESTAMP,
                    leads_found INTEGER DEFAULT 0,
                    leads_saved INTEGER DEFAULT 0,
                    leads_duplicates INTEGER DEFAULT 0,
                    leads_filtered INTEGER DEFAULT 0,
                    errors TEXT,
                    config TEXT,
                    duration_seconds REAL
                );
                
                -- Dominios procesados (para deduplicación global)
                CREATE TABLE IF NOT EXISTS seen_domains (
                    domain TEXT PRIMARY KEY,
                    first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    bot_type TEXT,
                    run_id TEXT
                );
                
                -- Errores y eventos
                CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    event_type TEXT,
                    bot_type TEXT,
                    message TEXT,
                    details TEXT
                );
                
                -- Checkpoints para recovery
                CREATE TABLE IF NOT EXISTS checkpoints (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    bot_type TEXT,
                    checkpoint_data TEXT
                );
                
                -- Índices
                CREATE INDEX IF NOT EXISTS idx_run_history_bot ON run_history(bot_type);
                CREATE INDEX IF NOT EXISTS idx_run_history_date ON run_history(started_at);
                CREATE INDEX IF NOT EXISTS idx_events_type ON events(event_type);
                CREATE INDEX IF NOT EXISTS idx_daily_date ON daily_counters(date);
            ''')
    
    @contextmanager
    def _get_connection(self):
        """Context manager para conexiones thread-safe"""
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
    
    # === WORKER STATE ===
    
    def get_state(self, key: str, default: Any = None) -> Any:
        """Obtener valor de estado"""
        with self._lock:
            with self._get_connection() as conn:
                row = conn.execute(
                    'SELECT value FROM worker_state WHERE key = ?', (key,)
                ).fetchone()
                if row:
                    try:
                        return json.loads(row['value'])
                    except:
                        return row['value']
                return default
    
    def set_state(self, key: str, value: Any):
        """Guardar valor de estado"""
        with self._lock:
            with self._get_connection() as conn:
                value_str = json.dumps(value) if not isinstance(value, str) else value
                conn.execute('''
                    INSERT OR REPLACE INTO worker_state (key, value, updated_at)
                    VALUES (?, ?, CURRENT_TIMESTAMP)
                ''', (key, value_str))
    
    def get_worker_status(self) -> str:
        """Obtener estado del worker: running, paused, stopped, error"""
        return self.get_state('worker_status', 'stopped')
    
    def set_worker_status(self, status: str):
        """Establecer estado del worker"""
        self.set_state('worker_status', status)
        self.log_event('worker_status_change', None, f'Status changed to: {status}')
    
    def get_last_heartbeat(self) -> Optional[datetime]:
        """Obtener último heartbeat"""
        ts = self.get_state('last_heartbeat')
        if ts:
            return datetime.fromisoformat(ts)
        return None
    
    def update_heartbeat(self):
        """Actualizar heartbeat"""
        self.set_state('last_heartbeat', datetime.now().isoformat())
    
    # === DAILY COUNTERS ===
    
    def get_daily_counter(self, bot_type: str, counter_type: str, target_date: date = None) -> int:
        """Obtener contador diario"""
        target_date = target_date or date.today()
        date_str = target_date.isoformat()
        
        with self._get_connection() as conn:
            row = conn.execute('''
                SELECT value FROM daily_counters
                WHERE date = ? AND bot_type = ? AND counter_type = ?
            ''', (date_str, bot_type, counter_type)).fetchone()
            return row['value'] if row else 0
    
    def increment_daily_counter(self, bot_type: str, counter_type: str, amount: int = 1):
        """Incrementar contador diario"""
        date_str = date.today().isoformat()
        
        with self._lock:
            with self._get_connection() as conn:
                conn.execute('''
                    INSERT INTO daily_counters (date, bot_type, counter_type, value)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(date, bot_type, counter_type)
                    DO UPDATE SET value = value + ?
                ''', (date_str, bot_type, counter_type, amount, amount))
    
    def get_leads_today(self, bot_type: str = None) -> int:
        """Obtener leads guardados hoy"""
        date_str = date.today().isoformat()
        
        with self._get_connection() as conn:
            if bot_type:
                row = conn.execute('''
                    SELECT SUM(value) as total FROM daily_counters
                    WHERE date = ? AND bot_type = ? AND counter_type = 'leads_saved'
                ''', (date_str, bot_type)).fetchone()
            else:
                row = conn.execute('''
                    SELECT SUM(value) as total FROM daily_counters
                    WHERE date = ? AND counter_type = 'leads_saved'
                ''', (date_str,)).fetchone()
            return row['total'] or 0 if row else 0
    
    def can_run_today(self, bot_type: str, daily_limit: int) -> bool:
        """Verificar si se puede ejecutar el bot hoy"""
        leads_today = self.get_leads_today(bot_type)
        return leads_today < daily_limit
    
    def remaining_leads_today(self, bot_type: str, daily_limit: int) -> int:
        """Leads restantes para hoy"""
        return max(0, daily_limit - self.get_leads_today(bot_type))
    
    # === RUN HISTORY ===
    
    def start_run(self, run_id: str, bot_type: str, config: Dict = None) -> int:
        """Registrar inicio de ejecución"""
        with self._lock:
            with self._get_connection() as conn:
                cursor = conn.execute('''
                    INSERT INTO run_history (run_id, bot_type, status, started_at, config)
                    VALUES (?, ?, 'running', CURRENT_TIMESTAMP, ?)
                ''', (run_id, bot_type, json.dumps(config or {})))
                
                self.set_state(f'current_run_{bot_type}', run_id)
                self.log_event('run_started', bot_type, f'Run {run_id} started')
                
                return cursor.lastrowid
    
    def end_run(self, run_id: str, status: str, stats: Dict = None):
        """Registrar fin de ejecución"""
        stats = stats or {}
        
        with self._lock:
            with self._get_connection() as conn:
                # Obtener tiempo de inicio
                row = conn.execute(
                    'SELECT started_at FROM run_history WHERE run_id = ?', (run_id,)
                ).fetchone()
                
                duration = None
                if row and row['started_at']:
                    started = datetime.fromisoformat(row['started_at'])
                    duration = (datetime.now() - started).total_seconds()
                
                conn.execute('''
                    UPDATE run_history SET
                        status = ?,
                        completed_at = CURRENT_TIMESTAMP,
                        leads_found = ?,
                        leads_saved = ?,
                        leads_duplicates = ?,
                        leads_filtered = ?,
                        errors = ?,
                        duration_seconds = ?
                    WHERE run_id = ?
                ''', (
                    status,
                    stats.get('leads_found', 0),
                    stats.get('leads_saved', 0),
                    stats.get('leads_duplicates', 0),
                    stats.get('leads_filtered', 0),
                    json.dumps(stats.get('errors', [])),
                    duration,
                    run_id
                ))
                
                # Actualizar contadores diarios
                bot_type = conn.execute(
                    'SELECT bot_type FROM run_history WHERE run_id = ?', (run_id,)
                ).fetchone()
                
                if bot_type:
                    self.increment_daily_counter(bot_type['bot_type'], 'leads_saved', stats.get('leads_saved', 0))
                    self.increment_daily_counter(bot_type['bot_type'], 'runs', 1)
                
                self.set_state(f'last_run_{bot_type}', {
                    'run_id': run_id,
                    'status': status,
                    'completed_at': datetime.now().isoformat(),
                    'stats': stats
                })
                
                self.log_event('run_completed', bot_type['bot_type'] if bot_type else None, 
                             f'Run {run_id} {status}: {stats.get("leads_saved", 0)} leads saved')
    
    def get_run_history(self, bot_type: str = None, limit: int = 50) -> List[Dict]:
        """Obtener historial de ejecuciones"""
        with self._get_connection() as conn:
            if bot_type:
                rows = conn.execute('''
                    SELECT * FROM run_history
                    WHERE bot_type = ?
                    ORDER BY started_at DESC LIMIT ?
                ''', (bot_type, limit)).fetchall()
            else:
                rows = conn.execute('''
                    SELECT * FROM run_history
                    ORDER BY started_at DESC LIMIT ?
                ''', (limit,)).fetchall()
            
            return [dict(row) for row in rows]
    
    def get_current_run(self, bot_type: str) -> Optional[str]:
        """Obtener run_id actual si hay uno en ejecución"""
        return self.get_state(f'current_run_{bot_type}')
    
    # === SEEN DOMAINS ===
    
    def is_domain_seen(self, domain: str) -> bool:
        """Verificar si dominio ya fue procesado"""
        with self._get_connection() as conn:
            row = conn.execute(
                'SELECT 1 FROM seen_domains WHERE domain = ?', (domain,)
            ).fetchone()
            return row is not None
    
    def mark_domain_seen(self, domain: str, bot_type: str, run_id: str = None):
        """Marcar dominio como procesado"""
        with self._lock:
            with self._get_connection() as conn:
                conn.execute('''
                    INSERT OR IGNORE INTO seen_domains (domain, bot_type, run_id)
                    VALUES (?, ?, ?)
                ''', (domain, bot_type, run_id))
    
    def get_seen_domains_count(self) -> int:
        """Contar dominios vistos"""
        with self._get_connection() as conn:
            row = conn.execute('SELECT COUNT(*) as count FROM seen_domains').fetchone()
            return row['count'] if row else 0
    
    # === EVENTS ===
    
    def log_event(self, event_type: str, bot_type: str = None, message: str = '', details: Dict = None):
        """Registrar evento"""
        with self._get_connection() as conn:
            conn.execute('''
                INSERT INTO events (event_type, bot_type, message, details)
                VALUES (?, ?, ?, ?)
            ''', (event_type, bot_type, message, json.dumps(details) if details else None))
    
    def get_events(self, event_type: str = None, limit: int = 100) -> List[Dict]:
        """Obtener eventos"""
        with self._get_connection() as conn:
            if event_type:
                rows = conn.execute('''
                    SELECT * FROM events WHERE event_type = ?
                    ORDER BY timestamp DESC LIMIT ?
                ''', (event_type, limit)).fetchall()
            else:
                rows = conn.execute('''
                    SELECT * FROM events
                    ORDER BY timestamp DESC LIMIT ?
                ''', (limit,)).fetchall()
            
            return [dict(row) for row in rows]
    
    # === CHECKPOINTS ===
    
    def save_checkpoint(self, bot_type: str, data: Dict):
        """Guardar checkpoint para recovery"""
        with self._lock:
            with self._get_connection() as conn:
                conn.execute('''
                    INSERT INTO checkpoints (bot_type, checkpoint_data)
                    VALUES (?, ?)
                ''', (bot_type, json.dumps(data)))
    
    def get_last_checkpoint(self, bot_type: str) -> Optional[Dict]:
        """Obtener último checkpoint"""
        with self._get_connection() as conn:
            row = conn.execute('''
                SELECT checkpoint_data FROM checkpoints
                WHERE bot_type = ?
                ORDER BY created_at DESC LIMIT 1
            ''', (bot_type,)).fetchone()
            
            if row:
                return json.loads(row['checkpoint_data'])
            return None
    
    def clear_checkpoints(self, bot_type: str):
        """Limpiar checkpoints antiguos"""
        with self._lock:
            with self._get_connection() as conn:
                conn.execute('''
                    DELETE FROM checkpoints WHERE bot_type = ?
                ''', (bot_type,))
    
    # === STATS ===
    
    def get_stats_summary(self) -> Dict:
        """Obtener resumen de estadísticas"""
        with self._get_connection() as conn:
            today = date.today().isoformat()
            
            # Leads hoy
            leads_today = conn.execute('''
                SELECT SUM(value) as total FROM daily_counters
                WHERE date = ? AND counter_type = 'leads_saved'
            ''', (today,)).fetchone()
            
            # Runs hoy
            runs_today = conn.execute('''
                SELECT COUNT(*) as total FROM run_history
                WHERE DATE(started_at) = ?
            ''', (today,)).fetchone()
            
            # Total dominios
            domains = conn.execute('SELECT COUNT(*) as total FROM seen_domains').fetchone()
            
            # Últimas 24h por bot
            by_bot = {}
            for bot in ['direct', 'resentment', 'social']:
                row = conn.execute('''
                    SELECT SUM(leads_saved) as leads, COUNT(*) as runs
                    FROM run_history
                    WHERE bot_type = ? AND started_at > datetime('now', '-24 hours')
                ''', (bot,)).fetchone()
                by_bot[bot] = {
                    'leads': row['leads'] or 0,
                    'runs': row['runs'] or 0
                }
            
            return {
                'leads_today': leads_today['total'] or 0 if leads_today else 0,
                'runs_today': runs_today['total'] or 0 if runs_today else 0,
                'total_domains': domains['total'] or 0 if domains else 0,
                'by_bot': by_bot,
                'worker_status': self.get_worker_status(),
                'last_heartbeat': self.get_state('last_heartbeat'),
            }
