#!/usr/bin/env python3
"""
BotScrap External - Multi-Bot Daemon
=====================================
Un solo daemon que gestiona TODOS los bots externos configurados en StaffKit.

- Consulta StaffKit cada minuto para ver qué bots están activos
- Ejecuta automáticamente los bots que deben correr
- Respeta horarios, límites y estados de cada bot
- No requiere configuración manual de servicios por bot

Uso:
    python multi_bot_daemon.py --api-key <staffkit_api_key>
"""

import argparse
import logging
import os
import signal
import subprocess
import sys
import time
import fcntl
from datetime import datetime
import requests

# Configuración de paths
BASE_DIR = '/var/www/vhosts/territoriodrasanvicr.com/b'
LOG_FILE = f'{BASE_DIR}/daemon.log'
PID_FILE = f'{BASE_DIR}/daemon.pid'
LOCK_FILE = f'{BASE_DIR}/daemon.lock'

os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

# ============================================================================
# PROTECCIÓN CONTRA MÚLTIPLES INSTANCIAS
# ============================================================================
def acquire_lock():
    """Adquirir lock exclusivo para evitar múltiples daemons"""
    global lock_file_handle
    try:
        lock_file_handle = open(LOCK_FILE, 'w')
        fcntl.flock(lock_file_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        # Escribir PID actual
        lock_file_handle.write(str(os.getpid()))
        lock_file_handle.flush()
        return True
    except (IOError, OSError) as e:
        # Otro proceso tiene el lock
        try:
            with open(PID_FILE, 'r') as f:
                existing_pid = f.read().strip()
            print(f"❌ ERROR: Ya hay un daemon corriendo (PID: {existing_pid})")
            print(f"   Si el proceso no existe, borra: {LOCK_FILE} y {PID_FILE}")
        except:
            print(f"❌ ERROR: No se pudo adquirir el lock. Otro daemon puede estar corriendo.")
        return False

def release_lock():
    """Liberar lock al terminar"""
    global lock_file_handle
    try:
        if lock_file_handle:
            fcntl.flock(lock_file_handle.fileno(), fcntl.LOCK_UN)
            lock_file_handle.close()
        # Limpiar archivos
        if os.path.exists(LOCK_FILE):
            os.remove(LOCK_FILE)
        if os.path.exists(PID_FILE):
            os.remove(PID_FILE)
    except:
        pass

def write_pid():
    """Escribir PID a archivo para referencia"""
    with open(PID_FILE, 'w') as f:
        f.write(str(os.getpid()))

def check_existing_daemon():
    """Verificar si hay otro daemon y matarlo si está zombie"""
    if os.path.exists(PID_FILE):
        try:
            with open(PID_FILE, 'r') as f:
                old_pid = int(f.read().strip())
            # Verificar si el proceso existe
            os.kill(old_pid, 0)  # Signal 0 = solo verifica existencia
            return old_pid  # Proceso existe
        except (ProcessLookupError, ValueError):
            # Proceso no existe, limpiar archivos stale
            print(f"🧹 Limpiando archivos de daemon anterior (PID no existe)")
            if os.path.exists(PID_FILE):
                os.remove(PID_FILE)
            if os.path.exists(LOCK_FILE):
                os.remove(LOCK_FILE)
            return None
        except PermissionError:
            return old_pid  # Proceso existe pero no tenemos permiso
    return None

lock_file_handle = None

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_FILE)
    ]
)
logger = logging.getLogger(__name__)

# Flag para shutdown graceful
shutdown_requested = False

def signal_handler(signum, frame):
    global shutdown_requested
    logger.info("🛑 Shutdown signal received")
    shutdown_requested = True

signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)


class MultiBotDaemon:
    def __init__(self, staffkit_url, api_key):
        self.staffkit_url = staffkit_url.rstrip('/')
        self.api_key = api_key
        self.start_time = datetime.now()
        self.last_bots_status = []
        self.pid = os.getpid()
        logger.info(f"🤖 Multi-Bot Daemon initialized")
        logger.info(f"   StaffKit: {self.staffkit_url}")
        logger.info(f"   PID: {self.pid}")
    
    def _headers(self):
        return {
            'Authorization': f'Bearer {self.api_key}',
            'Content-Type': 'application/json'
        }
    
    def get_uptime(self):
        """Retorna tiempo de ejecución del daemon"""
        delta = datetime.now() - self.start_time
        hours, remainder = divmod(int(delta.total_seconds()), 3600)
        minutes, seconds = divmod(remainder, 60)
        if hours > 24:
            days = hours // 24
            hours = hours % 24
            return f"{days}d {hours}h {minutes}m"
        return f"{hours}h {minutes}m {seconds}s"
    
    def get_all_bots(self):
        """Obtiene todos los bots externos de StaffKit"""
        try:
            r = requests.get(
                f"{self.staffkit_url}/api/v2/external-bot",
                headers=self._headers(),
                timeout=15
            )
            if r.status_code == 200:
                data = r.json()
                return data.get('bots', [])
            logger.error(f"API error: {r.status_code}")
            return []
        except Exception as e:
            logger.error(f"Connection error: {e}")
            return []
    
    def get_bot_config(self, bot_id):
        """Obtiene configuración de un bot específico"""
        try:
            r = requests.get(
                f"{self.staffkit_url}/api/v2/external-bot",
                params={'id': bot_id},
                headers=self._headers(),
                timeout=10
            )
            if r.status_code == 200:
                return r.json()
            return None
        except Exception as e:
            logger.error(f"Config error for bot {bot_id}: {e}")
            return None
    
    def report_start(self, bot_id, config):
        """Reporta inicio de ejecución"""
        try:
            r = requests.post(
                f"{self.staffkit_url}/api/v2/external-bot",
                json={'action': 'start_run', 'bot_id': bot_id, 'config': config},
                headers=self._headers(),
                timeout=10
            )
            if r.status_code == 200:
                return r.json().get('run_id')
        except Exception as e:
            logger.error(f"Report start error: {e}")
        return None
    
    def report_end(self, bot_id, run_id, status, error=None, leads_found=0, leads_saved=0, leads_duplicates=0):
        """Reporta fin de ejecución"""
        try:
            r = requests.post(
                f"{self.staffkit_url}/api/v2/external-bot",
                json={
                    'action': 'end_run',
                    'bot_id': bot_id,
                    'run_id': run_id,
                    'status': status,
                    'error': error,
                    'leads_found': leads_found,
                    'leads_saved': leads_saved,
                    'leads_duplicates': leads_duplicates
                },
                headers=self._headers(),
                timeout=10
            )
        except Exception as e:
            logger.error(f"Report end error: {e}")
    
    def update_daemon_status(self, status='running', bots_status=None):
        """Actualiza el estado del daemon en StaffKit (nuevo endpoint daemon-status)"""
        try:
            payload = {
                'action': 'heartbeat',
                'status': status,
                'started_at': self.start_time.strftime('%Y-%m-%d %H:%M:%S'),
                'active_bots': len([b for b in (bots_status or []) if b.get('should_run')]),
                'total_bots': len(bots_status or []),
                'version': '2.0',
                'pid': self.pid,
                'bots_status': bots_status or self.last_bots_status
            }
            r = requests.post(
                f"{self.staffkit_url}/api/v2/daemon-status",
                json=payload,
                headers=self._headers(),
                timeout=10
            )
        except Exception as e:
            pass  # Silent fail for status updates
    
    def log_to_staffkit(self, level, message, bot_id=None, bot_name=None):
        """Envía una entrada de log a StaffKit"""
        try:
            r = requests.post(
                f"{self.staffkit_url}/api/v2/daemon-status",
                json={
                    'action': 'log',
                    'level': level,
                    'bot_id': bot_id,
                    'bot_name': bot_name,
                    'message': message
                },
                headers=self._headers(),
                timeout=5
            )
        except:
            pass  # Silent fail
    
    def send_notification(self, bot_id, bot_name, notif_type, message):
        """Envía notificación a StaffKit"""
        try:
            r = requests.post(
                f"{self.staffkit_url}/api/v2/daemon-status",
                json={
                    'action': 'notification',
                    'bot_id': bot_id,
                    'bot_name': bot_name,
                    'type': notif_type,
                    'message': message
                },
                headers=self._headers(),
                timeout=5
            )
        except:
            pass
    
    def should_run_bot(self, bot):
        """Determina si un bot debe ejecutarse ahora"""
        # Si tiene run_now=1, ejecutar inmediatamente (forzado desde UI)
        if bot.get('run_now') and str(bot.get('run_now')) == '1':
            return True, "forced (run_now)"
        
        # Verificar si está habilitado
        if not bot.get('is_enabled') or bot.get('is_enabled') == '0':
            return False, "disabled"
        
        # Verificar si está pausado
        if bot.get('is_paused') and bot.get('is_paused') != '0':
            return False, "paused"
        
        # Verificar día de la semana (bitmask: 1=Lun, 2=Mar, 4=Mie, 8=Jue, 16=Vie, 32=Sab, 64=Dom)
        run_days = int(bot.get('run_days', 127) or 127)
        current_weekday = datetime.now().weekday()  # 0=Lun, 6=Dom
        day_bit = 1 << current_weekday  # Convertir a bitmask
        if not (run_days & day_bit):
            days_map = ['Lun', 'Mar', 'Mié', 'Jue', 'Vie', 'Sáb', 'Dom']
            return False, f"day off ({days_map[current_weekday]})"
        
        # Verificar horario
        current_hour = datetime.now().hour
        run_start = int(bot.get('run_hours_start', 0) or 0)
        run_end = int(bot.get('run_hours_end', 23) or 23)
        if not (run_start <= current_hour <= run_end):
            return False, f"outside schedule ({run_start}-{run_end}h)"
        
        # Verificar límite diario (NO aplica para bots SAP - son extractores)
        bot_type = bot.get('bot_type', 'direct')
        if bot_type not in ('sap', 'sap_sl'):
            leads_today = int(bot.get('leads_today', 0) or 0)
            daily_limit = int(bot.get('config_daily_limit', 50) or 50)
            if leads_today >= daily_limit:
                return False, f"daily limit reached ({leads_today}/{daily_limit})"
        
        # Verificar intervalo desde última ejecución
        last_run = bot.get('last_run_at')
        interval_minutes = int(bot.get('interval_minutes', 60) or 60)
        if last_run:
            try:
                last_run_dt = datetime.strptime(last_run, '%Y-%m-%d %H:%M:%S')
                minutes_since = (datetime.now() - last_run_dt).total_seconds() / 60
                if minutes_since < interval_minutes:
                    return False, f"interval not elapsed ({int(minutes_since)}/{interval_minutes} min)"
            except:
                pass
        
        return True, "ready"
    
    def execute_bot(self, bot):
        """Ejecuta un bot específico"""
        bot_id = bot.get('id')
        bot_name = bot.get('name', f'Bot #{bot_id}')
        bot_type = bot.get('bot_type', 'direct')
        query = bot.get('config_query', '')
        
        # Soportar múltiples países (config_countries tiene prioridad sobre config_country)
        countries = bot.get('config_countries', '') or bot.get('config_country', 'ES')
        
        max_leads = int(bot.get('config_daily_limit', 50) or 50)
        leads_today = int(bot.get('leads_today', 0) or 0)
        target_list_id = bot.get('target_list_id')
        config_file = bot.get('config_file', '')
        
        # Filtros adicionales
        cms_filter = bot.get('config_cms', 'all')
        max_speed = bot.get('config_max_speed', 100)
        
        # Configuración específica por tipo de bot
        social_platforms = bot.get('config_social_platforms', 'instagram,facebook,linkedin')
        social_min_followers = bot.get('config_social_min_followers', 100)
        social_keywords = bot.get('config_social_keywords', '')
        resentment_sources = bot.get('config_resentment_sources', 'trustpilot,google_reviews')
        resentment_min_score = bot.get('config_resentment_min_score', 2)
        resentment_keywords = bot.get('config_resentment_keywords', '')
        
        # Calcular cuántos leads buscar en esta ejecución
        # Bots SAP y Geographic son extractores/crawlers, no tienen límite tradicional
        if bot_type in ('sap', 'sap_sl', 'geographic'):
            leads_per_run = 0  # No usa este parámetro, tiene su propia lógica
        else:
            remaining = max_leads - leads_today
            leads_per_run = min(10, remaining)
            
            if leads_per_run <= 0:
                # Notificar límite alcanzado
                if bot.get('notify_on_limit'):
                    self.send_notification(bot_id, bot_name, 'limit_reached', 
                        f'Bot ha alcanzado su límite diario de {max_leads} leads')
                return {'success': True, 'leads_found': 0, 'leads_saved': 0, 'leads_duplicates': 0, 'at_limit': True}
        
        # Map bot_type to subcommand
        subcommand_map = {
            'bot_directo': 'direct',
            'bot_social': 'social', 
            'bot_resentment': 'resentment',
            'direct': 'direct',
            'social': 'social',
            'resentment': 'resentment',
            'autonomous': 'autonomous',
            'sap': 'sap',
            'sap_sl': 'sap_sl',  # SAP Service Layer (REST API)
            'geographic': 'geographic'
        }
        # Normalizar bot_type (strip espacios, lowercase)
        bot_type_clean = (bot_type or 'direct').strip().lower()
        subcommand = subcommand_map.get(bot_type_clean, 'direct')
        
        logger.debug(f"[{bot_name}] bot_type='{bot_type}' -> bot_type_clean='{bot_type_clean}' -> subcommand='{subcommand}'")
        
        # Comando base - se sobreescribe para tipos especiales
        cmd = None
        
        # Add parameters based on bot type
        if subcommand == 'direct':
            cmd = [
                '/var/www/vhosts/territoriodrasanvicr.com/b/venv/bin/python',
                'run_bot.py', 'direct',
            ]
            if query:
                cmd.extend(['--query', query])
            cmd.extend(['--limit', str(leads_per_run)])
            # Pasar países (puede ser múltiples separados por coma)
            if countries:
                cmd.extend(['--countries', countries])
            # Filtros adicionales
            if cms_filter and cms_filter != 'all':
                cmd.extend(['--cms', cms_filter])
            if max_speed and int(max_speed) < 100:
                cmd.extend(['--max-speed', str(max_speed)])
                
        elif subcommand == 'social':
            cmd = [
                '/var/www/vhosts/territoriodrasanvicr.com/b/venv/bin/python',
                'run_bot.py', 'social',
            ]
            cmd.extend(['--limit', str(leads_per_run)])
            if social_platforms:
                cmd.extend(['--platforms', social_platforms])
            if social_min_followers:
                cmd.extend(['--min-followers', str(social_min_followers)])
            if social_keywords:
                cmd.extend(['--keywords', social_keywords])
            if countries:
                cmd.extend(['--countries', countries])
            if config_file:
                cmd.extend(['--config', config_file])
                
        elif subcommand == 'resentment':
            cmd = [
                '/var/www/vhosts/territoriodrasanvicr.com/b/venv/bin/python',
                'run_bot.py', 'resentment',
            ]
            cmd.extend(['--limit', str(leads_per_run)])
            if resentment_sources:
                cmd.extend(['--sources', resentment_sources])
            if resentment_min_score:
                cmd.extend(['--max-score', str(resentment_min_score)])
            if resentment_keywords:
                cmd.extend(['--keywords', resentment_keywords])
            if countries:
                cmd.extend(['--countries', countries])
            if config_file:
                cmd.extend(['--config', config_file])
                
        elif subcommand == 'autonomous':
            cmd = [
                '/var/www/vhosts/territoriodrasanvicr.com/b/venv/bin/python',
                'run_bot.py', 'autonomous',
            ]
            cmd.extend(['--limit', str(leads_per_run)])
            if query:
                cmd.extend(['--seed-query', query])
            if countries:
                cmd.extend(['--countries', countries])
            if config_file:
                cmd.extend(['--config', config_file])
        
        elif subcommand == 'sap':
            # Bot SAP Business One - usa sap_sync.py (conexión SQL directa)
            cmd = [
                '/var/www/vhosts/territoriodrasanvicr.com/b/venv/bin/python',
                'sap_sync.py',
                '--bot-id', str(bot_id),
                '--api-key', self.api_key
            ]
        
        elif subcommand == 'sap_sl':
            # Bot SAP Service Layer - usa sap_service_layer.py (REST API)
            cmd = [
                '/var/www/vhosts/territoriodrasanvicr.com/b/venv/bin/python',
                'sap_service_layer.py',
                '--bot-id', str(bot_id),
                '--api-key', self.api_key
            ]
        
        elif subcommand == 'geographic':
            # Bot Geographic Crawler - barre países por sector
            # Las credenciales de DataForSEO se obtienen desde la BD via API
            searches_per_run = int(bot.get('config_geo_searches_per_run', 10) or 10)
            
            cmd = [
                '/var/www/vhosts/territoriodrasanvicr.com/b/venv/bin/python',
                'geographic_bot.py',
                '--bot-id', str(bot_id),
                '--api-key', self.api_key,
                '--searches-per-run', str(searches_per_run)
            ]
        
        # Solo añadir list-id si no es SAP ni Geographic (obtienen de su config)
        if target_list_id and subcommand not in ('sap', 'sap_sl', 'geographic'):
            cmd.extend(['--list-id', str(target_list_id)])
        
        # Validar que se creó un comando
        if cmd is None:
            logger.error(f"[{bot_name}] No command configured for bot_type '{bot_type}' (subcommand: {subcommand})")
            return {'success': False, 'error': f"Unknown bot type: {bot_type}"}
        
        logger.info(f"🚀 [{bot_name}] Type: {bot_type}")
        logger.info(f"🚀 [{bot_name}] Executing: {' '.join(cmd)}")
        
        result = {'leads_found': 0, 'leads_saved': 0, 'leads_duplicates': 0, 'success': False, 'error': None}
        
        try:
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                cwd='/var/www/vhosts/territoriodrasanvicr.com/b'
            )
            
            for line in process.stdout:
                line = line.strip()
                if line:
                    logger.info(f"  [{bot_name}] {line}")
                    # Parse output for stats
                    line_lower = line.lower()
                    if 'encontrados:' in line_lower:
                        try:
                            result['leads_found'] = int(line.split(':')[1].strip())
                        except: pass
                    elif 'guardados:' in line_lower:
                        try:
                            result['leads_saved'] = int(line.split(':')[1].strip())
                        except: pass
                    elif 'duplicados:' in line_lower:
                        try:
                            result['leads_duplicates'] = int(line.split(':')[1].strip())
                        except: pass
            
            process.wait()
            result['success'] = process.returncode == 0
            if not result['success']:
                result['error'] = f"Exit code: {process.returncode}"
                
        except Exception as e:
            result['error'] = str(e)
            logger.error(f"[{bot_name}] Execution error: {e}")
        
        return result
    
    def run_bot(self, bot):
        """Ejecuta un bot completo con reporting"""
        bot_id = bot.get('id')
        bot_name = bot.get('name', f'Bot #{bot_id}')
        notify_on_error = bot.get('notify_on_error', 1)
        notify_on_limit = bot.get('notify_on_limit', 1)
        
        # Obtener config actualizada
        config = self.get_bot_config(bot_id)
        if not config:
            logger.error(f"[{bot_name}] Could not get config")
            return
        
        # Report start
        run_id = self.report_start(bot_id, config.get('config', {}))
        if not run_id:
            logger.error(f"[{bot_name}] Could not register run start")
            return
        
        logger.info(f"▶️ [{bot_name}] Starting run #{run_id}")
        
        try:
            result = self.execute_bot(config.get('bot', {}))
            
            # Verificar si alcanzó límite
            if result.get('at_limit') and notify_on_limit:
                self.send_notification(bot_id, bot_name, 'limit_reached',
                    f'Bot ha alcanzado su límite diario')
            
            self.report_end(
                bot_id=bot_id,
                run_id=run_id,
                status='completed' if result['success'] else 'error',
                error=result.get('error'),
                leads_found=result['leads_found'],
                leads_saved=result['leads_saved'],
                leads_duplicates=result['leads_duplicates']
            )
            
            if result['success']:
                logger.info(f"✅ [{bot_name}] Run #{run_id} done: {result['leads_saved']} saved, {result['leads_duplicates']} duplicates")
            else:
                logger.error(f"❌ [{bot_name}] Run #{run_id} failed: {result.get('error')}")
                if notify_on_error:
                    self.send_notification(bot_id, bot_name, 'error',
                        f'Error en ejecución: {result.get("error", "Unknown")}')
                        
        except Exception as e:
            self.report_end(bot_id=bot_id, run_id=run_id, status='error', error=str(e))
            logger.error(f"❌ [{bot_name}] Run #{run_id} failed: {e}")
            if notify_on_error:
                self.send_notification(bot_id, bot_name, 'error', f'Excepción: {str(e)}')
    
    def check_and_run_bots(self):
        """Verifica todos los bots y ejecuta los que deben correr"""
        bots = self.get_all_bots()
        
        if not bots:
            logger.debug("No bots found or API error")
            self.update_daemon_status('running', [])
            return
        
        # Construir estado de cada bot
        bots_status = []
        active_count = 0
        
        for bot in bots:
            bot_id = bot.get('id')
            bot_name = bot.get('name', f'Bot #{bot_id}')
            
            should_run, reason = self.should_run_bot(bot)
            
            bot_status = {
                'id': bot_id,
                'name': bot_name,
                'should_run': should_run,
                'reason': reason,
                'leads_today': bot.get('leads_today', 0),
                'daily_limit': bot.get('config_daily_limit', 50),
                'is_enabled': bot.get('is_enabled', 0),
                'status': bot.get('status', 'idle')
            }
            bots_status.append(bot_status)
            
            if should_run:
                active_count += 1
                logger.info(f"🔄 [{bot_name}] Ready to run")
                self.log_to_staffkit('INFO', f"Starting execution", bot_id, bot_name)
                self.run_bot(bot)
                self.log_to_staffkit('INFO', f"Execution completed", bot_id, bot_name)
            else:
                logger.debug(f"⏸️ [{bot_name}] Skip: {reason}")
        
        # Guardar estado actual para heartbeats
        self.last_bots_status = bots_status
        
        # Actualizar estado del daemon
        self.update_daemon_status('running', bots_status)
        
        if active_count == 0:
            logger.info(f"💤 No bots to run right now ({len(bots)} configured) - Uptime: {self.get_uptime()}")
    
    def run_forever(self):
        """Loop principal del daemon"""
        logger.info("🚀 Multi-Bot Daemon starting...")
        logger.info(f"   Checking bots every 60 seconds")
        
        # Reportar inicio
        self.update_daemon_status('starting', [])
        self.log_to_staffkit('INFO', f'Daemon started - PID: {self.pid}')
        
        check_interval = 60  # Check every minute
        
        while not shutdown_requested:
            try:
                self.check_and_run_bots()
            except Exception as e:
                logger.error(f"Loop error: {e}")
                self.log_to_staffkit('ERROR', f'Loop error: {e}')
            
            # Sleep in small chunks to respond to shutdown quickly
            for _ in range(check_interval):
                if shutdown_requested:
                    break
                time.sleep(1)
        
        self.update_daemon_status('stopped', [])
        self.log_to_staffkit('INFO', 'Daemon stopped')
        logger.info("🛑 Daemon stopped")
        release_lock()


def main():
    parser = argparse.ArgumentParser(description='BotScrap Multi-Bot Daemon')
    parser.add_argument('--api-key', required=True, help='StaffKit API key')
    parser.add_argument('--staffkit-url', default='https://staff.replanta.dev', help='StaffKit URL')
    parser.add_argument('--once', action='store_true', help='Run once and exit (for testing)')
    parser.add_argument('--force', action='store_true', help='Force start, killing existing daemon')
    
    args = parser.parse_args()
    
    # ========================================================================
    # PROTECCIÓN: Verificar si ya hay un daemon corriendo
    # ========================================================================
    existing_pid = check_existing_daemon()
    if existing_pid:
        if args.force:
            print(f"⚠️ Forzando: matando daemon existente (PID: {existing_pid})")
            try:
                os.kill(existing_pid, signal.SIGTERM)
                time.sleep(2)
                # Si sigue vivo, SIGKILL
                try:
                    os.kill(existing_pid, 0)
                    os.kill(existing_pid, signal.SIGKILL)
                    time.sleep(1)
                except ProcessLookupError:
                    pass
            except:
                pass
            # Limpiar archivos
            if os.path.exists(PID_FILE):
                os.remove(PID_FILE)
            if os.path.exists(LOCK_FILE):
                os.remove(LOCK_FILE)
        else:
            print(f"❌ ERROR: Ya hay un daemon corriendo (PID: {existing_pid})")
            print(f"   Usa --force para matar el existente y reiniciar")
            print(f"   O ejecuta: kill {existing_pid}")
            sys.exit(1)
    
    # Adquirir lock exclusivo
    if not acquire_lock():
        sys.exit(1)
    
    # Escribir PID
    write_pid()
    print(f"✅ Daemon iniciando con PID: {os.getpid()}")
    
    try:
        daemon = MultiBotDaemon(
            staffkit_url=args.staffkit_url,
            api_key=args.api_key
        )
        
        if args.once:
            daemon.check_and_run_bots()
        else:
            daemon.run_forever()
    finally:
        release_lock()


if __name__ == '__main__':
    main()
