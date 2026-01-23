#!/usr/bin/env python3
"""
External Bot Daemon - Worker 24/7
Lee configuración desde StaffKit y ejecuta el bot según corresponda
"""

import os
import sys
import time
import json
import signal
import logging
import argparse
import subprocess
from datetime import datetime
import requests

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('daemon.log')
    ]
)
logger = logging.getLogger(__name__)

shutdown_requested = False

def signal_handler(signum, frame):
    global shutdown_requested
    logger.info("🛑 Shutdown signal received...")
    shutdown_requested = True

signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)


class ExternalBotDaemon:
    def __init__(self, bot_id, staffkit_url, api_key):
        self.bot_id = bot_id
        self.staffkit_url = staffkit_url.rstrip('/')
        self.api_key = api_key
        logger.info(f"🤖 Daemon initialized - Bot ID: {bot_id}")
    
    def _headers(self):
        return {
            'Authorization': f'Bearer {self.api_key}',
            'Content-Type': 'application/json'
        }
    
    def get_config(self):
        try:
            r = requests.get(
                f"{self.staffkit_url}/api/v2/external-bot",
                params={'id': self.bot_id},
                headers=self._headers(),
                timeout=10
            )
            if r.status_code == 200:
                return r.json()
            logger.error(f"Config error: {r.status_code}")
            return None
        except Exception as e:
            logger.error(f"Config error: {e}")
            return None
    
    def report_start(self, config):
        try:
            r = requests.post(
                f"{self.staffkit_url}/api/v2/external-bot",
                json={'action': 'start_run', 'bot_id': self.bot_id, 'config': config},
                headers=self._headers(),
                timeout=10
            )
            if r.status_code == 200:
                return r.json().get('run_id')
        except Exception as e:
            logger.error(f"Report start error: {e}")
        return None
    
    def report_end(self, run_id, status='completed', error=None, leads_found=0, leads_saved=0, leads_duplicates=0):
        try:
            requests.post(
                f"{self.staffkit_url}/api/v2/external-bot",
                json={
                    'action': 'end_run',
                    'bot_id': self.bot_id,
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
    
    def execute_bot(self, config):
        bot = config.get('bot', {})
        bot_type = bot.get('bot_type', 'direct')
        # Usar campos con prefijo config_ de la API
        query = bot.get('config_query', '')
        location = bot.get('config_country', 'ES')
        max_leads = int(bot.get('config_daily_limit', 50) or 50)
        target_list_id = bot.get('target_list_id')
        config_file = bot.get('config_file', '')
        
        # Map bot_type to subcommand
        subcommand_map = {
            'bot_directo': 'direct',
            'bot_social': 'social', 
            'bot_resentment': 'resentment',
            'direct': 'direct',
            'social': 'social',
            'resentment': 'resentment'
        }
        subcommand = subcommand_map.get(bot_type, 'direct')
        
        leads_per_run = min(10, max_leads)
        
        cmd = [
            '/var/www/vhosts/territoriodrasanvicr.com/b/venv/bin/python',
            'run_bot.py', subcommand,
        ]
        
        # Add parameters based on bot type
        if subcommand == 'direct':
            if query:
                cmd.extend(['--query', query])
            cmd.extend(['--limit', str(leads_per_run)])
            if location:
                cmd.extend(['--country', location])
        elif subcommand == 'resentment':
            cmd.extend(['--limit', str(leads_per_run)])
            if config_file:
                cmd.extend(['--config', config_file])
        elif subcommand == 'social':
            cmd.extend(['--limit', str(leads_per_run)])
            if config_file:
                cmd.extend(['--config', config_file])
        
        if target_list_id:
            cmd.extend(['--list-id', str(target_list_id)])
        
        logger.info(f"🚀 Executing: {' '.join(cmd)}")
        
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
                    logger.info(f"  {line}")
                    if 'leads validated' in line.lower():
                        try:
                            result['leads_found'] = int(line.split()[0])
                        except: pass
                    elif 'duplicate' in line.lower():
                        try:
                            result['leads_duplicates'] = int(line.split()[0])
                        except: pass
            
            process.wait()
            result['success'] = process.returncode == 0
            result['leads_saved'] = result['leads_found'] - result['leads_duplicates']
            if not result['success']:
                result['error'] = f"Exit code: {process.returncode}"
                
        except Exception as e:
            result['error'] = str(e)
            logger.error(f"Execution error: {e}")
        
        return result
    
    def should_run(self, config):
        if not config.get('should_run', False):
            return False
        if config.get('at_daily_limit', False):
            logger.info("📊 Daily limit reached")
            return False
        bot = config.get('bot', {})
        if not bot.get('is_enabled'):
            return False
        current_hour = datetime.now().hour
        run_start = int(bot.get('run_hours_start', 0) or 0)
        run_end = int(bot.get('run_hours_end', 23) or 23)
        if not (run_start <= current_hour <= run_end):
            logger.info(f"⏰ Outside schedule ({run_start}-{run_end}h, now: {current_hour}h)")
            return False
        return True
    
    def run_once(self):
        config = self.get_config()
        if not config:
            return False
        
        if not self.should_run(config):
            return False
        
        bot_config = config.get('config', {})
        run_id = self.report_start(bot_config)
        if not run_id:
            logger.error("❌ Failed to register run start")
            return False
        
        logger.info(f"▶️ Starting run #{run_id}")
        
        try:
            result = self.execute_bot(config)  # Pass full config, not bot_config
            self.report_end(
                run_id=run_id,
                status='completed' if result['success'] else 'error',
                error=result.get('error'),
                leads_found=result['leads_found'],
                leads_saved=result['leads_saved'],
                leads_duplicates=result['leads_duplicates']
            )
            logger.info(f"✅ Run #{run_id} done: {result['leads_saved']} saved")
            return True
        except Exception as e:
            self.report_end(run_id=run_id, status='error', error=str(e))
            return False
    
    def run_forever(self):
        logger.info("🔄 Starting daemon loop...")
        while not shutdown_requested:
            try:
                config = self.get_config()
                interval = 60
                if config and config.get('bot'):
                    interval = config['bot'].get('interval_minutes', 60)
                
                self.run_once()
                
                logger.info(f"💤 Sleeping {interval} min...")
                for _ in range(interval * 6):
                    if shutdown_requested:
                        break
                    time.sleep(10)
            except Exception as e:
                logger.error(f"Loop error: {e}")
                time.sleep(60)
        
        logger.info("🛑 Daemon stopped")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--bot-id', type=int, required=True)
    parser.add_argument('--staffkit-url', default=os.getenv('STAFFKIT_URL', 'https://staff.replanta.dev'))
    parser.add_argument('--api-key', default=os.getenv('STAFFKIT_API_KEY'))
    parser.add_argument('--once', action='store_true')
    
    args = parser.parse_args()
    
    if not args.api_key:
        print("❌ API Key required")
        sys.exit(1)
    
    daemon = ExternalBotDaemon(args.bot_id, args.staffkit_url, args.api_key)
    
    if args.once:
        daemon.run_once()
    else:
        daemon.run_forever()


if __name__ == '__main__':
    main()
