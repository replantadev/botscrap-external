#!/usr/bin/env python3
"""
Telegram Notifier - Enviar notificaciones a Telegram
"""

import os
import logging
import requests
from typing import Optional

from config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, STAFFKIT_URL, STAFFKIT_API_KEY

logger = logging.getLogger(__name__)


class TelegramNotifier:
    """Helper para enviar notificaciones a Telegram"""
    
    def __init__(self, token: str = None, chat_id: str = None):
        """
        Inicializar notificador
        
        Args:
            token: Token del bot de Telegram
            chat_id: ID del chat destino
        """
        self.token = token or TELEGRAM_BOT_TOKEN
        self.chat_id = chat_id or TELEGRAM_CHAT_ID
        self._config_loaded = False
        
        # Intentar cargar config de StaffKit si no hay local
        if not self.token or not self.chat_id:
            self._load_from_staffkit()
    
    def _load_from_staffkit(self):
        """Cargar configuración de Telegram desde StaffKit API"""
        if self._config_loaded:
            return
        
        if not STAFFKIT_URL or not STAFFKIT_API_KEY:
            return
        
        try:
            response = requests.get(
                f"{STAFFKIT_URL}/api/v2/integrations.php/telegram",
                headers={
                    'Authorization': f'Bearer {STAFFKIT_API_KEY}',
                    'Content-Type': 'application/json',
                },
                timeout=5
            )
            
            if response.status_code == 200:
                data = response.json()
                if data.get('enabled'):
                    self.token = data.get('token') or self.token
                    self.chat_id = data.get('chat_id') or self.chat_id
                    logger.info("✅ Telegram config loaded from StaffKit")
            
            self._config_loaded = True
            
        except Exception as e:
            logger.debug(f"Could not load Telegram config from StaffKit: {e}")
    
    @property
    def enabled(self) -> bool:
        """Verificar si Telegram está configurado"""
        return bool(self.token and self.chat_id)
    
    def send(self, message: str, parse_mode: str = None) -> bool:
        """
        Enviar mensaje a Telegram
        
        Args:
            message: Mensaje a enviar
            parse_mode: 'HTML' o 'Markdown' (opcional)
            
        Returns:
            True si se envió correctamente
        """
        if not self.enabled:
            logger.debug("Telegram not configured, skipping notification")
            return False
        
        try:
            url = f"https://api.telegram.org/bot{self.token}/sendMessage"
            
            data = {
                'chat_id': self.chat_id,
                'text': message,
            }
            
            if parse_mode:
                data['parse_mode'] = parse_mode
            
            response = requests.post(url, data=data, timeout=10)
            
            if response.status_code == 200:
                logger.debug("✅ Telegram message sent")
                return True
            else:
                logger.warning(f"Telegram send failed: {response.status_code}")
                return False
                
        except Exception as e:
            logger.warning(f"Telegram send error: {e}")
            return False
    
    def notify_lead(self, lead: dict, bot_name: str = "Bot"):
        """Notificar lead encontrado"""
        priority = lead.get('prioridad', lead.get('priority', 'media'))
        emoji = {'hot': '🔥', 'alta': '🔥', 'high': '🔥'}.get(priority, '⭐')
        
        msg = (
            f"{emoji} *{bot_name}* - Nuevo Lead\n\n"
            f"🏢 {lead.get('empresa', lead.get('company', 'N/A'))}\n"
            f"🌐 {lead.get('web', lead.get('website', 'N/A'))}\n"
        )
        
        if lead.get('email'):
            msg += f"📧 {lead['email']}\n"
        
        if lead.get('puntuacion', lead.get('score')):
            msg += f"📊 Score: {lead.get('puntuacion', lead.get('score'))}\n"
        
        self.send(msg)
    
    def notify_summary(self, stats: dict, bot_name: str = "Bot"):
        """Notificar resumen de ejecución"""
        msg = (
            f"📊 *{bot_name}* - Resumen\n\n"
            f"🎯 Encontrados: {stats.get('leads_found', 0)}\n"
            f"💾 Guardados: {stats.get('leads_saved', 0)}\n"
            f"🔄 Duplicados: {stats.get('leads_duplicates', 0)}\n"
        )
        
        self.send(msg)
