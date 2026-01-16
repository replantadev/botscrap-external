#!/usr/bin/env python3
"""
Notifier - Sistema de notificaciones por Telegram
"""

import logging
import requests
from datetime import datetime
from typing import Dict, Optional, List
import threading
import queue
import time

logger = logging.getLogger(__name__)


class Notifier:
    """
    Sistema de notificaciones por Telegram.
    Envía alertas sobre estado del worker, leads encontrados, errores, etc.
    """
    
    # Emojis para tipos de mensaje
    EMOJIS = {
        'success': '✅',
        'error': '❌',
        'warning': '⚠️',
        'info': 'ℹ️',
        'lead': '🎯',
        'start': '🚀',
        'stop': '🛑',
        'pause': '⏸️',
        'resume': '▶️',
        'critical': '🚨',
        'schedule': '📅',
        'stats': '📊',
    }
    
    def __init__(self, bot_token: str = None, chat_id: str = None, 
                 enabled: bool = True, async_mode: bool = True):
        """
        Args:
            bot_token: Token del bot de Telegram
            chat_id: ID del chat donde enviar mensajes
            enabled: Si las notificaciones están habilitadas
            async_mode: Si enviar mensajes de forma asíncrona
        """
        from config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
        
        self.bot_token = bot_token or TELEGRAM_BOT_TOKEN
        self.chat_id = chat_id or TELEGRAM_CHAT_ID
        self.enabled = enabled and bool(self.bot_token) and bool(self.chat_id)
        self.async_mode = async_mode
        
        # Cola para mensajes async
        self._message_queue: queue.Queue = queue.Queue()
        self._sender_thread: Optional[threading.Thread] = None
        self._running = False
        
        # Rate limiting
        self._last_message_time = None
        self._min_interval = 1  # Mínimo 1 segundo entre mensajes
        
        # Agrupación de mensajes similares
        self._message_counts: Dict[str, int] = {}
        self._last_message_type: Dict[str, datetime] = {}
        
        if self.enabled and self.async_mode:
            self._start_sender()
    
    def _start_sender(self):
        """Iniciar thread para envío async"""
        if self._running:
            return
        
        self._running = True
        self._sender_thread = threading.Thread(target=self._sender_loop, daemon=True)
        self._sender_thread.start()
    
    def _sender_loop(self):
        """Loop de envío de mensajes"""
        while self._running:
            try:
                # Esperar mensaje con timeout
                try:
                    message = self._message_queue.get(timeout=1)
                except queue.Empty:
                    continue
                
                # Rate limiting
                if self._last_message_time:
                    elapsed = (datetime.now() - self._last_message_time).total_seconds()
                    if elapsed < self._min_interval:
                        time.sleep(self._min_interval - elapsed)
                
                # Enviar
                self._send_telegram(message)
                self._last_message_time = datetime.now()
                
            except Exception as e:
                logger.error(f"Error in notifier sender: {e}")
    
    def stop(self):
        """Detener sender async"""
        self._running = False
        if self._sender_thread:
            self._sender_thread.join(timeout=5)
    
    def _send_telegram(self, message: str) -> bool:
        """
        Enviar mensaje a Telegram.
        
        Args:
            message: Texto del mensaje
            
        Returns:
            True si se envió correctamente
        """
        if not self.enabled:
            return False
        
        try:
            url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
            payload = {
                'chat_id': self.chat_id,
                'text': message,
                'parse_mode': 'HTML',
                'disable_web_page_preview': True,
            }
            
            response = requests.post(url, json=payload, timeout=10)
            
            if response.status_code == 200:
                return True
            else:
                logger.warning(f"Telegram API error: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"Error sending Telegram message: {e}")
            return False
    
    def _queue_message(self, message: str):
        """Añadir mensaje a la cola o enviar directamente"""
        if self.async_mode:
            self._message_queue.put(message)
        else:
            self._send_telegram(message)
    
    def _format_message(self, title: str, body: str = '', emoji: str = 'info') -> str:
        """Formatear mensaje con emoji y timestamp"""
        emoji_char = self.EMOJIS.get(emoji, 'ℹ️')
        timestamp = datetime.now().strftime('%H:%M')
        
        message = f"{emoji_char} <b>{title}</b> [{timestamp}]"
        if body:
            message += f"\n\n{body}"
        
        return message
    
    # === Métodos de notificación específicos ===
    
    def send(self, title: str, body: str = '', emoji: str = 'info'):
        """
        Enviar notificación genérica.
        
        Args:
            title: Título del mensaje
            body: Cuerpo del mensaje
            emoji: Tipo de emoji (success, error, warning, info, lead, etc.)
        """
        message = self._format_message(title, body, emoji)
        self._queue_message(message)
    
    def send_status(self, status: str):
        """Enviar actualización de estado"""
        self._queue_message(status)
    
    def send_leads_found(self, bot_type: str, leads_count: int, total_found: int = None):
        """
        Notificar leads encontrados.
        
        Args:
            bot_type: Tipo de bot
            leads_count: Leads guardados
            total_found: Total encontrados (opcional)
        """
        title = f"Leads encontrados ({bot_type})"
        
        body = f"💾 Guardados: <b>{leads_count}</b>"
        if total_found:
            body += f"\n🔍 Encontrados: {total_found}"
        
        self.send(title, body, 'lead')
    
    def send_error(self, bot_type: str, error: str):
        """
        Notificar error.
        
        Args:
            bot_type: Tipo de bot o componente
            error: Mensaje de error
        """
        title = f"Error en {bot_type}"
        self.send(title, f"<code>{error[:500]}</code>", 'error')
    
    def send_critical(self, message: str):
        """Enviar alerta crítica"""
        self.send("ALERTA CRÍTICA", message, 'critical')
    
    def send_daily_summary(self, stats: Dict):
        """
        Enviar resumen diario.
        
        Args:
            stats: Estadísticas del día
        """
        title = "📊 Resumen del día"
        
        body_parts = [
            f"🎯 Leads guardados: <b>{stats.get('leads_saved', 0)}</b>",
            f"🔄 Ejecuciones: {stats.get('runs', 0)}",
        ]
        
        # Por bot
        by_bot = stats.get('by_bot', {})
        if by_bot:
            body_parts.append("\n<b>Por bot:</b>")
            for bot, bot_stats in by_bot.items():
                body_parts.append(f"  • {bot}: {bot_stats.get('leads', 0)} leads")
        
        # Errores
        if stats.get('errors', 0) > 0:
            body_parts.append(f"\n⚠️ Errores: {stats['errors']}")
        
        body = "\n".join(body_parts)
        self.send(title, body, 'stats')
    
    def send_schedule_triggered(self, schedule_id: str, bot_type: str):
        """Notificar que un schedule se disparó"""
        self.send(
            f"Schedule ejecutado",
            f"📅 {schedule_id}\n🤖 Bot: {bot_type}",
            'schedule'
        )
    
    def send_worker_recovered(self, attempt: int):
        """Notificar recuperación del worker"""
        self.send(
            "Worker recuperado",
            f"🔄 Intento #{attempt} exitoso",
            'success'
        )
    
    def test_connection(self) -> bool:
        """
        Probar conexión con Telegram.
        
        Returns:
            True si la conexión funciona
        """
        if not self.bot_token or not self.chat_id:
            logger.warning("Telegram not configured")
            return False
        
        message = self._format_message("Test de conexión", "Bot conectado correctamente", 'success')
        return self._send_telegram(message)


class NotifierConfig:
    """Configuración de notificaciones"""
    
    # Tipos de eventos y si notificar por defecto
    DEFAULT_EVENTS = {
        'worker_started': True,
        'worker_stopped': True,
        'worker_paused': True,
        'worker_resumed': True,
        'leads_found': True,
        'error': True,
        'critical': True,
        'daily_summary': True,
        'schedule_triggered': False,  # Muy frecuente
        'worker_recovered': True,
        'health_check_failed': True,
    }
    
    def __init__(self, enabled_events: Dict[str, bool] = None):
        self.events = {**self.DEFAULT_EVENTS}
        if enabled_events:
            self.events.update(enabled_events)
    
    def is_enabled(self, event: str) -> bool:
        return self.events.get(event, False)
