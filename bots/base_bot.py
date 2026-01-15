#!/usr/bin/env python3
"""
Base Bot - Clase base para todos los bots
"""

import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict, List, Optional

from config import STAFFKIT_LIST_ID, BOT_NAME
from staffkit_client import StaffKitClient

logger = logging.getLogger(__name__)


class BaseBot(ABC):
    """Clase base para todos los bots de lead generation"""
    
    def __init__(self, dry_run: bool = False):
        """
        Inicializar bot
        
        Args:
            dry_run: Si True, no guarda leads (solo muestra)
        """
        self.dry_run = dry_run
        self.staffkit = StaffKitClient()
        
        # Estadísticas
        self.stats = {
            'leads_found': 0,
            'leads_saved': 0,
            'leads_duplicates': 0,
            'leads_errors': 0,
            'started_at': None,
            'completed_at': None,
        }
        
        self.run_id = None
        self.list_id = STAFFKIT_LIST_ID
    
    @abstractmethod
    def run(self, **kwargs) -> Dict:
        """Ejecutar el bot - implementar en subclases"""
        pass
    
    def save_lead(self, lead: Dict, list_id: int = None) -> Dict:
        """
        Guardar un lead en StaffKit
        
        Args:
            lead: Datos del lead
            list_id: ID de lista (override)
            
        Returns:
            Resultado del guardado
        """
        if self.dry_run:
            logger.info(f"[DRY RUN] Lead: {lead.get('website', lead.get('web', 'N/A'))}")
            self.stats['leads_found'] += 1
            return {'success': True, 'status': 'dry_run'}
        
        list_id = list_id or self.list_id
        
        result = self.staffkit.save_lead(
            lead=lead,
            list_id=list_id,
            run_id=self.run_id
        )
        
        if result.get('success'):
            if result.get('status') == 'duplicate':
                self.stats['leads_duplicates'] += 1
            else:
                self.stats['leads_saved'] += 1
        else:
            self.stats['leads_errors'] += 1
        
        self.stats['leads_found'] += 1
        
        return result
    
    def check_duplicate(self, domain: str) -> bool:
        """Verificar si un dominio ya existe"""
        if self.dry_run:
            return False
        return self.staffkit.check_duplicate(domain)
    
    def check_duplicates_batch(self, domains: List[str]) -> Dict[str, bool]:
        """Verificar duplicados en batch"""
        if self.dry_run:
            return {d: False for d in domains}
        return self.staffkit.check_duplicates_batch(domains)
    
    def update_progress(self, current_action: str = None):
        """Actualizar progreso en StaffKit"""
        if self.dry_run or not self.run_id:
            return
        
        self.staffkit.update_progress(
            run_id=self.run_id,
            leads_found=self.stats['leads_found'],
            leads_saved=self.stats['leads_saved'],
            leads_duplicates=self.stats['leads_duplicates'],
            current_action=current_action
        )
    
    def complete(self, status: str = 'completed', error: str = None):
        """Marcar ejecución como completada"""
        self.stats['completed_at'] = datetime.now().isoformat()
        
        if self.dry_run or not self.run_id:
            return
        
        self.staffkit.complete_run(
            run_id=self.run_id,
            leads_found=self.stats['leads_found'],
            leads_saved=self.stats['leads_saved'],
            leads_duplicates=self.stats['leads_duplicates'],
            status=status,
            error=error
        )
    
    def get_stats(self) -> Dict:
        """Obtener estadísticas de la ejecución"""
        return self.stats.copy()
