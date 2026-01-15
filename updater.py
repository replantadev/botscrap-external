#!/usr/bin/env python3
"""
Sistema de actualizaciones automáticas via Git
"""

import os
import subprocess
import logging
from datetime import datetime
from pathlib import Path
from dataclasses import dataclass
from typing import List, Optional, Tuple

logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).parent


@dataclass
class Commit:
    """Representa un commit de Git"""
    hash: str
    short_hash: str
    author: str
    date: str
    message: str


@dataclass 
class UpdateInfo:
    """Información de actualización disponible"""
    current_version: str
    latest_version: str
    commits_behind: int
    commits: List[Commit]
    has_updates: bool
    last_check: str


class Updater:
    """Gestor de actualizaciones via Git"""
    
    def __init__(self, repo_path: str = None):
        self.repo_path = Path(repo_path) if repo_path else BASE_DIR
        self.remote = 'origin'
        self.branch = self._get_current_branch()
    
    def _run_git(self, *args, capture_output: bool = True) -> Tuple[bool, str]:
        """Ejecutar comando git"""
        try:
            result = subprocess.run(
                ['git'] + list(args),
                cwd=str(self.repo_path),
                capture_output=capture_output,
                text=True,
                timeout=30
            )
            
            if result.returncode == 0:
                return True, result.stdout.strip()
            else:
                return False, result.stderr.strip()
                
        except subprocess.TimeoutExpired:
            return False, "Timeout ejecutando comando git"
        except FileNotFoundError:
            return False, "Git no está instalado"
        except Exception as e:
            return False, str(e)
    
    def _get_current_branch(self) -> str:
        """Obtener rama actual"""
        success, output = self._run_git('rev-parse', '--abbrev-ref', 'HEAD')
        return output if success else 'main'
    
    def get_current_version(self) -> str:
        """Obtener hash del commit actual"""
        success, output = self._run_git('rev-parse', '--short', 'HEAD')
        return output if success else 'unknown'
    
    def get_current_version_full(self) -> str:
        """Obtener hash completo del commit actual"""
        success, output = self._run_git('rev-parse', 'HEAD')
        return output if success else 'unknown'
    
    def fetch_updates(self) -> Tuple[bool, str]:
        """Descargar información de actualizaciones del remoto"""
        logger.info(f"Fetching updates from {self.remote}/{self.branch}...")
        return self._run_git('fetch', self.remote, self.branch)
    
    def check_for_updates(self) -> UpdateInfo:
        """
        Verificar si hay actualizaciones disponibles
        
        Returns:
            UpdateInfo con información de actualizaciones
        """
        # Fetch primero
        fetch_success, fetch_msg = self.fetch_updates()
        
        if not fetch_success:
            logger.warning(f"Could not fetch: {fetch_msg}")
            return UpdateInfo(
                current_version=self.get_current_version(),
                latest_version='unknown',
                commits_behind=0,
                commits=[],
                has_updates=False,
                last_check=datetime.now().isoformat()
            )
        
        # Obtener versión actual
        current = self.get_current_version()
        
        # Obtener versión remota
        success, remote_hash = self._run_git('rev-parse', '--short', f'{self.remote}/{self.branch}')
        latest = remote_hash if success else current
        
        # Contar commits detrás
        success, count = self._run_git(
            'rev-list', '--count', 
            f'HEAD..{self.remote}/{self.branch}'
        )
        commits_behind = int(count) if success and count.isdigit() else 0
        
        # Obtener lista de commits nuevos
        commits = []
        if commits_behind > 0:
            success, log_output = self._run_git(
                'log', '--oneline', '--format=%H|%h|%an|%ad|%s',
                '--date=short',
                f'HEAD..{self.remote}/{self.branch}'
            )
            
            if success and log_output:
                for line in log_output.split('\n'):
                    if line and '|' in line:
                        parts = line.split('|', 4)
                        if len(parts) >= 5:
                            commits.append(Commit(
                                hash=parts[0],
                                short_hash=parts[1],
                                author=parts[2],
                                date=parts[3],
                                message=parts[4]
                            ))
        
        return UpdateInfo(
            current_version=current,
            latest_version=latest,
            commits_behind=commits_behind,
            commits=commits,
            has_updates=commits_behind > 0,
            last_check=datetime.now().isoformat()
        )
    
    def get_local_changes(self) -> Tuple[bool, List[str]]:
        """Verificar si hay cambios locales sin commitear"""
        success, output = self._run_git('status', '--porcelain')
        
        if not success:
            return False, []
        
        changes = [line for line in output.split('\n') if line.strip()]
        return len(changes) > 0, changes
    
    def stash_changes(self) -> Tuple[bool, str]:
        """Guardar cambios locales en stash"""
        return self._run_git('stash', 'push', '-m', f'Auto-stash before update {datetime.now().isoformat()}')
    
    def pull_updates(self, force: bool = False) -> Tuple[bool, str]:
        """
        Aplicar actualizaciones
        
        Args:
            force: Si True, hace stash de cambios locales primero
            
        Returns:
            (success, message)
        """
        # Verificar cambios locales
        has_changes, changes = self.get_local_changes()
        
        if has_changes and not force:
            return False, f"Hay {len(changes)} cambios locales. Usa force=True para guardarlos en stash."
        
        if has_changes and force:
            logger.info("Stashing local changes...")
            stash_success, stash_msg = self.stash_changes()
            if not stash_success:
                return False, f"Error al guardar cambios: {stash_msg}"
        
        # Pull
        logger.info(f"Pulling from {self.remote}/{self.branch}...")
        success, output = self._run_git('pull', self.remote, self.branch)
        
        if success:
            new_version = self.get_current_version()
            return True, f"Actualizado a {new_version}. {output}"
        else:
            return False, f"Error al actualizar: {output}"
    
    def get_changelog(self, from_version: str = None, limit: int = 20) -> List[Commit]:
        """Obtener historial de cambios"""
        args = ['log', '--oneline', '--format=%H|%h|%an|%ad|%s', '--date=short', f'-{limit}']
        
        if from_version:
            args.append(f'{from_version}..HEAD')
        
        success, output = self._run_git(*args)
        
        commits = []
        if success and output:
            for line in output.split('\n'):
                if line and '|' in line:
                    parts = line.split('|', 4)
                    if len(parts) >= 5:
                        commits.append(Commit(
                            hash=parts[0],
                            short_hash=parts[1],
                            author=parts[2],
                            date=parts[3],
                            message=parts[4]
                        ))
        
        return commits
    
    def get_status(self) -> dict:
        """Obtener estado general del repositorio"""
        return {
            'repo_path': str(self.repo_path),
            'branch': self.branch,
            'remote': self.remote,
            'current_version': self.get_current_version(),
            'has_local_changes': self.get_local_changes()[0],
        }


# Singleton global
_updater = None

def get_updater() -> Updater:
    """Obtener instancia del updater"""
    global _updater
    if _updater is None:
        _updater = Updater()
    return _updater
