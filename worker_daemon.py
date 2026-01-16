#!/usr/bin/env python3
"""
Daemon de Worker Autónomo para systemd
Ejecuta el orquestador como servicio de sistema
"""

import os
import sys
import logging
import signal
import time
from pathlib import Path

# Añadir directorio al path
BASE_DIR = Path(__file__).parent
sys.path.insert(0, str(BASE_DIR))

from dotenv import load_dotenv
load_dotenv(BASE_DIR / '.env')

# Configurar logging para daemon
LOG_FILE = BASE_DIR / 'logs' / 'worker_daemon.log'
LOG_FILE.parent.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger('worker_daemon')


def main():
    """Entry point principal"""
    logger.info("=" * 50)
    logger.info("WORKER DAEMON STARTING")
    logger.info("=" * 50)
    
    try:
        from orchestrator import get_orchestrator
        from config import validate_config
        
        # Validar configuración
        validation = validate_config()
        if not validation['valid']:
            logger.error("Configuration invalid:")
            for error in validation['errors']:
                logger.error(f"  - {error}")
            sys.exit(1)
        
        for warning in validation.get('warnings', []):
            logger.warning(f"Config warning: {warning}")
        
        # Crear y ejecutar orquestador
        orchestrator = get_orchestrator()
        orchestrator.run_forever()
        
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")
    except Exception as e:
        logger.exception(f"Fatal error: {e}")
        sys.exit(1)
    
    logger.info("Worker daemon stopped")


if __name__ == '__main__':
    main()
