#!/usr/bin/env python3
"""
SAP Business One Bot v2 - Extrae contactos usando Branch y sincroniza con StaffKit
Soporta modo daemon para sincronización continua
"""

import argparse
import hashlib
import json
import logging
import os
import signal
import sys
import time
from datetime import datetime
from typing import Optional, List, Dict

import pymssql
import requests

# ============================================================================
# CONFIGURACIÓN
# ============================================================================

SAP_CONFIG = {
    'server': os.environ.get('SAP_SERVER', '213.97.178.117'),
    'port': int(os.environ.get('SAP_PORT', 1435)),
    'database': os.environ.get('SAP_DATABASE', 'DRASANVI'),
    'user': os.environ.get('SAP_USER', ''),
    'password': os.environ.get('SAP_PASSWORD', '')
}

# Dominios de email genéricos (no corporativos)
GENERIC_EMAIL_DOMAINS = [
    'gmail.com', 'hotmail.com', 'hotmail.es', 'outlook.com', 'outlook.es',
    'yahoo.com', 'yahoo.es', 'icloud.com', 'live.com', 'msn.com',
    'telefonica.net', 'movistar.es', 'vodafone.es', 'orange.es', 'ono.com',
    'terra.es', 'terra.com', 'wanadoo.es', 'jazztel.es', 'ya.com',
    'mail.com', 'protonmail.com', 'gmx.com', 'gmx.es', 'aol.com',
    'me.com', 'mac.com', 'yandex.com', 'zoho.com'
]

# Mapeo de Branch a categorías para UI
BRANCH_CATEGORIES = {
    'CANAL_FARMA': [
        'FARMACIA', 'PARAFARMACIA FA', 'MAYORISTA FARMA', 'DISTRIBUIDOR FARMA', 'ORTOPEDIA'
    ],
    'CANAL_HERBO': [
        'DIET+HERBORIST', 'PARAFARMACIA HE', 'SUPER-T.ECOLOGI', 'TIENDA CANNABIS'
    ],
    'PROFESIONALES': [
        'NATUROPATA', 'NUTRICION DEPOR', 'CLINICA', 'PROF.FISIO', 'PROF.MEDICO', 'ESTETICA BELLEZ'
    ],
    'VETERINARIO': [
        'CLINICA VETERIN', 'TIENDA VETERINA', 'DISTRIBUIDOR VE'
    ],
    'DISTRIBUCION': [
        'DISTRIBUIDOR', 'MAYORISTA', 'GRUPO', 'COOPERATIVA', 'EXPORTACION'
    ],
    'OTROS': [
        'MARKETING', 'OTROS', 'PERFUMERIA', 'CAFETERIA', 'CLUB DEPORTIVO', 'HOTEL', 
        'RESTAURANTE', 'ALIMENTACION', 'EMPRESAS', 'FRANQUICIA', 'GASOLINERAS'
    ],
    'POTENCIALES': [
        'CLTE POTENCIAL'
    ]
}

# ============================================================================
# LOGGING
# ============================================================================

LOG_DIR = os.environ.get('LOG_DIR', '/var/www/vhosts/territoriodrasanvicr.com/b/logs')
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(os.path.join(LOG_DIR, 'sap_bot.log'))
    ]
)
logger = logging.getLogger('sap_bot')

# ============================================================================
# SAP CONNECTION
# ============================================================================

class SAPConnection:
    """Conexión persistente a SAP SQL Server"""
    
    def __init__(self):
        self.conn = None
        self.cursor = None
        self.last_query_time = None
    
    def connect(self):
        """Establece conexión a SAP"""
        if self.conn:
            return
        
        logger.info(f"Conectando a SAP ({SAP_CONFIG['server']}:{SAP_CONFIG['port']})...")
        self.conn = pymssql.connect(
            server=SAP_CONFIG['server'],
            port=SAP_CONFIG['port'],
            database=SAP_CONFIG['database'],
            user=SAP_CONFIG['user'],
            password=SAP_CONFIG['password'],
            timeout=30,
            login_timeout=10
        )
        self.cursor = self.conn.cursor(as_dict=True)
        logger.info("Conectado a SAP")
    
    def disconnect(self):
        """Cierra conexión"""
        if self.conn:
            self.conn.close()
            self.conn = None
            self.cursor = None
            logger.info("Desconectado de SAP")
    
    def reconnect(self):
        """Reconecta si es necesario"""
        try:
            self.cursor.execute("SELECT 1")
            self.cursor.fetchone()
        except:
            logger.warning("Conexión perdida, reconectando...")
            self.disconnect()
            self.connect()
    
    def query(self, sql, params=None):
        """Ejecuta query y retorna resultados"""
        self.reconnect()
        self.cursor.execute(sql, params)
        self.last_query_time = datetime.now()
        return self.cursor.fetchall()

# ============================================================================
# STAFFKIT API
# ============================================================================

class StaffKitAPI:
    """Cliente para StaffKit API"""
    
    def __init__(self, api_key: str, base_url: str = "https://staff.replanta.dev"):
        self.api_key = api_key
        self.base_url = base_url.rstrip('/')
        self.session = requests.Session()
        self.session.headers['Authorization'] = f'Bearer {api_key}'
        self.session.headers['Content-Type'] = 'application/json'
    
    def check_duplicate(self, email: str) -> bool:
        """Verifica si email ya existe"""
        try:
            resp = self.session.post(
                f"{self.base_url}/api/v2/check-duplicate",
                json={'email': email}
            )
            if resp.ok:
                data = resp.json()
                return data.get('exists', False)
        except Exception as e:
            logger.error(f"Error check duplicate: {e}")
        return False
    
    def add_prospect(self, prospect: dict, list_id: int) -> dict:
        """Añade prospecto a lista"""
        try:
            payload = {
                'list_id': list_id,
                'prospects': [prospect]
            }
            resp = self.session.post(
                f"{self.base_url}/api/v2/prospects",
                json=payload
            )
            return resp.json()
        except Exception as e:
            logger.error(f"Error add prospect: {e}")
            return {'error': str(e)}
    
    def add_prospects_batch(self, prospects: list, list_id: int) -> dict:
        """Añade varios prospectos de una vez"""
        try:
            payload = {
                'list_id': list_id,
                'prospects': prospects
            }
            resp = self.session.post(
                f"{self.base_url}/api/v2/prospects",
                json=payload
            )
            return resp.json()
        except Exception as e:
            logger.error(f"Error add prospects batch: {e}")
            return {'error': str(e)}

# ============================================================================
# SAP BOT
# ============================================================================

class SAPBot:
    """Bot principal de extracción SAP"""
    
    def __init__(self, staffkit_api: StaffKitAPI, list_id: int):
        self.sap = SAPConnection()
        self.staffkit = staffkit_api
        self.list_id = list_id
        self.running = True
        self.stats = {
            'total_queried': 0,
            'emails_found': 0,
            'corporate_emails': 0,
            'duplicates': 0,
            'added': 0,
            'errors': 0
        }
        self.processed_hashes = set()
        self.state_file = f"/var/www/vhosts/territoriodrasanvicr.com/b/sap_bot_state_{list_id}.json"
        
        # Signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Maneja señales de terminación"""
        logger.info("Señal de terminación recibida, cerrando...")
        self.running = False
    
    def _load_state(self):
        """Carga estado guardado"""
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, 'r') as f:
                    state = json.load(f)
                    self.processed_hashes = set(state.get('processed_hashes', []))
                    logger.info(f"Estado cargado: {len(self.processed_hashes)} registros procesados")
            except:
                pass
    
    def _save_state(self):
        """Guarda estado"""
        try:
            with open(self.state_file, 'w') as f:
                json.dump({
                    'processed_hashes': list(self.processed_hashes),
                    'last_run': datetime.now().isoformat(),
                    'stats': self.stats
                }, f)
        except Exception as e:
            logger.error(f"Error guardando estado: {e}")
    
    def _record_hash(self, record: dict) -> str:
        """Genera hash único de registro"""
        key = f"{record.get('CardCode', '')}_{record.get('Email', '')}"
        return hashlib.md5(key.encode()).hexdigest()
    
    def _is_corporate_email(self, email: str) -> bool:
        """Verifica si es email corporativo"""
        if not email:
            return False
        email_lower = email.lower()
        domain = email_lower.split('@')[-1] if '@' in email_lower else ''
        return domain not in GENERIC_EMAIL_DOMAINS
    
    def get_columns(self) -> list:
        """Obtiene columnas de _WEB_Clientes"""
        self.sap.connect()
        results = self.sap.query("""
            SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = '_WEB_Clientes'
            ORDER BY ORDINAL_POSITION
        """)
        return [r['COLUMN_NAME'] for r in results]
    
    def get_branches(self) -> list:
        """Obtiene lista de branches disponibles con conteos"""
        self.sap.connect()
        results = self.sap.query("""
            SELECT Branch, COUNT(*) as total
            FROM _WEB_Clientes
            WHERE Branch IS NOT NULL
            GROUP BY Branch
            ORDER BY total DESC
        """)
        return results
    
    def get_contacts(self, branches: List[str] = None, corporate_only: bool = False, 
                     limit: int = None, exclude_potentials: bool = True,
                     only_new: bool = False, since_date: str = None) -> list:
        """
        Extrae contactos de SAP haciendo JOIN entre _WEB_Clientes y OCRD
        
        Args:
            branches: Lista de Branch a incluir (None = todos)
            corporate_only: Solo emails corporativos
            limit: Límite de registros
            exclude_potentials: Excluir CLTE POTENCIAL
            only_new: Solo registros nuevos desde última ejecución
            since_date: Fecha desde la que buscar (YYYY-MM-DD)
        """
        self.sap.connect()
        
        # Construir WHERE clause
        conditions = ["o.E_Mail IS NOT NULL", "o.E_Mail != ''"]
        
        if exclude_potentials:
            conditions.append("w.Branch != 'CLTE POTENCIAL'")
        
        if branches:
            branch_list = "', '".join(branches)
            conditions.append(f"w.Branch IN ('{branch_list}')")
        
        if since_date:
            conditions.append(f"o.UpdateDate >= '{since_date}'")
        
        where_clause = " AND ".join(conditions)
        
        # Query con JOIN entre _WEB_Clientes y OCRD
        sql = f"""
            SELECT 
                o.CardCode,
                o.CardName,
                o.E_Mail as Email,
                w.Branch,
                w.Telefono as Phone1,
                o.Phone2,
                w.Street as Address,
                w.City,
                w.ZipCode,
                w.County as Country,
                o.CreateDate,
                o.UpdateDate
            FROM _WEB_Clientes w
            INNER JOIN OCRD o ON w.CardCode = o.CardCode
            WHERE {where_clause}
            ORDER BY o.UpdateDate DESC
        """
        
        if limit:
            sql = sql.replace("SELECT", f"SELECT TOP {limit}")
        
        logger.info(f"Consultando SAP (branches={branches}, corporate_only={corporate_only}, limit={limit})...")
        results = self.sap.query(sql)
        self.stats['total_queried'] = len(results)
        logger.info(f"Obtenidos {len(results)} registros de SAP")
        
        # Filtrar corporate si es necesario
        if corporate_only:
            results = [r for r in results if self._is_corporate_email(r.get('Email', ''))]
            self.stats['corporate_emails'] = len(results)
            logger.info(f"Filtrados a {len(results)} emails corporativos")
        
        return results
    
    def sync_to_staffkit(self, contacts: list, check_duplicates: bool = True, 
                         batch_size: int = 50) -> dict:
        """
        Sincroniza contactos con StaffKit
        
        Args:
            contacts: Lista de contactos de SAP
            check_duplicates: Verificar duplicados en StaffKit
            batch_size: Tamaño de lote para envío
        """
        prospects_to_add = []
        
        for contact in contacts:
            if not self.running:
                break
            
            email = contact.get('Email', '').strip().lower()
            if not email:
                continue
            
            # Check si ya procesamos este registro
            record_hash = self._record_hash(contact)
            if record_hash in self.processed_hashes:
                continue
            
            self.stats['emails_found'] += 1
            
            # Check duplicado en StaffKit
            if check_duplicates and self.staffkit.check_duplicate(email):
                self.stats['duplicates'] += 1
                self.processed_hashes.add(record_hash)
                continue
            
            # Preparar prospecto
            prospect = {
                'email': email,
                'first_name': self._extract_first_name(contact.get('CardName', '')),
                'last_name': self._extract_last_name(contact.get('CardName', '')),
                'company': contact.get('CardName', ''),
                'phone': contact.get('Phone1') or contact.get('Phone2') or '',
                'city': contact.get('City', ''),
                'country': contact.get('Country', ''),
                'website': contact.get('Website', ''),
                'source': 'SAP B1',
                'custom_fields': {
                    'sap_cardcode': contact.get('CardCode', ''),
                    'sap_branch': contact.get('Branch', ''),
                    'sap_address': contact.get('Address', ''),
                    'sap_zipcode': contact.get('ZipCode', '')
                }
            }
            
            prospects_to_add.append(prospect)
            self.processed_hashes.add(record_hash)
            
            # Enviar batch si alcanzamos el tamaño
            if len(prospects_to_add) >= batch_size:
                self._send_batch(prospects_to_add)
                prospects_to_add = []
        
        # Enviar último batch
        if prospects_to_add:
            self._send_batch(prospects_to_add)
        
        self._save_state()
        return self.stats
    
    def _send_batch(self, prospects: list):
        """Envía lote de prospectos a StaffKit"""
        if not prospects:
            return
        
        logger.info(f"Enviando batch de {len(prospects)} prospectos a StaffKit...")
        result = self.staffkit.add_prospects_batch(prospects, self.list_id)
        
        if 'error' in result:
            self.stats['errors'] += len(prospects)
            logger.error(f"Error en batch: {result['error']}")
        else:
            added = result.get('added', len(prospects))
            self.stats['added'] += added
            logger.info(f"Añadidos {added} prospectos a lista {self.list_id}")
    
    def _extract_first_name(self, full_name: str) -> str:
        """Extrae nombre de nombre completo"""
        if not full_name:
            return ''
        parts = full_name.strip().split()
        return parts[0] if parts else ''
    
    def _extract_last_name(self, full_name: str) -> str:
        """Extrae apellido de nombre completo"""
        if not full_name:
            return ''
        parts = full_name.strip().split()
        return ' '.join(parts[1:]) if len(parts) > 1 else ''
    
    def run_once(self, branches: List[str] = None, corporate_only: bool = False, 
                 limit: int = None) -> dict:
        """Ejecuta extracción una vez"""
        self._load_state()
        
        contacts = self.get_contacts(
            branches=branches,
            corporate_only=corporate_only,
            limit=limit
        )
        
        stats = self.sync_to_staffkit(contacts)
        
        self.sap.disconnect()
        return stats
    
    def run_daemon(self, branches: List[str] = None, corporate_only: bool = False,
                   interval: int = 300, limit: int = None):
        """
        Ejecuta en modo daemon (sincronización continua)
        
        Args:
            branches: Branches a monitorear
            corporate_only: Solo emails corporativos
            interval: Segundos entre sincronizaciones
            limit: Límite por ciclo
        """
        self._load_state()
        logger.info(f"Iniciando modo daemon (intervalo={interval}s)...")
        
        while self.running:
            try:
                logger.info("=" * 60)
                logger.info(f"Ciclo de sincronización: {datetime.now()}")
                
                contacts = self.get_contacts(
                    branches=branches,
                    corporate_only=corporate_only,
                    limit=limit
                )
                
                # Solo procesar nuevos/actualizados
                new_contacts = [c for c in contacts 
                               if self._record_hash(c) not in self.processed_hashes]
                
                if new_contacts:
                    logger.info(f"Encontrados {len(new_contacts)} registros nuevos/actualizados")
                    self.sync_to_staffkit(new_contacts, check_duplicates=True)
                else:
                    logger.info("Sin cambios detectados")
                
                self._save_state()
                
                # Esperar próximo ciclo
                logger.info(f"Esperando {interval}s hasta próximo ciclo...")
                for _ in range(interval):
                    if not self.running:
                        break
                    time.sleep(1)
                    
            except Exception as e:
                logger.error(f"Error en ciclo: {e}")
                time.sleep(30)  # Espera corta ante error
        
        self.sap.disconnect()
        logger.info("Daemon finalizado")
        logger.info(f"Stats finales: {self.stats}")

# ============================================================================
# CLI
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description='SAP Business One Bot v2 para StaffKit')
    
    # Credenciales StaffKit
    parser.add_argument('--staffkit-api-key', required=True, help='API Key de StaffKit')
    parser.add_argument('--list-id', type=int, required=True, help='ID de lista en StaffKit')
    
    # Credenciales SAP (desde StaffKit UI)
    parser.add_argument('--sap-server', help='Servidor SAP')
    parser.add_argument('--sap-port', type=int, help='Puerto SAP')
    parser.add_argument('--sap-database', help='Base de datos SAP')
    parser.add_argument('--sap-user', help='Usuario SAP')
    parser.add_argument('--sap-password', help='Password SAP')
    
    # Filtros
    parser.add_argument('--branches', nargs='+', help='Branches a extraer (ej: FARMACIA DIET+HERBORIST)')
    parser.add_argument('--category', choices=list(BRANCH_CATEGORIES.keys()), 
                        help='Categoría de branches predefinida')
    parser.add_argument('--corporate-only', action='store_true', help='Solo emails corporativos')
    parser.add_argument('--include-potentials', action='store_true', help='Incluir CLTE POTENCIAL')
    parser.add_argument('--limit', type=int, help='Límite de registros')
    
    # Modo
    parser.add_argument('--daemon', action='store_true', help='Ejecutar en modo daemon')
    parser.add_argument('--interval', type=int, default=300, help='Intervalo en segundos para daemon (default: 300)')
    
    # Utilidades
    parser.add_argument('--list-branches', action='store_true', help='Listar branches disponibles')
    parser.add_argument('--list-columns', action='store_true', help='Listar columnas de _WEB_Clientes')
    parser.add_argument('--dry-run', action='store_true', help='Simular sin enviar a StaffKit')
    
    args = parser.parse_args()
    
    # Configurar SAP desde argumentos o env vars
    if args.sap_server:
        SAP_CONFIG['server'] = args.sap_server
    if args.sap_port:
        SAP_CONFIG['port'] = args.sap_port
    if args.sap_database:
        SAP_CONFIG['database'] = args.sap_database
    if args.sap_user:
        SAP_CONFIG['user'] = args.sap_user
    if args.sap_password:
        SAP_CONFIG['password'] = args.sap_password
    
    # Validar credenciales SAP
    if not SAP_CONFIG['user'] or not SAP_CONFIG['password']:
        logger.error("Faltan credenciales SAP. Usa --sap-user y --sap-password o variables de entorno SAP_USER/SAP_PASSWORD")
        sys.exit(1)
    
    # Resolver branches
    branches = args.branches
    if args.category:
        branches = BRANCH_CATEGORIES.get(args.category, [])
        logger.info(f"Categoría {args.category}: {branches}")
    
    # Crear API client
    staffkit = StaffKitAPI(args.staffkit_api_key)
    
    # Crear bot
    bot = SAPBot(staffkit, args.list_id)
    
    # Modo listar columnas
    if args.list_columns:
        bot.sap.connect()
        columns = bot.get_columns()
        print("\nColumnas de _WEB_Clientes:")
        print("-" * 50)
        for col in columns:
            print(f"  {col}")
        bot.sap.disconnect()
        return
    
    # Modo listar branches
    if args.list_branches:
        bot.sap.connect()
        branches_list = bot.get_branches()
        print("\nBranches disponibles en SAP:")
        print("-" * 50)
        total = 0
        for b in branches_list:
            print(f"  {b['Branch']}: {b['total']} registros")
            total += b['total']
        print("-" * 50)
        print(f"  TOTAL: {total} registros")
        bot.sap.disconnect()
        return
    
    # Ejecutar
    if args.daemon:
        bot.run_daemon(
            branches=branches,
            corporate_only=args.corporate_only,
            interval=args.interval,
            limit=args.limit
        )
    else:
        stats = bot.run_once(
            branches=branches,
            corporate_only=args.corporate_only,
            limit=args.limit
        )
        
        print("\n" + "=" * 60)
        print("RESUMEN DE EJECUCIÓN")
        print("=" * 60)
        print(f"  Total consultados: {stats['total_queried']}")
        print(f"  Emails encontrados: {stats['emails_found']}")
        print(f"  Emails corporativos: {stats['corporate_emails']}")
        print(f"  Duplicados: {stats['duplicates']}")
        print(f"  Añadidos: {stats['added']}")
        print(f"  Errores: {stats['errors']}")

if __name__ == '__main__':
    main()
