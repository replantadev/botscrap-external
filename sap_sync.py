#!/usr/bin/env python3
"""
SAP Business One → StaffKit Sync
================================
Sincronizador diario de contactos SAP a StaffKit.

- Una conexión por ejecución
- Extrae solo registros nuevos (desde último CardCode)
- Filtra emails corporativos
- Envía en batch a StaffKit
- Guarda estado para no repetir

Uso:
    python sap_sync.py --bot-id 3 --api-key "sk_xxx"
    
    O con parámetros manuales:
    python sap_sync.py --sap-server 1.2.3.4 --sap-database DB --branches FARMACIA --list-id 10
"""

import argparse
import json
import logging
import os
import re
import sys
from datetime import datetime

import pymssql
import requests

# ============================================================================
# CONFIGURACIÓN
# ============================================================================

STATE_DIR = '/var/www/vhosts/territoriodrasanvicr.com/b/sync_state'
STAFFKIT_URL = 'https://staff.replanta.dev'

# Dominios de email genéricos (no corporativos)
GENERIC_DOMAINS = {
    'gmail.com', 'hotmail.com', 'hotmail.es', 'outlook.com', 'outlook.es',
    'yahoo.com', 'yahoo.es', 'live.com', 'msn.com', 'icloud.com',
    'protonmail.com', 'mail.com', 'aol.com', 'zoho.com',
    'telefonica.net', 'orange.es', 'vodafone.es', 'movistar.es'
}

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


# ============================================================================
# FUNCIONES AUXILIARES
# ============================================================================

def is_corporate_email(email: str) -> bool:
    """Verifica si es email corporativo"""
    if not email or '@' not in email:
        return False
    domain = email.lower().split('@')[1]
    return domain not in GENERIC_DOMAINS


def clean_email(email: str) -> str:
    """Limpia y extrae primer email válido"""
    if not email:
        return ''
    # Si hay múltiples emails separados por ; o ,
    emails = re.split(r'[;,\s]+', str(email))
    for e in emails:
        e = e.strip().lower()
        if '@' in e and '.' in e.split('@')[1]:
            return e
    return ''


def load_state(bot_id: int) -> dict:
    """Carga estado del bot"""
    os.makedirs(STATE_DIR, exist_ok=True)
    state_file = os.path.join(STATE_DIR, f'sap_bot_{bot_id}.json')
    if os.path.exists(state_file):
        try:
            with open(state_file, 'r') as f:
                return json.load(f)
        except:
            pass
    return {'last_cardcode': '', 'last_sync': None, 'total_synced': 0}


def save_state(bot_id: int, state: dict):
    """Guarda estado del bot"""
    os.makedirs(STATE_DIR, exist_ok=True)
    state_file = os.path.join(STATE_DIR, f'sap_bot_{bot_id}.json')
    state['last_sync'] = datetime.now().isoformat()
    with open(state_file, 'w') as f:
        json.dump(state, f, indent=2)


# ============================================================================
# SAP EXTRACTION
# ============================================================================

def extract_from_sap(config: dict, last_cardcode: str = '') -> list:
    """
    Extrae contactos de SAP Business One.
    
    Args:
        config: Configuración de conexión SAP
        last_cardcode: Último CardCode procesado (para incrementales)
    
    Returns:
        Lista de contactos con email corporativo
    """
    server = config['server']
    port = config.get('port', 1435)
    database = config['database']
    user = config['user']
    password = config['password']
    branches = config.get('branches', [])
    corporate_only = config.get('corporate_only', True)
    limit = config.get('limit', 5000)
    
    logger.info(f"Conectando a SAP ({server}:{port})...")
    
    try:
        conn = pymssql.connect(
            server=server,
            port=port,
            user=user,
            password=password,
            database=database,
            timeout=30,
            login_timeout=15
        )
        cursor = conn.cursor(as_dict=True)
        logger.info("Conectado a SAP")
        
        # Construir query - Usa JOIN con _WEB_Clientes que tiene el Branch
        where_clauses = ["o.E_Mail IS NOT NULL", "o.E_Mail != ''"]
        
        # Filtrar por Branch si se especifica (Branch está en _WEB_Clientes)
        if branches:
            branch_list = "', '".join(branches)
            where_clauses.append(f"w.Branch IN ('{branch_list}')")
        
        # Solo registros nuevos (CardCode > último procesado)
        if last_cardcode:
            where_clauses.append(f"o.CardCode > '{last_cardcode}'")
        
        where_sql = " AND ".join(where_clauses)
        
        # Query con JOIN entre OCRD y _WEB_Clientes (donde está Branch)
        # Website: usa COALESCE con fallback U_DRA_Web -> IntrntSite -> NTSWebSite
        query = f"""
            SELECT TOP {limit}
                o.CardCode,
                o.CardName,
                o.E_Mail AS Email,
                o.Phone1 AS Phone,
                COALESCE(w.City, o.City) AS City,
                COALESCE(w.County, o.Country) AS Country,
                w.Branch,
                COALESCE(w.Street, o.Address) AS Address,
                COALESCE(w.ZipCode, o.ZipCode) AS ZipCode,
                COALESCE(NULLIF(o.U_DRA_Web, ''), NULLIF(o.IntrntSite, ''), o.NTSWebSite) AS Website
            FROM OCRD o
            INNER JOIN _WEB_Clientes w ON o.CardCode = w.CardCode
            WHERE {where_sql}
            ORDER BY o.CardCode ASC
        """
        
        logger.info(f"Ejecutando query (branches={branches}, desde={last_cardcode or 'inicio'})...")
        cursor.execute(query)
        rows = cursor.fetchall()
        logger.info(f"Obtenidos {len(rows)} registros de SAP")
        
        # Procesar y filtrar
        contacts = []
        for row in rows:
            email = clean_email(row.get('Email', ''))
            
            # Filtrar emails corporativos si se requiere
            if corporate_only and not is_corporate_email(email):
                continue
            
            if not email:
                continue
            
            contacts.append({
                'cardcode': row.get('CardCode', ''),
                'company': row.get('CardName', ''),
                'email': email,
                'phone': row.get('Phone', ''),
                'city': row.get('City', ''),
                'country': row.get('Country', 'ES'),
                'branch': row.get('Branch', ''),
                'address': row.get('Address', ''),
                'zipcode': row.get('ZipCode', ''),
                'website': row.get('Website', '')
            })
        
        logger.info(f"Filtrados a {len(contacts)} emails corporativos")
        
        cursor.close()
        conn.close()
        logger.info("Desconectado de SAP")
        
        return contacts
        
    except Exception as e:
        logger.error(f"Error SAP: {e}")
        return []


# ============================================================================
# STAFFKIT API
# ============================================================================

def send_to_staffkit(api_key: str, list_id: int, contacts: list) -> dict:
    """
    Envía contactos a StaffKit en batch.
    
    Returns:
        {'added': N, 'duplicates': N, 'errors': N}
    """
    if not contacts:
        return {'added': 0, 'duplicates': 0, 'errors': 0}
    
    headers = {
        'Authorization': f'Bearer {api_key}',
        'Content-Type': 'application/json'
    }
    
    stats = {'added': 0, 'duplicates': 0, 'errors': 0}
    
    logger.info(f"Enviando {len(contacts)} contactos a StaffKit (lista {list_id})...")
    
    for contact in contacts:
        lead_data = {
            'email': contact['email'],
            'company': contact['company'],
            'phone': contact['phone'],
            'city': contact['city'],
            'country': contact['country'],
            'website': contact.get('website', ''),
            'source': 'SAP B1',
            'notes': f"CardCode: {contact['cardcode']} | Branch: {contact['branch']}"
        }
        
        payload = {
            'action': 'save_lead',
            'list_id': list_id,
            'lead_data': json.dumps(lead_data)
        }
        
        try:
            resp = requests.post(
                f"{STAFFKIT_URL}/api/bots.php",
                json=payload,
                headers=headers,
                timeout=10
            )
            result = resp.json()
            
            if result.get('success'):
                if result.get('status') == 'duplicate':
                    stats['duplicates'] += 1
                else:
                    stats['added'] += 1
            else:
                stats['errors'] += 1
                logger.warning(f"Error: {contact['email']} - {result.get('error', 'Unknown')}")
                
        except Exception as e:
            stats['errors'] += 1
            logger.error(f"Error enviando {contact['email']}: {e}")
    
    logger.info(f"Completado: {stats['added']} añadidos, {stats['duplicates']} duplicados, {stats['errors']} errores")
    return stats


def get_bot_config(api_key: str, bot_id: int) -> dict:
    """Obtiene configuración del bot desde StaffKit"""
    try:
        headers = {'Authorization': f'Bearer {api_key}'}
        resp = requests.get(
            f"{STAFFKIT_URL}/api/v2/external-bot",
            params={'id': bot_id},
            headers=headers,
            timeout=10
        )
        if resp.ok:
            data = resp.json()
            bot = data.get('bot', {})
            return {
                'server': bot.get('config_sap_server', ''),
                'port': int(bot.get('config_sap_port', 1435)),
                'database': bot.get('config_sap_database', ''),
                'user': bot.get('config_sap_user', ''),
                'password': bot.get('config_sap_password', ''),
                'branches': [b.strip() for b in (bot.get('config_sap_branches', '') or '').split(',') if b.strip()],
                'corporate_only': bool(bot.get('config_sap_corporate_only', 1)),
                'limit': int(bot.get('config_sap_limit', 5000) or 5000),
                'list_id': int(bot.get('target_list_id', 0))
            }
    except Exception as e:
        logger.error(f"Error obteniendo config: {e}")
    return {}


# ============================================================================
# MAIN
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description='SAP → StaffKit Sync')
    
    # Modo 1: Usar configuración del bot en StaffKit
    parser.add_argument('--bot-id', type=int, help='ID del bot en StaffKit')
    parser.add_argument('--api-key', '--staffkit-api-key', dest='api_key', help='StaffKit API Key')
    
    # Modo 2: Parámetros manuales
    parser.add_argument('--sap-server', help='SAP Server IP')
    parser.add_argument('--sap-port', type=int, default=1435)
    parser.add_argument('--sap-database', help='SAP Database')
    parser.add_argument('--sap-user', help='SAP User')
    parser.add_argument('--sap-password', help='SAP Password')
    parser.add_argument('--branches', nargs='+', help='Branch filter (ej: FARMACIA)')
    parser.add_argument('--list-id', type=int, help='StaffKit List ID')
    parser.add_argument('--corporate-only', action='store_true', default=True)
    parser.add_argument('--limit', type=int, default=5000)
    
    # Opciones
    parser.add_argument('--full-sync', action='store_true', help='Ignorar último CardCode, sincronizar todo')
    parser.add_argument('--dry-run', action='store_true', help='No enviar a StaffKit, solo mostrar')
    
    args = parser.parse_args()
    
    # Determinar configuración
    if args.bot_id and args.api_key:
        # Modo 1: Obtener config de StaffKit
        logger.info(f"Obteniendo configuración del bot {args.bot_id}...")
        config = get_bot_config(args.api_key, args.bot_id)
        if not config.get('server'):
            logger.error("No se pudo obtener configuración del bot")
            sys.exit(1)
        bot_id = args.bot_id
        api_key = args.api_key
        list_id = config['list_id']
    elif args.sap_server and args.list_id and args.api_key:
        # Modo 2: Parámetros manuales
        config = {
            'server': args.sap_server,
            'port': args.sap_port,
            'database': args.sap_database,
            'user': args.sap_user,
            'password': args.sap_password,
            'branches': args.branches or [],
            'corporate_only': args.corporate_only,
            'limit': args.limit
        }
        bot_id = 0  # Sin estado persistente
        api_key = args.api_key
        list_id = args.list_id
    else:
        parser.print_help()
        sys.exit(1)
    
    # Cargar estado
    state = load_state(bot_id) if bot_id else {}
    last_cardcode = '' if args.full_sync else state.get('last_cardcode', '')
    
    logger.info("=" * 60)
    logger.info("SAP → StaffKit Sync")
    logger.info("=" * 60)
    logger.info(f"Branches: {config.get('branches') or 'Todos'}")
    logger.info(f"Lista destino: {list_id}")
    logger.info(f"Último CardCode: {last_cardcode or '(desde inicio)'}")
    logger.info("=" * 60)
    
    # Extraer de SAP
    contacts = extract_from_sap(config, last_cardcode)
    
    if not contacts:
        logger.info("No hay contactos nuevos para sincronizar")
        return
    
    # Enviar a StaffKit
    if args.dry_run:
        logger.info(f"[DRY RUN] Se enviarían {len(contacts)} contactos:")
        for c in contacts[:10]:
            logger.info(f"  - {c['email']} ({c['company']})")
        if len(contacts) > 10:
            logger.info(f"  ... y {len(contacts) - 10} más")
    else:
        stats = send_to_staffkit(api_key, list_id, contacts)
        
        # Actualizar estado
        if contacts and bot_id:
            state['last_cardcode'] = contacts[-1]['cardcode']
            state['total_synced'] = state.get('total_synced', 0) + stats['added']
            save_state(bot_id, state)
            logger.info(f"Estado guardado: último CardCode = {state['last_cardcode']}")
    
    logger.info("=" * 60)
    logger.info("Sincronización completada")
    logger.info("=" * 60)


if __name__ == '__main__':
    main()
