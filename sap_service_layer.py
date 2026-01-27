#!/usr/bin/env python3
"""
SAP Service Layer → StaffKit Sync
==================================
Sincronizador de Business Partners via SAP Service Layer REST API.

Soporta:
- Clientes (CardType = 'cCustomer')
- Proveedores (CardType = 'cSupplier')
- Filtrado por grupos
- Paginación automática
- Sincronización incremental

Uso:
    python sap_service_layer.py --bot-id 4 --api-key "sk_xxx"
    
    O con parámetros manuales:
    python sap_service_layer.py --sl-url "https://verdis.artesap.com:50000" \
        --company "CLOUD_VERDIS_ES" --user "manager" --password "9006" \
        --groups "100,200" --list-id 11
"""

import argparse
import json
import logging
import os
import re
import sys
import urllib3
from datetime import datetime
from typing import Optional, List, Dict, Any

import requests

# Desactivar warnings de SSL (muchos SAP usan certificados self-signed)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

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
# CLASE SAP SERVICE LAYER CLIENT
# ============================================================================

class SAPServiceLayerClient:
    """Cliente para SAP Business One Service Layer"""
    
    def __init__(self, base_url: str, company_db: str, username: str, password: str):
        self.base_url = base_url.rstrip('/')
        self.api_url = f"{self.base_url}/b1s/v1"
        self.company_db = company_db
        self.username = username
        self.password = password
        self.session = requests.Session()
        self.session.verify = False  # SAP suele usar certificados self-signed
        self.logged_in = False
        
    def login(self) -> bool:
        """Autenticarse en Service Layer"""
        try:
            response = self.session.post(
                f"{self.api_url}/Login",
                json={
                    "CompanyDB": self.company_db,
                    "UserName": self.username,
                    "Password": self.password
                },
                headers={"Content-Type": "application/json"},
                timeout=30
            )
            
            if response.status_code == 200:
                self.logged_in = True
                logger.info(f"Login exitoso en {self.company_db}")
                return True
            else:
                logger.error(f"Login fallido: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"Error en login: {e}")
            return False
    
    def logout(self):
        """Cerrar sesión"""
        if self.logged_in:
            try:
                self.session.post(f"{self.api_url}/Logout", timeout=10)
                logger.info("Logout completado")
            except:
                pass
            self.logged_in = False
    
    def get_business_partners(
        self,
        card_type: str = 'cCustomer',  # cCustomer, cSupplier, cLead
        groups: List[str] = None,
        last_cardcode: str = '',
        limit: int = 500,
        include_inactive: bool = False
    ) -> List[Dict[str, Any]]:
        """
        Obtener Business Partners con filtros.
        
        Args:
            card_type: Tipo de socio (cCustomer=cliente, cSupplier=proveedor)
            groups: Lista de códigos de grupo a filtrar
            last_cardcode: Para paginación incremental
            limit: Máximo de registros
            include_inactive: Incluir inactivos
        
        Returns:
            Lista de business partners
        """
        if not self.logged_in:
            logger.error("No autenticado")
            return []
        
        all_partners = []
        skip = 0
        page_size = 100  # Service Layer suele limitar a 100 por página
        
        # Construir filtro OData
        filters = [f"CardType eq '{card_type}'"]
        
        # Filtrar por grupos si se especifican
        if groups:
            group_filters = [f"GroupCode eq {g}" for g in groups]
            filters.append(f"({' or '.join(group_filters)})")
        
        # Solo con email
        filters.append("EmailAddress ne ''")
        filters.append("EmailAddress ne null")
        
        # Solo activos (salvo que se pida incluir inactivos)
        if not include_inactive:
            filters.append("Valid eq 'tYES'")
        
        # Incremental: desde último CardCode
        if last_cardcode:
            filters.append(f"CardCode gt '{last_cardcode}'")
        
        filter_str = " and ".join(filters)
        
        # Campos a seleccionar (nombres según Service Layer de SAP B1)
        select_fields = [
            "CardCode", "CardName", "CardForeignName",
            "EmailAddress", "Phone1", "Phone2", "Cellular",
            "Website", "GroupCode",
            "City", "Country", "Address", "ZipCode",
            "ContactPerson", "Notes", "Valid", "Frozen"
        ]
        
        logger.info(f"Extrayendo {card_type} (grupos={groups}, desde={last_cardcode or 'inicio'})...")
        
        while len(all_partners) < limit:
            try:
                response = self.session.get(
                    f"{self.api_url}/BusinessPartners",
                    params={
                        "$filter": filter_str,
                        "$select": ",".join(select_fields),
                        "$orderby": "CardCode asc",
                        "$top": min(page_size, limit - len(all_partners)),
                        "$skip": skip
                    },
                    timeout=60
                )
                
                if response.status_code != 200:
                    logger.error(f"Error API: {response.status_code} - {response.text[:200]}")
                    break
                
                data = response.json()
                partners = data.get('value', [])
                
                if not partners:
                    break
                
                all_partners.extend(partners)
                logger.info(f"  Página {skip//page_size + 1}: {len(partners)} registros")
                
                # ¿Hay más páginas?
                if len(partners) < page_size:
                    break
                
                skip += page_size
                
            except Exception as e:
                logger.error(f"Error obteniendo partners: {e}")
                break
        
        logger.info(f"Total extraídos: {len(all_partners)}")
        return all_partners
    
    def get_groups(self, card_type: str = 'cCustomer') -> List[Dict[str, Any]]:
        """Obtener lista de grupos de Business Partners"""
        if not self.logged_in:
            return []
        
        try:
            # SAP B1 usa BusinessPartnerGroups para todos los grupos
            # Type: bbpgt_CustomerGroup o bbpgt_VendorGroup
            response = self.session.get(
                f"{self.api_url}/BusinessPartnerGroups",
                params={"$select": "Code,Name,Type"},
                timeout=30
            )
            
            if response.status_code == 200:
                all_groups = response.json().get('value', [])
                # Filtrar por tipo
                if card_type == 'cSupplier':
                    return [g for g in all_groups if g.get('Type') == 'bbpgt_VendorGroup']
                else:
                    return [g for g in all_groups if g.get('Type') == 'bbpgt_CustomerGroup']
            else:
                logger.warning(f"No se pudieron obtener grupos: {response.status_code}")
                return []
                
        except Exception as e:
            logger.error(f"Error obteniendo grupos: {e}")
            return []


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
    emails = re.split(r'[;,\s]+', str(email))
    for e in emails:
        e = e.strip().lower()
        if '@' in e and '.' in e.split('@')[1]:
            return e
    return ''


def load_state(bot_id: int) -> dict:
    """Carga estado del bot"""
    os.makedirs(STATE_DIR, exist_ok=True)
    state_file = os.path.join(STATE_DIR, f'sap_sl_bot_{bot_id}.json')
    if os.path.exists(state_file):
        try:
            with open(state_file, 'r') as f:
                return json.load(f)
        except:
            pass
    return {
        'last_cardcode_customer': '',
        'last_cardcode_supplier': '',
        'last_sync': None,
        'total_synced': 0
    }


def save_state(bot_id: int, state: dict):
    """Guarda estado del bot"""
    os.makedirs(STATE_DIR, exist_ok=True)
    state_file = os.path.join(STATE_DIR, f'sap_sl_bot_{bot_id}.json')
    state['last_sync'] = datetime.now().isoformat()
    with open(state_file, 'w') as f:
        json.dump(state, f, indent=2)


def transform_partner(partner: dict, card_type: str) -> dict:
    """Transforma un Business Partner al formato de StaffKit"""
    email = clean_email(partner.get('EmailAddress', ''))
    
    if not email:
        return None
    
    # Website con fallback al dominio del email
    website = (partner.get('Website') or '').strip()
    if not website and email and is_corporate_email(email):
        domain = email.split('@')[1]
        website = f"https://{domain}"
    
    # Teléfono: preferir fijo, si no móvil
    phone = (partner.get('Phone1') or partner.get('Phone2') or '').strip()
    if not phone or phone == '0':
        phone = (partner.get('Cellular') or '').strip()
    
    # Nombre: CardForeignName suele ser el nombre comercial
    company_name = (partner.get('CardForeignName') or partner.get('CardName') or '').strip()
    contact_name = (partner.get('ContactPerson') or '').strip()
    
    return {
        'cardcode': partner.get('CardCode', ''),
        'company': company_name,
        'name': contact_name,
        'email': email,
        'phone': phone,
        'city': partner.get('City', ''),
        'country': partner.get('Country', 'ES'),
        'address': partner.get('Address', ''),
        'zipcode': partner.get('ZipCode', ''),
        'website': website,
        'group_code': partner.get('GroupCode', ''),
        'partner_type': 'customer' if card_type == 'cCustomer' else 'supplier',
        'notes': partner.get('Notes', '')
    }


def send_to_staffkit(contacts: list, list_id: int, api_key: str) -> dict:
    """Envía contactos a StaffKit en batch"""
    if not contacts:
        return {'success': True, 'saved': 0, 'duplicates': 0}
    
    saved = 0
    duplicates = 0
    
    for contact in contacts:
        try:
            response = requests.post(
                f"{STAFFKIT_URL}/api/bots.php",
                params={'action': 'sync_sap_lead'},
                json={
                    'api_key': api_key,
                    'list_id': list_id,
                    'cardcode': contact['cardcode'],
                    'company': contact['company'],
                    'name': contact['name'],
                    'email': contact['email'],
                    'phone': contact['phone'],
                    'city': contact['city'],
                    'country': contact['country'],
                    'address': contact['address'],
                    'zipcode': contact['zipcode'],
                    'website': contact['website']
                },
                timeout=30
            )
            
            if response.status_code == 200:
                result = response.json()
                if result.get('success'):
                    if result.get('action') == 'created':
                        saved += 1
                    else:
                        duplicates += 1
            
        except Exception as e:
            logger.warning(f"Error enviando {contact['cardcode']}: {e}")
    
    return {'success': True, 'saved': saved, 'duplicates': duplicates}


# ============================================================================
# FUNCIÓN PRINCIPAL
# ============================================================================

def get_bot_config(bot_id: int, api_key: str) -> Optional[dict]:
    """Obtiene configuración del bot desde StaffKit"""
    try:
        response = requests.get(
            f"{STAFFKIT_URL}/api/bots.php",
            params={'action': 'get_bot', 'bot_id': bot_id, 'api_key': api_key},
            timeout=30
        )
        if response.status_code == 200:
            data = response.json()
            if data.get('success'):
                return data.get('bot')
    except Exception as e:
        logger.error(f"Error obteniendo config del bot: {e}")
    return None


def main():
    parser = argparse.ArgumentParser(description='SAP Service Layer → StaffKit Sync')
    
    # Modo bot (obtiene config de StaffKit)
    parser.add_argument('--bot-id', type=int, help='ID del bot en StaffKit')
    parser.add_argument('--api-key', help='API Key de StaffKit')
    
    # Modo manual (parámetros directos)
    parser.add_argument('--sl-url', help='URL del Service Layer (ej: https://sap.empresa.com:50000)')
    parser.add_argument('--company', help='CompanyDB')
    parser.add_argument('--user', help='Usuario SAP')
    parser.add_argument('--password', help='Contraseña SAP')
    parser.add_argument('--groups', help='Grupos a extraer (separados por coma)')
    parser.add_argument('--list-id', type=int, help='ID de lista destino en StaffKit')
    
    # Opciones adicionales
    parser.add_argument('--card-types', default='customer,supplier', 
                       help='Tipos a extraer: customer, supplier o ambos')
    parser.add_argument('--corporate-only', action='store_true', default=True,
                       help='Solo emails corporativos')
    parser.add_argument('--limit', type=int, default=500, help='Límite de registros')
    parser.add_argument('--list-groups', action='store_true', 
                       help='Listar grupos disponibles y salir')
    parser.add_argument('--dry-run', action='store_true',
                       help='Solo mostrar qué se haría, sin enviar')
    
    args = parser.parse_args()
    
    # Obtener configuración
    config = {}
    
    if args.bot_id and args.api_key:
        # Modo bot: obtener config de StaffKit
        bot_config = get_bot_config(args.bot_id, args.api_key)
        if not bot_config:
            logger.error(f"No se pudo obtener configuración del bot {args.bot_id}")
            sys.exit(1)
        
        logger.info(f"Bot: {bot_config.get('name', f'Bot #{args.bot_id}')}")
        
        config = {
            'sl_url': bot_config.get('config_sap_server', ''),
            'company': bot_config.get('config_sap_database', ''),
            'user': bot_config.get('config_sap_user', ''),
            'password': bot_config.get('config_sap_password', ''),
            'groups': bot_config.get('config_sap_branches', ''),
            'list_id': bot_config.get('target_list_id'),
            'corporate_only': bool(bot_config.get('config_sap_corporate_only', 1)),
            'limit': int(bot_config.get('config_sap_limit', 500) or 500)
        }
    else:
        # Modo manual
        config = {
            'sl_url': args.sl_url,
            'company': args.company,
            'user': args.user,
            'password': args.password,
            'groups': args.groups or '',
            'list_id': args.list_id,
            'corporate_only': args.corporate_only,
            'limit': args.limit
        }
    
    # Validar configuración mínima
    if not all([config.get('sl_url'), config.get('company'), 
                config.get('user'), config.get('password')]):
        logger.error("Faltan parámetros de conexión SAP")
        parser.print_help()
        sys.exit(1)
    
    # Crear cliente
    client = SAPServiceLayerClient(
        base_url=config['sl_url'],
        company_db=config['company'],
        username=config['user'],
        password=config['password']
    )
    
    # Login
    if not client.login():
        logger.error("No se pudo conectar a SAP Service Layer")
        sys.exit(1)
    
    try:
        # Modo listar grupos
        if args.list_groups:
            print("\n=== GRUPOS DE CLIENTES ===")
            for g in client.get_groups('cCustomer'):
                print(f"  {g['Code']}: {g['Name']}")
            
            print("\n=== GRUPOS DE PROVEEDORES ===")
            for g in client.get_groups('cSupplier'):
                print(f"  {g['Code']}: {g['Name']}")
            
            client.logout()
            return
        
        # Cargar estado previo
        state = load_state(args.bot_id) if args.bot_id else {}
        
        # Parsear grupos
        groups = [g.strip() for g in config['groups'].split(',') if g.strip()] if config['groups'] else None
        
        # Parsear tipos a extraer
        card_types = []
        if 'customer' in args.card_types.lower():
            card_types.append('cCustomer')
        if 'supplier' in args.card_types.lower():
            card_types.append('cSupplier')
        
        all_contacts = []
        
        for card_type in card_types:
            type_name = 'clientes' if card_type == 'cCustomer' else 'proveedores'
            last_key = f'last_cardcode_{"customer" if card_type == "cCustomer" else "supplier"}'
            last_cardcode = state.get(last_key, '')
            
            logger.info(f"\n{'='*60}")
            logger.info(f"Extrayendo {type_name}...")
            logger.info(f"{'='*60}")
            
            partners = client.get_business_partners(
                card_type=card_type,
                groups=groups,
                last_cardcode=last_cardcode,
                limit=config['limit']
            )
            
            # Transformar y filtrar
            for partner in partners:
                contact = transform_partner(partner, card_type)
                if contact:
                    # Filtrar emails corporativos si está configurado
                    if config['corporate_only'] and not is_corporate_email(contact['email']):
                        continue
                    all_contacts.append(contact)
                    
                    # Actualizar último CardCode
                    if contact['cardcode'] > state.get(last_key, ''):
                        state[last_key] = contact['cardcode']
            
            logger.info(f"  {type_name.capitalize()} válidos: {len([c for c in all_contacts if c['partner_type'] == ('customer' if card_type == 'cCustomer' else 'supplier')])}")
        
        logger.info(f"\n{'='*60}")
        logger.info(f"TOTAL CONTACTOS: {len(all_contacts)}")
        logger.info(f"{'='*60}")
        
        # Dry run: solo mostrar
        if args.dry_run:
            for c in all_contacts[:10]:
                print(f"  [{c['partner_type']}] {c['cardcode']}: {c['company']} - {c['email']}")
            if len(all_contacts) > 10:
                print(f"  ... y {len(all_contacts) - 10} más")
            return
        
        # Enviar a StaffKit
        if config.get('list_id') and args.api_key:
            logger.info(f"\nEnviando a StaffKit (lista {config['list_id']})...")
            result = send_to_staffkit(all_contacts, config['list_id'], args.api_key)
            
            logger.info(f"  Guardados: {result['saved']}")
            logger.info(f"  Duplicados: {result['duplicates']}")
            
            # Guardar estado
            state['total_synced'] = state.get('total_synced', 0) + result['saved']
            if args.bot_id:
                save_state(args.bot_id, state)
        else:
            logger.warning("No se especificó list_id o api_key, no se enviarán los datos")
        
        print(f"\n✅ Sincronización completada: {len(all_contacts)} procesados")
        
    finally:
        client.logout()


if __name__ == '__main__':
    main()
