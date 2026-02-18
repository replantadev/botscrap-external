#!/usr/bin/env python3
"""
Hosting Sniper Bot v1.0
========================
Encuentra empresas pequeñas con web WordPress lenta en hosting caro/malo.
El lead perfecto para vender migración de hosting.

Flujo:
1. INPUT: Nicho + país → genera búsquedas combinando nicho × top 50 ciudades
2. DISCOVERY: Google CSE → lista de dominios
3. ANÁLISIS TÉCNICO de cada web:
   - TTFB (Time To First Byte) — calidad del hosting
   - PageSpeed Score (API gratis Google, 25k/día)
   - CMS: WordPress, WooCommerce, PrestaShop, Shopify...
   - Hosting: IP → ASN → proveedor (GoDaddy, Hostinger, IONOS...)
   - SSL: válido, expirado, días restantes
   - Tamaño: pocas páginas = negocio pequeño
4. SCORING: puntuar (lento + hosting caro + WordPress = HOT lead)
5. IMPORT a StaffKit con datos técnicos en notas

Uso:
  python hosting_sniper.py --bot-id 9 --api-key KEY
  python hosting_sniper.py --api-key KEY --list-id 5 --niches "floristería,pastelería" --country ES
  python hosting_sniper.py --api-key KEY --list-id 5 --niches "dentista" --country MX --dry-run
"""

import argparse
import requests
import json
import time
import sys
import os
import re
import ssl
import socket
import random
from datetime import datetime
from typing import Optional, Dict, List, Set, Tuple
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed

# ─── Config ───────────────────────────────────────────────────────────
STAFFKIT_URL = os.getenv('STAFFKIT_URL', 'https://staff.replanta.dev')

# Score mínimo para importar un lead (0-100)
MIN_IMPORT_SCORE = 50

# TLDs de dominios gubernamentales/educativos/sin ánimo de lucro → no son clientes
BLACKLIST_TLDS = {
    '.gov', '.gob', '.edu', '.mil', '.org', '.int',
    '.gov.co', '.gov.mx', '.gob.mx', '.gob.ar', '.gob.cl', '.gob.pe',
    '.edu.co', '.edu.mx', '.edu.ar', '.edu.cl', '.edu.pe', '.edu.es',
    '.ac.uk', '.gov.uk', '.nhs.uk',
}

# TLDs por país — si buscamos en CO, un .co.za o .org.uk no tiene sentido
COUNTRY_TLDS = {
    'ES': ['.es', '.com', '.net', '.cat', '.eus', '.gal'],
    'CO': ['.co', '.com.co', '.com', '.net'],
    'MX': ['.mx', '.com.mx', '.com', '.net'],
    'AR': ['.ar', '.com.ar', '.com', '.net'],
    'CL': ['.cl', '.com', '.net'],
    'PE': ['.pe', '.com.pe', '.com', '.net'],
    'EC': ['.ec', '.com.ec', '.com', '.net'],
    'UY': ['.uy', '.com.uy', '.com', '.net'],
    'CR': ['.cr', '.co.cr', '.com', '.net'],
    'PA': ['.pa', '.com.pa', '.com', '.net'],
    'BO': ['.bo', '.com.bo', '.com', '.net'],
    'PY': ['.py', '.com.py', '.com', '.net'],
}

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0',
]

# ─── Hosting providers reconocibles por ASN/IP/headers ───────────────
HOSTING_PROVIDERS = {
    # ASN patterns
    'AS13335': ('Cloudflare', 'cdn'),
    'AS16509': ('AWS', 'cloud'),
    'AS15169': ('Google Cloud', 'cloud'),
    'AS8075': ('Microsoft Azure', 'cloud'),
    'AS14061': ('DigitalOcean', 'cloud'),
    'AS24940': ('Hetzner', 'cloud'),
    'AS16276': ('OVH', 'cloud'),
    'AS63949': ('Linode/Akamai', 'cloud'),
    # Hosting compartido (target principal)
    'AS26496': ('GoDaddy', 'shared'),
    'AS398101': ('GoDaddy', 'shared'),
    'AS21499': ('GoDaddy', 'shared'),
    'AS46606': ('Unity/GoDaddy', 'shared'),
    'AS47583': ('Hostinger', 'shared'),
    'AS394695': ('Hostinger', 'shared'),
    'AS8560': ('IONOS/1&1', 'shared'),
    'AS8972': ('IONOS/PlusServer', 'shared'),
    'AS15003': ('Dreamhost', 'shared'),
    'AS40824': ('SiteGround', 'shared'),
    'AS19871': ('Network Solutions', 'shared'),
    'AS32244': ('Liquid Web', 'shared'),
    'AS22612': ('Namecheap', 'shared'),
    'AS133618': ('Hostgator', 'shared'),
    'AS21844': ('ThePlanet/Softlayer', 'shared'),
    'AS19994': ('Rackspace', 'shared'),
    'AS29854': ('Westhost', 'shared'),
    'AS36351': ('Softlayer/IBM', 'shared'),
    'AS30083': ('HEG/Host Europe', 'shared'),
    'AS20773': ('HostEurope', 'shared'),
    'AS35017': ('Swisscom', 'shared'),
    'AS197695': ('Dinahosting', 'shared'),
    'AS15704': ('XTRA Telecom', 'shared'),
    'AS60049': ('Raiola Networks', 'shared'),
    'AS29006': ('Línea Directa', 'shared'),
    'AS34788': ('NordLayer', 'shared'),
    'AS6739': ('Vodafone Spain', 'shared'),
    'AS12334': ('Bluehost', 'shared'),
    'AS46475': ('Bluehost', 'shared'),
    'AS53831': ('Squarespace', 'saas'),
    'AS54113': ('Fastly', 'cdn'),
}

# Headers que delatan hosting
HOSTING_HEADERS = {
    'x-powered-by-plesk': 'Plesk',
    'x-turbo-charged-by': 'LiteSpeed',
    'server': {
        'litespeed': 'LiteSpeed',
        'cloudflare': 'Cloudflare',
        'nginx': 'Nginx',
        'apache': 'Apache',
        'microsoft-iis': 'IIS/Windows',
        'openresty': 'OpenResty',
    }
}

# CMS detectable por patrones HTML/headers
CMS_SIGNATURES = {
    'wordpress': [
        '/wp-content/', '/wp-includes/', 'wp-json', 
        'name="generator" content="WordPress',
        'wp-embed.min.js',
    ],
    'woocommerce': [
        'woocommerce', 'wc-blocks', 'add-to-cart',
        '/product/', 'wc-add-to-cart',
    ],
    'prestashop': [
        'prestashop', '/modules/ps_', 'PrestaShop',
        'content="PrestaShop"',
    ],
    'shopify': [
        'shopify', 'cdn.shopify.com', 'Shopify.theme',
    ],
    'joomla': [
        '/components/com_', '/media/jui/', 'Joomla!',
    ],
    'drupal': [
        'Drupal', '/sites/default/files/', 'drupal.js',
    ],
    'wix': [
        'wixpress.com', 'wix.com', 'X-Wix-',
    ],
    'squarespace': [
        'squarespace.com', 'sqsp', 'static.squarespace',
    ],
    'webflow': [
        'webflow.com', 'wf-sections',
    ],
    'html_static': [],  # Fallback
}

# Top ciudades por país (aprox 50 más pobladas)
COUNTRY_CITIES = {
    'ES': {
        'name': 'España',
        'lang': 'es',
        'cities': [
            'Madrid', 'Barcelona', 'Valencia', 'Sevilla', 'Zaragoza', 'Málaga',
            'Murcia', 'Palma de Mallorca', 'Las Palmas', 'Bilbao', 'Alicante',
            'Córdoba', 'Valladolid', 'Vigo', 'Gijón', 'Hospitalet', 'Vitoria',
            'A Coruña', 'Granada', 'Elche', 'Oviedo', 'Badalona', 'Cartagena',
            'Terrassa', 'Jerez de la Frontera', 'Sabadell', 'Santa Cruz de Tenerife',
            'Móstoles', 'Alcalá de Henares', 'Pamplona', 'Fuenlabrada', 'Almería',
            'Leganés', 'San Sebastián', 'Getafe', 'Burgos', 'Albacete', 'Santander',
            'Castellón', 'Alcorcón', 'San Cristóbal de La Laguna', 'Logroño',
            'Badajoz', 'Salamanca', 'Huelva', 'Marbella', 'Lérida', 'Tarragona',
            'León', 'Cádiz',
        ]
    },
    'MX': {
        'name': 'México',
        'lang': 'es',
        'cities': [
            'Ciudad de México', 'Guadalajara', 'Monterrey', 'Puebla', 'Tijuana',
            'León', 'Juárez', 'Zapopan', 'Mérida', 'San Luis Potosí',
            'Aguascalientes', 'Hermosillo', 'Saltillo', 'Mexicali', 'Culiacán',
            'Querétaro', 'Chihuahua', 'Naucalpan', 'Morelia', 'Cancún',
            'Tlalnepantla', 'Acapulco', 'Durango', 'Toluca', 'Oaxaca',
            'Tuxtla Gutiérrez', 'Veracruz', 'Mazatlán', 'Irapuato', 'Villahermosa',
            'Torreón', 'Reynosa', 'Celaya', 'Tampico', 'Playa del Carmen',
            'San Nicolás', 'Cuernavaca', 'Pachuca', 'Xalapa', 'Ensenada',
            'Campeche', 'Tehuacán', 'Colima', 'Tepic', 'Zacatecas',
            'Los Mochis', 'Salamanca', 'La Paz', 'Puerto Vallarta', 'Chetumal',
        ]
    },
    'AR': {
        'name': 'Argentina',
        'lang': 'es',
        'cities': [
            'Buenos Aires', 'Córdoba', 'Rosario', 'Mendoza', 'Tucumán',
            'La Plata', 'Mar del Plata', 'Salta', 'Santa Fe', 'San Juan',
            'Resistencia', 'Neuquén', 'Santiago del Estero', 'Corrientes', 'Bahía Blanca',
            'San Salvador de Jujuy', 'Posadas', 'Paraná', 'Formosa', 'San Luis',
            'Catamarca', 'Río Cuarto', 'Comodoro Rivadavia', 'San Rafael', 'Tandil',
            'San Nicolás', 'Concordia', 'San Carlos de Bariloche', 'Rafaela', 'Olavarría',
            'Villa María', 'Reconquista', 'Pergamino', 'Zárate', 'Junín',
            'Trelew', 'Necochea', 'San Martín', 'Villa Mercedes', 'La Rioja',
            'Gualeguaychú', 'Venado Tuerto', 'Río Gallegos', 'Rawson', 'Ushuaia',
            'General Roca', 'Cipolletti', 'Puerto Madryn', 'Pilar', 'Luján',
        ]
    },
    'CO': {
        'name': 'Colombia',
        'lang': 'es',
        'cities': [
            'Bogotá', 'Medellín', 'Cali', 'Barranquilla', 'Cartagena',
            'Cúcuta', 'Soledad', 'Ibagué', 'Bucaramanga', 'Soacha',
            'Santa Marta', 'Villavicencio', 'Pereira', 'Bello', 'Pasto',
            'Manizales', 'Montería', 'Neiva', 'Palmira', 'Valledupar',
            'Buenaventura', 'Popayán', 'Sincelejo', 'Floridablanca', 'Envigado',
            'Armenia', 'Tuluá', 'Dosquebradas', 'Itagüí', 'Riohacha',
            'Turbo', 'Apartadó', 'Sogamoso', 'Girón', 'Tumaco',
            'Barrancabermeja', 'Duitama', 'Maicao', 'Ciénaga', 'Zipaquirá',
            'Cartago', 'Girardot', 'Fusagasugá', 'Facatativá', 'Piedecuesta',
            'Chía', 'Yopal', 'Pitalito', 'Tunja', 'Quibdó',
        ]
    },
    'CL': {
        'name': 'Chile',
        'lang': 'es',
        'cities': [
            'Santiago', 'Puente Alto', 'Antofagasta', 'Viña del Mar', 'Valparaíso',
            'Talcahuano', 'San Bernardo', 'Temuco', 'Iquique', 'Concepción',
            'Rancagua', 'La Serena', 'Arica', 'Talca', 'Chillán',
            'Los Ángeles', 'Puerto Montt', 'Calama', 'Coquimbo', 'Osorno',
            'Copiapó', 'Valdivia', 'Quilpué', 'Villa Alemana', 'Punta Arenas',
            'La Calera', 'Curicó', 'Ovalle', 'Linares', 'Los Andes',
            'San Felipe', 'Melipilla', 'Quillota', 'San Antonio', 'Constitución',
            'Coronel', 'Lota', 'Coyhaique', 'Angol', 'Victoria',
            'Collipulli', 'Cauquenes', 'Illapel', 'San Fernando', 'Rengo',
            'Parral', 'Castro', 'Ancud', 'Buin', 'Peñaflor',
        ]
    },
    'PE': {
        'name': 'Perú',
        'lang': 'es',
        'cities': [
            'Lima', 'Arequipa', 'Trujillo', 'Chiclayo', 'Piura',
            'Iquitos', 'Cusco', 'Huancayo', 'Chimbote', 'Pucallpa',
            'Tacna', 'Juliaca', 'Ica', 'Cajamarca', 'Sullana',
            'Ayacucho', 'Chincha Alta', 'Huánuco', 'Puno', 'Tarapoto',
            'Huaraz', 'Tumbes', 'Abancay', 'Cerro de Pasco', 'Moyobamba',
            'Jaén', 'Chachapoyas', 'Moquegua', 'Puerto Maldonado', 'Huacho',
            'Barranca', 'Nazca', 'Chepén', 'Lambayeque', 'Ferreñafe',
            'Pisco', 'Huaral', 'Chancay', 'San Vicente de Cañete', 'Andahuaylas',
            'Tingo María', 'Bagua Grande', 'Chulucanas', 'Paita', 'Talara',
            'La Oroya', 'Tarma', 'Satipo', 'Yurimaguas', 'Contamana',
        ]
    },
    'EC': {
        'name': 'Ecuador',
        'lang': 'es',
        'cities': [
            'Guayaquil', 'Quito', 'Cuenca', 'Santo Domingo', 'Machala',
            'Manta', 'Portoviejo', 'Durán', 'Ambato', 'Riobamba',
            'Loja', 'Ibarra', 'Esmeraldas', 'Quevedo', 'Babahoyo',
            'Milagro', 'Latacunga', 'Tulcán', 'Daule', 'Sangolquí',
            'Otavalo', 'Guaranda', 'Azogues', 'Nueva Loja', 'Tena',
            'Puyo', 'Santa Rosa', 'Pasaje', 'Huaquillas', 'Ventanas',
            'Vinces', 'Chone', 'Calceta', 'Jipijapa', 'Salinas',
            'La Libertad', 'Playas', 'El Carmen', 'Quinindé', 'Pedernales',
        ]
    },
    'UY': {
        'name': 'Uruguay',
        'lang': 'es',
        'cities': [
            'Montevideo', 'Salto', 'Ciudad de la Costa', 'Paysandú', 'Las Piedras',
            'Rivera', 'Maldonado', 'Tacuarembó', 'Melo', 'Mercedes',
            'Artigas', 'Minas', 'San José de Mayo', 'Durazno', 'Florida',
            'Treinta y Tres', 'Rocha', 'Fray Bentos', 'Trinidad', 'Colonia del Sacramento',
            'Punta del Este', 'San Carlos', 'Canelones', 'Dolores', 'Chuy',
        ]
    },
    'CR': {
        'name': 'Costa Rica',
        'lang': 'es',
        'cities': [
            'San José', 'Alajuela', 'Cartago', 'Heredia', 'Liberia',
            'Puntarenas', 'Limón', 'San Isidro', 'Nicoya', 'San Ramón',
            'Grecia', 'Turrialba', 'Ciudad Quesada', 'Guadalupe', 'Desamparados',
            'Curridabat', 'Escazú', 'Santa Cruz', 'Cañas', 'Paraíso',
        ]
    },
    'PA': {
        'name': 'Panamá',
        'lang': 'es',
        'cities': [
            'Ciudad de Panamá', 'San Miguelito', 'David', 'Colón', 'Santiago',
            'Chitré', 'Penonomé', 'Aguadulce', 'La Chorrera', 'Arraiján',
            'Las Tablas', 'Bocas del Toro', 'Changuinola', 'Puerto Armuelles', 'Chepo',
        ]
    },
    'DO': {
        'name': 'Rep. Dominicana',
        'lang': 'es',
        'cities': [
            'Santo Domingo', 'Santiago', 'San Pedro de Macorís', 'La Romana', 'San Cristóbal',
            'Puerto Plata', 'San Francisco de Macorís', 'Higüey', 'La Vega', 'Barahona',
            'Moca', 'Bonao', 'Azua', 'San Juan de la Maguana', 'Mao',
            'Cotuí', 'Nagua', 'Baní', 'Constanza', 'Punta Cana',
        ]
    },
    'GT': {
        'name': 'Guatemala',
        'lang': 'es',
        'cities': [
            'Ciudad de Guatemala', 'Mixco', 'Villa Nueva', 'Petapa', 'Quetzaltenango',
            'Escuintla', 'Chinautla', 'Huehuetenango', 'San Miguel Petapa', 'Cobán',
            'Chimaltenango', 'Antigua Guatemala', 'Mazatenango', 'Retalhuleu', 'Jutiapa',
            'Jalapa', 'Zacapa', 'Santa Cruz del Quiché', 'Sololá', 'Totonicapán',
        ]
    },
    'HN': {
        'name': 'Honduras',
        'lang': 'es',
        'cities': [
            'Tegucigalpa', 'San Pedro Sula', 'La Ceiba', 'Choloma', 'El Progreso',
            'Comayagua', 'Choluteca', 'Puerto Cortés', 'Danlí', 'Siguatepeque',
            'Juticalpa', 'Santa Rosa de Copán', 'Tela', 'Roatán', 'Tocoa',
        ]
    },
    'SV': {
        'name': 'El Salvador',
        'lang': 'es',
        'cities': [
            'San Salvador', 'Santa Ana', 'San Miguel', 'Santa Tecla', 'Soyapango',
            'Apopa', 'Mejicanos', 'Delgado', 'Usulután', 'Ahuachapán',
            'Sonsonate', 'San Vicente', 'Cojutepeque', 'Zacatecoluca', 'Chalatenango',
        ]
    },
    'NI': {
        'name': 'Nicaragua',
        'lang': 'es',
        'cities': [
            'Managua', 'León', 'Masaya', 'Matagalpa', 'Chinandega',
            'Estelí', 'Granada', 'Jinotega', 'Juigalpa', 'Bluefields',
            'Rivas', 'Ocotal', 'Somoto', 'Diriamba', 'Jinotepe',
        ]
    },
    'BO': {
        'name': 'Bolivia',
        'lang': 'es',
        'cities': [
            'Santa Cruz de la Sierra', 'La Paz', 'El Alto', 'Cochabamba', 'Sucre',
            'Oruro', 'Tarija', 'Potosí', 'Sacaba', 'Montero',
            'Quillacollo', 'Trinidad', 'Warnes', 'Yacuiba', 'Riberalta',
            'Cobija', 'Villamontes', 'Bermejo', 'Camiri', 'Tupiza',
        ]
    },
    'PY': {
        'name': 'Paraguay',
        'lang': 'es',
        'cities': [
            'Asunción', 'Ciudad del Este', 'San Lorenzo', 'Luque', 'Capiatá',
            'Lambaré', 'Fernando de la Mora', 'Encarnación', 'Pedro Juan Caballero', 'Caaguazú',
            'Coronel Oviedo', 'Concepción', 'Villarrica', 'Pilar', 'Itauguá',
        ]
    },
}

# Nichos ideales para hosting: negocios pequeños con web
DEFAULT_NICHES_ES = [
    'floristería', 'pastelería', 'dentista', 'veterinaria', 'peluquería',
    'restaurante', 'inmobiliaria', 'abogado', 'fontanero', 'electricista',
    'taller mecánico', 'clínica dental', 'óptica', 'panadería', 'farmacia',
    'academia', 'gimnasio', 'estética', 'hotel pequeño', 'tienda online',
]


class HostingSniper:
    """Busca webs WordPress lentas en hosting caro/malo como leads de migración"""

    def __init__(self, api_key: str, list_id: int, bot_id: int = 0,
                 run_id: int = 0,
                 google_api_key: str = '', google_cx: str = '',
                 niches: List[str] = None,
                 country: str = 'ES',
                 max_cities: int = 50,
                 searches_per_run: int = 10,
                 delay: float = 1.5,
                 dry_run: bool = False,
                 verbose: bool = False):

        self.api_key = api_key
        self.list_id = list_id
        self.bot_id = bot_id
        self.run_id = run_id
        self.google_api_key = google_api_key
        self.google_cx = google_cx
        self.niches = niches or DEFAULT_NICHES_ES[:5]
        self.country = country.upper()
        self.max_cities = max_cities
        self.searches_per_run = searches_per_run
        self.delay = delay
        self.dry_run = dry_run
        self.verbose = verbose

        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': random.choice(USER_AGENTS),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        })

        # Dominios ya vistos en esta ejecución
        self.seen_domains: Set[str] = set()

        self.stats = {
            'searches_done': 0,
            'domains_found': 0,
            'domains_analyzed': 0,
            'wordpress_found': 0,
            'slow_sites': 0,
            'shared_hosting': 0,
            'hot_leads': 0,
            'leads_imported': 0,
            'leads_duplicates': 0,
            'leads_errors': 0,
            'pagespeed_calls': 0,
            'errors': [],
        }

    def log(self, msg: str, level: str = 'INFO'):
        ts = datetime.now().strftime('%H:%M:%S')
        print(f"[{ts}] [{level}] {msg}")

    def debug(self, msg: str):
        if self.verbose:
            self.log(msg, 'DEBUG')

    # ─── FASE 1: DISCOVERY via Google CSE ─────────────────────────────

    def generate_searches(self) -> List[Tuple[str, str]]:
        """Genera pares (nicho, ciudad) para buscar en Google CSE"""
        country_data = COUNTRY_CITIES.get(self.country)
        if not country_data:
            self.log(f"País '{self.country}' no soportado. Disponibles: {', '.join(COUNTRY_CITIES.keys())}", 'ERROR')
            return []

        cities = country_data['cities'][:self.max_cities]
        pairs = []

        for niche in self.niches:
            for city in cities:
                pairs.append((niche, city))

        # Limitar al número de búsquedas por run
        random.shuffle(pairs)
        selected = pairs[:self.searches_per_run]

        self.log(f"📋 {len(selected)} búsquedas planificadas ({len(self.niches)} nichos × {len(cities)} ciudades, limitado a {self.searches_per_run}/run)")
        return selected

    def search_google(self, niche: str, city: str) -> List[str]:
        """Busca en Google CSE y devuelve URLs de webs encontradas"""
        if not self.google_api_key or not self.google_cx:
            self.log("⚠️  Sin Google CSE — no se puede buscar", 'WARNING')
            return []

        urls = []
        query = f'{niche} {city}'

        for start in range(1, 31, 10):  # Máx 30 resultados por búsqueda
            try:
                resp = self.session.get(
                    'https://www.googleapis.com/customsearch/v1',
                    params={
                        'key': self.google_api_key,
                        'cx': self.google_cx,
                        'q': query,
                        'start': start,
                        'num': 10,
                        'lr': f'lang_{COUNTRY_CITIES.get(self.country, {}).get("lang", "es")}',
                    },
                    timeout=15
                )

                if resp.status_code == 429:
                    self.log("  ⏳ Google rate limit, pausa 10s...", 'WARNING')
                    time.sleep(10)
                    continue
                if resp.status_code != 200:
                    self.debug(f"  Google CSE HTTP {resp.status_code}")
                    break

                data = resp.json()
                if 'error' in data:
                    self.log(f"  ⚠️  CSE error: {data['error'].get('message', '?')}", 'WARNING')
                    break

                items = data.get('items', [])
                if not items:
                    break

                for item in items:
                    link = item.get('link', '')
                    if link:
                        parsed = urlparse(link)
                        domain = parsed.netloc.lower().replace('www.', '')
                        # Filtrar: solo webs de empresas, no directorios/redes
                        skip = ['facebook.com', 'linkedin.com', 'twitter.com', 'instagram.com',
                                'youtube.com', 'yelp.com', 'tripadvisor.com', 'google.com',
                                'paginasamarillas', 'yellowpages', 'wikipedia.org', 'tiktok.com',
                                'x.com', 'whatsapp.com', 'pinterest.com']
                        if not any(s in domain for s in skip) and domain not in self.seen_domains:
                            # Filtrar dominios gubernamentales/educativos
                            if self._is_blacklisted_domain(domain):
                                continue
                            self.seen_domains.add(domain)
                            urls.append(f'https://{domain}')

                if len(items) < 10:
                    break

                time.sleep(0.3)

            except Exception as e:
                self.debug(f"Error CSE: {e}")
                break

        self.stats['searches_done'] += 1
        return urls

    def _is_blacklisted_domain(self, domain: str) -> bool:
        """Filtra dominios .gov, .edu, .org y TLDs fuera del país objetivo"""
        domain_lower = domain.lower()
        
        # 1. Blacklist por TLD institucional
        for tld in BLACKLIST_TLDS:
            if domain_lower.endswith(tld):
                return True
        
        # 2. Filtro geográfico: si busco en CO, un .co.za o .de no es relevante
        allowed_tlds = COUNTRY_TLDS.get(self.country)
        if allowed_tlds:
            # Si el dominio tiene TLD de país diferente, descartarlo
            foreign_country_tlds = [
                '.es', '.mx', '.ar', '.cl', '.pe', '.co', '.ec', '.uy', '.cr',
                '.pa', '.bo', '.py', '.br', '.pt', '.fr', '.de', '.it', '.uk',
                '.nl', '.be', '.at', '.ch', '.pl', '.cz', '.ru', '.cn', '.jp',
                '.kr', '.in', '.au', '.nz', '.za', '.ng', '.ke', '.eg',
            ]
            for tld in foreign_country_tlds:
                if domain_lower.endswith(tld) and tld not in allowed_tlds:
                    return True
        
        return False

    # ─── FASE 2: ANÁLISIS TÉCNICO ─────────────────────────────────────

    def analyze_website(self, url: str) -> Optional[Dict]:
        """Analiza una web: TTFB, CMS, hosting, SSL, PageSpeed"""
        parsed = urlparse(url)
        domain = parsed.netloc.replace('www.', '')
        result = {
            'url': url,
            'domain': domain,
            'ttfb': None,
            'pagespeed_score': None,
            'pagespeed_mobile': None,
            'cms': 'unknown',
            'hosting_provider': 'unknown',
            'hosting_type': 'unknown',
            'server_ip': None,
            'server_software': None,
            'ssl_valid': None,
            'ssl_days_left': None,
            'is_wordpress': False,
            'has_woocommerce': False,
            'page_title': '',
            'sniper_score': 0,
        }

        try:
            # 1. Medir TTFB + obtener headers + HTML
            self.session.headers['User-Agent'] = random.choice(USER_AGENTS)
            start_time = time.time()
            resp = self.session.get(url, timeout=15, allow_redirects=True)
            ttfb = time.time() - start_time
            result['ttfb'] = round(ttfb, 2)

            if resp.status_code >= 400:
                self.debug(f"  {domain}: HTTP {resp.status_code}")
                return None

            html = resp.text[:100000]  # Solo primeros 100KB
            headers = {k.lower(): v.lower() for k, v in resp.headers.items()}

            # URL final después de redirects
            final_url = resp.url
            result['url'] = final_url

            # 2. Detectar CMS
            result['cms'] = self._detect_cms(html, headers)
            result['is_wordpress'] = result['cms'] in ('wordpress', 'woocommerce')
            result['has_woocommerce'] = result['cms'] == 'woocommerce'

            # 3. Detectar servidor
            result['server_software'] = headers.get('server', 'unknown')

            # 4. Título de la página
            title_match = re.search(r'<title[^>]*>(.*?)</title>', html, re.IGNORECASE | re.DOTALL)
            if title_match:
                result['page_title'] = re.sub(r'\s+', ' ', title_match.group(1)).strip()[:150]

            # 5. Resolver IP + hosting
            try:
                ip = socket.gethostbyname(domain)
                result['server_ip'] = ip
                result['hosting_provider'], result['hosting_type'] = self._detect_hosting(ip, domain, headers)
            except socket.gaierror:
                pass

            # 6. SSL check
            result['ssl_valid'], result['ssl_days_left'] = self._check_ssl(domain)

            # 7. PageSpeed (si parece candidato interesante)
            if result['is_wordpress'] or result['ttfb'] > 1.5 or result['hosting_type'] == 'shared':
                ps = self._get_pagespeed(domain)
                if ps:
                    result['pagespeed_score'] = ps.get('desktop')
                    result['pagespeed_mobile'] = ps.get('mobile')

            # 8. Calcular Sniper Score
            result['sniper_score'] = self._calculate_score(result)

            self.stats['domains_analyzed'] += 1
            if result['is_wordpress']:
                self.stats['wordpress_found'] += 1
            if result['ttfb'] and result['ttfb'] > 2.0:
                self.stats['slow_sites'] += 1
            if result['hosting_type'] == 'shared':
                self.stats['shared_hosting'] += 1
            if result['sniper_score'] >= 60:
                self.stats['hot_leads'] += 1

            return result

        except requests.exceptions.Timeout:
            self.debug(f"  {domain}: Timeout (>15s) — hosting MUY lento")
            result['ttfb'] = 15.0
            result['sniper_score'] = 80  # Timeout = hosting terrible
            self.stats['domains_analyzed'] += 1
            self.stats['slow_sites'] += 1
            return result
        except requests.exceptions.SSLError:
            self.debug(f"  {domain}: SSL Error")
            result['ssl_valid'] = False
            result['sniper_score'] = 70
            self.stats['domains_analyzed'] += 1
            return result
        except Exception as e:
            self.debug(f"  {domain}: Error: {e}")
            return None

    def _detect_cms(self, html: str, headers: dict) -> str:
        """Detecta el CMS de una web por patrones en HTML y headers"""
        html_lower = html.lower()

        # Primero WooCommerce (es WordPress + tienda)
        woo_count = sum(1 for sig in CMS_SIGNATURES['woocommerce'] if sig.lower() in html_lower)
        if woo_count >= 2:
            return 'woocommerce'

        # WordPress
        wp_count = sum(1 for sig in CMS_SIGNATURES['wordpress'] if sig.lower() in html_lower)
        if wp_count >= 2:
            return 'wordpress'

        # Otros CMS
        for cms_name, signatures in CMS_SIGNATURES.items():
            if cms_name in ('wordpress', 'woocommerce', 'html_static'):
                continue
            count = sum(1 for sig in signatures if sig.lower() in html_lower)
            if count >= 2:
                return cms_name

        # Headers
        if 'x-powered-by' in headers:
            xpb = headers['x-powered-by']
            if 'wordpress' in xpb:
                return 'wordpress'

        return 'html_static'

    def _detect_hosting(self, ip: str, domain: str, headers: dict) -> Tuple[str, str]:
        """Detecta el proveedor de hosting por IP (ASN lookup gratis)"""
        try:
            resp = requests.get(f'https://ipinfo.io/{ip}/json', timeout=5)
            if resp.status_code == 200:
                data = resp.json()
                org = data.get('org', '')
                # ASN es el primer token del org
                asn = org.split()[0] if org else ''

                # Buscar en nuestro mapeo
                if asn in HOSTING_PROVIDERS:
                    return HOSTING_PROVIDERS[asn]

                # Fallback: buscar nombre en org
                org_lower = org.lower()
                for keyword, (name, htype) in [
                    ('godaddy', ('GoDaddy', 'shared')),
                    ('hostinger', ('Hostinger', 'shared')),
                    ('ionos', ('IONOS', 'shared')),
                    ('bluehost', ('Bluehost', 'shared')),
                    ('siteground', ('SiteGround', 'shared')),
                    ('namecheap', ('Namecheap', 'shared')),
                    ('dreamhost', ('DreamHost', 'shared')),
                    ('hostgator', ('HostGator', 'shared')),
                    ('ovh', ('OVH', 'cloud')),
                    ('hetzner', ('Hetzner', 'cloud')),
                    ('digitalocean', ('DigitalOcean', 'cloud')),
                    ('amazon', ('AWS', 'cloud')),
                    ('google', ('Google Cloud', 'cloud')),
                    ('microsoft', ('Azure', 'cloud')),
                    ('cloudflare', ('Cloudflare', 'cdn')),
                    ('dinahosting', ('Dinahosting', 'shared')),
                    ('raiola', ('Raiola Networks', 'shared')),
                    ('webempresa', ('Webempresa', 'shared')),
                    ('loading', ('Loading.es', 'shared')),
                    ('cdmon', ('CDmon', 'shared')),
                    ('arsys', ('Arsys', 'shared')),
                    ('acens', ('Acens', 'shared')),
                    ('strato', ('Strato', 'shared')),
                    ('1and1', ('1&1', 'shared')),
                    ('contabo', ('Contabo', 'vps')),
                    ('vultr', ('Vultr', 'cloud')),
                    ('linode', ('Linode', 'cloud')),
                    ('squarespace', ('Squarespace', 'saas')),
                    ('shopify', ('Shopify', 'saas')),
                    ('wix', ('Wix', 'saas')),
                    ('wpengine', ('WP Engine', 'managed')),
                    ('kinsta', ('Kinsta', 'managed')),
                    ('flywheel', ('Flywheel', 'managed')),
                ]:
                    if keyword in org_lower:
                        return (name, htype)

                # Si tiene org pero no reconocemos, es probablemente ISP local
                if org:
                    return (org[:40], 'unknown')

        except Exception:
            pass

        # Fallback por headers
        server = headers.get('server', '')
        if 'litespeed' in server:
            return ('LiteSpeed (shared probable)', 'shared')

        return ('Unknown', 'unknown')

    def _check_ssl(self, domain: str) -> Tuple[Optional[bool], Optional[int]]:
        """Verifica certificado SSL y días restantes"""
        try:
            ctx = ssl.create_default_context()
            with ctx.wrap_socket(socket.socket(), server_hostname=domain) as sock:
                sock.settimeout(5)
                sock.connect((domain, 443))
                cert = sock.getpeercertificate()
                if cert:
                    not_after = datetime.strptime(cert['notAfter'], '%b %d %H:%M:%S %Y %Z')
                    days_left = (not_after - datetime.utcnow()).days
                    return (True, days_left)
        except ssl.SSLCertVerificationError:
            return (False, 0)
        except Exception:
            pass
        return (None, None)

    def _get_pagespeed(self, domain: str) -> Optional[Dict]:
        """Obtiene PageSpeed scores via Google API (gratis: 25k/día)"""
        if not self.google_api_key:
            return None

        result = {}
        try:
            # Solo mobile (es el que más importa y ahorra cuota)
            resp = self.session.get(
                'https://www.googleapis.com/pagespeedonline/v5/runPagespeed',
                params={
                    'url': f'https://{domain}',
                    'key': self.google_api_key,
                    'strategy': 'mobile',
                    'category': 'performance',
                },
                timeout=60
            )
            self.stats['pagespeed_calls'] += 1

            if resp.status_code == 200:
                data = resp.json()
                lh = data.get('lighthouseResult', {})
                cats = lh.get('categories', {})
                perf = cats.get('performance', {})
                score = perf.get('score')
                if score is not None:
                    result['mobile'] = int(score * 100)

                # Extraer métricas detalladas
                audits = lh.get('audits', {})
                fcp = audits.get('first-contentful-paint', {}).get('numericValue')
                if fcp:
                    result['fcp_ms'] = round(fcp)
                lcp = audits.get('largest-contentful-paint', {}).get('numericValue')
                if lcp:
                    result['lcp_ms'] = round(lcp)
                ttfb_audit = audits.get('server-response-time', {}).get('numericValue')
                if ttfb_audit:
                    result['ttfb_pagespeed_ms'] = round(ttfb_audit)

        except Exception as e:
            self.debug(f"  PageSpeed error: {e}")

        return result if result else None

    def _calculate_score(self, analysis: Dict) -> int:
        """
        Calcula Sniper Score (0-100).
        Más alto = mejor lead para vender migración de hosting.
        
        Factores:
        - WordPress/WooCommerce (+25)
        - TTFB alto (+0 a +25)
        - Hosting compartido (+15)
        - PageSpeed bajo (+0 a +20)
        - SSL a punto de expirar (+10)
        - Hosting "caro/malo" conocido (+5)
        """
        score = 0

        # CMS: WordPress/Woo son migrables
        if analysis.get('has_woocommerce'):
            score += 25
        elif analysis.get('is_wordpress'):
            score += 25
        elif analysis.get('cms') in ('prestashop', 'joomla'):
            score += 15
        elif analysis.get('cms') in ('html_static',):
            score += 5
        # SaaS (Shopify, Squarespace, Wix) = no migrable
        elif analysis.get('cms') in ('shopify', 'squarespace', 'wix', 'webflow'):
            return 0  # No tiene sentido migrar estos

        # TTFB
        ttfb = analysis.get('ttfb') or 0
        if ttfb >= 5.0:
            score += 25
        elif ttfb >= 3.0:
            score += 20
        elif ttfb >= 2.0:
            score += 15
        elif ttfb >= 1.5:
            score += 10
        elif ttfb >= 1.0:
            score += 5

        # Hosting tipo
        if analysis.get('hosting_type') == 'shared':
            score += 15
        elif analysis.get('hosting_type') == 'unknown':
            score += 5
        # Cloud/managed = harder to sell migration
        elif analysis.get('hosting_type') in ('cloud', 'managed'):
            score -= 10

        # PageSpeed
        ps_mobile = analysis.get('pagespeed_mobile')
        if ps_mobile is not None:
            if ps_mobile < 30:
                score += 20
            elif ps_mobile < 50:
                score += 15
            elif ps_mobile < 70:
                score += 10
            elif ps_mobile < 85:
                score += 5

        # SSL
        ssl_days = analysis.get('ssl_days_left')
        if ssl_days is not None and ssl_days < 30:
            score += 10
        if analysis.get('ssl_valid') is False:
            score += 15

        # Hosting providers conocidos como "malos/caros"
        provider = (analysis.get('hosting_provider') or '').lower()
        bad_hosts = ['godaddy', 'hostinger', 'ionos', '1&1', 'bluehost',
                     'hostgator', 'namecheap', 'dreamhost', 'strato']
        if any(bh in provider for bh in bad_hosts):
            score += 5

        return min(score, 100)

    # ─── FASE 3: IMPORT a StaffKit ──────────────────────────────────

    def import_lead(self, analysis: Dict, niche: str, city: str) -> bool:
        """Importa un lead analizado a StaffKit"""
        if self.dry_run:
            emoji = '🔥' if analysis['sniper_score'] >= 60 else '⚡' if analysis['sniper_score'] >= 40 else '💤'
            self.log(f"  [DRY] {emoji} {analysis['domain']} | Score:{analysis['sniper_score']} | CMS:{analysis['cms']} | TTFB:{analysis['ttfb']}s | Host:{analysis['hosting_provider']} | PS:{analysis.get('pagespeed_mobile', '?')}")
            return True

        try:
            # Construir notas técnicas para el comercial
            notes_parts = [f"🎯 Hosting Sniper Score: {analysis['sniper_score']}/100"]
            notes_parts.append(f"CMS: {analysis['cms'].upper()}")
            notes_parts.append(f"TTFB: {analysis['ttfb']}s")
            if analysis.get('pagespeed_mobile') is not None:
                notes_parts.append(f"PageSpeed Mobile: {analysis['pagespeed_mobile']}/100")
            notes_parts.append(f"Hosting: {analysis['hosting_provider']} ({analysis['hosting_type']})")
            notes_parts.append(f"Servidor: {analysis.get('server_software', '?')}")
            if analysis.get('ssl_days_left') is not None:
                ssl_status = f"SSL: {'✅' if analysis['ssl_valid'] else '❌'} ({analysis['ssl_days_left']} días)"
                notes_parts.append(ssl_status)
            notes_parts.append(f"Nicho: {niche} | Ciudad: {city}")
            notes_parts.append(f"IP: {analysis.get('server_ip', '?')}")

            lead_data = {
                'email': '',
                'name': '',
                'company': analysis.get('page_title', analysis['domain']),
                'website': analysis['url'],
                'phone': '',
                'country': COUNTRY_CITIES.get(self.country, {}).get('name', self.country),
                'city': city,
                'sector': niche,
                'notes': ' | '.join(notes_parts),
                'score': analysis['sniper_score'],
                'hosting': analysis.get('hosting_provider', ''),
                'load_time': analysis.get('ttfb', 0),
                'wp_version': analysis['cms'] if analysis['is_wordpress'] else '',
                'needs_email_enrichment': True,
            }

            response = requests.post(
                f"{STAFFKIT_URL}/api/bots.php",
                data={
                    'action': 'save_lead',
                    'list_id': self.list_id,
                    'bot_id': self.bot_id,
                    'run_id': self.run_id,
                    'lead_data': json.dumps(lead_data),
                },
                headers={'Authorization': f'Bearer {self.api_key}'},
                timeout=20
            )

            if response.status_code == 200:
                result = response.json()
                if result.get('success'):
                    status = result.get('status', 'saved')
                    if status == 'duplicate':
                        self.stats['leads_duplicates'] += 1
                    else:
                        self.stats['leads_imported'] += 1
                    return True
                else:
                    self.debug(f"API error: {result.get('error', '?')}")
                    self.stats['leads_errors'] += 1
            else:
                self.debug(f"HTTP {response.status_code}")
                self.stats['leads_errors'] += 1

        except Exception as e:
            self.log(f"Error importando {analysis['domain']}: {e}", 'ERROR')
            self.stats['leads_errors'] += 1

        return False

    # ─── ORQUESTACIÓN ────────────────────────────────────────────────

    def run(self):
        """Ejecuta el Hosting Sniper completo"""
        start = datetime.now()
        country_data = COUNTRY_CITIES.get(self.country, {})

        self.log("=" * 60)
        self.log("🎯 HOSTING SNIPER v1.0")
        self.log(f"   País: {country_data.get('name', self.country)} ({self.country})")
        self.log(f"   Nichos: {', '.join(self.niches)}")
        self.log(f"   Búsquedas/run: {self.searches_per_run}")
        self.log(f"   Lista destino: #{self.list_id}")
        self.log(f"   Dry run: {'Sí' if self.dry_run else 'No'}")
        self.log("=" * 60)

        # Fase 1: Generar búsquedas
        searches = self.generate_searches()

        if not searches:
            self.log("❌ No hay búsquedas que hacer", 'ERROR')
            return self.stats

        # Procesar cada búsqueda
        for i, (niche, city) in enumerate(searches):
            self.log(f"\n🔍 [{i+1}/{len(searches)}] Buscando: '{niche}' en {city}")

            # Fase 1: Discovery
            urls = self.search_google(niche, city)
            self.stats['domains_found'] += len(urls)

            if not urls:
                self.log(f"  ⚠️  Sin resultados")
                continue

            self.log(f"  📡 {len(urls)} webs encontradas — analizando...")

            # Fase 2: Análisis en paralelo (3 hilos)
            with ThreadPoolExecutor(max_workers=3) as executor:
                futures = {executor.submit(self.analyze_website, url): url for url in urls}

                for future in as_completed(futures):
                    try:
                        analysis = future.result()
                        if analysis and analysis['sniper_score'] > 0:
                            # Fase 3: Import — solo leads con score suficiente
                            if analysis['sniper_score'] >= MIN_IMPORT_SCORE:
                                self.import_lead(analysis, niche, city)

                            emoji = '🔥' if analysis['sniper_score'] >= 60 else '⚡' if analysis['sniper_score'] >= 40 else '💤'
                            self.debug(f"  {emoji} {analysis['domain']} | Score:{analysis['sniper_score']} | {analysis['cms']} | TTFB:{analysis['ttfb']}s | {analysis['hosting_provider']}")
                    except Exception as e:
                        self.debug(f"  Error en análisis: {e}")

            time.sleep(self.delay)

        # ── Resumen ──
        elapsed = (datetime.now() - start).total_seconds()

        self.log("\n" + "=" * 60)
        self.log("📊 RESUMEN HOSTING SNIPER")
        self.log(f"   Búsquedas realizadas: {self.stats['searches_done']}")
        self.log(f"   Dominios encontrados: {self.stats['domains_found']}")
        self.log(f"   Dominios analizados:  {self.stats['domains_analyzed']}")
        self.log(f"   ─────────────────────────")
        self.log(f"   WordPress detectados: {self.stats['wordpress_found']}")
        self.log(f"   Sitios lentos (>2s):  {self.stats['slow_sites']}")
        self.log(f"   Hosting compartido:   {self.stats['shared_hosting']}")
        self.log(f"   🔥 HOT leads (60+):   {self.stats['hot_leads']}")
        self.log(f"   ─────────────────────────")
        self.log(f"   Leads importados:     {self.stats['leads_imported']}")
        self.log(f"   Leads duplicados:     {self.stats['leads_duplicates']}")
        self.log(f"   Errores:              {self.stats['leads_errors']}")
        self.log(f"   PageSpeed API calls:  {self.stats['pagespeed_calls']}")
        self.log(f"   ─────────────────────────")
        self.log(f"   Tiempo total:         {elapsed:.0f}s ({elapsed/60:.1f} min)")
        self.log("=" * 60)

        # ── STATS para daemon ──
        print(f"STATS:leads_found:{self.stats['domains_found']}")
        print(f"STATS:leads_saved:{self.stats['leads_imported']}")
        print(f"STATS:leads_duplicates:{self.stats['leads_duplicates']}")
        print(f"STATS:errors:{self.stats['leads_errors']}")

        return self.stats


# ─── Funciones auxiliares para modo daemon ────────────────────────────

def load_env_file():
    """Lee .env del CWD sin dependencia de python-dotenv"""
    env_path = os.path.join(os.getcwd(), '.env')
    if os.path.exists(env_path):
        try:
            with open(env_path) as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        k, v = line.split('=', 1)
                        k, v = k.strip(), v.strip()
                        if k and v and k not in os.environ:
                            os.environ[k] = v
        except Exception:
            pass


def fetch_bot_config(api_key: str, bot_id: int) -> dict:
    """Obtiene la configuración del bot desde StaffKit API"""
    try:
        resp = requests.get(
            f"{STAFFKIT_URL}/api/v2/external-bot",
            params={'id': bot_id},
            headers={'Authorization': f'Bearer {api_key}'},
            timeout=15
        )
        if resp.status_code == 200:
            data = resp.json()
            if data.get('success'):
                bots = data.get('bots', data.get('data', []))
                if isinstance(bots, list) and bots:
                    return bots[0]
                elif isinstance(bots, dict):
                    return bots
    except Exception as e:
        print(f"[WARNING] Could not fetch bot config: {e}")
    return {}


def report_run(api_key: str, bot_id: int, action: str, stats: dict = None):
    """Reporta inicio/fin de ejecución al daemon API"""
    try:
        payload = {'id': bot_id, 'action': action}
        if stats:
            payload['stats'] = json.dumps(stats)
        requests.post(
            f"{STAFFKIT_URL}/api/v2/external-bot",
            json=payload,
            headers={'Authorization': f'Bearer {api_key}'},
            timeout=10
        )
    except Exception:
        pass


def main():
    parser = argparse.ArgumentParser(
        description='Hosting Sniper - Encuentra webs lentas en hosting malo → leads de migración'
    )
    parser.add_argument('--api-key', required=True, help='API key de StaffKit')
    parser.add_argument('--bot-id', type=int, default=0, help='ID del bot externo (modo daemon)')
    parser.add_argument('--list-id', type=int, default=0, help='ID de la lista destino')
    parser.add_argument('--niches', default='', help='Nichos separados por coma (ej: floristería,dentista)')
    parser.add_argument('--country', default='ES', help='Código país (ES, MX, AR, CO, CL, PE...)')
    parser.add_argument('--max-cities', type=int, default=50, help='Máx ciudades por país')
    parser.add_argument('--searches-per-run', type=int, default=10, help='Búsquedas por ejecución')
    parser.add_argument('--delay', type=float, default=1.5, help='Delay entre búsquedas (seg)')
    parser.add_argument('--run-id', type=int, default=0, help='ID de la ejecución (bot_runs.id) para trazar source_run_id')
    parser.add_argument('--dry-run', action='store_true', help='Solo analizar, no importar')
    parser.add_argument('--verbose', action='store_true', help='Modo verbose')

    args = parser.parse_args()

    # Cargar .env
    load_env_file()

    list_id = args.list_id
    niches_str = args.niches
    country = args.country
    max_cities = args.max_cities
    searches_per_run = args.searches_per_run
    delay = args.delay

    # Si viene con --bot-id, obtener config del bot
    if args.bot_id:
        print(f"[INFO] Modo daemon — bot_id={args.bot_id}")
        bot_config = fetch_bot_config(args.api_key, args.bot_id)

        if bot_config:
            list_id = list_id or int(bot_config.get('target_list_id', 0) or 0)
            niches_str = niches_str or bot_config.get('config_sniper_niches', '')
            country = country if args.country != 'ES' else (bot_config.get('config_sniper_country', 'ES') or 'ES')
            max_cities = int(bot_config.get('config_sniper_max_cities', max_cities) or max_cities)
            searches_per_run = int(bot_config.get('config_sniper_searches_per_run', searches_per_run) or searches_per_run)
            delay = float(bot_config.get('config_sniper_delay', delay) or delay)
            print(f"[INFO] Config: list={list_id}, country={country}, niches={niches_str[:60]}...")

        report_run(args.api_key, args.bot_id, 'start_run')

    if not list_id:
        print("[ERROR] Se requiere --list-id o un bot configurado con target_list_id")
        sys.exit(1)

    # Parsear nichos
    niches = None
    if niches_str:
        niches = [n.strip() for n in niches_str.split(',') if n.strip()]

    # Obtener Google keys
    google_key = os.getenv('GOOGLE_API_KEY', '')
    google_cx = os.getenv('CX_ID', '')

    if not google_key or not google_cx:
        # Intentar config.json de StaffKit
        config_paths = [
            os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data', 'config.json'),
            '/home/replanta/staff.replanta.dev/data/config.json',
        ]
        for cp in config_paths:
            if os.path.exists(cp):
                try:
                    with open(cp) as f:
                        config = json.load(f)
                    google_key = google_key or config.get('google', {}).get('api_key', '')
                    google_cx = google_cx or config.get('google', {}).get('cx_id', '')
                    if google_key:
                        print(f"[INFO] Google keys from {cp}")
                        break
                except Exception:
                    pass

    if not google_key:
        print("[ERROR] Google API Key no encontrada (ni en .env, ni env vars, ni config.json)")
        sys.exit(1)

    sniper = HostingSniper(
        api_key=args.api_key,
        list_id=list_id,
        bot_id=args.bot_id,
        run_id=args.run_id,
        google_api_key=google_key,
        google_cx=google_cx,
        niches=niches,
        country=country,
        max_cities=max_cities,
        searches_per_run=searches_per_run,
        delay=delay,
        dry_run=args.dry_run,
        verbose=args.verbose,
    )

    stats = sniper.run()

    # Reportar fin al daemon
    if args.bot_id:
        report_run(args.api_key, args.bot_id, 'end_run', stats)

    sys.exit(1 if len(stats.get('errors', [])) > stats['leads_imported'] else 0)


if __name__ == '__main__':
    main()
