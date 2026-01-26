#!/usr/bin/env python3
"""
Test conexión SAP Verdis via Service Layer
"""
import requests
import json
import urllib3

# Desactivar warnings SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Config
SAP_URL = "https://verdis.artesap.com:50000/b1s/v1"
COMPANY_DB = "CLOUD_VERDIS_ES"
USERNAME = "manager"
PASSWORD = "9006"

def login():
    """Login y obtener sesión"""
    print(f"🔐 Conectando a {SAP_URL}...")
    
    try:
        response = requests.post(
            f"{SAP_URL}/Login",
            json={
                "CompanyDB": COMPANY_DB,
                "UserName": USERNAME,
                "Password": PASSWORD
            },
            verify=False,
            timeout=30
        )
        
        print(f"Status: {response.status_code}")
        print(f"Headers: {dict(response.headers)}")
        
        if response.status_code == 200:
            data = response.json()
            print(f"✅ Login OK: {json.dumps(data, indent=2)}")
            return response.cookies
        else:
            print(f"❌ Error: {response.text}")
            return None
            
    except Exception as e:
        print(f"❌ Excepción: {e}")
        return None

def get_business_partners(cookies, top=5):
    """Obtener Business Partners (clientes)"""
    print(f"\n📋 Obteniendo Business Partners...")
    
    try:
        # Primero sin $select para ver todos los campos
        response = requests.get(
            f"{SAP_URL}/BusinessPartners",
            params={
                "$top": top
            },
            cookies=cookies,
            verify=False,
            timeout=30
        )
        
        if response.status_code == 200:
            data = response.json()
            print(f"✅ Encontrados: {len(data.get('value', []))} registros")
            for bp in data.get('value', []):
                print(f"\n--- {bp.get('CardCode')} ---")
                # Mostrar todos los campos disponibles
                for key, value in bp.items():
                    if value and value != "" and value != [] and value != None:
                        print(f"  {key}: {value}")
            return data
        else:
            print(f"❌ Error {response.status_code}: {response.text}")
            return None
            
    except Exception as e:
        print(f"❌ Excepción: {e}")
        return None

def get_schema(cookies):
    """Ver esquema de BusinessPartners"""
    print(f"\n🔍 Obteniendo esquema de BusinessPartners...")
    
    try:
        response = requests.get(
            f"{SAP_URL}/$metadata",
            cookies=cookies,
            verify=False,
            timeout=30
        )
        
        if response.status_code == 200:
            # Es XML, mostrar solo parte relevante
            content = response.text
            print(f"✅ Metadata obtenida ({len(content)} bytes)")
            # Buscar campos de BusinessPartner
            if "BusinessPartner" in content:
                print("✅ BusinessPartner entity encontrada")
        else:
            print(f"❌ Error: {response.status_code}")
            
    except Exception as e:
        print(f"❌ Excepción: {e}")

def get_bp_with_filters(cookies):
    """Obtener BPs con filtros similares al bot actual"""
    print(f"\n📋 Obteniendo clientes con email...")
    
    try:
        # Filtrar solo los que tienen email
        response = requests.get(
            f"{SAP_URL}/BusinessPartners",
            params={
                "$top": 10,
                "$filter": "E_Mail ne ''",
                "$select": "CardCode,CardName,CardType,Phone1,Phone2,E_Mail,Cellular,Website,City,Country,Address,FreeText"
            },
            cookies=cookies,
            verify=False,
            timeout=30
        )
        
        if response.status_code == 200:
            data = response.json()
            print(f"✅ Con email: {len(data.get('value', []))} registros")
            for bp in data.get('value', []):
                print(f"\n--- {bp.get('CardCode')}: {bp.get('CardName')} ---")
                print(f"  Email: {bp.get('E_Mail')}")
                print(f"  Tel: {bp.get('Phone1')} / {bp.get('Phone2')}")
                print(f"  Web: {bp.get('Website')}")
                print(f"  Ciudad: {bp.get('City')}, {bp.get('Country')}")
            return data
        else:
            print(f"❌ Error {response.status_code}: {response.text}")
            return None
            
    except Exception as e:
        print(f"❌ Excepción: {e}")
        return None

def logout(cookies):
    """Cerrar sesión"""
    try:
        requests.post(f"{SAP_URL}/Logout", cookies=cookies, verify=False, timeout=10)
        print("\n👋 Sesión cerrada")
    except:
        pass

if __name__ == "__main__":
    print("=" * 60)
    print("TEST SAP VERDIS - Service Layer")
    print("=" * 60)
    
    cookies = login()
    
    if cookies:
        get_business_partners(cookies, top=3)
        get_bp_with_filters(cookies)
        logout(cookies)
    else:
        print("\n⚠️ No se pudo conectar. Verificar credenciales/servidor.")
