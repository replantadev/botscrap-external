#!/usr/bin/env python3
"""
Test rápido de conexión SAP Service Layer
"""
import requests
import urllib3
urllib3.disable_warnings()

SAP_URL = "https://verdis.artesap.com:50000/b1s/v1"
COMPANY = "CLOUD_VERDIS_ES"
USER = "manager"
PASSWORD = "9006"

session = requests.Session()
session.verify = False

print("=" * 60)
print("TEST CONEXIÓN SAP SERVICE LAYER")
print("=" * 60)

# 1. Login
print("\n1. Login...")
try:
    r = session.post(f"{SAP_URL}/Login", json={
        "CompanyDB": COMPANY,
        "UserName": USER,
        "Password": PASSWORD
    }, timeout=30)
    if r.status_code == 200:
        print(f"   ✅ Login OK")
    else:
        print(f"   ❌ Login FALLÓ: {r.status_code} - {r.text[:200]}")
        exit(1)
except Exception as e:
    print(f"   ❌ Error conexión: {e}")
    exit(1)

# 2. Contar TODOS los BP
print("\n2. Contando Business Partners...")
try:
    r = session.get(f"{SAP_URL}/BusinessPartners/$count", timeout=30)
    print(f"   Total BP (sin filtros): {r.text}")
except Exception as e:
    print(f"   Error: {e}")

# 3. Contar BP con email
print("\n3. BP con email...")
try:
    r = session.get(f"{SAP_URL}/BusinessPartners/$count", 
                   params={"$filter": "EmailAddress ne '' and EmailAddress ne null"},
                   timeout=30)
    print(f"   BP con email: {r.text}")
except Exception as e:
    print(f"   Error: {e}")

# 4. Contar BP del grupo 100
print("\n4. BP del grupo 100...")
try:
    r = session.get(f"{SAP_URL}/BusinessPartners/$count",
                   params={"$filter": "GroupCode eq 100"},
                   timeout=30)
    print(f"   BP en grupo 100: {r.text}")
except Exception as e:
    print(f"   Error: {e}")

# 5. Contar BP del grupo 100 CON email
print("\n5. BP del grupo 100 CON email...")
try:
    r = session.get(f"{SAP_URL}/BusinessPartners/$count",
                   params={"$filter": "GroupCode eq 100 and EmailAddress ne '' and EmailAddress ne null"},
                   timeout=30)
    print(f"   BP grupo 100 + email: {r.text}")
except Exception as e:
    print(f"   Error: {e}")

# 6. Ver ejemplos de BP con email
print("\n6. Ejemplos de BP con email:")
try:
    r = session.get(f"{SAP_URL}/BusinessPartners",
                   params={
                       "$filter": "EmailAddress ne '' and EmailAddress ne null",
                       "$select": "CardCode,CardName,EmailAddress,GroupCode,CardType",
                       "$top": "10"
                   },
                   timeout=30)
    data = r.json()
    for bp in data.get('value', []):
        email = bp.get('EmailAddress', '')
        domain = email.split('@')[-1] if '@' in email else 'N/A'
        print(f"   {bp.get('CardCode')}: {bp.get('CardName')[:30]:<30} | {email:<35} | Grupo: {bp.get('GroupCode')} | Tipo: {bp.get('CardType')}")
except Exception as e:
    print(f"   Error: {e}")

# 7. Ver grupos disponibles
print("\n7. Grupos de clientes disponibles:")
try:
    r = session.get(f"{SAP_URL}/BusinessPartnerGroups",
                   params={"$select": "Code,Name,Type"},
                   timeout=30)
    data = r.json()
    for g in data.get('value', [])[:15]:
        print(f"   {g.get('Code')}: {g.get('Name')} ({g.get('Type')})")
except Exception as e:
    print(f"   Error: {e}")

# Logout
try:
    session.post(f"{SAP_URL}/Logout", timeout=5)
except:
    pass

print("\n" + "=" * 60)
