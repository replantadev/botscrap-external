#!/usr/bin/env python3
"""Contar emails corporativos vs genéricos"""
import requests
import urllib3
urllib3.disable_warnings()

GENERIC_DOMAINS = {
    'gmail.com', 'hotmail.com', 'hotmail.es', 'outlook.com', 'outlook.es',
    'yahoo.com', 'yahoo.es', 'live.com', 'msn.com', 'icloud.com',
    'protonmail.com', 'mail.com', 'aol.com', 'zoho.com',
    'telefonica.net', 'orange.es', 'vodafone.es', 'movistar.es'
}

SAP_URL = 'https://verdis.artesap.com:50000/b1s/v1'
session = requests.Session()
session.verify = False

session.post(f'{SAP_URL}/Login', json={'CompanyDB':'CLOUD_VERDIS_ES','UserName':'manager','Password':'9006'})

# Obtener todos los clientes del grupo 100 con email y activos
all_bp = []
skip = 0
while True:
    r = session.get(f'{SAP_URL}/BusinessPartners', 
                   params={
                       '$filter': "CardType eq 'cCustomer' and GroupCode eq 100 and EmailAddress ne '' and EmailAddress ne null and Valid eq 'tYES'",
                       '$select': 'CardCode,CardName,EmailAddress',
                       '$top': '100',
                       '$skip': str(skip)
                   })
    data = r.json().get('value', [])
    if not data:
        break
    all_bp.extend(data)
    skip += 100
    if skip > 1000:  # Limitar para el test
        break

print(f"Total BP activos con email (muestra): {len(all_bp)}")

generic_count = 0
corporate_count = 0
corporate_examples = []

for bp in all_bp:
    email = bp.get('EmailAddress', '').lower()
    if '@' in email:
        domain = email.split('@')[-1]
        if domain in GENERIC_DOMAINS:
            generic_count += 1
        else:
            corporate_count += 1
            if len(corporate_examples) < 10:
                corporate_examples.append(f"{bp.get('CardCode')}: {bp.get('CardName')[:30]} | {email}")

print(f"\nEmails genéricos (gmail, hotmail, etc): {generic_count}")
print(f"Emails corporativos: {corporate_count}")
print(f"Porcentaje corporativo: {corporate_count*100//len(all_bp) if all_bp else 0}%")

print(f"\nEjemplos de emails CORPORATIVOS:")
for ex in corporate_examples:
    print(f"  {ex}")
