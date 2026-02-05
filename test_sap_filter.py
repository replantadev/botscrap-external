#!/usr/bin/env python3
"""Test del filtro exacto del bot"""
import requests
import urllib3
urllib3.disable_warnings()

SAP_URL = 'https://verdis.artesap.com:50000/b1s/v1'
session = requests.Session()
session.verify = False

# Login
r = session.post(f'{SAP_URL}/Login', json={'CompanyDB':'CLOUD_VERDIS_ES','UserName':'manager','Password':'9006'})
print('Login:', r.status_code)

# Probar el filtro EXACTO que usa el bot
filters = [
    "CardType eq 'cCustomer'",
    "(GroupCode eq 100)",
    "EmailAddress ne ''",
    "EmailAddress ne null",
    "Valid eq 'tYES'"
]
filter_str = ' and '.join(filters)
print(f'\nFiltro completo:')
print(f'  {filter_str}')

r = session.get(f'{SAP_URL}/BusinessPartners/$count', params={'$filter': filter_str})
print(f'\nResultado con filtro completo: {r.text} (status: {r.status_code})')

# Probar sin el filtro Valid
filters2 = [
    "CardType eq 'cCustomer'",
    "(GroupCode eq 100)",
    "EmailAddress ne ''",
    "EmailAddress ne null"
]
filter_str2 = ' and '.join(filters2)
r2 = session.get(f'{SAP_URL}/BusinessPartners/$count', params={'$filter': filter_str2})
print(f'Sin filtro Valid: {r2.text} (status: {r2.status_code})')

# Probar solo clientes del grupo 100
r3 = session.get(f'{SAP_URL}/BusinessPartners/$count', 
                params={'$filter': "CardType eq 'cCustomer' and GroupCode eq 100"})
print(f'Solo clientes grupo 100: {r3.text} (status: {r3.status_code})')

# Ver algunos ejemplos
print('\nEjemplos de clientes grupo 100 con email:')
r4 = session.get(f'{SAP_URL}/BusinessPartners', 
                params={
                    '$filter': "CardType eq 'cCustomer' and GroupCode eq 100 and EmailAddress ne '' and EmailAddress ne null",
                    '$select': 'CardCode,CardName,EmailAddress,Valid',
                    '$top': '5'
                })
for bp in r4.json().get('value', []):
    print(f"  {bp.get('CardCode')}: {bp.get('CardName')[:40]:<40} | {bp.get('EmailAddress'):<30} | Valid: {bp.get('Valid')}")
