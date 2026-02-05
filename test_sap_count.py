#!/usr/bin/env python3
"""Test SAP count by group"""
import requests
import urllib3
urllib3.disable_warnings()

url = 'https://verdis.artesap.com:50000/b1s/v1'
s = requests.Session()
s.verify = False

# Login
r = s.post(f'{url}/Login', json={
    'CompanyDB': 'CLOUD_VERDIS_ES',
    'UserName': 'manager',
    'Password': '9006'
})
print(f'Login: {r.status_code}')

# Ejecutar SQL query
query_url = f"{url}/SQLQueries('BP_COUNT_GRP')/List"
print(f'URL: {query_url}')
r = s.get(query_url)
print(f'Status: {r.status_code}')
print(f'Response: {r.text[:1500]}')

s.post(f'{url}/Logout')
