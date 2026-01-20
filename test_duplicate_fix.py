#!/usr/bin/env python3
"""
TEST: Verificar que el fix de duplicados funciona correctamente
Envía leads de prueba con diferentes combinaciones de email/website vacíos
"""

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from staffkit_client import StaffKitClient

# Config
STAFFKIT_URL = os.getenv('STAFFKIT_URL', 'https://staff.replanta.dev')
STAFFKIT_API_KEY = os.getenv('STAFFKIT_API_KEY', '')

print("\n" + "="*70)
print("🧪 TEST: Verificación del fix de duplicados")
print("="*70 + "\n")

if not STAFFKIT_URL or not STAFFKIT_API_KEY:
    print("❌ Error: Faltan STAFFKIT_URL o STAFFKIT_API_KEY en .env")
    sys.exit(1)

client = StaffKitClient(STAFFKIT_URL, STAFFKIT_API_KEY)

# ID de lista para testing (cambiar según necesites)
TEST_LIST_ID = 4  # "Clientes Descontentos"

print(f"📋 Lista de prueba: ID={TEST_LIST_ID}")
print(f"🔗 API: {STAFFKIT_URL}\n")

# Casos de prueba
test_cases = [
    {
        'name': 'Lead con email Y website',
        'lead': {
            'email': 'test1@example.com',
            'web': 'https://example1.com',
            'contacto': 'Test User 1',
            'empresa': 'Test Company 1',
            'notas': 'TEST: Lead con ambos campos completos'
        },
        'expected': 'saved'
    },
    {
        'name': 'Lead con email pero SIN website',
        'lead': {
            'email': 'test2@example.com',
            'web': '',
            'contacto': 'Test User 2',
            'empresa': 'Test Company 2',
            'notas': 'TEST: Lead solo con email'
        },
        'expected': 'saved'
    },
    {
        'name': 'Lead con website pero SIN email',
        'lead': {
            'email': '',
            'web': 'https://example3.com',
            'contacto': 'Test User 3',
            'empresa': 'Test Company 3',
            'notas': 'TEST: Lead solo con website'
        },
        'expected': 'saved'
    },
    {
        'name': 'Lead SIN email NI website (el bug)',
        'lead': {
            'email': '',
            'web': '',
            'contacto': 'Test User 4',
            'empresa': 'Test Company 4',
            'notas': 'TEST: Lead sin email ni website - ANTES era falso duplicado'
        },
        'expected': 'saved'
    },
    {
        'name': 'Duplicado real (mismo email del caso 1)',
        'lead': {
            'email': 'test1@example.com',
            'web': 'https://different-website.com',
            'contacto': 'Different Name',
            'empresa': 'Different Company',
            'notas': 'TEST: Duplicado real por email'
        },
        'expected': 'duplicate'
    }
]

print("🚀 Ejecutando casos de prueba...\n")

results = []
for i, test in enumerate(test_cases, 1):
    print(f"[{i}/{len(test_cases)}] {test['name']}...")
    
    result = client.save_lead(
        lead=test['lead'],
        list_id=TEST_LIST_ID
    )
    
    status = result.get('status', 'error')
    success = result.get('success', False)
    
    # Verificar resultado esperado
    if status == test['expected']:
        print(f"  ✅ PASS: {status}")
        results.append(True)
    else:
        print(f"  ❌ FAIL: Esperado '{test['expected']}', obtuvo '{status}'")
        results.append(False)
    
    print()

# Resumen
print("="*70)
print("📊 RESUMEN")
print("="*70)
passed = sum(results)
total = len(results)
print(f"\n✅ Passed: {passed}/{total}")
print(f"❌ Failed: {total - passed}/{total}")

if all(results):
    print("\n🎉 ¡TODOS LOS TESTS PASARON! El fix funciona correctamente.\n")
    sys.exit(0)
else:
    print("\n⚠️  Algunos tests fallaron. Revisar el código.\n")
    sys.exit(1)
