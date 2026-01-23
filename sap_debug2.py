#!/usr/bin/env python3
"""SAP Debug v2 - Estructura OCRD"""
import pymssql

SAP_CONFIG = {
    'server': '213.97.178.117',
    'port': 1435,
    'database': 'DRASANVI',
    'user': 'web2',
    'password': 'eba-WWWhai'
}

def main():
    print("=== SAP DEBUG v2 - ESTRUCTURA OCRD ===\n")
    
    conn = pymssql.connect(
        server=f"{SAP_CONFIG['server']}:{SAP_CONFIG['port']}",
        user=SAP_CONFIG['user'],
        password=SAP_CONFIG['password'],
        database=SAP_CONFIG['database'],
        timeout=30
    )
    print("✅ Conectado!\n")
    cursor = conn.cursor()
    
    # Estructura OCRD
    print("=== ESTRUCTURA OCRD (Business Partners) ===")
    cursor.execute("""
        SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = 'OCRD'
        ORDER BY ORDINAL_POSITION
    """)
    for c in cursor.fetchall():
        size = f"({c[2]})" if c[2] else ""
        print(f"  - {c[0]} {c[1]}{size}")
    
    # Conteo total
    cursor.execute("SELECT COUNT(*) FROM OCRD")
    print(f"\nTotal registros OCRD: {cursor.fetchone()[0]}")
    
    # Conteo por CardType
    print("\n=== CONTEO POR CARDTYPE ===")
    cursor.execute("SELECT CardType, COUNT(*) FROM OCRD GROUP BY CardType")
    for r in cursor.fetchall():
        tipo = {'C':'Cliente','S':'Proveedor','L':'Lead'}.get(r[0], r[0])
        print(f"  {tipo} ({r[0]}): {r[1]}")
    
    # Campos U_ (UDF)
    print("\n=== CAMPOS PERSONALIZADOS (U_*) EN OCRD ===")
    cursor.execute("""
        SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = 'OCRD' AND COLUMN_NAME LIKE 'U_%'
    """)
    udfs = [r[0] for r in cursor.fetchall()]
    for u in udfs:
        print(f"  - {u}")
    
    # Valores de U_DRA_CATEGORIZACION si existe
    if 'U_DRA_CATEGORIZACION' in udfs:
        print("\n=== VALORES U_DRA_CATEGORIZACION ===")
        cursor.execute("""
            SELECT U_DRA_CATEGORIZACION, COUNT(*) as cnt 
            FROM OCRD WHERE U_DRA_CATEGORIZACION IS NOT NULL 
            GROUP BY U_DRA_CATEGORIZACION ORDER BY cnt DESC
        """)
        for r in cursor.fetchall():
            print(f"  {r[0]}: {r[1]}")
    
    # GroupCode
    print("\n=== GROUPCODE EN OCRD ===")
    cursor.execute("""
        SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME='OCRD' AND COLUMN_NAME='GroupCode'
    """)
    if cursor.fetchone()[0] > 0:
        cursor.execute("SELECT GroupCode, COUNT(*) FROM OCRD GROUP BY GroupCode ORDER BY COUNT(*) DESC")
        for r in cursor.fetchall():
            print(f"  Grupo {r[0]}: {r[1]}")
    else:
        print("  No existe GroupCode")
    
    # Emails disponibles
    print("\n=== EMAILS DISPONIBLES POR TIPO ===")
    cursor.execute("""
        SELECT CardType,
            SUM(CASE WHEN E_Mail IS NOT NULL AND E_Mail != '' THEN 1 ELSE 0 END) as ConEmail,
            COUNT(*) as Total
        FROM OCRD GROUP BY CardType
    """)
    for r in cursor.fetchall():
        tipo = {'C':'Cliente','S':'Proveedor','L':'Lead'}.get(r[0], r[0])
        print(f"  {tipo}: {r[1]} con email de {r[2]} total")
    
    # Ejemplo 10 clientes con email
    print("\n=== EJEMPLO 10 REGISTROS CON EMAIL ===")
    cursor.execute("""
        SELECT TOP 10 CardCode, CardName, CardType, E_Mail, Phone1, City
        FROM OCRD 
        WHERE E_Mail IS NOT NULL AND E_Mail != ''
    """)
    for r in cursor.fetchall():
        tipo = {'C':'Cliente','S':'Proveedor','L':'Lead'}.get(r[2], r[2])
        print(f"\n  [{r[0]}] {r[1]}")
        print(f"    Tipo: {tipo} | Email: {r[3]}")
        print(f"    Tel: {r[4]} | Ciudad: {r[5]}")
    
    # Ver si hay OCRG
    print("\n=== TABLA OCRG (Grupos) ===")
    cursor.execute("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME='OCRG'")
    if cursor.fetchone()[0] > 0:
        cursor.execute("SELECT TOP 20 * FROM OCRG")
        for r in cursor.fetchall():
            print(f"  {r}")
    else:
        print("  No existe tabla OCRG")
    
    # Ver OITB (Grupos de artículos - a veces usado para categorías)
    print("\n=== TABLA OITB (ItemGroups) - posible categorización ===")
    cursor.execute("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME='OITB'")
    if cursor.fetchone()[0] > 0:
        cursor.execute("SELECT TOP 20 * FROM OITB")
        cols = [d[0] for d in cursor.description]
        print(f"  Columnas: {cols}")
        for r in cursor.fetchall():
            print(f"  {r}")
    else:
        print("  No existe tabla OITB")
    
    conn.close()
    print("\n\n=== DEBUG v2 COMPLETADO ===")

if __name__ == '__main__':
    main()
