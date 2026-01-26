#!/usr/bin/env python3
"""
Test: Ver branches disponibles en SAP Drasanvi
"""
import pymssql

SAP_CONFIG = {
    'server': '213.97.178.117',
    'port': 1435,
    'database': 'DRASANVI',
    'user': 'web2',
    'password': 'web2'
}

def main():
    print("Conectando a SAP Drasanvi...")
    conn = pymssql.connect(
        server=SAP_CONFIG['server'],
        port=SAP_CONFIG['port'],
        user=SAP_CONFIG['user'],
        password=SAP_CONFIG['password'],
        database=SAP_CONFIG['database'],
        timeout=30
    )
    cursor = conn.cursor(as_dict=True)
    print("Conectado!\n")
    
    # Ver branches disponibles en _WEB_Clientes
    print("=" * 60)
    print("BRANCHES en _WEB_Clientes:")
    print("=" * 60)
    cursor.execute("""
        SELECT Branch, COUNT(*) as total
        FROM _WEB_Clientes
        WHERE Branch IS NOT NULL AND Branch != ''
        GROUP BY Branch
        ORDER BY total DESC
    """)
    for row in cursor.fetchall():
        print(f"  {row['Branch']}: {row['total']} registros")
    
    # Ver algunos registros de ejemplo con email
    print("\n" + "=" * 60)
    print("EJEMPLO: 5 registros con email (branch FARMACIA o similar):")
    print("=" * 60)
    cursor.execute("""
        SELECT TOP 5
            w.CardCode, w.Branch, o.CardName, o.E_Mail, w.City
        FROM _WEB_Clientes w
        INNER JOIN OCRD o ON w.CardCode = o.CardCode
        WHERE o.E_Mail IS NOT NULL AND o.E_Mail != ''
          AND w.Branch LIKE '%FARM%'
    """)
    for row in cursor.fetchall():
        print(f"  {row['CardCode']}: {row['CardName']}")
        print(f"    Branch: {row['Branch']}, Email: {row['E_Mail']}, City: {row['City']}")
    
    # Si no hay FARM%, mostrar cualquiera
    print("\n" + "=" * 60)
    print("EJEMPLO: 10 registros con email (cualquier branch):")
    print("=" * 60)
    cursor.execute("""
        SELECT TOP 10
            w.CardCode, w.Branch, o.CardName, o.E_Mail, w.City
        FROM _WEB_Clientes w
        INNER JOIN OCRD o ON w.CardCode = o.CardCode
        WHERE o.E_Mail IS NOT NULL AND o.E_Mail != ''
        ORDER BY o.UpdateDate DESC
    """)
    for row in cursor.fetchall():
        print(f"  {row['CardCode']}: {row['CardName']}")
        print(f"    Branch: {row['Branch']}, Email: {row['E_Mail']}")
    
    conn.close()
    print("\nDesconectado.")

if __name__ == "__main__":
    main()
