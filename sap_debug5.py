#!/usr/bin/env python3
"""
Debug script to analyze Branch/Industry field in SAP B1
"""

import pymssql

SAP_CONFIG = {
    'server': '213.97.178.117',
    'port': 1435,
    'database': 'DRASANVI',
    'user': 'web2',
    'password': 'eba-WWWhai'
}

def main():
    conn = pymssql.connect(**SAP_CONFIG)
    cursor = conn.cursor()
    
    print("=" * 80)
    print("VALORES DE BRANCH EN _WEB_Clientes:")
    print("=" * 80)
    cursor.execute("""
        SELECT Branch, COUNT(*) as total
        FROM _WEB_Clientes
        GROUP BY Branch
        ORDER BY total DESC
    """)
    for row in cursor.fetchall():
        print(f"  {row[0] or '(NULL)'}: {row[1]}")
    
    # Primero ver columnas de la vista
    print("\n" + "=" * 80)
    print("COLUMNAS DE _WEB_Clientes:")
    print("=" * 80)
    cursor.execute("""
        SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = '_WEB_Clientes'
        ORDER BY ORDINAL_POSITION
    """)
    for row in cursor.fetchall():
        print(f"  {row[0]}")
    
    print("\n" + "=" * 80)
    print("EMAILS POR BRANCH (con ratio corporativo):")
    print("=" * 80)
    cursor.execute("""
        SELECT 
            w.Branch,
            COUNT(DISTINCT w.Email) as total_emails,
            SUM(CASE WHEN w.Email NOT LIKE '%gmail%' 
                     AND w.Email NOT LIKE '%hotmail%' 
                     AND w.Email NOT LIKE '%outlook%'
                     AND w.Email NOT LIKE '%yahoo%'
                THEN 1 ELSE 0 END) as corporate_emails
        FROM _WEB_Clientes w
        WHERE w.Email IS NOT NULL AND w.Email != ''
        GROUP BY w.Branch
        ORDER BY total_emails DESC
    """)
    print(f"  {'BRANCH':<25} {'TOTAL':>8} {'CORP':>8} {'%CORP':>8}")
    print(f"  {'-'*25} {'-'*8} {'-'*8} {'-'*8}")
    for row in cursor.fetchall():
        branch = row[0] or '(NULL)'
        total = row[1]
        corporate = row[2]
        pct = (corporate / total * 100) if total > 0 else 0
        print(f"  {branch:<25} {total:>8} {corporate:>8} {pct:>7.1f}%")
    
    conn.close()

if __name__ == '__main__':
    main()
