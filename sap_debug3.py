#!/usr/bin/env python3
"""SAP Debug v3 - Grupos y Tipos de Cliente"""
import pymssql

SAP_CONFIG = {
    'server': '213.97.178.117',
    'port': 1435,
    'database': 'DRASANVI',
    'user': 'web2',
    'password': 'eba-WWWhai'
}

def main():
    print("=== SAP DEBUG v3 - GRUPOS Y TIPOS ===\n")
    
    conn = pymssql.connect(
        server=f"{SAP_CONFIG['server']}:{SAP_CONFIG['port']}",
        user=SAP_CONFIG['user'],
        password=SAP_CONFIG['password'],
        database=SAP_CONFIG['database'],
        timeout=30
    )
    cursor = conn.cursor()
    
    # Valores de U_DRA_Tipo_Cliente
    print("=== U_DRA_Tipo_Cliente (VALORES) ===")
    cursor.execute("""
        SELECT U_DRA_Tipo_Cliente, COUNT(*) as cnt 
        FROM OCRD 
        GROUP BY U_DRA_Tipo_Cliente 
        ORDER BY cnt DESC
    """)
    for r in cursor.fetchall():
        print(f"  '{r[0]}': {r[1]}")
    
    # Cruce GroupCode con CardType
    print("\n=== GROUPCODE x CARDTYPE ===")
    cursor.execute("""
        SELECT GroupCode, CardType, COUNT(*) as cnt
        FROM OCRD
        GROUP BY GroupCode, CardType
        ORDER BY GroupCode, CardType
    """)
    for r in cursor.fetchall():
        tipo = {'C':'Cliente','S':'Proveedor','L':'Lead'}.get(r[1], r[1])
        print(f"  Grupo {r[0]} - {tipo}: {r[2]}")
    
    # Ejemplos por GroupCode (solo clientes con email)
    print("\n=== EJEMPLOS POR GROUPCODE (Clientes con email) ===")
    for gc in [100, 110, 112, 108, 103, 101, 115, 117]:
        cursor.execute(f"""
            SELECT TOP 3 CardCode, CardName, City, U_DRA_Tipo_Cliente, U_DRA_CATEGORIZACION
            FROM OCRD 
            WHERE GroupCode = {gc} AND CardType = 'C' AND E_Mail IS NOT NULL AND E_Mail != ''
        """)
        rows = cursor.fetchall()
        if rows:
            print(f"\n  --- Grupo {gc} ---")
            for r in rows:
                print(f"    [{r[0]}] {r[1]} | {r[2]} | Tipo:{r[3]} | Cat:{r[4]}")
    
    # Buscar campos que puedan indicar sector/canal
    print("\n=== CAMPOS QUE PUEDEN INDICAR SECTOR ===")
    for field in ['U_DRA_POT_VENTA', 'U_DRA_POT_RECOMENDACION', 'U_DRA_Cluster2024']:
        cursor.execute(f"""
            SELECT [{field}], COUNT(*) as cnt 
            FROM OCRD 
            WHERE [{field}] IS NOT NULL AND CAST([{field}] AS NVARCHAR(MAX)) != ''
            GROUP BY [{field}] 
            ORDER BY cnt DESC
        """)
        vals = cursor.fetchall()
        if vals:
            print(f"\n  {field}:")
            for v in vals[:15]:
                print(f"    '{v[0]}': {v[1]}")
    
    # Ver QryGroup (Query Groups) - pueden ser categorías
    print("\n=== QRYGROUPS ACTIVOS (Y/N flags) ===")
    for i in range(1, 11):
        cursor.execute(f"""
            SELECT COUNT(*) FROM OCRD WHERE QryGroup{i} = 'Y'
        """)
        cnt = cursor.fetchone()[0]
        if cnt > 0:
            print(f"  QryGroup{i}: {cnt} registros con 'Y'")
    
    # Tabla DRA_Dist_Farmacias - puede tener info de farmacias!
    print("\n=== TABLA DRA_Dist_Farmacias ===")
    cursor.execute("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME='DRA_Dist_Farmacias'")
    if cursor.fetchone()[0] > 0:
        cursor.execute("SELECT TOP 10 * FROM DRA_Dist_Farmacias")
        cols = [d[0] for d in cursor.description]
        print(f"  Columnas: {cols}")
        for r in cursor.fetchall():
            print(f"  {r}")
    
    # Tabla DRA_Dist_Grupos
    print("\n=== TABLA DRA_Dist_Grupos ===")
    cursor.execute("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME='DRA_Dist_Grupos'")
    if cursor.fetchone()[0] > 0:
        cursor.execute("SELECT * FROM DRA_Dist_Grupos")
        cols = [d[0] for d in cursor.description]
        print(f"  Columnas: {cols}")
        for r in cursor.fetchall():
            print(f"  {r}")
    
    # Conteo de clientes con email por categorización
    print("\n=== CLIENTES CON EMAIL POR CATEGORIZACION ===")
    cursor.execute("""
        SELECT U_DRA_CATEGORIZACION, COUNT(*) as total,
            SUM(CASE WHEN E_Mail IS NOT NULL AND E_Mail != '' THEN 1 ELSE 0 END) as con_email
        FROM OCRD
        WHERE CardType = 'C'
        GROUP BY U_DRA_CATEGORIZACION
        ORDER BY total DESC
    """)
    for r in cursor.fetchall():
        pct = (r[2]*100//r[1]) if r[1] > 0 else 0
        print(f"  {r[0]}: {r[2]} con email de {r[1]} ({pct}%)")
    
    conn.close()
    print("\n=== DEBUG v3 COMPLETADO ===")

if __name__ == '__main__':
    main()
