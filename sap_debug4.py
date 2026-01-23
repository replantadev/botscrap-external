#!/usr/bin/env python3
"""SAP Debug v4 - Identificar tipos de negocio por GroupCode"""
import pymssql

SAP_CONFIG = {
    'server': '213.97.178.117',
    'port': 1435,
    'database': 'DRASANVI',
    'user': 'web2',
    'password': 'eba-WWWhai'
}

def main():
    print("=== SAP DEBUG v4 - TIPOS DE NEGOCIO ===\n")
    
    conn = pymssql.connect(
        server=f"{SAP_CONFIG['server']}:{SAP_CONFIG['port']}",
        user=SAP_CONFIG['user'],
        password=SAP_CONFIG['password'],
        database=SAP_CONFIG['database'],
        timeout=30
    )
    cursor = conn.cursor()
    
    # Buscar tabla de grupos de clientes
    print("=== TABLAS QUE CONTENGAN 'GROUP' ===")
    cursor.execute("""
        SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_NAME LIKE '%GROUP%' OR TABLE_NAME LIKE '%OCRG%' OR TABLE_NAME LIKE '%GRP%'
    """)
    for r in cursor.fetchall():
        print(f"  {r[0]}")
    
    # Ver ejemplos por GroupCode para identificar el tipo
    print("\n=== EJEMPLOS POR GROUPCODE (Clientes) ===")
    groups = [100, 102, 103, 104, 105, 106, 107, 110, 113, 114, 115, 116, 117, 119, 120, 122, 123, 124]
    
    for gc in groups:
        cursor.execute(f"""
            SELECT TOP 5 CardCode, CardName, City 
            FROM OCRD 
            WHERE GroupCode = {gc} AND CardType = 'C'
            ORDER BY UpdateDate DESC
        """)
        rows = cursor.fetchall()
        if rows:
            # Contar total y con email
            cursor.execute(f"""
                SELECT COUNT(*) as total,
                    SUM(CASE WHEN E_Mail IS NOT NULL AND E_Mail != '' AND E_Mail LIKE '%@%' THEN 1 ELSE 0 END) as con_email
                FROM OCRD WHERE GroupCode = {gc} AND CardType = 'C'
            """)
            stats = cursor.fetchone()
            
            print(f"\n--- GRUPO {gc} ({stats[0]} clientes, {stats[1]} con email) ---")
            for r in rows:
                print(f"  [{r[0]}] {r[1]} | {r[2]}")
    
    # Buscar patrones en CardName para identificar tipo
    print("\n\n=== PATRONES EN CARDNAME ===")
    patterns = [
        ("FARMACIA", "CardName LIKE '%FARMACIA%'"),
        ("HERBOLARIO", "CardName LIKE '%HERBOLARIO%' OR CardName LIKE '%HERBORISTERIA%'"),
        ("PARAFARMACIA", "CardName LIKE '%PARAFARMACIA%'"),
        ("DIETÉTICA", "CardName LIKE '%DIETETICA%' OR CardName LIKE '%DIETÉTICA%'"),
        ("ECOLÓGICO/BIO", "CardName LIKE '%ECOLOGIC%' OR CardName LIKE '%BIO %' OR CardName LIKE '% BIO'"),
        ("HOSPITAL/CLÍNICA", "CardName LIKE '%HOSPITAL%' OR CardName LIKE '%CLINICA%' OR CardName LIKE '%CLÍNICA%'"),
    ]
    
    for name, where in patterns:
        cursor.execute(f"""
            SELECT GroupCode, COUNT(*) as cnt
            FROM OCRD
            WHERE CardType = 'C' AND ({where})
            GROUP BY GroupCode
            ORDER BY cnt DESC
        """)
        rows = cursor.fetchall()
        if rows:
            print(f"\n  {name}:")
            for r in rows:
                print(f"    Grupo {r[0]}: {r[1]}")
    
    # Analizar dominios de email para ver corporativos vs genericos
    print("\n\n=== ANÁLISIS DE EMAILS ===")
    cursor.execute("""
        SELECT 
            CASE 
                WHEN E_Mail LIKE '%@gmail%' THEN 'gmail'
                WHEN E_Mail LIKE '%@hotmail%' OR E_Mail LIKE '%@outlook%' OR E_Mail LIKE '%@live%' THEN 'hotmail/outlook'
                WHEN E_Mail LIKE '%@yahoo%' THEN 'yahoo'
                WHEN E_Mail LIKE '%@icloud%' OR E_Mail LIKE '%@me.com%' THEN 'icloud'
                WHEN E_Mail LIKE '%@telefonica%' OR E_Mail LIKE '%@movistar%' THEN 'telefonica'
                ELSE 'corporativo'
            END as tipo_email,
            COUNT(*) as cnt
        FROM OCRD
        WHERE CardType = 'C' AND E_Mail IS NOT NULL AND E_Mail LIKE '%@%'
        GROUP BY 
            CASE 
                WHEN E_Mail LIKE '%@gmail%' THEN 'gmail'
                WHEN E_Mail LIKE '%@hotmail%' OR E_Mail LIKE '%@outlook%' OR E_Mail LIKE '%@live%' THEN 'hotmail/outlook'
                WHEN E_Mail LIKE '%@yahoo%' THEN 'yahoo'
                WHEN E_Mail LIKE '%@icloud%' OR E_Mail LIKE '%@me.com%' THEN 'icloud'
                WHEN E_Mail LIKE '%@telefonica%' OR E_Mail LIKE '%@movistar%' THEN 'telefonica'
                ELSE 'corporativo'
            END
        ORDER BY cnt DESC
    """)
    for r in cursor.fetchall():
        print(f"  {r[0]}: {r[1]}")
    
    # Desglose corporativo por grupo
    print("\n=== EMAILS CORPORATIVOS POR GRUPO ===")
    cursor.execute("""
        SELECT GroupCode, COUNT(*) as total,
            SUM(CASE WHEN E_Mail NOT LIKE '%@gmail%' 
                      AND E_Mail NOT LIKE '%@hotmail%' 
                      AND E_Mail NOT LIKE '%@outlook%'
                      AND E_Mail NOT LIKE '%@live%'
                      AND E_Mail NOT LIKE '%@yahoo%'
                      AND E_Mail NOT LIKE '%@icloud%'
                      AND E_Mail NOT LIKE '%@me.com%'
                      AND E_Mail NOT LIKE '%@telefonica%'
                      AND E_Mail NOT LIKE '%@movistar%'
                THEN 1 ELSE 0 END) as corporativos
        FROM OCRD
        WHERE CardType = 'C' AND E_Mail IS NOT NULL AND E_Mail LIKE '%@%'
        GROUP BY GroupCode
        ORDER BY total DESC
    """)
    for r in cursor.fetchall():
        pct = (r[2]*100//r[1]) if r[1] > 0 else 0
        print(f"  Grupo {r[0]}: {r[2]} corporativos de {r[1]} ({pct}%)")
    
    conn.close()
    print("\n=== DEBUG v4 COMPLETADO ===")

if __name__ == '__main__':
    main()
