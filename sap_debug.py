#!/usr/bin/env python3
"""
SAP Business One - Debug/Exploración de datos
==============================================
Conecta directamente a SQL Server para explorar estructura de datos SAP B1.

Tablas principales SAP B1:
- OCRD: Business Partners (clientes, proveedores, leads)
- OCPR: Contact Persons (personas de contacto)
- CRD1: Direcciones
- OCRG: Grupos de socios
- OSLP: Vendedores
"""

import sys
try:
    import pymssql
except ImportError:
    print("Instalando pymssql...")
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "pymssql"])
    import pymssql

# Configuración de conexión
SAP_CONFIG = {
    'server': '213.97.178.117',
    'port': 1435,
    'database': 'DRASANVI',
    'user': 'web2',
    'password': 'eba-WWWhai'
}

def connect():
    """Conectar a SQL Server"""
    print(f"🔌 Conectando a {SAP_CONFIG['server']}:{SAP_CONFIG['port']}...")
    print(f"   Base de datos: {SAP_CONFIG['database']}")
    
    conn = pymssql.connect(
        server=f"{SAP_CONFIG['server']}:{SAP_CONFIG['port']}",
        user=SAP_CONFIG['user'],
        password=SAP_CONFIG['password'],
        database=SAP_CONFIG['database'],
        timeout=30
    )
    print("✅ Conectado!")
    return conn

def explore_tables(conn):
    """Listar tablas disponibles"""
    cursor = conn.cursor()
    
    print("\n" + "="*60)
    print("📋 TABLAS SAP B1 PRINCIPALES")
    print("="*60)
    
    # Buscar tablas de SAP
    cursor.execute("""
        SELECT TABLE_NAME 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_TYPE = 'BASE TABLE'
        AND (TABLE_NAME LIKE 'OCRD%' OR TABLE_NAME LIKE 'OCPR%' 
             OR TABLE_NAME LIKE 'CRD%' OR TABLE_NAME LIKE 'OCRG%'
             OR TABLE_NAME LIKE 'OSLP%')
        ORDER BY TABLE_NAME
    """)
    
    tables = cursor.fetchall()
    for t in tables:
        print(f"  - {t[0]}")
    
    return [t[0] for t in tables]

def describe_table(conn, table_name):
    """Mostrar estructura de una tabla"""
    cursor = conn.cursor()
    
    print(f"\n📊 Estructura de {table_name}:")
    print("-" * 50)
    
    cursor.execute(f"""
        SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = '{table_name}'
        ORDER BY ORDINAL_POSITION
    """)
    
    for col in cursor.fetchall():
        col_name, data_type, max_len, nullable = col
        size = f"({max_len})" if max_len else ""
        print(f"  {col_name}: {data_type}{size} {'NULL' if nullable == 'YES' else 'NOT NULL'}")

def explore_business_partners(conn):
    """Explorar estructura y datos de Business Partners (OCRD)"""
    cursor = conn.cursor()
    
    print("\n" + "="*60)
    print("👥 BUSINESS PARTNERS (OCRD)")
    print("="*60)
    
    # Contar por tipo
    print("\n📊 Conteo por CardType:")
    cursor.execute("""
        SELECT CardType, COUNT(*) as Total
        FROM OCRD
        GROUP BY CardType
    """)
    for row in cursor.fetchall():
        card_type = row[0]
        tipo = {'C': 'Cliente', 'S': 'Proveedor', 'L': 'Lead'}.get(card_type, card_type)
        print(f"  {tipo} ({card_type}): {row[1]}")
    
    # Contar por grupo
    print("\n📊 Conteo por GroupCode (top 20):")
    cursor.execute("""
        SELECT TOP 20 g.GroupName, o.GroupCode, COUNT(*) as Total
        FROM OCRD o
        LEFT JOIN OCRG g ON o.GroupCode = g.GroupCode
        GROUP BY o.GroupCode, g.GroupName
        ORDER BY Total DESC
    """)
    for row in cursor.fetchall():
        print(f"  [{row[1]}] {row[0] or 'Sin grupo'}: {row[2]}")
    
    # Contar con email
    print("\n📊 Partners con email:")
    cursor.execute("""
        SELECT 
            COUNT(*) as Total,
            SUM(CASE WHEN E_Mail IS NOT NULL AND E_Mail != '' THEN 1 ELSE 0 END) as ConEmail
        FROM OCRD
    """)
    row = cursor.fetchone()
    print(f"  Total: {row[0]}, Con email: {row[1]} ({100*row[1]//row[0]}%)")
    
    # Ejemplo de datos
    print("\n📋 Ejemplo de 5 Business Partners (Clientes con email):")
    print("-" * 80)
    cursor.execute("""
        SELECT TOP 5 
            CardCode, CardName, CardType, GroupCode, 
            E_Mail, Phone1, Phone2, Cellular,
            City, Country, frozenFor
        FROM OCRD
        WHERE CardType = 'C' AND E_Mail IS NOT NULL AND E_Mail != ''
    """)
    
    for row in cursor.fetchall():
        print(f"\n  CardCode: {row[0]}")
        print(f"  CardName: {row[1]}")
        print(f"  CardType: {row[2]} | GroupCode: {row[3]}")
        print(f"  Email: {row[4]}")
        print(f"  Phones: {row[5]} / {row[6]} / {row[7]}")
        print(f"  Location: {row[8]}, {row[9]}")
        print(f"  Frozen: {row[10]}")

def explore_contact_persons(conn):
    """Explorar Contact Persons (OCPR)"""
    cursor = conn.cursor()
    
    print("\n" + "="*60)
    print("👤 CONTACT PERSONS (OCPR)")
    print("="*60)
    
    # Contar contactos
    print("\n📊 Estadísticas de contactos:")
    cursor.execute("""
        SELECT 
            COUNT(*) as Total,
            SUM(CASE WHEN E_MailL IS NOT NULL AND E_MailL != '' THEN 1 ELSE 0 END) as ConEmail,
            COUNT(DISTINCT CardCode) as Partners
        FROM OCPR
    """)
    row = cursor.fetchone()
    print(f"  Total contactos: {row[0]}")
    print(f"  Con email: {row[1]}")
    print(f"  Partners con contactos: {row[2]}")
    
    # Partners con múltiples contactos
    print("\n📊 Partners con múltiples contactos (top 10):")
    cursor.execute("""
        SELECT TOP 10 p.CardCode, p.CardName, COUNT(*) as NumContactos
        FROM OCPR c
        JOIN OCRD p ON c.CardCode = p.CardCode
        GROUP BY p.CardCode, p.CardName
        HAVING COUNT(*) > 1
        ORDER BY NumContactos DESC
    """)
    for row in cursor.fetchall():
        print(f"  {row[0]} - {row[1]}: {row[2]} contactos")
    
    # Ejemplo de contactos
    print("\n📋 Ejemplo de contactos con email:")
    print("-" * 80)
    cursor.execute("""
        SELECT TOP 10
            c.CardCode, c.Name, c.FirstName, c.LastName,
            c.E_MailL, c.Tel1, c.Tel2, c.Cellolar,
            c.Position, c.Active,
            p.CardName
        FROM OCPR c
        JOIN OCRD p ON c.CardCode = p.CardCode
        WHERE c.E_MailL IS NOT NULL AND c.E_MailL != ''
    """)
    
    for row in cursor.fetchall():
        print(f"\n  Partner: {row[0]} - {row[10]}")
        print(f"  Contacto: {row[1]} ({row[2]} {row[3]})")
        print(f"  Email: {row[4]}")
        print(f"  Teléfonos: {row[5]} / {row[6]} / {row[7]}")
        print(f"  Cargo: {row[8]} | Activo: {row[9]}")

def explore_groups(conn):
    """Explorar grupos de Business Partners"""
    cursor = conn.cursor()
    
    print("\n" + "="*60)
    print("📁 GRUPOS DE BUSINESS PARTNERS (OCRG)")
    print("="*60)
    
    cursor.execute("""
        SELECT GroupCode, GroupName, GroupType
        FROM OCRG
        ORDER BY GroupCode
    """)
    
    print("\n  Code | Tipo | Nombre")
    print("  " + "-" * 50)
    for row in cursor.fetchall():
        tipo = {'C': 'Cliente', 'S': 'Proveedor', 'L': 'Lead'}.get(row[2], row[2])
        print(f"  {row[0]:4} | {tipo:10} | {row[1]}")

def explore_udf_fields(conn):
    """Explorar campos definidos por usuario (UDF)"""
    cursor = conn.cursor()
    
    print("\n" + "="*60)
    print("🔧 CAMPOS PERSONALIZADOS (UDF) EN OCRD")
    print("="*60)
    
    cursor.execute("""
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = 'OCRD' AND COLUMN_NAME LIKE 'U_%'
        ORDER BY COLUMN_NAME
    """)
    
    udfs = cursor.fetchall()
    if udfs:
        print("\n  Campos UDF encontrados:")
        for udf in udfs:
            print(f"    - {udf[0]}")
    else:
        print("\n  No se encontraron campos UDF (U_*)")

def search_emails_query(conn):
    """Query completa para extraer todos los emails"""
    cursor = conn.cursor()
    
    print("\n" + "="*60)
    print("📧 QUERY COMPLETA - EXTRACCIÓN DE EMAILS")
    print("="*60)
    
    # Query que une partners + contactos
    query = """
        -- Emails de Business Partners directos
        SELECT 
            'partner' as source,
            p.CardCode,
            p.CardName as company,
            NULL as contact_name,
            p.E_Mail as email,
            p.Phone1 as phone,
            p.City,
            p.Country,
            p.CardType,
            p.GroupCode,
            g.GroupName
        FROM OCRD p
        LEFT JOIN OCRG g ON p.GroupCode = g.GroupCode
        WHERE p.E_Mail IS NOT NULL AND p.E_Mail != ''
          AND p.frozenFor = 'N'
        
        UNION ALL
        
        -- Emails de Contact Persons
        SELECT 
            'contact' as source,
            c.CardCode,
            p.CardName as company,
            CONCAT(c.FirstName, ' ', c.LastName) as contact_name,
            c.E_MailL as email,
            COALESCE(c.Tel1, c.Cellolar) as phone,
            p.City,
            p.Country,
            p.CardType,
            p.GroupCode,
            g.GroupName
        FROM OCPR c
        JOIN OCRD p ON c.CardCode = p.CardCode
        LEFT JOIN OCRG g ON p.GroupCode = g.GroupCode
        WHERE c.E_MailL IS NOT NULL AND c.E_MailL != ''
          AND c.Active = 'Y'
          AND p.frozenFor = 'N'
    """
    
    print("\n📋 Conteo por tipo y fuente:")
    cursor.execute(f"""
        SELECT CardType, source, COUNT(*) as total
        FROM ({query}) q
        GROUP BY CardType, source
        ORDER BY CardType, source
    """)
    
    for row in cursor.fetchall():
        tipo = {'C': 'Cliente', 'S': 'Proveedor', 'L': 'Lead'}.get(row[0], row[0])
        print(f"  {tipo} ({row[1]}): {row[2]} emails")
    
    print("\n📋 Ejemplo de resultados (10 primeros):")
    cursor.execute(f"SELECT TOP 10 * FROM ({query}) q ORDER BY CardCode")
    
    for row in cursor.fetchall():
        print(f"\n  [{row[0]}] {row[1]} - {row[2]}")
        if row[3]:
            print(f"    Contacto: {row[3]}")
        print(f"    Email: {row[4]}")
        print(f"    Phone: {row[5]}")
        print(f"    Tipo: {row[8]} | Grupo: [{row[9]}] {row[10]}")

def main():
    print("="*60)
    print("🏢 SAP BUSINESS ONE - DEBUG DE DATOS")
    print("="*60)
    
    try:
        conn = connect()
        
        # Explorar estructura
        explore_tables(conn)
        explore_groups(conn)
        explore_business_partners(conn)
        explore_contact_persons(conn)
        explore_udf_fields(conn)
        search_emails_query(conn)
        
        conn.close()
        print("\n✅ Debug completado!")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == '__main__':
    main()
