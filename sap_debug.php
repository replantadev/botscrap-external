<?php
header('Content-Type: text/plain; charset=utf-8');

$server = '213.97.178.117,1435';
$database = 'DRASANVI';
$user = 'web2';
$password = 'eba-WWWhai';

echo "=== SAP DEBUG v2 - ESTRUCTURA OCRD ===\n\n";

try {
    $conn = new PDO("dblib:host=$server;dbname=$database", $user, $password);
    $conn->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
    echo "✅ Conectado a $database\n\n";
} catch (Exception $e) {
    die("❌ Error: " . $e->getMessage());
}

// Estructura de OCRD (Business Partners)
echo "=== ESTRUCTURA OCRD (Business Partners) ===\n";
$cols = $conn->query("
    SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
    FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_NAME = 'OCRD'
    ORDER BY ORDINAL_POSITION
")->fetchAll();
foreach ($cols as $c) {
    $size = $c['CHARACTER_MAXIMUM_LENGTH'] ? "({$c['CHARACTER_MAXIMUM_LENGTH']})" : "";
    echo "  - {$c['COLUMN_NAME']} {$c['DATA_TYPE']}$size\n";
}

// Conteo total
$total = $conn->query("SELECT COUNT(*) FROM OCRD")->fetchColumn();
echo "\nTotal registros OCRD: $total\n";

// Conteo por CardType
echo "\n=== CONTEO POR CARDTYPE ===\n";
$stmt = $conn->query("SELECT CardType, COUNT(*) as cnt FROM OCRD GROUP BY CardType");
foreach ($stmt->fetchAll() as $r) {
    $tipo = ['C'=>'Cliente','S'=>'Proveedor','L'=>'Lead'][$r['CardType']] ?? $r['CardType'];
    echo "  $tipo ({$r['CardType']}): {$r['cnt']}\n";
}

// Campos U_ (UDF - User Defined Fields)
echo "\n=== CAMPOS PERSONALIZADOS (U_*) EN OCRD ===\n";
$udfs = $conn->query("
    SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_NAME = 'OCRD' AND COLUMN_NAME LIKE 'U_%'
")->fetchAll(PDO::FETCH_COLUMN);
foreach ($udfs as $u) {
    echo "  - $u\n";
}

// Ver valores únicos de campos UDF importantes
if (in_array('U_DRA_CATEGORIZACION', $udfs)) {
    echo "\n=== VALORES U_DRA_CATEGORIZACION ===\n";
    $stmt = $conn->query("SELECT U_DRA_CATEGORIZACION, COUNT(*) as cnt FROM OCRD WHERE U_DRA_CATEGORIZACION IS NOT NULL GROUP BY U_DRA_CATEGORIZACION ORDER BY cnt DESC");
    foreach ($stmt->fetchAll() as $r) {
        echo "  {$r['U_DRA_CATEGORIZACION']}: {$r['cnt']}\n";
    }
}

// Buscar campo de grupo
echo "\n=== CAMPO GROUPCODE (si existe) ===\n";
$hasGroup = $conn->query("SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='OCRD' AND COLUMN_NAME='GroupCode'")->fetchColumn();
if ($hasGroup) {
    $stmt = $conn->query("SELECT GroupCode, COUNT(*) as cnt FROM OCRD GROUP BY GroupCode ORDER BY cnt DESC");
    foreach ($stmt->fetchAll() as $r) {
        echo "  Grupo {$r['GroupCode']}: {$r['cnt']}\n";
    }
} else {
    echo "  No existe GroupCode en OCRD\n";
}

// Emails disponibles
echo "\n=== EMAILS DISPONIBLES ===\n";
$stmt = $conn->query("
    SELECT CardType,
        SUM(CASE WHEN E_Mail IS NOT NULL AND E_Mail != '' THEN 1 ELSE 0 END) as ConEmail,
        COUNT(*) as Total
    FROM OCRD GROUP BY CardType
");
foreach ($stmt->fetchAll() as $r) {
    $tipo = ['C'=>'Cliente','S'=>'Proveedor','L'=>'Lead'][$r['CardType']] ?? $r['CardType'];
    echo "  $tipo: {$r['ConEmail']} con email de {$r['Total']} total\n";
}

// Ejemplo de 10 clientes con email
echo "\n=== EJEMPLO 10 CLIENTES CON EMAIL ===\n";
$stmt = $conn->query("
    SELECT TOP 10 CardCode, CardName, CardType, E_Mail, Phone1, Phone2, Cellular, City, Country
    FROM OCRD 
    WHERE E_Mail IS NOT NULL AND E_Mail != ''
    ORDER BY CardCode
");
foreach ($stmt->fetchAll() as $r) {
    echo "\n  [{$r['CardCode']}] {$r['CardName']}\n";
    echo "    Tipo: {$r['CardType']} | Email: {$r['E_Mail']}\n";
    echo "    Tel: {$r['Phone1']} / {$r['Phone2']} / {$r['Cellular']}\n";
    echo "    Ciudad: {$r['City']} | País: {$r['Country']}\n";
}

// Ver si hay tabla de grupos OCRG
echo "\n=== TABLA OCRG (Grupos) ===\n";
$hasOCRG = $conn->query("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME='OCRG'")->fetchColumn();
if ($hasOCRG) {
    $stmt = $conn->query("SELECT * FROM OCRG ORDER BY GroupCode");
    foreach ($stmt->fetchAll() as $r) {
        print_r($r);
    }
} else {
    echo "  No existe tabla OCRG\n";
}

// Ver estructura CRD1 (direcciones)
echo "\n=== ESTRUCTURA CRD1 (Direcciones) ===\n";
$cols = $conn->query("
    SELECT COLUMN_NAME, DATA_TYPE
    FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_NAME = 'CRD1'
    ORDER BY ORDINAL_POSITION
")->fetchAll();
foreach ($cols as $c) {
    echo "  - {$c['COLUMN_NAME']} ({$c['DATA_TYPE']})\n";
}

echo "\n\n=== DEBUG v2 COMPLETADO ===\n";

