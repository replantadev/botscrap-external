-- ============================================================================
-- Consultas SQL para inspeccionar Leads en SAP Business One
-- Base de datos: DRASANVI
-- ============================================================================

-- 1️⃣ CONTAR REGISTROS POR CARDTYPE
-- Muestra cuántos registros hay de cada tipo
SELECT 
    CardType,
    CASE 
        WHEN CardType = 'C' THEN 'Cliente'
        WHEN CardType = 'S' THEN 'Proveedor'
        WHEN CardType = 'L' THEN 'Lead/Prospecto'
        ELSE 'Desconocido'
    END AS TipoDescripcion,
    COUNT(*) AS Total
FROM OCRD
GROUP BY CardType
ORDER BY CardType;

-- 2️⃣ TOTAL DE LEADS (CardType = 'L')
SELECT COUNT(*) AS TotalLeads
FROM OCRD
WHERE CardType = 'L';

-- 3️⃣ LEADS CON DISTRIBUCIÓN POR BRANCH
-- Ver cuántos leads hay por tipo de negocio
SELECT 
    o.GroupCode,
    g.GroupName,
    COUNT(*) AS CantidadLeads
FROM OCRD o
LEFT JOIN OCRG g ON o.GroupCode = g.GroupCode
WHERE o.CardType = 'L'
GROUP BY o.GroupCode, g.GroupName
ORDER BY COUNT(*) DESC;

-- 4️⃣ LEADS CON EMAIL CORPORATIVO VS SIN EMAIL
SELECT 
    CASE 
        WHEN E_Mail IS NOT NULL AND E_Mail <> '' THEN 'Con email'
        ELSE 'Sin email'
    END AS TieneEmail,
    COUNT(*) AS Cantidad
FROM OCRD
WHERE CardType = 'L'
GROUP BY CASE 
    WHEN E_Mail IS NOT NULL AND E_Mail <> '' THEN 'Con email'
    ELSE 'Sin email'
END;

-- 5️⃣ MUESTRA DE 20 LEADS
-- Ver datos principales de los primeros 20 leads
SELECT TOP 20
    CardCode,
    CardName,
    E_Mail,
    Phone1,
    Website,
    GroupCode,
    CreateDate,
    City,
    County AS Provincia
FROM OCRD
WHERE CardType = 'L'
ORDER BY CreateDate DESC;

-- 6️⃣ LEADS CON EMAIL CORPORATIVO (NO GENÉRICOS)
-- Los que pasarían el filtro corporativo del bot
SELECT COUNT(*) AS LeadsConEmailCorporativo
FROM OCRD
WHERE CardType = 'L'
    AND E_Mail IS NOT NULL 
    AND E_Mail <> ''
    AND E_Mail NOT LIKE '%@gmail.%'
    AND E_Mail NOT LIKE '%@hotmail.%'
    AND E_Mail NOT LIKE '%@outlook.%'
    AND E_Mail NOT LIKE '%@yahoo.%'
    AND E_Mail NOT LIKE '%@live.%'
    AND E_Mail NOT LIKE '%@icloud.%';

-- 7️⃣ LEADS POR ESTADO (ACTIVO/INACTIVO)
SELECT 
    CASE 
        WHEN frozenFor = 'Y' THEN 'Inactivo/Bloqueado'
        WHEN validFor = 'N' THEN 'No válido'
        ELSE 'Activo'
    END AS Estado,
    COUNT(*) AS Cantidad
FROM OCRD
WHERE CardType = 'L'
GROUP BY CASE 
    WHEN frozenFor = 'Y' THEN 'Inactivo/Bloqueado'
    WHEN validFor = 'N' THEN 'No válido'
    ELSE 'Activo'
END;

-- 8️⃣ LEADS CON TELÉFONO Y/O EMAIL
SELECT 
    'Con teléfono' AS Tipo,
    COUNT(*) AS Cantidad
FROM OCRD
WHERE CardType = 'L' AND (Phone1 IS NOT NULL AND Phone1 <> '')
UNION ALL
SELECT 
    'Con email' AS Tipo,
    COUNT(*) AS Cantidad
FROM OCRD
WHERE CardType = 'L' AND (E_Mail IS NOT NULL AND E_Mail <> '')
UNION ALL
SELECT 
    'Con teléfono Y email' AS Tipo,
    COUNT(*) AS Cantidad
FROM OCRD
WHERE CardType = 'L' 
    AND (Phone1 IS NOT NULL AND Phone1 <> '')
    AND (E_Mail IS NOT NULL AND E_Mail <> '');

-- 9️⃣ LEADS CREADOS POR AÑO
SELECT 
    YEAR(CreateDate) AS Año,
    COUNT(*) AS CantidadLeads
FROM OCRD
WHERE CardType = 'L'
GROUP BY YEAR(CreateDate)
ORDER BY Año DESC;

-- 🔟 BRANCHES MÁS COMUNES EN LEADS
-- Top 10 tipos de negocio con más leads
SELECT TOP 10
    g.GroupName,
    COUNT(*) AS NumLeads,
    SUM(CASE WHEN o.E_Mail IS NOT NULL AND o.E_Mail <> '' THEN 1 ELSE 0 END) AS ConEmail,
    SUM(CASE WHEN o.Phone1 IS NOT NULL AND o.Phone1 <> '' THEN 1 ELSE 0 END) AS ConTelefono
FROM OCRD o
LEFT JOIN OCRG g ON o.GroupCode = g.GroupCode
WHERE o.CardType = 'L'
GROUP BY g.GroupName
ORDER BY COUNT(*) DESC;
