#!/bin/bash
# ============================================================================
# run_sap_bots.sh - Ejecuta todos los bots SAP configurados
# ============================================================================
# Este script se ejecuta via cron, típicamente 1-2 veces al día
# 
# Cron recomendado:
#   0 6,18 * * * /var/www/vhosts/territoriodrasanvicr.com/b/run_sap_bots.sh >> /var/log/sap_bots.log 2>&1
#
# El script:
#   1. Consulta StaffKit para obtener bots SAP habilitados
#   2. Ejecuta cada uno secuencialmente
#   3. Registra resultados en StaffKit
# ============================================================================

cd /var/www/vhosts/territoriodrasanvicr.com/b
source venv/bin/activate

# Configuración
STAFFKIT_URL="https://staff.replanta.dev"
API_KEY="sk_d40a7992c1fa4a3774134d31658ff10cc93ce505d95a3ce7d37e8459759578e9"

echo "========================================"
echo "SAP Bots Runner - $(date '+%Y-%m-%d %H:%M:%S')"
echo "========================================"

# Obtener lista de bots SAP habilitados
BOTS=$(curl -s -H "Authorization: Bearer $API_KEY" \
    "$STAFFKIT_URL/api/v2/external-bots?type=sap,sap_sl&enabled=1" | \
    python3 -c "import sys,json; bots=json.load(sys.stdin).get('bots',[]); print(' '.join([str(b['id'])+':'+b['bot_type'] for b in bots]))")

if [ -z "$BOTS" ]; then
    echo "No hay bots SAP habilitados"
    exit 0
fi

echo "Bots a ejecutar: $BOTS"

for BOT_INFO in $BOTS; do
    BOT_ID=$(echo $BOT_INFO | cut -d: -f1)
    BOT_TYPE=$(echo $BOT_INFO | cut -d: -f2)
    
    echo ""
    echo "--- Ejecutando Bot #$BOT_ID ($BOT_TYPE) ---"
    
    if [ "$BOT_TYPE" = "sap" ]; then
        # SAP ODBC (conexión SQL directa)
        python sap_sync.py --bot-id $BOT_ID --api-key "$API_KEY"
    elif [ "$BOT_TYPE" = "sap_sl" ]; then
        # SAP Service Layer (REST API)
        python sap_service_layer.py --bot-id $BOT_ID --api-key "$API_KEY"
    fi
    
    EXIT_CODE=$?
    
    if [ $EXIT_CODE -eq 0 ]; then
        echo "✅ Bot #$BOT_ID completado exitosamente"
    else
        echo "❌ Bot #$BOT_ID falló con código $EXIT_CODE"
    fi
    
    # Pequeña pausa entre bots
    sleep 5
done

echo ""
echo "========================================"
echo "SAP Bots completados - $(date '+%Y-%m-%d %H:%M:%S')"
echo "========================================"
