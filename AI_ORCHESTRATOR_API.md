# API Externa - AI Orchestrator

API para que StaffKit pueda controlar los bots externos mediante IA.

## Autenticación

Todas las peticiones requieren header:
```
X-API-Key: <STAFFKIT_API_KEY>
```

## Endpoints

### 1. Ejecutar Búsqueda

**POST** `/api/execute-search`

Programa una búsqueda con el bot especificado.

#### Request

```json
{
  "bot_type": "direct|resentment|social",
  "list_id": 123,
  "max_leads": 10,
  "keywords": ["keyword1", "keyword2"],
  "priority": "high|normal|low",
  "callback_url": "https://staff.replanta.dev/api/bot-result"
}
```

**Campos:**
- `bot_type` **(requerido)**: Tipo de bot (`direct`, `resentment`, `social`)
- `list_id` **(requerido)**: ID de la lista destino en StaffKit
- `max_leads` (opcional): Máximo de leads a buscar (default: 10)
- `keywords` (opcional): Solo para Social Bot, keywords personalizadas
- `priority` (opcional): Prioridad del job (`high`, `normal`, `low`)
- `callback_url` (opcional): URL donde notificar cuando termine

#### Response

```json
{
  "success": true,
  "job_id": "uuid-xxx-xxx",
  "message": "Búsqueda de direct programada",
  "estimated_time": "5-10 minutos",
  "status": "pending"
}
```

#### Errores

- **401**: API Key inválida
- **400**: Parámetros incorrectos
- **500**: Error interno

---

### 2. Consultar Estado de Job

**GET** `/api/job-status/<job_id>`

Consulta el estado y resultado de un job.

#### Request

Headers:
```
X-API-Key: <STAFFKIT_API_KEY>
```

#### Response

```json
{
  "success": true,
  "job": {
    "id": "uuid-xxx",
    "bot_type": "direct",
    "status": "completed",
    "progress": 100,
    "created_at": "2026-01-19T14:30:00",
    "started_at": "2026-01-19T14:30:05",
    "completed_at": "2026-01-19T14:35:20",
    "result": {
      "leads_found": 15,
      "leads_saved": 12,
      "leads_duplicates": 2,
      "leads_filtered": 1,
      "duration": 315.2
    },
    "error": null
  }
}
```

**Estados posibles:**
- `pending`: En cola
- `running`: Ejecutándose
- `completed`: Completado exitosamente
- `failed`: Falló (ver campo `error`)
- `cancelled`: Cancelado

---

### 3. Callback (Webhook)

Cuando el job termina, se envía un POST al `callback_url` configurado:

```json
{
  "job_id": "uuid-xxx",
  "status": "completed",
  "result": {
    "leads_found": 15,
    "leads_saved": 12,
    "leads_duplicates": 2,
    "leads_filtered": 1,
    "duration": 315.2
  },
  "timestamp": "2026-01-19T14:35:20"
}
```

---

## Ejemplos de Uso

### Python

```python
import requests

API_KEY = "tu-staffkit-api-key"
BOT_URL = "https://b.territoriodrasanvicr.com:5000"

headers = {"X-API-Key": API_KEY}

# Ejecutar búsqueda
response = requests.post(
    f"{BOT_URL}/api/execute-search",
    headers=headers,
    json={
        "bot_type": "direct",
        "list_id": 123,
        "max_leads": 20,
        "priority": "high",
        "callback_url": "https://staff.replanta.dev/api/bot-result"
    }
)

job = response.json()
print(f"Job ID: {job['job_id']}")

# Consultar estado
response = requests.get(
    f"{BOT_URL}/api/job-status/{job['job_id']}",
    headers=headers
)

status = response.json()
print(f"Status: {status['job']['status']}")
print(f"Progress: {status['job']['progress']}%")
```

### JavaScript (Node.js)

```javascript
const axios = require('axios');

const API_KEY = 'tu-staffkit-api-key';
const BOT_URL = 'https://b.territoriodrasanvicr.com:5000';

const headers = { 'X-API-Key': API_KEY };

// Ejecutar búsqueda
const response = await axios.post(
  `${BOT_URL}/api/execute-search`,
  {
    bot_type: 'resentment',
    list_id: 456,
    max_leads: 30,
    callback_url: 'https://staff.replanta.dev/api/bot-result'
  },
  { headers }
);

console.log(`Job ID: ${response.data.job_id}`);

// Consultar estado
const status = await axios.get(
  `${BOT_URL}/api/job-status/${response.data.job_id}`,
  { headers }
);

console.log(`Status: ${status.data.job.status}`);
```

---

## Integración con StaffKit

### Arquitectura

```
┌─────────────────┐
│   StaffKit      │
│   (Frontend)    │
│                 │
│  ┌───────────┐  │
│  │  Chat IA  │  │  Usuario: "busca 20 leads direct en lista Premium"
│  └─────┬─────┘  │
│        │        │
│  ┌─────▼─────┐  │
│  │   OpenAI  │  │  Interpreta: bot_type=direct, max_leads=20
│  └─────┬─────┘  │
│        │        │
│  ┌─────▼───────────┐
│  │ Bot Controller  │  Envía POST /api/execute-search
│  └─────┬───────────┘
└────────┼────────────┘
         │
         ▼
┌────────────────────┐
│   BotScrap VPS     │
│                    │
│  ┌──────────────┐  │
│  │   Worker     │  │  Ejecuta bot
│  └──────┬───────┘  │
│         │          │
│  ┌──────▼───────┐  │
│  │  Direct Bot  │  │  Busca leads
│  └──────┬───────┘  │
│         │          │
│  ┌──────▼───────┐  │
│  │   StaffKit   │  │  Guarda en API
│  │     API      │  │
│  └──────────────┘  │
└────────────────────┘
```

### Implementación en StaffKit

#### 1. Tabla `external_bots`

```sql
CREATE TABLE external_bots (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    bot_type ENUM('direct', 'resentment', 'social') NOT NULL,
    api_url VARCHAR(255) NOT NULL,
    api_key VARCHAR(255) NOT NULL,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

#### 2. Controlador PHP

```php
// api/execute-external-bot.php
class ExternalBotController {
    
    public function executeSearch($botId, $params) {
        $bot = $this->getBot($botId);
        
        $payload = [
            'bot_type' => $bot['bot_type'],
            'list_id' => $params['list_id'],
            'max_leads' => $params['max_leads'] ?? 10,
            'callback_url' => 'https://staff.replanta.dev/api/bot-result'
        ];
        
        $response = $this->httpPost(
            $bot['api_url'] . '/api/execute-search',
            $payload,
            ['X-API-Key: ' . $bot['api_key']]
        );
        
        return $response;
    }
    
    public function handleCallback() {
        $data = json_decode(file_get_contents('php://input'), true);
        
        // Guardar resultado
        $this->saveJobResult($data['job_id'], $data['status'], $data['result']);
        
        // Notificar usuario
        $this->notifyUser($data);
        
        return ['success' => true];
    }
}
```

#### 3. Chat IA (OpenAI)

```php
// app/BotChatController.php
class BotChatController {
    
    public function handleMessage($userId, $message) {
        // Enviar a OpenAI
        $response = $this->openai->chat([
            'model' => 'gpt-4',
            'messages' => [
                [
                    'role' => 'system',
                    'content' => 'Eres un asistente que controla bots de búsqueda. 
                                 Puedes ejecutar: direct (Google), resentment (Trustpilot), 
                                 social (Reddit/Twitter). Extrae bot_type, max_leads y list_id.'
                ],
                [
                    'role' => 'user',
                    'content' => $message
                ]
            ],
            'functions' => [
                [
                    'name' => 'execute_search',
                    'description' => 'Ejecutar búsqueda con bot externo',
                    'parameters' => [
                        'type' => 'object',
                        'properties' => [
                            'bot_type' => ['type' => 'string', 'enum' => ['direct', 'resentment', 'social']],
                            'max_leads' => ['type' => 'integer'],
                            'list_id' => ['type' => 'integer']
                        ]
                    ]
                ]
            ]
        ]);
        
        // Si OpenAI llama a función
        if ($response['function_call']) {
            $params = json_decode($response['function_call']['arguments'], true);
            
            $botController = new ExternalBotController();
            $result = $botController->executeSearch(1, $params);
            
            return [
                'message' => "✅ Búsqueda iniciada. Job ID: {$result['job_id']}",
                'job_id' => $result['job_id']
            ];
        }
        
        return ['message' => $response['choices'][0]['message']['content']];
    }
}
```

---

## Notas de Seguridad

1. **API Key**: Mantener `STAFFKIT_API_KEY` secreto
2. **HTTPS**: Usar siempre HTTPS en producción
3. **Rate Limiting**: Considerar límite de requests/minuto
4. **Callback URL**: Validar que sea dominio confiable
5. **IP Whitelist**: Opcional, restringir por IP

---

## Testing

### Postman Collection

```json
{
  "info": {
    "name": "BotScrap External API",
    "schema": "https://schema.getpostman.com/json/collection/v2.1.0/collection.json"
  },
  "item": [
    {
      "name": "Execute Search",
      "request": {
        "method": "POST",
        "header": [
          {
            "key": "X-API-Key",
            "value": "{{api_key}}"
          }
        ],
        "body": {
          "mode": "raw",
          "raw": "{\n  \"bot_type\": \"direct\",\n  \"list_id\": 123,\n  \"max_leads\": 10\n}"
        },
        "url": {
          "raw": "{{base_url}}/api/execute-search",
          "host": ["{{base_url}}"],
          "path": ["api", "execute-search"]
        }
      }
    },
    {
      "name": "Job Status",
      "request": {
        "method": "GET",
        "header": [
          {
            "key": "X-API-Key",
            "value": "{{api_key}}"
          }
        ],
        "url": {
          "raw": "{{base_url}}/api/job-status/:job_id",
          "host": ["{{base_url}}"],
          "path": ["api", "job-status", ":job_id"],
          "variable": [
            {
              "key": "job_id",
              "value": "uuid-here"
            }
          ]
        }
      }
    }
  ],
  "variable": [
    {
      "key": "base_url",
      "value": "https://b.territoriodrasanvicr.com:5000"
    },
    {
      "key": "api_key",
      "value": "your-staffkit-api-key"
    }
  ]
}
```
