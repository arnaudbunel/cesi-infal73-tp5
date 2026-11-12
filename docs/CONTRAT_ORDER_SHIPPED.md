# Contrat `OrderShipped` — `schemaVersion = 1`

Publié vers l'exchange `business.events` avec la routing key `order.shipped`, JSON UTF-8, message **persistant**.

```json
{
  "eventId": "evt-10042",
  "eventType": "OrderShipped",
  "schemaVersion": 1,
  "occurredAt": "2026-01-01T09:00:00.000Z",
  "source": "orders-service",
  "aggregateId": "ORD-102",
  "aggregateVersion": 5,
  "data": { "orderId": "ORD-102", "status": "SHIPPED" }
}
```

| Champ | Règle (validée à l'exécution par `validateOrderShippedValue`) |
|---|---|
| `eventId` | Identifie le **fait métier**, pas la livraison. **Inchangé** quand le même événement est republié. Lettres, chiffres, `. _ : -`, 128 max |
| `eventType` | exactement `OrderShipped` |
| `schemaVersion` | exactement `1` (entier) |
| `occurredAt` | ISO 8601 **UTC** (suffixe `Z`) |
| `source` | exactement `orders-service` |
| `aggregateId` | identifiant de commande (`ORD-…`) |
| `aggregateVersion` | entier ≥ 1 : **version métier** de la commande (dans ce dépôt : le champ `sequence` de la commande après la transition). Ce n'est **pas** un compteur de livraisons RabbitMQ. Le `5` de l'exemple est illustratif : `ORD-102` fournie part de la version 0 et vaut 1 après son expédition |
| `data.orderId` | identique à `aggregateId` |
| `data.status` | exactement `SHIPPED` |

Aucun champ supplémentaire n'est accepté (`UNKNOWN_FIELD`).

## Diagnostics (`@infal73/contracts`)

`validateOrderShippedMessage(buffer)` / `validateOrderShippedValue(value)` renvoient `{ ok: true, event }` ou `{ ok: false, violation: { code, field?, reason } }`, `describeViolation(violation)` donnant une ligne de log uniforme.

| `code` | Signification | Issue attendue d'un consommateur |
|---|---|---|
| `INVALID_JSON`, `NOT_AN_OBJECT` | corps illisible | erreur **définitive** |
| `UNSUPPORTED_SCHEMA_VERSION` | version inconnue (ex. 99) | erreur **définitive** : la rejouer ne changera rien |
| `WRONG_EVENT_TYPE`, `UNKNOWN_FIELD`, `INVALID_FIELD`, `INCONSISTENT` | autre violation du contrat | erreur **définitive** |

À vous de décider (et de justifier) ce que « définitive » implique pour la livraison : voir `DeliveryContext` dans `apps/event-workers/src/runtime/delivery.ts`.

## Ce que le contrat ne dit pas (à décider et justifier)
- Comment obtenir un `eventId` stable et unique pour un fait donné ;
- ce que doit faire le producteur si la publication échoue alors que la transition est déjà enregistrée ;
- ce que doit faire un consommateur qui reçoit deux fois le même `eventId`.
