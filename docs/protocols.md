# Protocoles du TP3

## 1. gRPC interne Commandes → Stocks
Contrat : `packages/contracts/proto/stock.proto` (package `inventory.v1`, numéros de champs stables).
Chargement dynamique avec `@grpc/proto-loader` (`keepCase: false` : `product_id` → `productId`).
Les deux applications ont chacune leur petit chargeur (`src/grpc/stock-proto.ts`).

| Cas | Statut gRPC attendu côté Stocks | Réponse REST attendue côté Commandes (à justifier) |
|---|---|---|
| P12 | OK : `available=12`, `reservable=true` | 200, même représentation que le TP2 |
| P13 | OK : `available=0`, `reservable=false` | 200 (`canFulfill` faux) |
| identifiant absent ou vide | `INVALID_ARGUMENT` | 400 |
| produit inconnu | `NOT_FOUND` | 404 `application/problem+json` |
| Stocks arrêté | `UNAVAILABLE` (côté client) | 502 ou 503 |
| deadline dépassée | `DEADLINE_EXCEEDED` (côté client) | 504 |

Contraintes : consultation uniquement (aucune réservation) ; aucun détail interne dans la réponse REST.
Une deadline expirée ne prouve pas que le traitement distant n'a pas eu lieu.
`GRPC_STOCK_LATENCY_MS=800` avec `STOCK_GRPC_DEADLINE_MS=500` (défaut) reproduit une deadline.

## 2. WebSocket `GET /ws/orders`
- Authentification : cookie de session signé obtenu par `POST /api/demo/login` (`{"user":"alice"|"bob"}`).
  Sans session valide : 401 avant la mise à niveau. Origine étrangère : 403.
- Taille maximale d'un message : 1024 octets (connexion fermée, code 1009, au-delà).

| Sens | Message |
|---|---|
| client → serveur | `{"type":"SUBSCRIBE","orderId":"ORD-102"}` (aucun autre champ ; jamais d'identité) |
| serveur → client | `{"type":"SUBSCRIBED","orderId":"ORD-102"}` |
| serveur → client | `{"type":"ERROR","code":"FORBIDDEN_SUBSCRIPTION"}` (commande non autorisée **ou** inexistante) |
| serveur → client | `{"type":"ERROR","code":"INVALID_MESSAGE"}` (JSON invalide, type inconnu, forme incorrecte) |
| serveur → client | `{"type":"ERROR","code":"NOT_IMPLEMENTED"}` (tant que le TODO de la phase C n'est pas fait) |
| serveur → client | `{"type":"ORDER_STATUS_CHANGED","orderId":"ORD-102","sequence":5,"status":"SHIPPED"}` |

`sequence` : entier ≥ 0 propre à chaque commande, +1 par transition effectivement enregistrée, identique dans REST (`GET /api/orders/:id`) et dans les événements. Le `5` ci-dessus n'est qu'un exemple.

Limites assumées (mono-instance) : registre d'abonnements en mémoire, pas de rejeu après coupure (d'où la resynchronisation par REST), aucune garantie de lecture côté navigateur.

## 3. Identités de démonstration
| Identité | Commandes possédées |
|---|---|
| `alice` | ORD-101, ORD-102, ORD-103 (et les commandes créées via l'API) |
| `bob` | ORD-104 |

## 4. En production (hors périmètre du TP)
TLS (gRPCs) et WSS, vraie authentification (cookie `Secure`, OIDC), autorisation centralisée, bus d'événements partagé entre instances, outbox/rejeu durable. Les outils de laboratoire sont refusés si `NODE_ENV=production`.
