# INFAL73 — TP5 : intégration événementielle du fil rouge

Module CESI INFAL73 · Master 1 MAALSI · **80 minutes** · groupes de 2 à 4 · Node.js/TypeScript · RabbitMQ (AMQP 0-9-1) · PostgreSQL.

**Mission.** Quand une commande passe de `CONFIRMED` à `SHIPPED`, le service Commandes publie un événement `OrderShipped` vers un exchange *topic* ; trois consommateurs indépendants (Notifications, Statistiques, Audit) le reçoivent chacun dans leur propre queue. Vous devez **prouver** que pannes, retries et doublons ne compromettent pas les résultats métier.

Le dépôt repose sur la **version corrigée du TP4** : REST (TP1–TP2), gRPC (TP3), WebSocket (TP3) et préparation asynchrone `orders.prepare` (TP4) fonctionnent et **ne doivent pas être modifiés**. L'infrastructure, la topologie RabbitMQ, les migrations, le worker Audit et les scripts de panne sont **fournis** : vous les observez et les expliquez, vous ne les réinstallez pas.

## Démarrage (≈ 8 min)

Prérequis : Node 24, Docker (`docker info` doit répondre sans erreur ; « permission denied » sous Linux : groupe `docker` puis reconnexion).

```bash
npm ci                 # installation (construit aussi @infal73/contracts et @infal73/messaging)
npm run setup          # RabbitMQ + PostgreSQL (health checks) + topologie + migrations
npm run start          # Stocks, Commandes, worker TP4 et les 3 consommateurs, en arrière-plan
npm run status         # qui tourne ?
npm run stop           # arrêt propre de tout ce que « start » a lancé
```
PowerShell : mêmes commandes (`npm ci; npm run setup`). RabbitMQ Management : <http://127.0.0.1:15672> (identifiants factices : voir `.env.example`). Si un port est occupé (ex. un RabbitMQ du TP4 encore lancé), copiez `.env.example` vers `.env` et changez `RABBITMQ_AMQP_PORT`, `RABBITMQ_MGMT_PORT` ou `POSTGRES_PORT`.

> Au départ, **Notifications et Statistiques s'arrêtent à la première livraison** avec le message `TODO(TP5) … non réalisé` (code de sortie 3) : le message reste dans sa queue, rien n'est perdu ni mis en DLQ. Et **rien n'est publié** tant que la zone B n'est pas réalisée. C'est l'état normal du starter. Le worker **Audit** fonctionne dès le départ.

Mode « un seul terminal » pour développer : `npm run dev` (Stocks, Commandes, worker TP4 et 3 consommateurs avec rechargement ; `DEV_EVENTS=0` pour ne pas lancer les consommateurs). Un consommateur seul : `npm run dev:notifications` / `dev:statistics` / `dev:audit`.

## Commandes

| But | Commande |
|---|---|
| Infrastructure + topologie + migrations (idempotent) | `npm run setup` (`infra:topology`, `db:migrate` séparément) |
| Démarrer / arrêter / état / journal | `npm run start` · `stop` · `status` · `npm run logs -- notifications` (aussi : `-- orders`, `statistics`…) |
| Un seul service | `npm run start -- orders` · `npm run stop -- statistics` |
| Arrêter / reprendre **un** consommateur (T3) | `npm run worker:stop -- notifications` · `npm run worker:start -- notifications` |
| Vérification de compilation | `npm run typecheck` · `npm run build` |
| Smoke (sans Docker pour la partie statique) | `npm run smoke` (`SKIP_INFRA_TESTS=1` ignore explicitement les tests qui exigent Docker) |
| Non-régression TP1–TP4 | `npm run test:regression` (arrêter `npm run start` avant : les tests TP4 lancent leurs propres workers) |
| Recette TP5 (T1–T7) | `npm run test:acceptance` (arrêter `npm run start` avant) · `-- --only T1,T5` · `npm run test:acceptance:tp5:strict` |
| Audit de routage (motifs `order.shipped`, `order.*`, `#`) sans toucher aux vraies queues | `npm run lab:routing-check` |
| Relevé d'état pour vos fiches | `npm run lab:state` (queues ready/unacked/consumers, tables SQL, journal d'audit) |
| Remise à zéro des données de test | `npm run reset` (ciblé) · `npm run reset -- --topology` · `infra:down` · `infra:destroy` |

### Injections de panne (laboratoire local uniquement, refusées si `NODE_ENV=production`)

| Besoin | Commande |
|---|---|
| Doublon contrôlé (même eventId, même corps) — T5 | `npm run lab:event -- duplicate [--event-id evt-lab-dup-1] [--order ORD-102] [--count 2]` |
| Republier à l'identique un événement déjà vu par Audit — T5 | `npm run lab:event -- republish --event-id <eventId>` |
| Événement de version inconnue — T6 | `npm run lab:event -- bad-version [--version 99]` |
| Statistiques : N prochains appels de dépendance en 503 — T4 | `npm run lab:statistics-fail -- --attempts 2` · `-- --clear` |
| Arrêt du processus Commandes entre le COMMIT et la publication — T7 | `npm run lab:failpoint -- set AFTER_SHIP_COMMIT_BEFORE_EVENT_PUBLISH [--mode throw\|exit]` · `-- clear` |
| Arrêter RabbitMQ (TP4) | `node scripts/lab/broker.mjs stop\|start\|restart` |

Ces scripts injectent via l'API Management ou un fichier d'état local (`.lab-state/`) ; ils ne touchent pas à vos données hors laboratoire.

### Rejouer ORD-102
Les commandes du fil rouge sont **en mémoire** (TP1–TP4) : `ORD-102` n'est expédiable qu'**une fois** par démarrage. Pour recommencer : `npm run reset` (remet les commandes à l'état initial si Commandes tourne en `LAB_MODE`, ce que font `npm run start` et `npm run dev`), ou créez une **nouvelle commande** (`POST /api/orders` avec un en-tête `Idempotency-Key`, puis `PATCH … {"status":"CONFIRMED"}`). `ORD-103` (`SHIPPED`) reste la commande de la transition refusée : `POST /api/orders/ORD-103/cancellations` → 409.

## Ce qui est fourni / ce qui est à vous

| Fourni et opérationnel | À compléter (`TODO(TP5)`, voir `docs/TODO_MAP.md`) |
|---|---|
| Exchanges, queues, bindings, retries et DLQ par consommateur (`docker/rabbitmq/definitions.json`) | **B** — publication de `OrderShipped` (déclenchement, enveloppe, publish, issues) |
| Tables `processed_events`, `notification_records`, `shipment_stats` | **C1** — handler Notifications |
| Publisher confirmé du TP4 (`confirm` + `mandatory` + `basic.return`) | **C2** — handler Statistiques |
| Contrat `OrderShipped` + validation à l'exécution (`@infal73/contracts`) | Recette **T1–T7**, preuves, analyse T7 |
| Runtime des consommateurs (ACK manuel, retry borné, DLQ, reconnexion), `withTransaction` | |
| Worker Audit de référence, injections de panne, scripts `setup/start/stop/reset` | |

## Documentation

- `docs/ARCHITECTURE_TP5.md` — architecture, topologie, circuits d'erreur, chronologie.
- `docs/CONTRAT_ORDER_SHIPPED.md` — contrat de l'événement et diagnostics.
- `docs/RECETTE_ETUDIANTS.md` — déclencheurs et préconditions T1–T7, comment relever les preuves.
- `docs/TODO_MAP.md` — où se trouvent les `TODO(TP5)` et ce qu'ils bloquent.
- `docs/test-record-template.md` — fiche de recette à remplir (prédiction → conclusion).
- `docs/student-setup.md`, `docs/limitations.md`, `docs/protocols.md` — mise en route, limites assumées, protocoles TP3.

## Chronologie conseillée (80 min)

| Phase | Durée | Résultat attendu |
|---|---|---|
| **A** Diagnostic | 8 min | Démarrer, repérer producteurs/exchanges/queues/consommateurs ; prédire le sort d'un événement si Statistiques est arrêté |
| **B** Publication | 15 min | Publier `OrderShipped` uniquement après transition validée ; confirm + routage |
| **C** Consommation | 17 min | Notifications et Statistiques sans double effet |
| **D** Panne partielle | 15 min | Statistiques échoue puis reprend ; les autres continuent |
| **E** Recette | 13 min | T1–T7 : preuves, analyse, limites |
| **F** Restitution | 12 min | Schéma, démonstration d'une panne, justification individuelle |

**Extensions facultatives** (groupes avancés, aucune solution fournie) : Transactional Outbox + relay · détection de trous d'`aggregateVersion` et resynchronisation · nouvel abonné tardif et rattrapage d'historique · Saga stock + paiement.

## Règles du laboratoire
Données de test jetables, identifiants factices, services liés à `127.0.0.1`. **Aucun e-mail réel** n'est envoyé : la « notification » est une ligne en base. Une explication sans exécution ni preuve ne suffit pas : consignez ce que vous avez réellement observé (RabbitMQ, logs avec `eventId`, SQL).
