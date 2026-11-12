# Recette T1–T7 — déclencheurs, préconditions, preuves

Pour chaque test : **prédiction écrite avant**, action, preuves (RabbitMQ Management, logs avec `eventId`, SQL), conclusion. Utilisez `docs/test-record-template.md`.
Le starter n'est pas censé réussir tous les tests : tant que B, C1 et C2 ne sont pas réalisés, la recette automatique affiche `TODO ATTENDU` (`docs/TODO_MAP.md`).

## Avant de commencer
```bash
npm run setup && npm run start && npm run status
npm run reset            # état propre : tables vidées, queues vidées, commandes remises à l'état initial
npm run lab:state        # point de départ à consigner
```
Fixtures : `ORD-102` `CONFIRMED` (expédiable une fois), `ORD-103` `SHIPPED` (annulation refusée). Pour rejouer un scénario : `npm run reset`, ou une **nouvelle commande** (voir `README.md`).
Expédier : `curl -X PATCH http://127.0.0.1:3000/api/orders/ORD-102 -H "content-type: application/json" -d '{"status":"SHIPPED"}'` (PowerShell : `Invoke-RestMethod -Method Patch -Uri http://127.0.0.1:3000/api/orders/ORD-102 -ContentType application/json -Body '{"status":"SHIPPED"}'`).

## Observer sans assertion temporelle fragile
Les compteurs *ready / unacked* d'une queue ne se « photographient » qu'au bon moment (un message peut être consommé en quelques millisecondes). Préférez, dans cet ordre :
1. les **logs** (`npm run logs -- <service>`) : `delivery.received`, `acked`, `failed_temporarily`, `retry_scheduled`, `dead_lettered`, avec `eventId` et `deliveryTag` ;
2. l'**état SQL** (`npm run lab:state`) : ce qui est *traité* est ce qui est *committé* ;
3. **RabbitMQ Management** (*Queues*, *Exchanges* → `business.events` → *Message rates*) et, pour un backlog, la lecture **avant** de relancer le consommateur ;
4. des **attentes conditionnelles bornées** (boucle avec délai maximal) plutôt qu'un `sleep` fixe. La recette automatique fait ainsi.

## Les sept tests

| Test | Précondition | Déclencheur | À démontrer |
|---|---|---|---|
| **T1** | `npm run reset`, tout démarré | Expédier `ORD-102` | Une publication logique ; une copie dans chacune des trois queues ; traitement isolé par consommateur |
| **T2** | idem | `POST /api/orders/ORD-103/cancellations` (transition refusée, 409) | Aucun nouvel `OrderShipped`. Pensez à ce qui rend cette absence **probante** |
| **T3** | `npm run worker:stop -- notifications` | Expédier `ORD-102` **pendant** que Notifications est arrêté | Statistiques et Audit continuent ; le backlog de `notifications.events` est conservé ; que se passe-t-il à la reprise (`npm run worker:start -- notifications`) ? |
| **T4** | `npm run lab:statistics-fail -- --attempts 2` | Expédier `ORD-102` | Retry différé **dans le circuit de Statistiques**, puis exactement un incrément ; Notifications et Audit non affectés |
| **T5** | `npm run reset` | `npm run lab:event -- duplicate --event-id evt-lab-dup-1` (même événement, deux livraisons) ; variante : expédier puis `npm run lab:event -- republish --event-id <eventId>` | Pas de second effet métier chez chaque consommateur |
| **T6** | `npm run reset` | `npm run lab:event -- bad-version` (`schemaVersion=99`) | Rejet contrôlé, diagnostic lisible, DLQ, **aucune boucle** de redelivery |
| **T7** | Commandes lancé par `npm run start` (`LAB_MODE`) | `npm run lab:failpoint -- set AFTER_SHIP_COMMIT_BEFORE_EVENT_PUBLISH`, puis expédier `ORD-102` | La fenêtre de perte existe : que constatent le client, la base de commandes, RabbitMQ ? **Proposez** une correction architecturale (implémentation facultative) |

`--mode exit` du failpoint tue le processus Commandes (SIGKILL) : les commandes étant **en mémoire** dans ce dépôt, leur état est perdu au redémarrage ; le mode par défaut (`throw`) garde le processus vivant et rend la fenêtre observable. Pensez à `npm run lab:failpoint -- clear` après vos essais.

## Recette automatique
```bash
npm run stop                         # la recette lance ses propres processus : aucun consommateur ne doit déjà tourner
npm run test:acceptance              # PASS · FAIL · TODO ATTENDU · NON TESTÉ
npm run test:acceptance -- --only T1,T5
```
- `PASS` : comportement observé en base / RabbitMQ / logs. `FAIL` : comportement faux ou acquis cassé. `TODO ATTENDU` : échec **et** marqueur `TODO(TP5)` encore présent dans la zone bloquante. `NON TESTÉ` : prérequis absent (Docker, migration, consommateur déjà actif) — rien n'a été exécuté.
- Elle ne remplace **ni** vos prédictions **ni** vos preuves : un `PASS` sans fiche renseignée ne valide pas le TP. T7 est une analyse : le test vérifie seulement la fenêtre de perte.
- Aucun test ne s'appuie sur des données simulées : tout est lu dans RabbitMQ, PostgreSQL et les journaux des vrais processus.
