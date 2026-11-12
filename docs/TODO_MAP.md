# Carte des `TODO(TP5)`

Cherchez `TODO(TP5)` dans le dépôt : chaque marqueur porte une zone entre crochets. Les marqueurs décrivent **quoi** décider, pas **comment** l'écrire. Le starter compile et démarre sans modifier le schéma SQL ni l'infrastructure.

| Zone | Fichier | Marqueurs | Ce qui est fourni autour | Recettes bloquées tant que la zone est ouverte |
|---|---|---|---|---|
| **B** Publication | `apps/order-service/src/services/orders.service.ts` (`changeStatus`) | `[B1]` déclenchement après une transition validée | Failpoint T7 déjà câblé juste avant ; `integration.shippedPublisher` injecté | T1 · T2 · T3 · T4 · T7 (rien n'est publié) |
| **B** Publication | `apps/order-service/src/events/order-shipped-publisher.ts` | `[B2]` enveloppe · `[B3]` publication · `[B4]` issues et logs | Publisher confirmé TP4, types et noms du contrat, `PublishError` | idem |
| **C1** Notifications | `apps/event-workers/src/handlers/notifications.handler.ts` | `[C1.1]` contrat · `[C1.2]` transaction · `[C1.3]` ACK après COMMIT · `[C1.4]` erreurs | Runtime (ACK manuel, retry, DLQ), `withTransaction`, validateur, schéma SQL | T1 · T3 (reprise) · T4 · T5 · T6 |
| **C2** Statistiques | `apps/event-workers/src/handlers/statistics.handler.ts` | `[C2.1]` contrat · `[C2.2]` compteur + déduplication · `[C2.3]` panne, retry, ACK | idem + `faults.callDependency` (503 contrôlé) | T1 · T3 · T4 · T5 · T6 |
| Recette T7 | fiche de recette | analyse écrite (fenêtre de perte, piste Outbox) | Failpoint `AFTER_SHIP_COMMIT_BEFORE_EVENT_PUBLISH` | — (analyse non évaluée par la recette automatique) |

Tant qu'une zone est ouverte, `npm run test:acceptance` affiche `TODO ATTENDU` pour les tests qu'elle bloque. Cette qualification n'est accordée que si le marqueur `TODO(TP5) [zone…]` est **encore dans le code** : retirez un marqueur sans que le comportement fonctionne et le test passe en `FAIL`. `npm run test:acceptance:tp5:strict` traite aussi `TODO ATTENDU` comme un échec.

## Comportement du starter (pour ne pas le confondre avec une panne)
- Tant que **B** est ouverte : l'API répond (`PATCH … SHIPPED` → 200), mais **aucun événement** n'est publié.
- Tant que **C1** / **C2** sont ouvertes : le worker concerné s'arrête à la première livraison (code 3, journal `todo.not-implemented`) ; le message revient dans sa queue (*Ready*), rien n'est acquitté ni envoyé en DLQ.
- Le worker **Audit** et tous les composants TP1–TP4 fonctionnent.
