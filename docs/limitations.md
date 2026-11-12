# Limites assumées du laboratoire TP4

- **Queues classiques, un seul nœud** : aucune haute disponibilité ni réplication. En production, réévaluer le type de queue (par ex. quorum queues), la réplication et la gestion de configuration par *policy*.
- **At-least-once, jamais exactly-once** : l'ACK RabbitMQ n'est pas atomique avec le COMMIT PostgreSQL ; une redelivery ou une republication peut faire réapparaître une opération. L'idempotence côté base garantit l'effet métier, pas l'unicité des livraisons.
- **Retries à TTL fixes** : choisis pour visualiser les délais. Un TTL est un délai minimal, pas une échéance garantie. En production : *policy*, délais avec gigue (*jitter*), supervision.
- **DLQ** : elle isole un message, elle ne résout pas l'incident ; diagnostic, correction et décision de rejeu restent nécessaires (rejeu automatique hors périmètre).
- **Confirmation incertaine** : si la connexion se ferme avant le confirm, on ne sait pas si le message est en queue ; republier est sûr uniquement grâce à l'`operationId` stable et à la déduplication.
- **`operationId` / `orderId`** : une demande contradictoire (même commande, autre `operationId`) n'est pas un doublon autorisé ; son traitement est une décision de conception à documenter. Le message de préparation de l'API vérifie l'existence de la commande et refuse `CANCELLED` / `SHIPPED`, mais ne sait pas si elle a déjà été préparée (PostgreSQL appartient au worker) : un second POST republie la même opération, dédupliquée par le worker.
- **Sécurité** : AMQP, HTTP, gRPC et WebSocket sans TLS ; identifiants factices ; ports liés à `127.0.0.1`. Le mode démo, les identifiants de laboratoire et `FAULT` sont refusés si `NODE_ENV=production`.
- **Effets externes** : une transaction locale ne rend pas un appel externe (paiement, e-mail) exactement-une-fois : voir Transactional Outbox et clés d'idempotence du prestataire.
- **Multi-instances** : le registre d'abonnements WebSocket reste en mémoire (TP3) ; plusieurs workers partagent la même base et sa contrainte d'unicité.
- Les compteurs du Management sont rafraîchis toutes les 500 ms dans ce laboratoire (5 s par défaut) : lisez-les après stabilisation.

# Limites assumées du laboratoire TP5 (en plus de celles du TP4)

- **Commandes en mémoire** : les transitions de commande (TP1–TP4) ne sont pas dans PostgreSQL. Le « COMMIT » de `CONFIRMED -> SHIPPED` est donc l'écriture en mémoire du dépôt de commandes ; un arrêt brutal perd aussi l'état. L'analyse de T7 reste valable (état enregistré, événement non publié) ; elle est observable de façon fiable avec le failpoint en mode `throw`.
- **Pas de Transactional Outbox** dans le parcours obligatoire : la publication n'est pas atomique avec la transition. La proposition d'architecture est demandée en T7 ; son implémentation est une extension facultative.
- **Livraison au moins une fois** : doublons possibles (redelivery après crash avant ACK, republication d'un retry, relais). L'idempotence par `(consumer_name, event_id)` garantit l'**effet**, pas l'unicité des livraisons. `eventId` doit rester stable à la republication.
- **Durabilité** : queues classiques durables + messages persistants survivent à un redémarrage du broker ; pas de réplication (un seul nœud), pas de protection contre la perte de disque. Une publication sans confirm n'est pas durable ; un confirm incertain impose de republier le **même** `eventId`.
- **Ordre** : pas de garantie d'ordre entre retries et nouvelles livraisons (un événement en retry peut être dépassé). `aggregateVersion` sert à détecter un trou ou un désordre (extension facultative).
- **Retry à TTL fixe** (3 s) et borné (3 retries) ; pas de gigue, pas de rejeu automatique depuis la DLQ : la DLQ isole un message, elle ne corrige pas l'incident.
- **Base partagée** entre consommateurs dans ce TP (frontières logiques seulement) ; en production, une base par service.
- **Notifications simulées** : aucune intégration e-mail/SMS ; un envoi réel ne serait pas exactement-une-fois sans clé d'idempotence côté prestataire.
- **Laboratoire** : AMQP sans TLS, identifiants factices, ports sur `127.0.0.1`, injections de panne et failpoint refusés si `NODE_ENV=production`.
