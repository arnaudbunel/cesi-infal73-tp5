# Fiche de recette TP5 — groupe : ______ · date : ______

Une ligne par test. Écrivez la **prédiction avant** de lancer ; ne consignez que ce que vous avez réellement observé (aucune donnée sensible). Une prédiction fausse mais bien analysée vaut mieux qu'un résultat recopié.
`npm run lab:state` donne queues (ready/unacked/consumers), tables SQL et journal d'audit ; les logs sont horodatés (ISO 8601) et portent l'`eventId`.

| Test | Prédiction (avant) | Action / commande exacte | Preuve RabbitMQ (ready / unacked / consumers, avant → après) | Logs / eventId (horodatage) | SQL (processed_events, notification_records, shipment_stats) | Conclusion (écart prédiction ↔ observation) |
|---|---|---|---|---|---|---|
| T1 Expédition valide ORD-102 | | | | | | |
| T2 Transition refusée ORD-103 | | | | | | |
| T3 Notifications arrêté | | | | | | |
| T4 Statistiques en 503 temporaire | | | | | | |
| T5 Même eventId deux fois | | | | | | |
| T6 schemaVersion inconnue | | | | | | |
| T7 Arrêt après COMMIT, avant publish | | | | | | |

## T7 — analyse (obligatoire) et proposition (facultative à implémenter)
1. Quelle est précisément la fenêtre de perte, et qui la constate (client, base, consommateurs) ?
2. Quelle correction architecturale proposez-vous ? Quels nouveaux risques apporte-t-elle (ex. doublons au relais) ?

## Questions de justification (individuelles)
1. Pourquoi un *confirm* positif ne suffit-il pas à prouver le routage ? Que change `mandatory` ?
2. Quelle erreur produit un ACK **avant** le COMMIT ? Et un COMMIT sans ACK ?
3. Comment prouvez-vous qu'une deuxième livraison du même `eventId` n'a pas produit un deuxième effet ?
4. Pourquoi le retry de Statistiques ne doit-il pas repasser par `business.events` ?

## Décisions d'architecture (5 à 8 lignes)
Deux choix de votre groupe : risque contrôlé, compromis, limite qui demeure.
