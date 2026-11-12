-- État initial déterministe du laboratoire TP5 : aucun événement traité, aucune notification, compteur `shipped` à 0.
-- Ne touche QUE les tables du TP5 (processed_events, notification_records, shipment_stats).
TRUNCATE TABLE processed_events, notification_records;
INSERT INTO shipment_stats(metric, counter) VALUES ('shipped', 0)
  ON CONFLICT (metric) DO UPDATE SET counter = 0, updated_at = now();
