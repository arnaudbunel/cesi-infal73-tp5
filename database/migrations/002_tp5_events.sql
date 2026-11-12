-- Migration 002 : tables du laboratoire TP5 (idempotente : CREATE ... IF NOT EXISTS). Les tables du TP4 (001) sont inchangées.
-- Frontières logiques : une table de déduplication commune, des données métier propres à chaque consommateur.

-- Déduplication par consommateur : (consumer_name, event_id). `event_id` = eventId du FAIT métier, jamais l'identifiant de livraison.
CREATE TABLE IF NOT EXISTS processed_events (
  consumer_name text NOT NULL,
  event_id text NOT NULL,
  processed_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (consumer_name, event_id)
);

-- Effet métier de Notifications : une notification SIMULÉE (aucun e-mail réel, aucune donnée client).
CREATE TABLE IF NOT EXISTS notification_records (
  id bigserial PRIMARY KEY,
  event_id text NOT NULL,
  order_id text NOT NULL,
  channel text NOT NULL DEFAULT 'SIMULATED',
  message text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

-- Effet métier de Statistiques : compteurs nommés (métrique `shipped` = nombre de commandes expédiées).
CREATE TABLE IF NOT EXISTS shipment_stats (
  metric text PRIMARY KEY,
  counter bigint NOT NULL DEFAULT 0 CHECK (counter >= 0),
  updated_at timestamptz NOT NULL DEFAULT now()
);
INSERT INTO shipment_stats(metric, counter) VALUES ('shipped', 0) ON CONFLICT (metric) DO NOTHING;
