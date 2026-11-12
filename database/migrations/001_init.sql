-- Migration 001 : tables du laboratoire TP4 (idempotente : CREATE ... IF NOT EXISTS).
CREATE TABLE IF NOT EXISTS processed_operations (
  consumer_name text NOT NULL,
  operation_id text NOT NULL,
  processed_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (consumer_name, operation_id)
);
CREATE TABLE IF NOT EXISTS prepared_orders (
  order_id text PRIMARY KEY,
  operation_id text NOT NULL UNIQUE,
  prepared_at timestamptz NOT NULL DEFAULT now()
);
