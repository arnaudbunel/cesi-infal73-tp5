// Configuration de LABORATOIRE partagée par les scripts (valeurs factices, 127.0.0.1). Refuse NODE_ENV=production.
import { fileURLToPath } from 'node:url';
import './load-env.mjs';

if (process.env.NODE_ENV === 'production') {
  console.error('Refus : les scripts de laboratoire (infra, reset, injections de panne) sont interdits si NODE_ENV=production.');
  process.exit(1);
}

const env = process.env;
export const lab = {
  rabbitUser: env.RABBITMQ_USER ?? 'lab',
  rabbitPassword: env.RABBITMQ_PASSWORD ?? 'lab-only-change-me',
  amqpPort: Number(env.RABBITMQ_AMQP_PORT ?? 5672),
  mgmtPort: Number(env.RABBITMQ_MGMT_PORT ?? 15672),
  pgUser: env.POSTGRES_USER ?? 'lab',
  pgPassword: env.POSTGRES_PASSWORD ?? 'lab-only-change-me',
  pgPort: Number(env.POSTGRES_PORT ?? 55432),
  pgDatabase: env.POSTGRES_DB ?? 'infal73_tp5_lab',
  mgmtUrl: `http://127.0.0.1:${Number(env.RABBITMQ_MGMT_PORT ?? 15672)}`,
  databaseUrl:
    env.DATABASE_URL ??
    `postgres://${env.POSTGRES_USER ?? 'lab'}:${encodeURIComponent(env.POSTGRES_PASSWORD ?? 'lab-only-change-me')}@127.0.0.1:${Number(env.POSTGRES_PORT ?? 55432)}/${env.POSTGRES_DB ?? 'infal73_tp5_lab'}`,
  stateDir: env.LAB_STATE_DIR ?? fileURLToPath(new URL('../../.lab-state', import.meta.url)),
};

/** Files réellement touchées par un nettoyage ciblé (jamais d'autres queues du broker). */
export const TP4_QUEUES = ['orders.prepare', 'orders.retry.2s', 'orders.retry.5s', 'orders.retry.10s', 'orders.prepare.dlq'];
/** TP5 : par consommateur, la queue d'abonnement, sa queue de retry et sa DLQ (circuits isolés). */
export const TP5_CONSUMERS = ['notifications', 'statistics', 'audit'];
export const TP5_QUEUES = TP5_CONSUMERS.flatMap((c) => [`${c}.events`, `${c}.events.retry`, `${c}.events.dlq`]);
export const LAB_QUEUES = [...TP4_QUEUES, ...TP5_QUEUES];
export const TP4_TABLES = ['processed_operations', 'prepared_orders'];
export const TP5_TABLES = ['processed_events', 'notification_records', 'shipment_stats'];
export const LAB_TABLES = [...TP4_TABLES, ...TP5_TABLES];
export const ORDERS_URL = env.ORDERS_URL ?? `http://127.0.0.1:${env.PORT ?? 3000}`;

/** Garde-fou des scripts destructifs : base dédiée (*_lab) ET hôte local. Retourne null si OK, sinon la raison du refus. */
export function destructiveGuard() {
  if (!/_lab$/.test(lab.pgDatabase)) return `base « ${lab.pgDatabase} » : le nom doit se terminer par _lab`;
  try {
    const host = new URL(lab.databaseUrl).hostname;
    if (!['127.0.0.1', 'localhost', '::1', '[::1]'].includes(host)) return `hôte PostgreSQL « ${host} » non local`;
  } catch {
    return 'DATABASE_URL illisible';
  }
  return null;
}
