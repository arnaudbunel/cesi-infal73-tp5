import { fileURLToPath } from 'node:url';
import { EVENT_CONSUMERS, EVENT_ERROR_CIRCUITS, EVENT_QUEUES, EVENT_WORKER_PREFETCH } from '@infal73/contracts';
import type { EventConsumerKey, EventConsumerName } from '@infal73/contracts';

export const CONSUMER_KEYS = Object.keys(EVENT_CONSUMERS) as EventConsumerKey[];

export interface EventWorkerConfig {
  consumer: EventConsumerKey;
  /** Valeur de `processed_events.consumer_name` et des logs. */
  consumerName: EventConsumerName;
  queue: string;
  circuit: (typeof EVENT_ERROR_CIRCUITS)[EventConsumerKey];
  rabbitUrl: string;
  databaseUrl: string;
  /** Nom d'instance (logs) : plusieurs instances d'une MÊME queue se partagent les livraisons. */
  workerName: string;
  prefetch: number;
  /** Dossier d'état de laboratoire (injections de panne, journal d'audit). */
  labStateDir: string;
}

const LAB_PASSWORD = 'lab-only-change-me';

/** Lit et valide l'environnement d'un worker ; erreur explicite sinon. Refuse les valeurs de laboratoire en production. */
export function readEventWorkerConfig(consumerArg: string | undefined, env: NodeJS.ProcessEnv = process.env): EventWorkerConfig {
  const consumer = consumerArg ?? env['CONSUMER'];
  if (!consumer || !(CONSUMER_KEYS as string[]).includes(consumer)) {
    throw new Error(`consumer must be one of: ${CONSUMER_KEYS.join(', ')} (argument or CONSUMER=...), got "${consumer ?? ''}"`);
  }
  const key = consumer as EventConsumerKey;
  const production = env['NODE_ENV'] === 'production';

  const user = encodeURIComponent(env['RABBITMQ_USER'] ?? 'lab');
  const password = encodeURIComponent(env['RABBITMQ_PASSWORD'] ?? LAB_PASSWORD);
  const amqpPort = env['RABBITMQ_AMQP_PORT'] ?? '5672';
  const pgPort = env['POSTGRES_PORT'] ?? '55432';
  const pgUser = encodeURIComponent(env['POSTGRES_USER'] ?? 'lab');
  const pgPassword = encodeURIComponent(env['POSTGRES_PASSWORD'] ?? LAB_PASSWORD);
  const pgDatabase = env['POSTGRES_DB'] ?? 'infal73_tp5_lab';

  let rabbitUrl = env['RABBITMQ_URL'];
  let databaseUrl = env['DATABASE_URL'];
  if (production) {
    if (!rabbitUrl || !databaseUrl) throw new Error('RABBITMQ_URL and DATABASE_URL are required when NODE_ENV=production (no lab defaults)');
    if (rabbitUrl.includes(LAB_PASSWORD) || databaseUrl.includes(LAB_PASSWORD)) throw new Error('the lab credentials are forbidden when NODE_ENV=production');
  }
  rabbitUrl ??= `amqp://${user}:${password}@127.0.0.1:${amqpPort}`;
  databaseUrl ??= `postgres://${pgUser}:${pgPassword}@127.0.0.1:${pgPort}/${pgDatabase}`;

  return {
    consumer: key,
    consumerName: EVENT_CONSUMERS[key],
    queue: EVENT_QUEUES[key],
    circuit: EVENT_ERROR_CIRCUITS[key],
    rabbitUrl,
    databaseUrl,
    workerName: env['WORKER_NAME'] ?? `${key}-${process.pid}`,
    prefetch: EVENT_WORKER_PREFETCH,
    labStateDir: env['LAB_STATE_DIR'] ?? fileURLToPath(new URL('../../../.lab-state', import.meta.url)),
  };
}
