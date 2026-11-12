import { fileURLToPath } from 'node:url';
import { WORKER_PREFETCH } from '@infal73/contracts';

export const KNOWN_FAULTS = ['EXIT_AFTER_COMMIT_BEFORE_ACK'] as const;
export type Fault = (typeof KNOWN_FAULTS)[number];

export interface WorkerConfig {
  rabbitUrl: string;
  databaseUrl: string;
  /** Nom d'instance (logs, T6 : deux workers). */
  workerName: string;
  prefetch: number;
  /** Injection de panne de laboratoire (jamais en production). */
  fault: Fault | null;
  /** Dossier d'état de laboratoire (simulation de dépendance 503). */
  labStateDir: string;
}

const LAB_PASSWORD = 'lab-only-change-me';

/** Lit et valide l'environnement du worker ; erreur explicite si invalide. Refuse tout outil de laboratoire en production. */
export function readWorkerConfig(env: NodeJS.ProcessEnv = process.env): WorkerConfig {
  const production = env['NODE_ENV'] === 'production';
  const faultRaw = env['FAULT'];
  if (production && faultRaw) throw new Error('FAULT injection is forbidden when NODE_ENV=production');
  if (faultRaw && !(KNOWN_FAULTS as readonly string[]).includes(faultRaw)) {
    throw new Error(`unknown FAULT "${faultRaw}" (known: ${KNOWN_FAULTS.join(', ')})`);
  }

  const user = encodeURIComponent(env['RABBITMQ_USER'] ?? 'lab');
  const password = encodeURIComponent(env['RABBITMQ_PASSWORD'] ?? LAB_PASSWORD);
  const amqpPort = env['RABBITMQ_AMQP_PORT'] ?? '5672';
  const pgPort = env['POSTGRES_PORT'] ?? '55432';
  const pgUser = encodeURIComponent(env['POSTGRES_USER'] ?? 'lab');
  const pgPassword = encodeURIComponent(env['POSTGRES_PASSWORD'] ?? LAB_PASSWORD);
  const pgDatabase = env['POSTGRES_DB'] ?? 'infal73_tp4_lab';

  let rabbitUrl = env['RABBITMQ_URL'];
  let databaseUrl = env['DATABASE_URL'];
  if (production) {
    if (!rabbitUrl || !databaseUrl) throw new Error('RABBITMQ_URL and DATABASE_URL are required when NODE_ENV=production (no lab defaults)');
    if (rabbitUrl.includes(LAB_PASSWORD) || databaseUrl.includes(LAB_PASSWORD)) {
      throw new Error('the lab credentials are forbidden when NODE_ENV=production');
    }
  }
  rabbitUrl ??= `amqp://${user}:${password}@127.0.0.1:${amqpPort}`;
  databaseUrl ??= `postgres://${pgUser}:${pgPassword}@127.0.0.1:${pgPort}/${pgDatabase}`;

  return {
    rabbitUrl,
    databaseUrl,
    workerName: env['WORKER_NAME'] ?? `worker-${process.pid}`,
    prefetch: WORKER_PREFETCH,
    fault: (faultRaw as Fault | undefined) ?? null,
    labStateDir: env['LAB_STATE_DIR'] ?? fileURLToPath(new URL('../../../.lab-state', import.meta.url)),
  };
}
