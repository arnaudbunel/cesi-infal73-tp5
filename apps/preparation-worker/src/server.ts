import amqp from 'amqplib';
import type { ChannelModel, ConsumeMessage } from 'amqplib';
import { QUEUES } from '@infal73/contracts';
import { readWorkerConfig } from './config.js';
import type { WorkerConfig } from './config.js';
import { handleDelivery } from './consumer.js';
import type { ConsumerDeps } from './consumer.js';
import { createPool } from './db.js';
import { createLabFaults } from './lab-faults.js';
import { createLogger } from './logger.js';
import type { Logger } from './logger.js';
import { ConfirmedRetryScheduler } from './retry.js';
import { PgPreparationStore } from './store.js';

let config: WorkerConfig;
try {
  config = readWorkerConfig();
} catch (error) {
  console.error(`[preparation-worker] invalid configuration: ${(error as Error).message}`);
  process.exit(1);
}

const log = createLogger(config.workerName);
const pool = createPool(config.databaseUrl, log);
const faults = createLabFaults(config, log);
const store = new PgPreparationStore(pool);

let stopping = false;
let failures = 0;
let current: ChannelModel | null = null;
const inFlight = new Set<Promise<void>>();
const sleep = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));

/** Journal « livraison reçue » : tolérant, ne valide rien (la validation est faite dans consumer.ts). */
function describeDelivery(msg: ConsumeMessage): Record<string, unknown> {
  const fields: Record<string, unknown> = {
    deliveryTag: msg.fields.deliveryTag,
    redelivered: msg.fields.redelivered,
    messageId: msg.properties.messageId,
    correlationId: msg.properties.correlationId,
    bytes: msg.content.length,
  };
  try {
    const parsed: unknown = JSON.parse(msg.content.toString('utf8'));
    if (typeof parsed === 'object' && parsed !== null) {
      const { operationId, orderId, attempt } = parsed as Record<string, unknown>;
      Object.assign(fields, { operationId, orderId, attempt });
    }
  } catch {
    fields['body'] = 'unparseable';
  }
  return fields;
}

/** Une session = une connexion AMQP. Se termine quand la connexion se ferme (coupure, redémarrage du broker, arrêt). */
async function runSession(): Promise<void> {
  const connection = await amqp.connect(config.rabbitUrl, { timeout: 3_000 });
  current = connection;
  // Écouteurs obligatoires sur `error` (sinon Node arrête le processus) ; `close` clôt la session.
  connection.on('error', (error: Error) => log('broker.connection.error', { message: error.message }));
  const closed = new Promise<void>((resolve) => connection.once('close', () => resolve()));

  const channel = await connection.createChannel();
  const retryChannel = await connection.createConfirmChannel();
  for (const ch of [channel, retryChannel]) ch.on('error', (error: Error) => log('broker.channel.error', { message: error.message }));
  // Un canal fermé (erreur de protocole) invalide la session : on repart sur une connexion neuve.
  channel.on('close', () => void connection.close().catch(() => undefined));
  retryChannel.on('close', () => void connection.close().catch(() => undefined));

  // Vérification passive : la topologie doit exister (npm run infra:up / npm run infra:topology).
  await channel.checkQueue(QUEUES.prepare);
  await channel.prefetch(config.prefetch);

  const deps: ConsumerDeps = {
    channel,
    config,
    store,
    retries: new ConfirmedRetryScheduler(retryChannel, log),
    faults,
    log,
    abortSession: (reason) => {
      log('session.abort', { reason });
      void connection.close().catch(() => undefined);
    },
  };
  const { consumerTag } = await channel.consume(
    QUEUES.prepare,
    (msg) => {
      if (msg === null) {
        // Consommateur annulé par le broker (queue supprimée...) : on ferme pour repartir proprement.
        log('consumer.cancelled-by-broker');
        void connection.close().catch(() => undefined);
        return;
      }
      log('delivery.received', describeDelivery(msg));
      const task = handleDelivery(deps, msg)
        .catch((error: unknown) => {
          log('delivery.handler-error', { deliveryTag: msg.fields.deliveryTag, error: error instanceof Error ? `${error.name}: ${error.message}` : String(error) });
        })
        .finally(() => inFlight.delete(task));
      inFlight.add(task);
    },
    { noAck: false }, // ACK manuel obligatoire
  );
  log('worker.ready', { queue: QUEUES.prepare, prefetch: config.prefetch, consumerTag, fault: config.fault });

  await closed;
  current = null;
}

async function main(): Promise<void> {
  log('worker.starting', { fault: config.fault });
  while (!stopping) {
    const startedAt = Date.now();
    try {
      await runSession();
    } catch (error) {
      log('worker.session-error', { error: error instanceof Error ? error.message : String(error) });
    }
    if (!stopping) {
      // Reconnexion avec attente croissante (1 s, 2 s, 4 s... 10 s) : pas de boucle rapide. Une session qui a tenu > 10 s remet à zéro.
      failures = Date.now() - startedAt > 10_000 ? 1 : failures + 1;
      const delay = Math.min(10_000, 1_000 * 2 ** Math.min(failures - 1, 4));
      log('worker.reconnecting', { inMs: delay });
      await sleep(delay);
    }
  }
}

async function shutdown(signal: string, logger: Logger = log): Promise<void> {
  if (stopping) return;
  stopping = true;
  logger('worker.stopping', { signal, inFlight: inFlight.size });
  await Promise.race([Promise.allSettled([...inFlight]), sleep(5_000)]);
  // Fermer la connexion remet en queue les messages non acquittés ; ils seront re-livrés (redelivered=true).
  await current?.close().catch(() => undefined);
  await pool.end().catch(() => undefined);
  process.exit(0);
}
process.on('SIGINT', () => void shutdown('SIGINT'));
process.on('SIGTERM', () => void shutdown('SIGTERM'));

await main();
