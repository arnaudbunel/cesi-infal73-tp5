import amqp from 'amqplib';
import type { ChannelModel, ConsumeMessage } from 'amqplib';
import type { EventWorkerConfig } from '../config.js';
import { TodoNotImplementedError } from '../errors.js';
import type { Logger } from '../logger.js';
import { Delivery, settleAfterError } from './delivery.js';
import type { DeliveryContext } from './delivery.js';
import { ConfirmedEventRetryScheduler } from './retry.js';

/** Un traitement d'événement. Voir `DeliveryContext` pour les trois issues possibles d'une livraison. */
export interface EventHandler {
  handle(ctx: DeliveryContext): Promise<void>;
}

/** Code de sortie d'un worker dont la zone TODO(TP5) n'est pas encore réalisée (le message reste dans la queue, non consommé). */
export const EXIT_TODO_NOT_IMPLEMENTED = 3;

/**
 * Applique la discipline d'issue à UNE livraison (exporté pour être testé sans broker) :
 *   - TodoNotImplementedError : ni ACK ni DLQ, on délègue à `onTodo` (arrêt du worker) ;
 *   - autre erreur non traitée : temporaire -> retry borné, sinon DLQ (jamais d'ACK, jamais de requeue) ;
 *   - retour sans issue : DLQ, bruyamment (jamais d'ACK silencieux, jamais de requeue infini).
 */
export async function processDelivery(handler: EventHandler, delivery: Delivery, onTodo: (error: TodoNotImplementedError) => void): Promise<void> {
  try {
    await handler.handle(delivery);
  } catch (error) {
    if (error instanceof TodoNotImplementedError) {
      onTodo(error);
      return;
    }
    delivery.log('delivery.handler-error', { error: error instanceof Error ? `${error.name}: ${error.message}` : String(error) });
    await settleAfterError(delivery, error);
    return;
  }
  if (!delivery.settled) {
    delivery.log('delivery.unsettled', { hint: 'handler returned without ack(), retryLater() or deadLetter()' });
    delivery.deadLetter('handler returned without settling the delivery');
  }
}

const sleep = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));

/**
 * Boucle de consommation FOURNIE (comme `apps/preparation-worker/src/server.ts` du TP4) : connexion, vérification passive de la
 * queue, prefetch, ACK MANUEL, reconnexion avec attente croissante, arrêt propre sur SIGINT/SIGTERM.
 * Elle ne contient aucune logique métier : celle-ci est dans le handler du consommateur.
 */
export class ConsumerRuntime {
  private stopping = false;
  private failures = 0;
  private current: ChannelModel | null = null;
  private readonly inFlight = new Set<Promise<void>>();

  constructor(
    private readonly config: EventWorkerConfig,
    private readonly handler: EventHandler,
    private readonly log: Logger,
    private readonly cleanup: () => Promise<void>,
  ) {}

  async run(): Promise<void> {
    process.on('SIGINT', () => void this.shutdown('SIGINT', 0));
    process.on('SIGTERM', () => void this.shutdown('SIGTERM', 0));
    this.log('worker.starting', { consumer: this.config.consumerName, queue: this.config.queue });
    while (!this.stopping) {
      const startedAt = Date.now();
      try {
        await this.runSession();
      } catch (error) {
        this.log('worker.session-error', { error: error instanceof Error ? error.message : String(error) });
      }
      if (!this.stopping) {
        // Reconnexion avec attente croissante (1 s ... 10 s) : pas de boucle rapide. Une session qui a tenu > 10 s remet à zéro.
        this.failures = Date.now() - startedAt > 10_000 ? 1 : this.failures + 1;
        const delay = Math.min(10_000, 1_000 * 2 ** Math.min(this.failures - 1, 4));
        this.log('worker.reconnecting', { inMs: delay });
        await sleep(delay);
      }
    }
  }

  private async runSession(): Promise<void> {
    const connection = await amqp.connect(this.config.rabbitUrl, { timeout: 3_000 });
    this.current = connection;
    connection.on('error', (error: Error) => this.log('broker.connection.error', { message: error.message }));
    const closed = new Promise<void>((resolve) => connection.once('close', () => resolve()));

    const channel = await connection.createChannel();
    const retryChannel = await connection.createConfirmChannel();
    for (const ch of [channel, retryChannel]) ch.on('error', (error: Error) => this.log('broker.channel.error', { message: error.message }));
    channel.on('close', () => void connection.close().catch(() => undefined));
    retryChannel.on('close', () => void connection.close().catch(() => undefined));

    // Vérification passive : la topologie doit exister (npm run setup / npm run infra:topology).
    await channel.checkQueue(this.config.queue);
    await channel.prefetch(this.config.prefetch);

    const retries = new ConfirmedEventRetryScheduler(retryChannel, this.config.consumer, this.log);
    const abortSession = (reason: string): void => {
      this.log('session.abort', { reason });
      void connection.close().catch(() => undefined);
    };

    const { consumerTag } = await channel.consume(
      this.config.queue,
      (msg) => {
        if (msg === null) {
          this.log('consumer.cancelled-by-broker');
          void connection.close().catch(() => undefined);
          return;
        }
        const delivery = new Delivery({ channel, retries, consumerName: this.config.consumerName, queue: this.config.queue, log: this.log, abortSession }, msg);
        delivery.log('delivery.received', this.describe(msg));
        const task = this.process(delivery)
          .catch((error: unknown) => delivery.log('delivery.handler-error', { error: error instanceof Error ? `${error.name}: ${error.message}` : String(error) }))
          .finally(() => this.inFlight.delete(task));
        this.inFlight.add(task);
      },
      { noAck: false }, // ACK manuel obligatoire
    );
    this.log('worker.ready', { consumer: this.config.consumerName, queue: this.config.queue, prefetch: this.config.prefetch, consumerTag });

    await closed;
    this.current = null;
  }

  private process(delivery: Delivery): Promise<void> {
    return processDelivery(this.handler, delivery, (error) => {
      // Zone pédagogique non réalisée : on N'acquitte PAS, on ne met PAS en DLQ. On s'arrête ; la fermeture de la connexion
      // remet le message en queue (il y reste prêt, non consommé) tant que la zone n'est pas implémentée.
      delivery.log('todo.not-implemented', { zone: error.zone, message: error.message });
      void this.shutdown('TODO_NOT_IMPLEMENTED', EXIT_TODO_NOT_IMPLEMENTED);
    });
  }

  private describe(msg: ConsumeMessage): Record<string, unknown> {
    const fields: Record<string, unknown> = {
      redelivered: msg.fields.redelivered,
      exchange: msg.fields.exchange,
      routingKey: msg.fields.routingKey,
      correlationId: msg.properties.correlationId,
      bytes: msg.content.length,
    };
    try {
      const parsed: unknown = JSON.parse(msg.content.toString('utf8'));
      if (typeof parsed === 'object' && parsed !== null) {
        const { eventId, eventType, schemaVersion, aggregateId } = parsed as Record<string, unknown>;
        Object.assign(fields, { eventId, eventType, schemaVersion, aggregateId });
      }
    } catch {
      fields['body'] = 'unparseable';
    }
    return fields;
  }

  private async shutdown(signal: string, exitCode: number): Promise<void> {
    if (this.stopping) return;
    this.stopping = true;
    this.log('worker.stopping', { signal, inFlight: this.inFlight.size });
    if (exitCode === 0) await Promise.race([Promise.allSettled([...this.inFlight]), sleep(5_000)]);
    await this.current?.close().catch(() => undefined);
    await this.cleanup().catch(() => undefined);
    process.exit(exitCode);
  }
}
