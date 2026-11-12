import { EVENT_MAX_RETRIES, EVENT_RETRY_COUNT_HEADER } from '@infal73/contracts';
import type { Channel, ConsumeMessage } from 'amqplib';
import { isTemporary } from '../errors.js';
import type { Logger } from '../logger.js';
import type { EventRetryScheduler } from './retry.js';

/**
 * Ce que reçoit un handler pour UNE livraison. Une livraison doit se terminer par EXACTEMENT un des trois verbes :
 *
 *   ack()              après l'effet métier validé (COMMIT PostgreSQL), ou si l'événement était déjà traité ;
 *   retryLater(reason) erreur TEMPORAIRE : republication confirmée dans le circuit de retry de CE consommateur, puis ack ;
 *                      au-delà de la limite de retries, la livraison part en DLQ ;
 *   deadLetter(reason) erreur DÉFINITIVE : nack(requeue=false) -> DLX -> DLQ de CE consommateur.
 *
 * Il n'existe volontairement aucun « nack avec requeue » : une boucle de redelivery immédiate et infinie est interdite.
 * Une livraison qu'un handler laisse sans issue n'est ni acquittée en silence ni remise en queue : le runtime la met en DLQ
 * et le journal l'indique (`delivery.unsettled`).
 */
export interface DeliveryContext {
  readonly consumer: string;
  readonly queue: string;
  readonly exchange: string;
  readonly routingKey: string;
  readonly body: Buffer;
  readonly messageId: string | undefined;
  readonly correlationId: string | undefined;
  readonly deliveryTag: number;
  readonly redelivered: boolean;
  /** Retries déjà effectués pour ce fait dans CE circuit (en-tête `x-retry-count`, 0 à la première livraison). */
  readonly retryCount: number;
  readonly maxRetries: number;
  /** Journal JSON déjà préfixé (consommateur, deliveryTag, messageId). */
  readonly log: Logger;
  readonly settled: boolean;
  ack(): void;
  retryLater(reason: string): Promise<void>;
  deadLetter(reason: string): void;
}

export interface DeliveryDeps {
  channel: Pick<Channel, 'ack' | 'nack'>;
  retries: EventRetryScheduler;
  consumerName: string;
  queue: string;
  log: Logger;
  /** Ferme la session AMQP : les messages non acquittés sont remis en queue (état indéterminé après un retry non confirmé). */
  abortSession: (reason: string) => void;
}

export class Delivery implements DeliveryContext {
  readonly consumer: string;
  readonly queue: string;
  readonly exchange: string;
  readonly routingKey: string;
  readonly body: Buffer;
  readonly messageId: string | undefined;
  readonly correlationId: string | undefined;
  readonly deliveryTag: number;
  readonly redelivered: boolean;
  readonly retryCount: number;
  readonly maxRetries = EVENT_MAX_RETRIES;
  readonly log: Logger;
  private done = false;

  constructor(
    private readonly deps: DeliveryDeps,
    private readonly msg: ConsumeMessage,
  ) {
    this.consumer = deps.consumerName;
    this.queue = deps.queue;
    this.exchange = msg.fields.exchange;
    this.routingKey = msg.fields.routingKey;
    this.body = msg.content;
    this.messageId = typeof msg.properties.messageId === 'string' ? msg.properties.messageId : undefined;
    this.correlationId = typeof msg.properties.correlationId === 'string' ? msg.properties.correlationId : undefined;
    this.deliveryTag = msg.fields.deliveryTag;
    this.redelivered = msg.fields.redelivered;
    const header: unknown = msg.properties.headers?.[EVENT_RETRY_COUNT_HEADER];
    this.retryCount = typeof header === 'number' && Number.isInteger(header) && header >= 0 ? header : 0;
    const base = { consumer: this.consumer, deliveryTag: this.deliveryTag, messageId: this.messageId, retryCount: this.retryCount };
    this.log = (event, fields = {}) => deps.log(event, { ...base, ...fields });
  }

  get settled(): boolean {
    return this.done;
  }

  private settle(verb: string): void {
    if (this.done) throw new Error(`delivery ${this.deliveryTag} already settled (cannot ${verb})`);
    this.done = true;
  }

  ack(): void {
    this.settle('ack');
    this.deps.channel.ack(this.msg);
    this.log('acked');
  }

  deadLetter(reason: string): void {
    this.settle('dead-letter');
    this.deps.channel.nack(this.msg, false, false);
    this.log('dead_lettered', { reason });
  }

  async retryLater(reason: string): Promise<void> {
    if (this.done) throw new Error(`delivery ${this.deliveryTag} already settled (cannot retry)`);
    if (this.retryCount >= EVENT_MAX_RETRIES) {
      this.deadLetter(`retries exhausted after ${this.retryCount}: ${reason}`);
      return;
    }
    let body: unknown;
    try {
      body = JSON.parse(this.body.toString('utf8'));
    } catch {
      this.deadLetter(`body is not JSON, cannot be retried: ${reason}`);
      return;
    }
    this.log('failed_temporarily', { reason });
    try {
      await this.deps.retries.scheduleRetry(body, { retryCount: this.retryCount, ...(this.correlationId === undefined ? {} : { correlationId: this.correlationId }) });
    } catch (error) {
      // Retry non confirmé / incertain : on n'ACK PAS l'original ; la fermeture de session force une redelivery contrôlée.
      this.done = true;
      this.log('retry_publish_failed', { reason: error instanceof Error ? error.message : String(error) });
      this.deps.abortSession('retry publication not confirmed');
      return;
    }
    this.done = true;
    this.deps.channel.ack(this.msg);
    this.log('retry_scheduled', { reason, nextRetryCount: this.retryCount + 1 });
    this.log('acked', { outcome: 'retried' });
  }
}

/** Issue appliquée à une erreur non traitée par le handler : temporaire -> retry borné, sinon DLQ. Jamais d'ACK. */
export async function settleAfterError(delivery: Delivery, error: unknown): Promise<void> {
  if (delivery.settled) return;
  const reason = error instanceof Error ? `${error.name}: ${error.message}` : String(error);
  if (isTemporary(error)) await delivery.retryLater(reason);
  else delivery.deadLetter(reason);
}
