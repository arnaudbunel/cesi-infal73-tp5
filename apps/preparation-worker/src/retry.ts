import { EXCHANGES, MAX_RETRIES, ROUTING_KEYS } from '@infal73/contracts';
import type { PreparationCommand } from '@infal73/contracts';
import { ConfirmedPublisher } from '@infal73/messaging';
import type { ConfirmChannelLike } from '@infal73/messaging';
import type { Logger } from './logger.js';

export interface RetryScheduler {
  /**
   * Republie `command` (operationId INCHANGÉ, attempt + 1) vers `orders.retry`, et ne se résout qu'après confirmation
   * (et absence de basic.return). Se rejette sinon : l'original ne doit alors PAS être acquitté.
   */
  scheduleRetry(command: PreparationCommand, context: { correlationId?: string }): Promise<void>;
}

const RETRY_KEYS = [ROUTING_KEYS.retry1, ROUTING_KEYS.retry2, ROUTING_KEYS.retry3] as const;

/** attempt courant -> clé de retry : 0 -> retry.1 (2 s), 1 -> retry.2 (5 s), 2 -> retry.3 (10 s). */
export function retryRouteFor(currentAttempt: number): string {
  const key = RETRY_KEYS[currentAttempt];
  if (currentAttempt < 0 || currentAttempt >= MAX_RETRIES || key === undefined) {
    throw new RangeError(`no retry route for attempt=${currentAttempt} (max ${MAX_RETRIES} retries)`);
  }
  return key;
}

export class ConfirmedRetryScheduler implements RetryScheduler {
  private readonly publisher: ConfirmedPublisher;

  constructor(
    retryChannel: ConfirmChannelLike,
    private readonly log: Logger,
  ) {
    this.publisher = new ConfirmedPublisher(async () => retryChannel, { log: (event, fields) => this.log(event, fields) });
  }

  async scheduleRetry(command: PreparationCommand, context: { correlationId?: string }): Promise<void> {
    const routingKey = retryRouteFor(command.attempt); // lève si attempt >= 3 : l'appelant doit alors passer en DLQ
    const next: PreparationCommand = { ...command, attempt: command.attempt + 1 };
    await this.publisher.publishConfirmed({
      exchange: EXCHANGES.retry,
      routingKey,
      payload: next,
      mandatory: true,
      persistent: true,
      ...(context.correlationId === undefined ? {} : { correlationId: context.correlationId }),
    });
    this.log('retried', { operationId: next.operationId, orderId: next.orderId, attempt: next.attempt, routingKey });
  }
}
