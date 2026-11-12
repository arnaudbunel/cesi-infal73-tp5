import { EVENT_EXCHANGES, EVENT_MAX_RETRIES, EVENT_RETRY_COUNT_HEADER } from '@infal73/contracts';
import type { EventConsumerKey } from '@infal73/contracts';
import { EVENT_ERROR_CIRCUITS } from '@infal73/contracts';
import { ConfirmedPublisher } from '@infal73/messaging';
import type { ConfirmChannelLike } from '@infal73/messaging';
import type { Logger } from '../logger.js';

export interface EventRetryScheduler {
  /**
   * Republie le MÊME corps (donc le même eventId) dans le circuit de retry de CE consommateur, avec `x-retry-count + 1`.
   * Ne se résout qu'après confirmation (et absence de basic.return) ; se rejette sinon : l'original ne doit alors PAS être acquitté.
   */
  scheduleRetry(body: unknown, context: { retryCount: number; correlationId?: string }): Promise<void>;
}

/**
 * Circuit : `business.retry` --[<consumer>.retry]--> <consumer>.events.retry (TTL fixe) --DLX business.requeue [<consumer>]--> <consumer>.events.
 * Les files et les clés sont propres à chaque consommateur : un retry de Statistiques n'atteint jamais Notifications ni Audit.
 */
export class ConfirmedEventRetryScheduler implements EventRetryScheduler {
  private readonly publisher: ConfirmedPublisher;

  constructor(
    retryChannel: ConfirmChannelLike,
    private readonly consumer: EventConsumerKey,
    private readonly log: Logger,
  ) {
    this.publisher = new ConfirmedPublisher(async () => retryChannel, { log: (event, fields) => this.log(event, fields) });
  }

  async scheduleRetry(body: unknown, context: { retryCount: number; correlationId?: string }): Promise<void> {
    if (context.retryCount >= EVENT_MAX_RETRIES) throw new RangeError(`retries exhausted (max ${EVENT_MAX_RETRIES}): dead-letter instead`);
    const next = context.retryCount + 1;
    await this.publisher.publishConfirmed({
      exchange: EVENT_EXCHANGES.retry,
      routingKey: EVENT_ERROR_CIRCUITS[this.consumer].retryKey,
      payload: body,
      mandatory: true,
      persistent: true,
      headers: { [EVENT_RETRY_COUNT_HEADER]: next },
      ...(context.correlationId === undefined ? {} : { correlationId: context.correlationId }),
    });
  }
}
