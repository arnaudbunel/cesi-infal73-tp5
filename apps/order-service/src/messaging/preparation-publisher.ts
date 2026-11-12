import { ConfirmedPublisher } from '@infal73/messaging';
import type { PublishInput, PublishReceipt, PublisherLog } from '@infal73/messaging';
import type { BrokerConnection } from './broker-connection.js';

export type { PublishInput, PublishReceipt };

/**
 * Contrat de `publishConfirmed` :
 *  - se résout UNIQUEMENT après confirmation du broker ET absence de `basic.return` (message routé) ;
 *  - sinon se rejette avec une `PublishError` (voir @infal73/messaging) ;
 *  - le booléen de `channel.publish()` (back-pressure) n'est PAS une confirmation.
 */
export interface PreparationPublisher {
  publishConfirmed(input: PublishInput): Promise<PublishReceipt>;
}

export class RabbitPreparationPublisher implements PreparationPublisher {
  private readonly publisher: ConfirmedPublisher;

  constructor(broker: BrokerConnection, log?: PublisherLog) {
    this.publisher = new ConfirmedPublisher(() => broker.getConfirmChannel(), log ? { log } : {});
  }

  publishConfirmed(input: PublishInput): Promise<PublishReceipt> {
    return this.publisher.publishConfirmed(input);
  }
}
