import { TodoNotImplementedError } from '@infal73/messaging';
import type { Order } from '../domain/order.js';
import type { PreparationPublisher } from '../messaging/preparation-publisher.js';

/**
 * Publication de l'événement `OrderShipped` (TP5, zone B).
 *
 * Ce qui est FOURNI : le Publisher confirmé du TP4 (`PreparationPublisher.publishConfirmed` : Confirm Channel,
 * `mandatory`, détection de `basic.return`, `PublishError` typée) et le contrat `OrderShippedEvent` + noms de topologie
 * dans `@infal73/contracts` (EVENT_EXCHANGES, EVENT_ROUTING_KEYS...).
 * Ce qui est À VOUS : l'enveloppe, la publication, la gestion des issues.
 */
export interface OrderShippedPublisher {
  /**
   * Publie le fait « la commande a été expédiée ». Se résout UNIQUEMENT si le broker a confirmé une publication routée ;
   * se rejette sinon (l'appelant décide de ce que cela implique pour la commande).
   */
  publishShipped(order: Order): Promise<void>;
}

export class BrokerOrderShippedPublisher implements OrderShippedPublisher {
  constructor(
    // Utilisé par votre implémentation (B3).
    protected readonly publisher: PreparationPublisher,
    protected readonly log: (event: string, fields?: Record<string, unknown>) => void = () => undefined,
  ) {}

  async publishShipped(order: Order): Promise<void> {
    // TODO(TP5) [B2] Construire l'enveloppe OrderShipped complète (contrat : docs/CONTRAT_ORDER_SHIPPED.md, type OrderShippedEvent).
    //   Questions : d'où vient `aggregateVersion` ? Qu'est-ce qui rend `eventId` stable si ce même fait doit être republié ?
    // TODO(TP5) [B3] Publier vers l'exchange et la routing key du contrat, message persistant et `mandatory`.
    //   `publishConfirmed` ne se résout qu'après confirm ET absence de basic.return : sachez dire pourquoi un confirm seul ne suffit pas.
    // TODO(TP5) [B4] Journaliser (eventId, orderId, messageId, issue) et traiter les `PublishError` (voir leurs `kind`).
    //   Question de recette T7 : que se passe-t-il si le processus s'arrête entre l'enregistrement de la transition et cette publication ?
    void this.publisher;
    void this.log;
    void order;
    throw new TodoNotImplementedError('B2-B4', 'BrokerOrderShippedPublisher.publishShipped');
  }
}
