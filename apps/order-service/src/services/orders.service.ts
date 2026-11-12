import type { NewOrderInput, Order, OrderStatus } from '../domain/order.js';
import type { OrderShippedPublisher } from '../events/order-shipped-publisher.js';
import type { LabHooks } from '../lab.js';
import type { Failpoints } from '../lab-failpoints.js';
import type { OrdersRepository } from '../repositories/orders.repository.js';
import { OrderEvents } from './order-events.js';

export type OrderResult =
  | { kind: 'ok'; order: Order }
  | { kind: 'not-found' }
  | { kind: 'conflict'; from: OrderStatus };

export type CreateResult =
  | { kind: 'ok'; order: Order; replayed: boolean }
  | { kind: 'key-conflict' };

/**
 * Entrée d'idempotence. `result` est une promesse : elle représente à la fois
 * « opération en cours » (non résolue) et « résultat enregistré » (résolue).
 * Les entrées ne sont JAMAIS supprimées après un effet métier (voir `create`).
 */
interface IdempotencyEntry {
  fingerprint: string;
  result: Promise<Order>;
}

/**
 * Empreinte du contenu logique : seuls les champs métier, dans un ordre fixe.
 * L'ordre des lignes est significatif (tableau ordonné) ; l'ordre des clés JSON ne l'est pas.
 */
function fingerprintOf(input: NewOrderInput): string {
  return JSON.stringify({
    customerId: input.customerId,
    items: input.items.map((i) => ({ productId: i.productId, quantity: i.quantity })),
  });
}

export class OrdersService {
  /**
   * Registre d'idempotence en mémoire : mono-processus, perdu au redémarrage, non partagé
   * entre instances, non borné (pas d'expiration). Portée de la clé : globale.
   */
  private readonly idempotency = new Map<string, IdempotencyEntry>();

  constructor(
    private readonly repository: OrdersRepository,
    private readonly lab: LabHooks = {},
    readonly events: OrderEvents = new OrderEvents(),
    /** TP5 : publication des événements métier et failpoints de laboratoire (tous deux optionnels). */
    private readonly integration: { shippedPublisher?: OrderShippedPublisher; failpoints?: Failpoints } = {},
  ) {}

  list(): Promise<Order[]> {
    return this.repository.findAll();
  }

  get(id: string): Promise<Order | null> {
    return this.repository.findById(id);
  }

  async create(key: string, input: NewOrderInput, ownerId?: string): Promise<CreateResult> {
    const fingerprint = fingerprintOf(input);

    // 1) Consultation. Aucune suspension entre la consultation et la réservation (étape 2).
    const known = this.idempotency.get(key);
    if (known) {
      if (known.fingerprint !== fingerprint) return { kind: 'key-conflict' };
      await this.lab.afterIdempotencyCheck?.(key, 'known');
      // Soit le résultat est déjà là, soit on attend l'opération en cours : jamais de 2e création.
      return { kind: 'ok', order: structuredClone(await known.result), replayed: true };
    }

    // 2) Réservation atomique AVANT tout await : une promesse « en cours » est publiée.
    const pending = Promise.withResolvers<Order>();
    pending.promise.catch(() => undefined); // évite un unhandledRejection si personne n'attend
    this.idempotency.set(key, { fingerprint, result: pending.promise });

    // 3) Travail asynchrone. Un échec ICI survient avant tout effet métier confirmé :
    //    on libère la clé pour autoriser un nouvel essai, et on propage l'erreur aux éventuels
    //    requérants en attente.
    let order: Order;
    try {
      await this.lab.afterIdempotencyCheck?.(key, 'reserved');
      order = await this.repository.create(input, ownerId);
    } catch (error) {
      this.idempotency.delete(key);
      pending.reject(error);
      throw error;
    }

    // 4) Le résultat est enregistré. À partir d'ici l'entrée n'est plus jamais effacée.
    pending.resolve(order);

    // 5) Étape post-commit : si elle échoue (ou si le client abandonne), la trace subsiste ;
    //    un retry avec la même clé recevra le résultat initial.
    await this.lab.afterMutation?.(order);
    return { kind: 'ok', order: structuredClone(order), replayed: false };
  }

  async changeStatus(id: string, target: OrderStatus): Promise<OrderResult> {
    const outcome = await this.repository.transition(id, target);
    if (outcome.kind === 'illegal') return { kind: 'conflict', from: outcome.from };
    if (outcome.kind === 'ok') {
      // Unique point de publication : la transition est déjà enregistrée (sequence incluse).
      this.events.emit({ orderId: outcome.order.id, sequence: outcome.order.sequence, status: outcome.order.status });
      // Failpoint de laboratoire (FOURNI, désarmé par défaut) : transition enregistrée, rien encore publié (test T7).
      if (outcome.order.status === 'SHIPPED') this.integration.failpoints?.hit('AFTER_SHIP_COMMIT_BEFORE_EVENT_PUBLISH', { orderId: outcome.order.id });
      // TODO(TP5) [B1] Déclencher la publication de OrderShipped via `this.integration.shippedPublisher` (peut être absent).
      //   Seulement pour une transition CONFIRMED -> SHIPPED effectivement enregistrée (jamais sur refus ou rejeu).
      //   Décidez et justifiez : que renvoie l'API si la publication échoue alors que la commande est déjà SHIPPED ?
    }
    return outcome;
  }

  /** Laboratoire (TP5, LAB_MODE=1) : restaure les commandes initiales ORD-101..104. Les idempotences de création sont conservées. */
  resetOrdersForLab(): Promise<void> {
    return this.repository.resetToSeed();
  }

  cancel(id: string): Promise<OrderResult> {
    return this.changeStatus(id, 'CANCELLED');
  }
}
