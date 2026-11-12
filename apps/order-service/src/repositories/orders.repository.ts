import { canTransition } from '../domain/order.js';
import type { NewOrderInput, Order, OrderStatus } from '../domain/order.js';

function seedOrder(id: string, status: OrderStatus, customerId: string, ownerId: string, productId: string, quantity = 1): Order {
  const date = '2026-01-01T09:00:00.000Z';
  return { id, customerId, items: [{ productId, quantity }], status, ownerId, sequence: 0, createdAt: date, updatedAt: date };
}

const DEFAULT_OWNER = 'alice';

export type TransitionOutcome =
  | { kind: 'ok'; order: Order }
  | { kind: 'not-found' }
  | { kind: 'illegal'; from: OrderStatus };

/**
 * Stockage en mémoire : chaque instance repart des mêmes données initiales.
 * L'interface est asynchrone pour rester remplaçable par une vraie base (TP suivants).
 */
export class OrdersRepository {
  private readonly orders = new Map<string, Order>();
  private nextId = 105;

  constructor() {
    this.seed();
  }

  /** Données initiales du fil rouge (aussi utilisées par la remise à zéro de laboratoire du TP5). */
  private seed(): void {
    this.nextId = 105;
    this.orders.clear();
    for (const order of [
      // Propriétaires (identités démo) : alice = ORD-101/102/103, bob = ORD-104.
      seedOrder('ORD-101', 'CREATED', 'C100', 'alice', 'P1'),
      seedOrder('ORD-102', 'CONFIRMED', 'C101', 'alice', 'P12', 2),
      seedOrder('ORD-103', 'SHIPPED', 'C102', 'alice', 'P3'),
      seedOrder('ORD-104', 'CANCELLED', 'C103', 'bob', 'P4'),
    ]) {
      this.orders.set(order.id, order);
    }
  }

  /** Laboratoire uniquement : revient exactement aux commandes initiales (ORD-102 CONFIRMED, ORD-103 SHIPPED...). */
  async resetToSeed(): Promise<void> {
    this.seed();
  }

  async findAll(): Promise<Order[]> {
    return [...this.orders.values()].map((o) => structuredClone(o));
  }

  async findById(id: string): Promise<Order | null> {
    const order = this.orders.get(id);
    return order ? structuredClone(order) : null;
  }

  /** Attribution d'identifiant et insertion en une seule étape (équivalent d'un INSERT ... RETURNING). */
  async create(input: NewOrderInput, ownerId: string = DEFAULT_OWNER): Promise<Order> {
    const id = `ORD-${this.nextId++}`;
    const now = new Date().toISOString();
    const order: Order = {
      id,
      customerId: input.customerId,
      items: structuredClone(input.items),
      status: 'CREATED',
      ownerId, // identité de session si présente, sinon alice (règle démo, déterministe)
      sequence: 0,
      createdAt: now,
      updatedAt: now,
    };
    this.orders.set(id, order);
    return structuredClone(order);
  }

  /**
   * Lecture + contrôle de la transition + écriture SANS point de suspension entre les trois :
   * en mono-thread, cette section est atomique. C'est l'équivalent en mémoire de
   * `UPDATE orders SET status = $to WHERE id = $id AND status IN (...)`.
   * Deux demandes concurrentes ne peuvent donc pas valider la même transition.
   */
  async transition(id: string, target: OrderStatus): Promise<TransitionOutcome> {
    const order = this.orders.get(id);
    if (!order) return { kind: 'not-found' };
    if (!canTransition(order.status, target)) return { kind: 'illegal', from: order.status };
    order.status = target;
    order.sequence += 1; // une seule fois, uniquement ici : transition effectivement enregistrée
    order.updatedAt = new Date().toISOString();
    return { kind: 'ok', order: structuredClone(order) };
  }
}
