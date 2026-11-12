import type { OrderStatus } from '../domain/order.js';

/** Événement métier publié APRÈS enregistrement d'une transition effective (jamais sur refus). */
export interface OrderStatusChanged {
  orderId: string;
  sequence: number;
  status: OrderStatus;
}

type Listener = (event: OrderStatusChanged) => void;

/**
 * Point de publication interne, mono-instance et en mémoire : pas de durabilité, pas de rejeu,
 * pas de diffusion entre instances. Un listener qui échoue ne doit jamais faire échouer la transition.
 */
export class OrderEvents {
  private readonly listeners = new Set<Listener>();

  subscribe(listener: Listener): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  emit(event: OrderStatusChanged): void {
    for (const listener of [...this.listeners]) {
      try {
        listener(event);
      } catch {
        // isolé volontairement : la transition est déjà enregistrée
      }
    }
  }
}
