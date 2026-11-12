import type { WebSocket } from 'ws';
import type { OrderStatusChanged } from '../services/order-events.js';
import { sendJson } from './protocol.js';

/**
 * Registre d'abonnements EN MÉMOIRE, mono-instance : pas de durabilité, pas de rejeu, pas de diffusion
 * entre instances. Deux index cohérents :
 *  - byOrder  : orderId -> sockets abonnées (diffusion ciblée) ;
 *  - bySocket : socket -> commandes suivies (nettoyage en O(abonnements de la socket)).
 * Les Set rendent les abonnements multiples idempotents (aucun doublon, donc un seul envoi par événement).
 */
export class OrderSubscriptions {
  private readonly byOrder = new Map<string, Set<WebSocket>>();
  private readonly bySocket = new Map<WebSocket, Set<string>>();

  subscribe(orderId: string, socket: WebSocket): void {
    let sockets = this.byOrder.get(orderId);
    if (!sockets) this.byOrder.set(orderId, (sockets = new Set()));
    sockets.add(socket);
    let orders = this.bySocket.get(socket);
    if (!orders) this.bySocket.set(socket, (orders = new Set()));
    orders.add(orderId);
  }

  removeSocket(socket: WebSocket): void {
    for (const orderId of this.bySocket.get(socket) ?? []) {
      const sockets = this.byOrder.get(orderId);
      sockets?.delete(socket);
      if (sockets?.size === 0) this.byOrder.delete(orderId); // pas de Set vide résiduel
    }
    this.bySocket.delete(socket);
  }

  publish(event: OrderStatusChanged): void {
    for (const socket of [...(this.byOrder.get(event.orderId) ?? [])]) {
      try {
        sendJson(socket, { type: 'ORDER_STATUS_CHANGED', orderId: event.orderId, sequence: event.sequence, status: event.status });
      } catch {
        this.removeSocket(socket); // une socket défaillante ne doit pas empêcher les autres de recevoir
      }
    }
  }

  /** Vide le registre (arrêt du service). */
  clear(): void {
    this.byOrder.clear();
    this.bySocket.clear();
  }

  /** Observation (tests et diagnostic) : nombre de sockets et d'index non vides. */
  stats(): { sockets: number; orders: number } {
    return { sockets: this.bySocket.size, orders: this.byOrder.size };
  }
}
