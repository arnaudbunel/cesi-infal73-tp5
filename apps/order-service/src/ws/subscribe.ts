import type { WebSocket } from 'ws';
import { canViewOrder } from '../auth/permissions.js';
import type { OrdersService } from '../services/orders.service.js';
import { sendJson } from './protocol.js';
import type { OrderSubscriptions } from './subscriptions.js';

export interface SubscribeContext {
  service: OrdersService;
  subscriptions: OrderSubscriptions;
  /** Identité vérifiée par le serveur (cookie signé) : seule source d'identité autorisée. */
  userId: string;
}

/**
 * SUBSCRIBE déjà validé syntaxiquement. Les droits sont revérifiés à CHAQUE abonnement (y compris après
 * reconnexion). Commande inexistante et commande interdite : réponse strictement identique, pour ne pas
 * révéler l'existence d'une commande (ni son statut, ni son propriétaire).
 */
export async function handleSubscribe(ctx: SubscribeContext, socket: WebSocket, orderId: string): Promise<void> {
  const order = await ctx.service.get(orderId);
  if (!order || !canViewOrder(ctx.userId, order)) {
    return sendJson(socket, { type: 'ERROR', code: 'FORBIDDEN_SUBSCRIPTION' });
  }
  if (socket.readyState !== socket.OPEN) return; // fermée pendant la vérification : ne rien enregistrer
  ctx.subscriptions.subscribe(orderId, socket);
  sendJson(socket, { type: 'SUBSCRIBED', orderId });
}
