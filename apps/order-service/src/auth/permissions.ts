import type { Order } from '../domain/order.js';

/**
 * Règle d'autorisation de lecture, SÉPARÉE de l'authentification (qui est cette personne ?) :
 * un utilisateur ne consulte que les commandes dont il est propriétaire.
 * Utilisée par l'abonnement WebSocket ET par la lecture REST de la démo (`/api/demo/orders/:id`).
 */
export function canViewOrder(userId: string, order: Order): boolean {
  return order.ownerId === userId;
}
