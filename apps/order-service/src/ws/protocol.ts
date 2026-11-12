import type { WebSocket } from 'ws';
import type { OrderStatusChanged } from '../services/order-events.js';

/** Messages client -> serveur. L'identité n'y figure JAMAIS : elle vient de la session serveur. */
export type ClientMessage = { type: 'SUBSCRIBE'; orderId: string };

export type ServerMessage =
  | { type: 'SUBSCRIBED'; orderId: string }
  | { type: 'ORDER_STATUS_CHANGED'; orderId: string; sequence: number; status: OrderStatusChanged['status'] }
  | { type: 'ERROR'; code: 'FORBIDDEN_SUBSCRIPTION' | 'INVALID_MESSAGE' | 'NOT_IMPLEMENTED' };

/** Taille maximale d'un message entrant (octets) ; au-delà, `ws` ferme la connexion (code 1009). */
export const MAX_MESSAGE_BYTES = 1024;

const ORDER_ID = /^[A-Za-z0-9-]{1,32}$/;

/** Validation à l'exécution d'un message brut. Retourne null si JSON invalide, type inconnu ou forme incorrecte. */
export function parseClientMessage(raw: Buffer | ArrayBuffer | Buffer[], isBinary: boolean): ClientMessage | null {
  if (isBinary) return null;
  let value: unknown;
  try {
    value = JSON.parse(Buffer.isBuffer(raw) ? raw.toString('utf8') : Buffer.concat(Array.isArray(raw) ? raw : [Buffer.from(raw)]).toString('utf8'));
  } catch {
    return null;
  }
  if (typeof value !== 'object' || value === null || Array.isArray(value)) return null;
  const { type, orderId, ...rest } = value as Record<string, unknown>;
  if (type === 'SUBSCRIBE' && typeof orderId === 'string' && ORDER_ID.test(orderId) && Object.keys(rest).length === 0) {
    return { type, orderId };
  }
  return null;
}

export function sendJson(socket: WebSocket, message: ServerMessage): void {
  if (socket.readyState === socket.OPEN) socket.send(JSON.stringify(message));
}
