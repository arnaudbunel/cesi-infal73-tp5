import type { FastifyPluginAsync } from 'fastify';
import type { WebSocket } from 'ws';
import { resolveSession } from '../auth/demo-auth.js';
import { PROBLEM_TYPES, sendProblem } from '../problems.js';
import type { OrdersService } from '../services/orders.service.js';
import { parseClientMessage, sendJson } from './protocol.js';
import { handleSubscribe } from './subscribe.js';
import type { OrderSubscriptions } from './subscriptions.js';

function originHost(origin: string): string | null {
  try {
    return new URL(origin).host;
  } catch {
    return null; // Origin invalide ou « null » : refusé
  }
}

/**
 * Handshake `GET /ws/orders`. Prérequis : @fastify/websocket et @fastify/cookie déjà enregistrés (voir app.ts).
 *  - session absente/invalide -> 401 AVANT la mise à niveau (aucune socket créée) ;
 *  - en-tête Origin présent mais d'un autre site -> 403 (protection contre le détournement depuis un autre site).
 * Les listeners `message` et `close` sont attachés SYNCHRONEMENT dans le handler.
 */
export function ordersWsRoutes(service: OrdersService, subscriptions: OrderSubscriptions, allowedOrigins: string[] = []): FastifyPluginAsync {
  return async (app) => {
    app.get(
      '/ws/orders',
      {
        websocket: true,
        preValidation: async (request, reply) => {
          const origin = request.headers.origin;
          if (origin !== undefined && originHost(origin) !== request.headers.host && !allowedOrigins.includes(origin)) {
            return sendProblem(request, reply, 403, PROBLEM_TYPES.forbiddenOrigin, 'Cross-origin WebSocket connections are refused.');
          }
          if (!resolveSession(request)) {
            return sendProblem(request, reply, 401, PROBLEM_TYPES.unauthenticated, 'A valid demo session is required.');
          }
        },
      },
      (socket: WebSocket, request) => {
        const userId = resolveSession(request);
        if (!userId) return socket.close(1008, 'unauthenticated');

        socket.on('message', (raw: Buffer | ArrayBuffer | Buffer[], isBinary: boolean) => {
          const message = parseClientMessage(raw, isBinary);
          if (!message) return sendJson(socket, { type: 'ERROR', code: 'INVALID_MESSAGE' });
          handleSubscribe({ service, subscriptions, userId }, socket, message.orderId).catch((error: unknown) => {
            request.log.error({ err: error }, 'subscribe failed');
            sendJson(socket, { type: 'ERROR', code: 'INVALID_MESSAGE' }); // pas de détail interne vers le client
          });
        });
        socket.on('close', () => subscriptions.removeSocket(socket));
        socket.on('error', (error: Error) => {
          request.log.warn({ err: error }, 'websocket error');
          subscriptions.removeSocket(socket); // `close` suit normalement, mais on ne s'y fie pas
        });
      },
    );
  };
}
