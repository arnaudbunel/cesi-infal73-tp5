import type { FastifyPluginAsync, FastifyRequest } from 'fastify';
import { PROBLEM_TYPES, sendProblem } from '../problems.js';
import { canViewOrder } from './permissions.js';
import { represent } from '../routes/orders.routes.js';
import type { OrdersService } from '../services/orders.service.js';

/**
 * Authentification PÉDAGOGIQUE locale : deux identités fixes, sans mot de passe.
 * L'identité est portée par un cookie SIGNÉ côté serveur (HttpOnly, SameSite=Strict) ; le serveur
 * ne lit JAMAIS une identité fournie dans un message WebSocket ou un corps JSON.
 * Ce mécanisme n'est enregistré que si NODE_ENV != production (voir app.ts).
 * En production : vraie authentification (OIDC...), cookie `Secure`, TLS/WSS.
 */
export const DEMO_USERS = ['alice', 'bob'] as const;
export type DemoUser = (typeof DEMO_USERS)[number];
export const SESSION_COOKIE = 'demo_session';

function isDemoUser(value: unknown): value is DemoUser {
  return typeof value === 'string' && (DEMO_USERS as readonly string[]).includes(value);
}

/** Identité vérifiée (signature valide) de la requête, ou null. Synchrone : utilisable dans un handler WS. */
export function resolveSession(request: FastifyRequest): DemoUser | null {
  const raw = request.cookies[SESSION_COOKIE];
  if (!raw) return null;
  const unsigned = request.unsignCookie(raw);
  return unsigned.valid && isDemoUser(unsigned.value) ? unsigned.value : null;
}

export function demoAuthRoutes(service: OrdersService): FastifyPluginAsync {
  return async (app) => {
  app.post<{ Body: { user: DemoUser } }>(
    '/api/demo/login',
    {
      schema: {
        body: {
          type: 'object',
          required: ['user'],
          additionalProperties: false,
          properties: { user: { type: 'string', enum: [...DEMO_USERS] } },
        },
      },
    },
    async (request, reply) => {
      reply.setCookie(SESSION_COOKIE, request.body.user, {
        signed: true,
        httpOnly: true,
        sameSite: 'strict',
        path: '/',
      });
      return { user: request.body.user };
    },
  );

  app.post('/api/demo/logout', async (_request, reply) => {
    reply.clearCookie(SESSION_COOKIE, { path: '/' });
    return reply.code(204).send();
  });

  app.get('/api/demo/me', async (request, reply) => {
    const user = resolveSession(request);
    if (!user) return sendProblem(request, reply, 401, PROBLEM_TYPES.unauthenticated, 'No valid demo session.');
    return { user };
  });

  /**
   * Façade de lecture AUTORISÉE pour le client de démonstration : même représentation que
   * `GET /api/orders/:id`, mais réservée au propriétaire. Commande inexistante et commande d'autrui
   * donnent la même réponse 404 (aucune révélation d'existence).
   * ATTENTION : l'ancien `GET /api/orders/:id` (TP1/TP2) reste ouvert à tous, pour ne pas casser le
   * contrat historique. Il n'est PAS acceptable dans un système multi-utilisateur en production :
   * il faudrait authentifier toutes les routes et appliquer `canViewOrder` partout.
   */
  app.get<{ Params: { id: string } }>('/api/demo/orders/:id', async (request, reply) => {
    const user = resolveSession(request);
    if (!user) return sendProblem(request, reply, 401, PROBLEM_TYPES.unauthenticated, 'No valid demo session.');
    const order = await service.get(request.params.id);
    if (!order || !canViewOrder(user, order)) {
      return sendProblem(request, reply, 404, PROBLEM_TYPES.orderNotFound, `Order ${request.params.id} does not exist.`);
    }
    return represent(order);
  });
  };
}
