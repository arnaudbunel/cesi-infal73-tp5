import { randomBytes } from 'node:crypto';
import cookie from '@fastify/cookie';
import websocket from '@fastify/websocket';
import Fastify from 'fastify';
import type { FastifyError, FastifyInstance } from 'fastify';
import { demoAuthRoutes } from './auth/demo-auth.js';
import { DEFAULT_ORDER_CONFIG } from './config.js';
import type { OrderServiceConfig } from './config.js';
import type { OrderShippedPublisher } from './events/order-shipped-publisher.js';
import type { LabHooks } from './lab.js';
import type { Failpoints } from './lab-failpoints.js';
import { labRoutes } from './routes/lab.routes.js';
import { PROBLEM_TYPES, sendProblem } from './problems.js';
import { OrdersRepository } from './repositories/orders.repository.js';
import { availabilityRoutes } from './routes/availability.routes.js';
import { diagnosticsRoutes } from './routes/diagnostics.routes.js';
import { ordersRoutes } from './routes/orders.routes.js';
import { preparationRoutes } from './routes/preparation.routes.js';
import type { PreparationPublisher } from './messaging/preparation-publisher.js';
import { OrdersService } from './services/orders.service.js';
import { StockClient } from './services/stock.client.js';
import { StockGrpcClient } from './grpc/stock-grpc.client.js';
import { demoWebRoutes } from './routes/demo-web.routes.js';
import type { OrderEvents } from './services/order-events.js';
import type { StockGateway } from './services/stock-gateway.js';
import { UpstreamError } from './services/upstream-errors.js';
import { MAX_MESSAGE_BYTES } from './ws/protocol.js';
import { ordersWsRoutes } from './ws/orders.ws.js';
import { OrderSubscriptions } from './ws/subscriptions.js';

export interface BuildAppOptions extends Partial<OrderServiceConfig> {
  logger?: boolean;
  /** Mode laboratoire : active les points d'injection de panne. Interdit en production. */
  labMode?: boolean;
  lab?: LabHooks;
  /** Secret de signature des cookies de session démo. Par défaut : aléatoire à chaque démarrage (aucun secret committé). */
  sessionSecret?: string;
  /** Dépôt injectable (tests, ou simulation d'une base partagée entre deux instances). */
  repository?: OrdersRepository;
  /** Origines WebSocket acceptées en plus de la même origine (laboratoire local). */
  allowedOrigins?: string[];
  /** Force l'activation des outils de démonstration. Interdit en production. */
  demoAuth?: boolean;
  /** Publication des demandes de préparation (TP4). Absent : la route répond 503 « messaging not configured ». */
  publisher?: PreparationPublisher;
  /** Publication de OrderShipped (TP5, zone B). Absent : aucune publication d'événement métier. */
  shippedPublisher?: OrderShippedPublisher;
  /** Failpoints de laboratoire (TP5, T7). Exige labMode ; interdit en production. */
  failpoints?: Failpoints;
}

/** Chaque appel crée une application neuve, avec des données initiales neuves. */
export function buildApp(options: BuildAppOptions = {}): FastifyInstance {
  const { logger = false, labMode = false, lab } = options;
  const config: OrderServiceConfig = {
    stockBaseUrl: options.stockBaseUrl ?? DEFAULT_ORDER_CONFIG.stockBaseUrl,
    stockTimeoutMs: options.stockTimeoutMs ?? DEFAULT_ORDER_CONFIG.stockTimeoutMs,
    stockBackend: options.stockBackend ?? DEFAULT_ORDER_CONFIG.stockBackend,
    stockGrpcTarget: options.stockGrpcTarget ?? DEFAULT_ORDER_CONFIG.stockGrpcTarget,
    stockGrpcDeadlineMs: options.stockGrpcDeadlineMs ?? DEFAULT_ORDER_CONFIG.stockGrpcDeadlineMs,
  };
  const production = process.env['NODE_ENV'] === 'production';
  if (production) {
    // Refus de démarrer avec les outils de démonstration actifs ou un secret de session faible.
    if (options.demoAuth === true || process.env['DEMO_AUTH'] === '1') {
      throw new Error('demo authentication is forbidden when NODE_ENV=production');
    }
    const explicitSecret = options.sessionSecret ?? process.env['SESSION_SECRET'];
    if (explicitSecret !== undefined && explicitSecret.length < 32) {
      throw new Error('SESSION_SECRET must contain at least 32 characters when NODE_ENV=production');
    }
  }

  if (labMode && process.env['NODE_ENV'] === 'production') {
    throw new Error('labMode is forbidden when NODE_ENV=production');
  }
  if (lab && !labMode) {
    throw new Error('Lab hooks require buildApp({ labMode: true })');
  }

  const app = Fastify({
    logger,
    // Validation stricte : champs inconnus refusés (et non supprimés), pas de coercion de types.
    ajv: { customOptions: { removeAdditional: false, coerceTypes: false, allErrors: true } },
  });
  if (options.failpoints && !labMode) throw new Error('failpoints require buildApp({ labMode: true })');
  const service = new OrdersService(options.repository ?? new OrdersRepository(), labMode ? lab : undefined, undefined, {
    ...(options.shippedPublisher && { shippedPublisher: options.shippedPublisher }),
    ...(options.failpoints && { failpoints: options.failpoints }),
  });
  const stock: StockGateway = config.stockBackend === 'grpc' ? new StockGrpcClient(config) : new StockClient(config);
  const subscriptions = new OrderSubscriptions();
  // Publication : chaque transition effectivement enregistrée est transmise au registre d'abonnements.
  const unsubscribeEvents = service.events.subscribe((event) => subscriptions.publish(event));
  app.addHook('onClose', async () => {
    unsubscribeEvents();
    subscriptions.clear();
    await stock.close?.();
  });

  if (labMode && lab?.onRequest) {
    const onRequest = lab.onRequest;
    app.addHook('onRequest', async (request) => {
      await onRequest({ method: request.method, url: request.url, headers: { ...request.headers } });
    });
  }

  app.setNotFoundHandler((request, reply) =>
    sendProblem(request, reply, 404, PROBLEM_TYPES.routeNotFound, `No route matches ${request.method} ${request.url.split('?')[0]}.`),
  );

  app.setErrorHandler((error: FastifyError, request, reply) => {
    if (error.validation) {
      const context = error.validationContext ?? 'request';
      const errors = error.validation.map((v) => ({
        field: `${context}${v.instancePath.replaceAll('/', '.')}`,
        message: v.message ?? 'invalid',
      }));
      return sendProblem(request, reply, 400, PROBLEM_TYPES.validation, 'The request does not satisfy the API contract.', { errors });
    }
    if (error instanceof UpstreamError) {
      // Échec du service Stocks : détail technique dans les logs seulement, réponse générique.
      request.log.warn(
        { requestId: request.id, failure: error.kind, upstreamStatus: error.upstreamStatus, elapsedMs: error.elapsedMs, cause: error.cause instanceof Error ? error.cause.message : undefined },
        'stock call failed',
      );
      return sendProblem(request, reply, error.httpStatus, error.problemType, error.publicDetail);
    }
    const status = error.statusCode;
    if (typeof status === 'number' && status >= 400 && status < 500) {
      // Erreurs 4xx levées par Fastify lui-même (JSON invalide, media type...) : messages sûrs.
      return sendProblem(request, reply, status, PROBLEM_TYPES.http, error.message);
    }
    // Erreur inattendue : détail uniquement dans les logs, jamais dans la réponse.
    request.log.error({ err: error }, 'unhandled error');
    return sendProblem(request, reply, 500, PROBLEM_TYPES.internal, 'An unexpected error occurred.');
  });

  // @fastify/websocket est enregistré AVANT toutes les routes. maxPayload : taille maximale d'un message.
  void app.register(websocket, { options: { maxPayload: MAX_MESSAGE_BYTES } });
  void app.register(cookie, { secret: options.sessionSecret ?? process.env['SESSION_SECRET'] ?? randomBytes(32).toString('hex') });

  void app.register(ordersRoutes(service));
  void app.register(preparationRoutes(service, options.publisher));
  void app.register(availabilityRoutes(service, stock));
  void app.register(ordersWsRoutes(service, subscriptions, options.allowedOrigins ?? []));
  // Outils de laboratoire (connexion démo, interface de démonstration, diagnostics) : jamais en production.
  if (!production) {
    void app.register(demoAuthRoutes(service));
    void app.register(demoWebRoutes);
    void app.register(diagnosticsRoutes);
  }
  if (labMode) void app.register(labRoutes(service));
  app.decorate('wsStats', () => subscriptions.stats());
  app.decorate('orderEvents', service.events);
  return app;
}

declare module 'fastify' {
  interface FastifyInstance {
    /** Observation du registre d'abonnements (tests, diagnostic local). */
    wsStats: () => { sockets: number; orders: number };
    /** Point de publication interne (TP3 : simulation d'un bus partagé entre instances). */
    orderEvents: OrderEvents;
  }
}
