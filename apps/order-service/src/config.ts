export type StockBackend = 'http' | 'grpc';

export interface OrderServiceConfig {
  /** URL de base du service Stocks HTTP (backend historique du TP2). */
  stockBaseUrl: string;
  /** Budget de temps (ms) alloué à un appel HTTP vers Stocks. Valeur nominale du TP2 : 2000. */
  stockTimeoutMs: number;
  /** Backend utilisé par GET /api/orders/:id/availability. `http` par défaut ; `grpc` après complétion du TP3. */
  stockBackend: StockBackend;
  /** Cible gRPC du service Stocks (host:port). */
  stockGrpcTarget: string;
  /** Deadline (ms) d'un appel gRPC vers Stocks. 500 pour la démonstration du TP3. */
  stockGrpcDeadlineMs: number;
}

export const DEFAULT_ORDER_CONFIG: OrderServiceConfig = {
  stockBaseUrl: 'http://127.0.0.1:3001',
  stockTimeoutMs: 2000,
  stockBackend: 'http',
  stockGrpcTarget: '127.0.0.1:50051',
  stockGrpcDeadlineMs: 500,
};

function integerEnv(name: string, raw: string | undefined, fallback: number, min: number, max: number): number {
  if (raw === undefined || raw === '') return fallback;
  if (!/^\d+$/.test(raw.trim())) throw new Error(`${name} must be an integer, got "${raw}"`);
  const value = Number(raw);
  if (value < min || value > max) throw new Error(`${name} must be between ${min} and ${max}, got ${value}`);
  return value;
}

/** Lit et valide les variables d'environnement du service Commandes (erreur explicite si invalide). */
export function readOrderConfig(env: NodeJS.ProcessEnv = process.env): OrderServiceConfig {
  const backend = env['STOCK_BACKEND'] ?? DEFAULT_ORDER_CONFIG.stockBackend;
  if (backend !== 'http' && backend !== 'grpc') throw new Error(`STOCK_BACKEND must be "http" or "grpc", got "${backend}"`);
  return {
    stockBaseUrl: env['STOCK_BASE_URL'] ?? DEFAULT_ORDER_CONFIG.stockBaseUrl,
    stockTimeoutMs: DEFAULT_ORDER_CONFIG.stockTimeoutMs,
    stockBackend: backend,
    stockGrpcTarget: env['STOCK_GRPC_TARGET'] ?? DEFAULT_ORDER_CONFIG.stockGrpcTarget,
    stockGrpcDeadlineMs: integerEnv('STOCK_GRPC_DEADLINE_MS', env['STOCK_GRPC_DEADLINE_MS'], DEFAULT_ORDER_CONFIG.stockGrpcDeadlineMs, 1, 60_000),
  };
}

const LAB_RABBITMQ_PASSWORD = 'lab-only-change-me';

/**
 * URL AMQP du broker. Laboratoire : valeur par défaut factice sur 127.0.0.1.
 * En production : `RABBITMQ_URL` est obligatoire et ne doit pas contenir le mot de passe de laboratoire.
 */
export function readRabbitUrl(env: NodeJS.ProcessEnv = process.env): string {
  const explicit = env['RABBITMQ_URL'];
  if (env['NODE_ENV'] === 'production') {
    if (!explicit) throw new Error('RABBITMQ_URL is required when NODE_ENV=production (no lab default)');
    if (explicit.includes(LAB_RABBITMQ_PASSWORD)) throw new Error('the lab RabbitMQ credentials are forbidden when NODE_ENV=production');
    return explicit;
  }
  if (explicit) return explicit;
  const user = env['RABBITMQ_USER'] ?? 'lab';
  const password = env['RABBITMQ_PASSWORD'] ?? LAB_RABBITMQ_PASSWORD;
  const port = integerEnv('RABBITMQ_AMQP_PORT', env['RABBITMQ_AMQP_PORT'], 5672, 1, 65535);
  return `amqp://${encodeURIComponent(user)}:${encodeURIComponent(password)}@127.0.0.1:${port}`;
}
