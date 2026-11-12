/**
 * Noms et paramètres publics de la topologie RabbitMQ du TP4.
 * La déclaration effective est dans `docker/rabbitmq/definitions.json` (appliquée par `npm run infra:topology`).
 */
export const EXCHANGES = {
  commands: 'orders.commands',
  retry: 'orders.retry',
  dead: 'orders.dead',
} as const;

export const QUEUES = {
  prepare: 'orders.prepare',
  retry2s: 'orders.retry.2s',
  retry5s: 'orders.retry.5s',
  retry10s: 'orders.retry.10s',
  dlq: 'orders.prepare.dlq',
} as const;

export const ROUTING_KEYS = {
  prepare: 'order.prepare',
  failed: 'order.failed',
  retry1: 'retry.1',
  retry2: 'retry.2',
  retry3: 'retry.3',
} as const;

/** TTL (ms) de chaque queue de retry. Ce sont des délais minimaux, pas des échéances garanties. */
export const RETRY_TTL_MS = {
  [QUEUES.retry2s]: 2_000,
  [QUEUES.retry5s]: 5_000,
  [QUEUES.retry10s]: 10_000,
} as const;

/** Nombre maximal de nouvelles tentatives : `attempt` vaut 0 puis au plus 3. */
export const MAX_RETRIES = 3;

/** Prefetch du consommateur de préparation. */
export const WORKER_PREFETCH = 2;

/** Identité du consommateur dans `processed_operations.consumer_name`. */
export const WORKER_CONSUMER_NAME = 'preparation-worker';

// ───────────────────────── TP5 : diffusion d'événements métier ─────────────────────────
// Ressources AJOUTÉES (celles du TP4 ci-dessus sont inchangées). Déclarées dans docker/rabbitmq/definitions.json :
// à vous de les OBSERVER et de les EXPLIQUER (RabbitMQ Management), pas de les redéclarer.

/** Exchanges du TP5. `business.events` est le seul sur lequel le producteur publie. */
export const EVENT_EXCHANGES = {
  /** Topic durable : un événement publié, une copie par queue dont le binding correspond. */
  business: 'business.events',
  /** Direct : circuit de retry PAR consommateur (clé `<consumer>.retry`). Jamais de fan-out. */
  retry: 'business.retry',
  /** Direct : retour d'un retry vers la queue d'UN seul consommateur (clé `<consumer>`). */
  requeue: 'business.requeue',
  /** Direct : dead-letter PAR consommateur (clé `<consumer>`). */
  dead: 'business.dead',
} as const;

export const EVENT_ROUTING_KEYS = {
  orderShipped: 'order.shipped',
} as const;

/** Identité de chaque consommateur : valeur de `processed_events.consumer_name`, de la clé de retry/dead-letter, des logs. */
export const EVENT_CONSUMERS = {
  notifications: 'notifications-worker',
  statistics: 'statistics-worker',
  audit: 'audit-worker',
} as const;
export type EventConsumerKey = keyof typeof EVENT_CONSUMERS;
export type EventConsumerName = (typeof EVENT_CONSUMERS)[EventConsumerKey];

export const EVENT_QUEUES = {
  notifications: 'notifications.events',
  statistics: 'statistics.events',
  audit: 'audit.events',
} as const;

/** Bindings de `business.events` vers les queues d'abonnement (clé de binding : un motif, pas une clé de publication). */
export const EVENT_BINDING_KEYS = {
  notifications: 'order.shipped',
  statistics: 'order.*',
  audit: '#',
} as const;

/** Circuit d'erreur PROPRE à chaque consommateur : files et clés isolées (un retry de Statistiques ne touche jamais Notifications). */
export const EVENT_ERROR_CIRCUITS = {
  notifications: { retryQueue: 'notifications.events.retry', dlq: 'notifications.events.dlq', retryKey: 'notifications.retry', returnKey: 'notifications', deadKey: 'notifications' },
  statistics: { retryQueue: 'statistics.events.retry', dlq: 'statistics.events.dlq', retryKey: 'statistics.retry', returnKey: 'statistics', deadKey: 'statistics' },
  audit: { retryQueue: 'audit.events.retry', dlq: 'audit.events.dlq', retryKey: 'audit.retry', returnKey: 'audit', deadKey: 'audit' },
} as const satisfies Record<EventConsumerKey, { retryQueue: string; dlq: string; retryKey: string; returnKey: string; deadKey: string }>;

/** Politique d'erreur BORNÉE, identique pour les trois consommateurs : au plus 3 retries espacés de 3 s, puis DLQ. */
export const EVENT_MAX_RETRIES = 3;
export const EVENT_RETRY_DELAY_MS = 3_000;
/** En-tête AMQP (pas le corps !) portant le nombre de retries déjà effectués : l'enveloppe métier et son `eventId` ne changent jamais. */
export const EVENT_RETRY_COUNT_HEADER = 'x-retry-count';
/** Prefetch des consommateurs d'événements. */
export const EVENT_WORKER_PREFETCH = 1;
