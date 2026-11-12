import { MAX_RETRIES } from './topology.js';

/**
 * Message métier de demande de préparation.
 *
 * - `operationId` : opération métier, STABLE entre republications et retries.
 * - `attempt`     : 0 à la première livraison logique, au plus MAX_RETRIES (3).
 * - Le `messageId` AMQP (transport) peut changer à chaque publication ; le `correlationId` sert à la traçabilité.
 */
export interface PreparationCommand {
  schemaVersion: 1;
  type: 'OrderPreparationRequested';
  operationId: string;
  orderId: string;
  attempt: number;
  requestedAt: string;
}

export type ValidationResult =
  | { ok: true; command: PreparationCommand }
  | { ok: false; reason: string };

const ID_PATTERN = /^[A-Za-z0-9_-]{1,64}$/;
const KEYS = ['schemaVersion', 'type', 'operationId', 'orderId', 'attempt', 'requestedAt'];

/** Identifiant d'opération de la commande initiale de préparation : une opération par commande. */
export function operationIdFor(orderId: string): string {
  return `prepare-${orderId}`;
}

/** Construit la commande initiale (attempt = 0). */
export function createPreparationCommand(orderId: string, now: Date = new Date()): PreparationCommand {
  return {
    schemaVersion: 1,
    type: 'OrderPreparationRequested',
    operationId: operationIdFor(orderId),
    orderId,
    attempt: 0,
    requestedAt: now.toISOString(),
  };
}

/** Validation à l'exécution d'une valeur déjà décodée (un simple cast TypeScript ne valide rien). */
export function validatePreparationValue(value: unknown): ValidationResult {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) return { ok: false, reason: 'not an object' };
  const record = value as Record<string, unknown>;
  const unknown = Object.keys(record).filter((k) => !KEYS.includes(k));
  if (unknown.length > 0) return { ok: false, reason: `unknown field(s): ${unknown.join(', ')}` };
  if (record['schemaVersion'] !== 1) return { ok: false, reason: 'schemaVersion must be 1' };
  if (record['type'] !== 'OrderPreparationRequested') return { ok: false, reason: 'type must be OrderPreparationRequested' };
  const { operationId, orderId, attempt, requestedAt } = record;
  if (typeof operationId !== 'string' || !ID_PATTERN.test(operationId)) return { ok: false, reason: 'operationId invalid' };
  if (typeof orderId !== 'string' || !ID_PATTERN.test(orderId)) return { ok: false, reason: 'orderId invalid' };
  if (typeof attempt !== 'number' || !Number.isInteger(attempt) || attempt < 0 || attempt > MAX_RETRIES) {
    return { ok: false, reason: `attempt must be an integer between 0 and ${MAX_RETRIES}` };
  }
  if (typeof requestedAt !== 'string' || Number.isNaN(Date.parse(requestedAt)) || !/^\d{4}-\d{2}-\d{2}T/.test(requestedAt)) {
    return { ok: false, reason: 'requestedAt must be an ISO 8601 date' };
  }
  return { ok: true, command: { schemaVersion: 1, type: 'OrderPreparationRequested', operationId, orderId, attempt, requestedAt } };
}

/** Décode puis valide un corps AMQP (octets). Le JSON invalide est une erreur DÉFINITIVE (jamais de retry). */
export function validatePreparationMessage(content: Uint8Array | Buffer): ValidationResult {
  let parsed: unknown;
  try {
    parsed = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(content));
  } catch {
    return { ok: false, reason: 'body is not valid UTF-8 JSON' };
  }
  return validatePreparationValue(parsed);
}
