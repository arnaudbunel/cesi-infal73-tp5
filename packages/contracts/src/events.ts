/**
 * Contrat public de l'événement métier `OrderShipped` (schemaVersion 1) — voir docs/CONTRAT_ORDER_SHIPPED.md.
 *
 * Ce module FOURNIT les types et la validation à l'exécution (un simple cast TypeScript ne valide rien).
 * Il ne fournit volontairement PAS de fabrique d'enveloppe : construire l'événement après une transition
 * validée fait partie de l'exercice B.
 */

export const ORDER_SHIPPED_EVENT_TYPE = 'OrderShipped';
export const ORDER_SHIPPED_SCHEMA_VERSION = 1;
export const ORDER_SHIPPED_SOURCE = 'orders-service';

export interface OrderShippedData {
  orderId: string;
  status: 'SHIPPED';
}

export interface OrderShippedEvent {
  /** Identifie le FAIT MÉTIER : inchangé à chaque republication du même événement. */
  eventId: string;
  eventType: typeof ORDER_SHIPPED_EVENT_TYPE;
  schemaVersion: typeof ORDER_SHIPPED_SCHEMA_VERSION;
  /** ISO 8601 UTC (suffixe Z). */
  occurredAt: string;
  source: typeof ORDER_SHIPPED_SOURCE;
  aggregateId: string;
  /** Version MÉTIER de la commande (pas un compteur de livraisons). */
  aggregateVersion: number;
  data: OrderShippedData;
}

/**
 * Codes de diagnostic : un code stable par famille de défaut, pour des logs et des DLQ lisibles.
 *  - INVALID_JSON / NOT_AN_OBJECT : corps illisible ;
 *  - UNSUPPORTED_SCHEMA_VERSION   : version inconnue de ce consommateur (erreur DÉFINITIVE : la rejouer ne changera rien) ;
 *  - WRONG_EVENT_TYPE / UNKNOWN_FIELD / INVALID_FIELD / INCONSISTENT : autre violation du contrat.
 */
export type EventContractCode =
  | 'INVALID_JSON'
  | 'NOT_AN_OBJECT'
  | 'UNSUPPORTED_SCHEMA_VERSION'
  | 'WRONG_EVENT_TYPE'
  | 'UNKNOWN_FIELD'
  | 'INVALID_FIELD'
  | 'INCONSISTENT';

export interface EventContractViolation {
  code: EventContractCode;
  /** Champ concerné (ex. `data.status`), absent pour un défaut global. */
  field?: string;
  reason: string;
}

export type OrderShippedValidation = { ok: true; event: OrderShippedEvent } | { ok: false; violation: EventContractViolation };

const ID_PATTERN = /^[A-Za-z0-9._:-]{1,128}$/;
const ORDER_ID_PATTERN = /^[A-Za-z0-9_-]{1,64}$/;
const UTC_ISO_PATTERN = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,6})?Z$/;
const ENVELOPE_KEYS = ['eventId', 'eventType', 'schemaVersion', 'occurredAt', 'source', 'aggregateId', 'aggregateVersion', 'data'];
const DATA_KEYS = ['orderId', 'status'];

const fail = (code: EventContractCode, reason: string, field?: string): OrderShippedValidation => ({
  ok: false,
  violation: field === undefined ? { code, reason } : { code, reason, field },
});

/** Libellé uniforme d'un diagnostic, pour les logs : `UNSUPPORTED_SCHEMA_VERSION (schemaVersion): ...`. */
export function describeViolation(violation: EventContractViolation): string {
  return `${violation.code}${violation.field === undefined ? '' : ` (${violation.field})`}: ${violation.reason}`;
}

/** Valide une valeur déjà décodée. L'ordre des contrôles est stable : version d'abord, pour un diagnostic net sur un rejeu futur. */
export function validateOrderShippedValue(value: unknown): OrderShippedValidation {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) return fail('NOT_AN_OBJECT', 'the event must be a JSON object');
  const record = value as Record<string, unknown>;

  const version = record['schemaVersion'];
  if (version !== ORDER_SHIPPED_SCHEMA_VERSION) {
    return fail('UNSUPPORTED_SCHEMA_VERSION', `schemaVersion ${JSON.stringify(version)} is not supported (expected ${ORDER_SHIPPED_SCHEMA_VERSION})`, 'schemaVersion');
  }
  const unknown = Object.keys(record).filter((key) => !ENVELOPE_KEYS.includes(key));
  if (unknown.length > 0) return fail('UNKNOWN_FIELD', `unknown field(s): ${unknown.join(', ')}`, unknown[0]);
  if (record['eventType'] !== ORDER_SHIPPED_EVENT_TYPE) return fail('WRONG_EVENT_TYPE', `eventType must be ${ORDER_SHIPPED_EVENT_TYPE}`, 'eventType');

  const { eventId, occurredAt, source, aggregateId, aggregateVersion, data } = record;
  if (typeof eventId !== 'string' || !ID_PATTERN.test(eventId)) return fail('INVALID_FIELD', 'eventId must be a non-empty identifier (letters, digits, . _ : -, 128 max)', 'eventId');
  if (typeof occurredAt !== 'string' || !UTC_ISO_PATTERN.test(occurredAt) || Number.isNaN(Date.parse(occurredAt))) {
    return fail('INVALID_FIELD', 'occurredAt must be an ISO 8601 UTC date (e.g. 2026-01-01T09:00:00.000Z)', 'occurredAt');
  }
  if (source !== ORDER_SHIPPED_SOURCE) return fail('INVALID_FIELD', `source must be ${ORDER_SHIPPED_SOURCE}`, 'source');
  if (typeof aggregateId !== 'string' || !ORDER_ID_PATTERN.test(aggregateId)) return fail('INVALID_FIELD', 'aggregateId must be an order identifier', 'aggregateId');
  if (typeof aggregateVersion !== 'number' || !Number.isInteger(aggregateVersion) || aggregateVersion < 1) {
    return fail('INVALID_FIELD', 'aggregateVersion must be an integer >= 1', 'aggregateVersion');
  }
  if (typeof data !== 'object' || data === null || Array.isArray(data)) return fail('INVALID_FIELD', 'data must be an object', 'data');
  const payload = data as Record<string, unknown>;
  const unknownData = Object.keys(payload).filter((key) => !DATA_KEYS.includes(key));
  if (unknownData.length > 0) return fail('UNKNOWN_FIELD', `unknown field(s): ${unknownData.join(', ')}`, `data.${unknownData[0]}`);
  if (typeof payload['orderId'] !== 'string' || !ORDER_ID_PATTERN.test(payload['orderId'])) return fail('INVALID_FIELD', 'data.orderId must be an order identifier', 'data.orderId');
  if (payload['status'] !== 'SHIPPED') return fail('INVALID_FIELD', 'data.status must be SHIPPED', 'data.status');
  if (payload['orderId'] !== aggregateId) return fail('INCONSISTENT', 'data.orderId must equal aggregateId', 'data.orderId');

  return {
    ok: true,
    event: {
      eventId,
      eventType: ORDER_SHIPPED_EVENT_TYPE,
      schemaVersion: ORDER_SHIPPED_SCHEMA_VERSION,
      occurredAt,
      source: ORDER_SHIPPED_SOURCE,
      aggregateId,
      aggregateVersion,
      data: { orderId: payload['orderId'], status: 'SHIPPED' },
    },
  };
}

/** Décode (UTF-8 strict puis JSON) puis valide un corps AMQP. Un JSON illisible est une erreur DÉFINITIVE. */
export function validateOrderShippedMessage(content: Uint8Array | Buffer): OrderShippedValidation {
  let parsed: unknown;
  try {
    parsed = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(content));
  } catch {
    return fail('INVALID_JSON', 'body is not valid UTF-8 JSON');
  }
  return validateOrderShippedValue(parsed);
}
