// Contrat OrderShipped : validation à l'exécution et diagnostics. Sans infrastructure.
import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { describeViolation, validateOrderShippedMessage, validateOrderShippedValue } from '@infal73/contracts';

const valid = () => ({
  eventId: 'evt-10042',
  eventType: 'OrderShipped',
  schemaVersion: 1,
  occurredAt: '2026-01-01T09:00:00.000Z',
  source: 'orders-service',
  aggregateId: 'ORD-102',
  aggregateVersion: 5,
  data: { orderId: 'ORD-102', status: 'SHIPPED' },
});
const codeOf = (value: unknown): string => {
  const result = validateOrderShippedValue(value);
  return result.ok ? 'OK' : `${result.violation.code}${result.violation.field ? `@${result.violation.field}` : ''}`;
};

describe('smoke - TP5 contrat OrderShipped (schemaVersion 1)', () => {
  it("accepte l'exemple du sujet", () => {
    const result = validateOrderShippedValue(valid());
    assert.equal(result.ok, true);
  });

  it('schemaVersion inconnue : diagnostic UNSUPPORTED_SCHEMA_VERSION, même si le reste est valide', () => {
    assert.equal(codeOf({ ...valid(), schemaVersion: 99 }), 'UNSUPPORTED_SCHEMA_VERSION@schemaVersion');
    assert.equal(codeOf({ ...valid(), schemaVersion: '1' }), 'UNSUPPORTED_SCHEMA_VERSION@schemaVersion');
    const result = validateOrderShippedValue({ ...valid(), schemaVersion: 99 });
    assert.ok(!result.ok && /99/.test(describeViolation(result.violation)));
  });

  it('champs invalides : un code et un champ stables', () => {
    assert.equal(codeOf({ ...valid(), eventId: '' }), 'INVALID_FIELD@eventId');
    assert.equal(codeOf({ ...valid(), occurredAt: '2026-01-01T09:00:00+02:00' }), 'INVALID_FIELD@occurredAt'); // pas UTC (suffixe Z)
    assert.equal(codeOf({ ...valid(), occurredAt: 'hier' }), 'INVALID_FIELD@occurredAt');
    assert.equal(codeOf({ ...valid(), source: 'other' }), 'INVALID_FIELD@source');
    assert.equal(codeOf({ ...valid(), aggregateVersion: 0 }), 'INVALID_FIELD@aggregateVersion');
    assert.equal(codeOf({ ...valid(), aggregateVersion: 1.5 }), 'INVALID_FIELD@aggregateVersion');
    assert.equal(codeOf({ ...valid(), data: { orderId: 'ORD-102', status: 'CANCELLED' } }), 'INVALID_FIELD@data.status');
    assert.equal(codeOf({ ...valid(), eventType: 'OrderCancelled' }), 'WRONG_EVENT_TYPE@eventType');
    assert.equal(codeOf({ ...valid(), extra: 1 }), 'UNKNOWN_FIELD@extra');
    assert.equal(codeOf({ ...valid(), data: { orderId: 'ORD-102', status: 'SHIPPED', x: 1 } }), 'UNKNOWN_FIELD@data.x');
    assert.equal(codeOf({ ...valid(), data: { orderId: 'ORD-999', status: 'SHIPPED' } }), 'INCONSISTENT@data.orderId');
    assert.equal(codeOf(null), 'NOT_AN_OBJECT');
    assert.equal(codeOf([valid()]), 'NOT_AN_OBJECT');
  });

  it('corps AMQP : JSON tronqué et UTF-8 invalide sont INVALID_JSON ; un corps valide passe', () => {
    assert.equal(validateOrderShippedMessage(Buffer.from('{"eventId":"evt-1"')).ok, false);
    assert.equal(validateOrderShippedMessage(Buffer.from([0xff, 0xfe, 0x7b])).ok, false);
    const bad = validateOrderShippedMessage(Buffer.from('{'));
    assert.ok(!bad.ok && bad.violation.code === 'INVALID_JSON');
    assert.equal(validateOrderShippedMessage(Buffer.from(JSON.stringify(valid()))).ok, true);
  });
});
