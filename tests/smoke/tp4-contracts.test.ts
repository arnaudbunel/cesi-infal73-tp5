import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { createPreparationCommand, operationIdFor, validatePreparationMessage, validatePreparationValue } from '@infal73/contracts';

const valid = () => ({ schemaVersion: 1, type: 'OrderPreparationRequested', operationId: 'prepare-ORD-123', orderId: 'ORD-123', attempt: 0, requestedAt: '2026-11-10T10:00:00.000Z' });
const bytes = (value: unknown) => Buffer.from(typeof value === 'string' ? value : JSON.stringify(value));

describe('smoke - TP4 contrat de message (validation à l\'exécution)', () => {
  it('accepte un message conforme, y compris attempt = 3', () => {
    assert.equal(validatePreparationMessage(bytes(valid())).ok, true);
    assert.equal(validatePreparationValue({ ...valid(), attempt: 3 }).ok, true);
  });

  it('rejette JSON tronqué, non-objet, version, type, attempt, date, champs inconnus', () => {
    const cases: unknown[] = [
      '{"schemaVersion":1,"type":',
      '[]',
      'null',
      { ...valid(), schemaVersion: 2 },
      { ...valid(), type: 'Other' },
      { ...valid(), attempt: 4 },
      { ...valid(), attempt: -1 },
      { ...valid(), attempt: 1.5 },
      { ...valid(), attempt: '0' },
      { ...valid(), requestedAt: 'hier' },
      { ...valid(), operationId: '' },
      { ...valid(), orderId: 42 },
      { ...valid(), extra: true },
    ];
    for (const value of cases) assert.equal(validatePreparationMessage(bytes(value)).ok, false, JSON.stringify(value));
  });

  it('refuse les octets qui ne sont pas de l\'UTF-8', () => {
    assert.equal(validatePreparationMessage(Buffer.from([0xff, 0xfe, 0x7b])).ok, false);
  });

  it('la commande initiale est cohérente : operationId stable par commande, attempt = 0', () => {
    const command = createPreparationCommand('ORD-102', new Date('2026-11-10T10:00:00Z'));
    assert.equal(command.operationId, operationIdFor('ORD-102'));
    assert.equal(command.operationId, 'prepare-ORD-102');
    assert.equal(command.attempt, 0);
    assert.equal(validatePreparationValue(command).ok, true);
    assert.equal(createPreparationCommand('ORD-102').operationId, command.operationId);
  });
});
