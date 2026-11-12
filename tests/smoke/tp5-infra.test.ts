// Smoke AVEC infrastructure : nécessite `npm run setup` (Docker). Échoue avec un message d'action si absente.
// SKIP_INFRA_TESTS=1 les ignore explicitement (ils sont alors rapportés « skipped », jamais « passed »).
import assert from 'node:assert/strict';
import { after, describe, it } from 'node:test';
import { EVENT_ERROR_CIRCUITS, EVENT_QUEUES } from '@infal73/contracts';
import { closeDb, db, requireInfra } from '../helpers/lab.js';
import { mgmt } from '../../scripts/lib/mgmt.mjs';

const skip = process.env['SKIP_INFRA_TESTS'] === '1' ? 'SKIP_INFRA_TESTS=1' : false;
after(closeDb);

describe('smoke - TP5 infrastructure (topologie événements + tables)', { skip }, () => {
  it('socle TP4 prêt (Management, PostgreSQL migré)', async () => {
    await requireInfra();
  });

  it('les queues du TP5 existent, durables, avec leur circuit de retry/DLQ propre', async () => {
    const queues = (await mgmt('GET', '/queues/%2F')) as { name: string; durable: boolean; arguments: Record<string, unknown> }[];
    const byName = new Map(queues.map((q) => [q.name, q]));
    for (const key of ['notifications', 'statistics', 'audit'] as const) {
      for (const name of [EVENT_QUEUES[key], EVENT_ERROR_CIRCUITS[key].retryQueue, EVENT_ERROR_CIRCUITS[key].dlq]) assert.ok(byName.get(name)?.durable, `${name} absente ou non durable (npm run infra:topology)`);
    }
  });

  it('business.events est un topic durable lié aux trois queues (order.shipped, order.*, #)', async () => {
    const exchange = (await mgmt('GET', '/exchanges/%2F/business.events')) as { type: string; durable: boolean };
    assert.equal(exchange.type, 'topic');
    assert.equal(exchange.durable, true);
    const bindings = (await mgmt('GET', '/exchanges/%2F/business.events/bindings/source')) as { destination: string; routing_key: string }[];
    const keys = new Set(bindings.map((b) => `${b.destination}|${b.routing_key}`));
    for (const expected of ['notifications.events|order.shipped', 'statistics.events|order.*', 'audit.events|#']) assert.ok(keys.has(expected), `binding manquant : ${expected}`);
  });

  it('tables TP5 : processed_events (PK composite), notification_records, shipment_stats initialisée', async () => {
    const pk = (await db().query<{ column_name: string }>(
      "SELECT kcu.column_name FROM information_schema.table_constraints tc JOIN information_schema.key_column_usage kcu USING (constraint_name, table_name) WHERE tc.table_name = 'processed_events' AND tc.constraint_type = 'PRIMARY KEY' ORDER BY kcu.ordinal_position",
    )).rows.map((r) => r.column_name);
    assert.deepEqual(pk, ['consumer_name', 'event_id']);
    await db().query('SELECT 1 FROM notification_records LIMIT 1');
    assert.equal((await db().query("SELECT 1 FROM shipment_stats WHERE metric = 'shipped'")).rowCount, 1);
  });
});
