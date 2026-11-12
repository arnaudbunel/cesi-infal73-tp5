// Smoke AVEC infrastructure : nécessite `npm run infra:up` (Docker). Échoue avec un message d'action si absente.
// SKIP_INFRA_TESTS=1 les ignore explicitement (ils sont alors rapportés « skipped », jamais « passed »).
import assert from 'node:assert/strict';
import { after, describe, it } from 'node:test';
import { QUEUES, RETRY_TTL_MS } from '@infal73/contracts';
import { closeDb, db, lab, requireInfra } from '../helpers/lab.js';
import { mgmt } from '../../scripts/lib/mgmt.mjs';

const skip = process.env['SKIP_INFRA_TESTS'] === '1' ? 'SKIP_INFRA_TESTS=1' : false;
after(closeDb);

interface QueueInfo {
  name: string;
  durable: boolean;
  arguments: Record<string, unknown>;
}

describe('smoke - TP4 infrastructure (RabbitMQ + PostgreSQL)', { skip }, () => {
  it('RabbitMQ Management joignable, PostgreSQL migré, aucun worker étranger', async () => {
    await requireInfra();
  });

  it('les queues du TP4 existent avec leurs arguments (durables, TTL 2/5/10 s, DLX)', async () => {
    const queues = (await mgmt('GET', '/queues/%2F')) as QueueInfo[];
    const byName = new Map(queues.map((q) => [q.name, q]));
    for (const name of Object.values(QUEUES)) assert.ok(byName.get(name)?.durable, `${name} absente ou non durable (npm run infra:topology)`);
    for (const [name, ttl] of Object.entries(RETRY_TTL_MS)) assert.equal(byName.get(name)?.arguments['x-message-ttl'], ttl);
    assert.equal(byName.get(QUEUES.prepare)?.arguments['x-dead-letter-routing-key'], 'order.failed');
  });

  it('bindings présents sur les trois exchanges', async () => {
    const bindings = (await mgmt('GET', '/bindings/%2F')) as { source: string; destination: string; routing_key: string }[];
    const keys = new Set(bindings.map((b) => `${b.source}|${b.destination}|${b.routing_key}`));
    for (const expected of [
      'orders.commands|orders.prepare|order.prepare',
      'orders.retry|orders.retry.2s|retry.1',
      'orders.retry|orders.retry.5s|retry.2',
      'orders.retry|orders.retry.10s|retry.3',
      'orders.dead|orders.prepare.dlq|order.failed',
    ]) assert.ok(keys.has(expected), `binding manquant : ${expected}`);
  });

  it('les tables existent avec leurs clés (PK composite, UNIQUE operation_id)', async () => {
    const constraints = (
      await db().query<{ table_name: string; constraint_type: string }>(
        "SELECT table_name, constraint_type FROM information_schema.table_constraints WHERE table_name IN ('processed_operations','prepared_orders') AND constraint_type IN ('PRIMARY KEY','UNIQUE')",
      )
    ).rows.map((r) => `${r.table_name}:${r.constraint_type}`);
    for (const expected of ['processed_operations:PRIMARY KEY', 'prepared_orders:PRIMARY KEY', 'prepared_orders:UNIQUE']) assert.ok(constraints.includes(expected), expected);
    assert.match(lab.pgDatabase, /_lab$/);
  });
});
