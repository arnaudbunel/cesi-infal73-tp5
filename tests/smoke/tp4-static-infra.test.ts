// Vérifications SANS Docker : fichiers de topologie, migrations et Compose conformes au contrat public.
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { describe, it } from 'node:test';
import { EXCHANGES, QUEUES, RETRY_TTL_MS, ROUTING_KEYS } from '@infal73/contracts';

const read = (path: string) => readFileSync(new URL(`../../${path}`, import.meta.url), 'utf8');

interface Definitions {
  exchanges: { name: string; type: string; durable: boolean }[];
  queues: { name: string; durable: boolean; arguments: Record<string, unknown> }[];
  bindings: { source: string; destination: string; destination_type: string; routing_key: string }[];
}
const definitions = JSON.parse(read('docker/rabbitmq/definitions.json')) as Definitions;

describe('smoke - TP4 topologie RabbitMQ déclarée (definitions.json)', () => {
  it('3 exchanges direct durables', () => {
    for (const name of Object.values(EXCHANGES)) {
      const exchange = definitions.exchanges.find((e) => e.name === name);
      assert.ok(exchange, name);
      assert.equal(exchange.type, 'direct');
      assert.equal(exchange.durable, true);
    }
  });

  it('queue principale : durable, DLX orders.dead avec la clé order.failed', () => {
    const queue = definitions.queues.find((q) => q.name === QUEUES.prepare);
    assert.ok(queue);
    assert.equal(queue.durable, true);
    assert.equal(queue.arguments['x-dead-letter-exchange'], EXCHANGES.dead);
    assert.equal(queue.arguments['x-dead-letter-routing-key'], ROUTING_KEYS.failed);
  });

  it('queues de retry : TTL 2/5/10 s, retour vers orders.commands avec order.prepare', () => {
    for (const [name, ttl] of Object.entries(RETRY_TTL_MS)) {
      const queue = definitions.queues.find((q) => q.name === name);
      assert.ok(queue, name);
      assert.equal(queue.durable, true);
      assert.equal(queue.arguments['x-message-ttl'], ttl);
      assert.equal(queue.arguments['x-dead-letter-exchange'], EXCHANGES.commands);
      assert.equal(queue.arguments['x-dead-letter-routing-key'], ROUTING_KEYS.prepare);
    }
    assert.deepEqual(Object.values(RETRY_TTL_MS), [2000, 5000, 10000]);
  });

  it('DLQ durable et bindings exacts', () => {
    assert.equal(definitions.queues.find((q) => q.name === QUEUES.dlq)?.durable, true);
    // TP5 ajoute des ressources `business.*` : le contrat TP4 ne porte que sur les bindings dont la source est un exchange `orders.*`.
    const actual = definitions.bindings.filter((b) => b.source.startsWith('orders.')).map((b) => `${b.source}|${b.destination}|${b.routing_key}`).sort();
    const expected = [
      `${EXCHANGES.commands}|${QUEUES.prepare}|${ROUTING_KEYS.prepare}`,
      `${EXCHANGES.retry}|${QUEUES.retry2s}|${ROUTING_KEYS.retry1}`,
      `${EXCHANGES.retry}|${QUEUES.retry5s}|${ROUTING_KEYS.retry2}`,
      `${EXCHANGES.retry}|${QUEUES.retry10s}|${ROUTING_KEYS.retry3}`,
      `${EXCHANGES.dead}|${QUEUES.dlq}|${ROUTING_KEYS.failed}`,
    ].sort();
    assert.deepEqual(actual, expected);
  });
});

describe('smoke - TP4 migrations et Compose', () => {
  it('la migration 001 crée les deux tables avec leurs contraintes', () => {
    const sql = read('database/migrations/001_init.sql').replace(/\s+/g, ' ');
    assert.match(sql, /CREATE TABLE IF NOT EXISTS processed_operations/);
    assert.match(sql, /PRIMARY KEY \(consumer_name, operation_id\)/);
    assert.match(sql, /CREATE TABLE IF NOT EXISTS prepared_orders \( order_id text PRIMARY KEY, operation_id text NOT NULL UNIQUE/);
  });

  it('la remise à zéro SQL ne touche que les deux tables du laboratoire', () => {
    const sql = read('database/fixtures/001_lab_state.sql');
    assert.match(sql, /TRUNCATE TABLE processed_operations, prepared_orders;/);
    assert.doesNotMatch(sql, /DROP|DELETE FROM (?!processed|prepared)/i);
  });

  it('Compose : tous les ports sont liés à 127.0.0.1 ; PostgreSQL hors 5432 par défaut ; health checks', () => {
    const compose = read('compose.yaml');
    const ports = [...compose.matchAll(/^\s+- "([^"]+:\d+)"\s*$/gm)].map((m) => m[1]);
    assert.equal(ports.length, 3);
    for (const port of ports) assert.match(String(port), /^127\.0\.0\.1:/);
    assert.match(compose, /POSTGRES_PORT:-55432/);
    assert.equal((compose.match(/healthcheck:/g) ?? []).length, 2);
    assert.match(compose, /rabbitmq:4-management/);
  });

  it('.env.example : aucun secret réel, valeurs de laboratoire', () => {
    const env = read('.env.example');
    assert.match(env, /RABBITMQ_PASSWORD=lab-only-change-me/);
    assert.match(env, /POSTGRES_PORT=55432/);
  });
});
