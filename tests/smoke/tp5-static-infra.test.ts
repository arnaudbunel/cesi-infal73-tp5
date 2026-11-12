// Vérifications SANS Docker : topologie TP5 déclarée, migration 002, fixtures, .env.example, Compose.
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { describe, it } from 'node:test';
import { EVENT_BINDING_KEYS, EVENT_ERROR_CIRCUITS, EVENT_EXCHANGES, EVENT_MAX_RETRIES, EVENT_QUEUES, EVENT_RETRY_DELAY_MS } from '@infal73/contracts';

const read = (path: string) => readFileSync(new URL(`../../${path}`, import.meta.url), 'utf8');
interface Definitions {
  exchanges: { name: string; type: string; durable: boolean }[];
  queues: { name: string; durable: boolean; arguments: Record<string, unknown> }[];
  bindings: { source: string; destination: string; destination_type: string; routing_key: string }[];
}
const definitions = JSON.parse(read('docker/rabbitmq/definitions.json')) as Definitions;
const bindingKeys = definitions.bindings.map((b) => `${b.source}|${b.destination}|${b.routing_key}`);
const consumers = ['notifications', 'statistics', 'audit'] as const;

describe('smoke - TP5 topologie RabbitMQ déclarée', () => {
  it('business.events : topic durable ; exchanges de retry/requeue/dead : direct durables', () => {
    const type = (name: string) => definitions.exchanges.find((e) => e.name === name);
    assert.equal(type(EVENT_EXCHANGES.business)?.type, 'topic');
    for (const name of [EVENT_EXCHANGES.retry, EVENT_EXCHANGES.requeue, EVENT_EXCHANGES.dead]) assert.equal(type(name)?.type, 'direct');
    for (const name of Object.values(EVENT_EXCHANGES)) assert.equal(type(name)?.durable, true, name);
  });

  it('contrat du sujet : trois queues, bindings order.shipped / order.* / #', () => {
    assert.deepEqual([EVENT_BINDING_KEYS.notifications, EVENT_BINDING_KEYS.statistics, EVENT_BINDING_KEYS.audit], ['order.shipped', 'order.*', '#']);
    assert.ok(bindingKeys.includes('business.events|notifications.events|order.shipped'));
    assert.ok(bindingKeys.includes('business.events|statistics.events|order.*'));
    assert.ok(bindingKeys.includes('business.events|audit.events|#'));
    // Seules les trois queues d'abonnement sont liées à l'exchange topic : un retry ne peut jamais être rediffusé.
    assert.equal(definitions.bindings.filter((b) => b.source === 'business.events').length, 3);
  });

  it('circuits isolés par consommateur : retry TTL borné, retour vers SA queue, DLQ propre, tout durable', () => {
    for (const key of consumers) {
      const circuit = EVENT_ERROR_CIRCUITS[key];
      const queue = (name: string) => definitions.queues.find((q) => q.name === name);
      assert.equal(queue(EVENT_QUEUES[key])?.durable, true);
      assert.equal(queue(EVENT_QUEUES[key])?.arguments['x-dead-letter-exchange'], 'business.dead');
      assert.equal(queue(EVENT_QUEUES[key])?.arguments['x-dead-letter-routing-key'], circuit.deadKey);
      assert.equal(queue(circuit.retryQueue)?.arguments['x-message-ttl'], EVENT_RETRY_DELAY_MS);
      assert.equal(queue(circuit.retryQueue)?.arguments['x-dead-letter-exchange'], 'business.requeue');
      assert.equal(queue(circuit.retryQueue)?.arguments['x-dead-letter-routing-key'], circuit.returnKey);
      assert.equal(queue(circuit.dlq)?.durable, true);
      assert.ok(bindingKeys.includes(`business.retry|${circuit.retryQueue}|${circuit.retryKey}`));
      assert.ok(bindingKeys.includes(`business.requeue|${EVENT_QUEUES[key]}|${circuit.returnKey}`));
      assert.ok(bindingKeys.includes(`business.dead|${circuit.dlq}|${circuit.deadKey}`));
    }
    // Sur CHAQUE exchange, une clé par consommateur : aucun retry ni dead-letter ne peut atteindre un autre consommateur.
    for (const field of ['retryKey', 'returnKey', 'deadKey'] as const) {
      const keys = consumers.map((k) => EVENT_ERROR_CIRCUITS[k][field]);
      assert.equal(new Set(keys).size, keys.length, field);
    }
    assert.equal(EVENT_MAX_RETRIES, 3);
  });

  it('les ressources TP4 sont conservées (orders.prepare, retries, DLQ)', () => {
    for (const name of ['orders.prepare', 'orders.retry.2s', 'orders.retry.5s', 'orders.retry.10s', 'orders.prepare.dlq']) assert.ok(definitions.queues.some((q) => q.name === name), name);
    for (const name of ['orders.commands', 'orders.retry', 'orders.dead']) assert.ok(definitions.exchanges.some((e) => e.name === name), name);
  });
});

describe('smoke - TP5 migration, fixtures, Compose, .env', () => {
  it('migration 002 : trois tables, PK composite, idempotente, sans doublon de déduplication sur notification_records', () => {
    const sql = read('database/migrations/002_tp5_events.sql').replace(/\s+/g, ' ');
    assert.match(sql, /CREATE TABLE IF NOT EXISTS processed_events \( consumer_name text NOT NULL, event_id text NOT NULL, processed_at timestamptz NOT NULL DEFAULT now\(\), PRIMARY KEY \(consumer_name, event_id\)/);
    assert.match(sql, /CREATE TABLE IF NOT EXISTS notification_records/);
    assert.match(sql, /CREATE TABLE IF NOT EXISTS shipment_stats/);
    assert.doesNotMatch(sql, /notification_records[^;]*UNIQUE/i); // la déduplication reste l'exercice, pas un filet caché
    assert.doesNotMatch(sql, /DROP |TRUNCATE /i);
  });

  it('migration 001 (TP4) inchangée dans son contrat', () => {
    assert.match(read('database/migrations/001_init.sql').replace(/\s+/g, ' '), /PRIMARY KEY \(consumer_name, operation_id\)/);
  });

  it('fixture de remise à zéro : uniquement les tables TP5', () => {
    const sql = read('database/fixtures/002_tp5_lab_state.sql');
    assert.match(sql, /TRUNCATE TABLE processed_events, notification_records;/);
    assert.doesNotMatch(sql, /DROP|prepared_orders|processed_operations/i);
  });

  it('Compose : ports liés à 127.0.0.1 et configurables, base *_lab, aucun secret actif', () => {
    const compose = read('compose.yaml');
    const ports = [...compose.matchAll(/^\s+- "([^"]+:\d+)"\s*$/gm)].map((m) => String(m[1]));
    assert.equal(ports.length, 3);
    for (const port of ports) assert.match(port, /^127\.0\.0\.1:\$\{[A-Z_]+:-\d+\}:/);
    assert.match(compose, /POSTGRES_DB:-infal73_tp5_lab/);
    assert.equal((compose.match(/healthcheck:/g) ?? []).length, 2);
  });

  it('.env.example : valeurs factices de laboratoire seulement', () => {
    const env = read('.env.example');
    assert.match(env, /^POSTGRES_DB=infal73_tp5_lab$/m);
    assert.match(env, /^RABBITMQ_PASSWORD=lab-only-change-me$/m);
    assert.doesNotMatch(env, /(AKIA|sk-[A-Za-z0-9]{10,}|BEGIN [A-Z ]*PRIVATE KEY|smtp)/i);
  });
});
