// Consommateurs d'événements SANS broker : discipline ACK/retry/DLQ du runtime, worker Audit de référence, configuration, injections.
import assert from 'node:assert/strict';
import { mkdirSync, mkdtempSync, readdirSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { after, describe, it } from 'node:test';
import { EVENT_MAX_RETRIES, EVENT_RETRY_COUNT_HEADER } from '@infal73/contracts';
import type { ConsumeMessage } from 'amqplib';
import { AuditHandler } from '../../apps/event-workers/src/handlers/audit.handler.js';
import { readEventWorkerConfig } from '../../apps/event-workers/src/config.js';
import { TemporaryDependencyError } from '../../apps/event-workers/src/errors.js';
import { createStatisticsFaults, STATISTICS_FAULT_FILE } from '../../apps/event-workers/src/lab-faults.js';
import { Delivery, settleAfterError } from '../../apps/event-workers/src/runtime/delivery.js';
import type { DeliveryDeps } from '../../apps/event-workers/src/runtime/delivery.js';
import { processDelivery } from '../../apps/event-workers/src/runtime/consumer-runtime.js';
import type { EventHandler } from '../../apps/event-workers/src/runtime/consumer-runtime.js';
import { TodoNotImplementedError } from '@infal73/messaging';
import { ConfirmedEventRetryScheduler } from '../../apps/event-workers/src/runtime/retry.js';

const tmp = mkdtempSync(join(tmpdir(), 'tp5-smoke-'));
after(() => rmSync(tmp, { recursive: true, force: true }));

const event = (eventId = 'evt-1', schemaVersion = 1) => ({
  eventId, eventType: 'OrderShipped', schemaVersion, occurredAt: '2026-01-01T09:00:00.000Z', source: 'orders-service', aggregateId: 'ORD-102', aggregateVersion: 1, data: { orderId: 'ORD-102', status: 'SHIPPED' },
});

function harness(body: unknown, retryCount?: number, retryFails = false) {
  const calls: string[] = [];
  const retries: { retryCount: number }[] = [];
  const msg = {
    content: Buffer.from(typeof body === 'string' ? body : JSON.stringify(body)),
    fields: { deliveryTag: 7, redelivered: false, exchange: 'business.events', routingKey: 'order.shipped' },
    properties: { messageId: 'm-1', headers: retryCount === undefined ? {} : { [EVENT_RETRY_COUNT_HEADER]: retryCount } },
  } as unknown as ConsumeMessage;
  const deps: DeliveryDeps = {
    channel: {
      ack: () => void calls.push('ack'),
      nack: (_m: unknown, allUpTo?: boolean, requeue?: boolean) => void calls.push(`nack(allUpTo=${String(allUpTo)},requeue=${String(requeue)})`),
    },
    retries: {
      scheduleRetry: async (_b, context) => {
        if (retryFails) throw new Error('not confirmed');
        retries.push({ retryCount: context.retryCount });
      },
    },
    consumerName: 'statistics-worker',
    queue: 'statistics.events',
    log: () => undefined,
    abortSession: () => void calls.push('abortSession'),
  };
  return { delivery: new Delivery(deps, msg), calls, retries };
}

describe('smoke - TP5 runtime des consommateurs (discipline ACK / retry / DLQ)', () => {
  it('retry : republication confirmée PUIS ack ; le compteur est lu dans l\'en-tête', async () => {
    const { delivery, calls, retries } = harness(event(), 1);
    assert.equal(delivery.retryCount, 1);
    await delivery.retryLater('503');
    assert.deepEqual(retries, [{ retryCount: 1 }]);
    assert.deepEqual(calls, ['ack']);
  });

  it('retry non confirmé : PAS d\'ack, la session est fermée (redelivery contrôlée)', async () => {
    const { delivery, calls } = harness(event(), 0, true);
    await delivery.retryLater('503');
    assert.deepEqual(calls, ['abortSession']);
  });

  it(`politique bornée : au-delà de ${EVENT_MAX_RETRIES} retries, DLQ (nack sans requeue), jamais de retry`, async () => {
    const { delivery, calls, retries } = harness(event(), EVENT_MAX_RETRIES);
    await delivery.retryLater('503');
    assert.deepEqual(calls, ['nack(allUpTo=false,requeue=false)']);
    assert.equal(retries.length, 0);
  });

  it('corps non JSON : jamais de retry, DLQ', async () => {
    const { delivery, calls } = harness('{"eventId":', 0);
    await delivery.retryLater('x');
    assert.deepEqual(calls, ['nack(allUpTo=false,requeue=false)']);
  });

  it('une livraison ne se règle qu\'une fois', () => {
    const { delivery } = harness(event());
    delivery.ack();
    assert.throws(() => delivery.deadLetter('late'), /already settled/);
    assert.throws(() => delivery.ack(), /already settled/);
  });

  it('erreur non traitée : temporaire -> retry ; définitive ou inconnue -> DLQ ; jamais d\'ack', async () => {
    const temp = harness(event(), 0);
    await settleAfterError(temp.delivery, new TemporaryDependencyError());
    assert.deepEqual(temp.calls, ['ack']); // ack de l'ORIGINAL après republication confirmée (outcome retried)
    assert.equal(temp.retries.length, 1);
    const unknown = harness(event(), 0);
    await settleAfterError(unknown.delivery, new Error('bug'));
    assert.deepEqual(unknown.calls, ['nack(allUpTo=false,requeue=false)']);
    assert.equal(unknown.retries.length, 0);
  });

  it('handler sans issue : mis en DLQ bruyamment (ni ack silencieux, ni requeue)', async () => {
    const { delivery, calls } = harness(event());
    const lazy: EventHandler = { handle: async () => undefined };
    await processDelivery(lazy, delivery, () => assert.fail('no todo'));
    assert.deepEqual(calls, ['nack(allUpTo=false,requeue=false)']);
  });

  it('zone TODO non réalisée : ni ack ni nack, le worker est prévenu', async () => {
    const { delivery, calls } = harness(event());
    let zone = '';
    const todo: EventHandler = { handle: async () => { throw new TodoNotImplementedError('C1', 'x'); } };
    await processDelivery(todo, delivery, (e) => { zone = e.zone; });
    assert.equal(zone, 'C1');
    assert.deepEqual(calls, []);
  });
});

describe('smoke - TP5 worker Audit de référence', () => {
  const dir = join(tmp, 'audit-state');
  const deps = { pool: null as never, log: () => undefined, faults: createStatisticsFaults(dir), labStateDir: dir };

  it('enregistre un fait une seule fois ; un doublon est acquitté sans nouvel effet', async () => {
    const handler = new AuditHandler(deps);
    const first = harness(event('evt-audit-1'));
    await handler.handle(first.delivery);
    const second = harness(event('evt-audit-1'));
    await handler.handle(second.delivery);
    assert.deepEqual(first.calls, ['ack']);
    assert.deepEqual(second.calls, ['ack']);
    assert.deepEqual(readdirSync(join(dir, 'audit')), ['evt-audit-1.json']);
  });

  it('contrat invalide (schemaVersion=99, JSON tronqué) : DLQ, jamais ack, jamais retry', async () => {
    const handler = new AuditHandler(deps);
    for (const body of [event('evt-bad', 99), '{"eventId":']) {
      const h = harness(body);
      await handler.handle(h.delivery);
      assert.deepEqual(h.calls, ['nack(allUpTo=false,requeue=false)']);
      assert.equal(h.retries.length, 0);
    }
    assert.deepEqual(readdirSync(join(dir, 'audit')), ['evt-audit-1.json']);
  });
});

describe('smoke - TP5 configuration et injections de laboratoire', () => {
  it('consommateur inconnu refusé ; valeurs de laboratoire par défaut sur 127.0.0.1', () => {
    assert.throws(() => readEventWorkerConfig('billing', {}), /consumer must be one of/);
    const config = readEventWorkerConfig('statistics', {});
    assert.equal(config.queue, 'statistics.events');
    assert.equal(config.consumerName, 'statistics-worker');
    assert.match(config.rabbitUrl, /@127\.0\.0\.1:5672$/);
    assert.match(config.databaseUrl, /@127\.0\.0\.1:55432\/infal73_tp5_lab$/);
  });

  it('production : URL explicites obligatoires, identifiants de laboratoire interdits', () => {
    assert.throws(() => readEventWorkerConfig('audit', { NODE_ENV: 'production' }), /required when NODE_ENV=production/);
    assert.throws(() => readEventWorkerConfig('audit', { NODE_ENV: 'production', RABBITMQ_URL: 'amqp://x:lab-only-change-me@h', DATABASE_URL: 'postgres://u:p@h/db' }), /forbidden/);
  });

  it('panne Statistiques : exactement N échecs 503 puis retour à la normale (déterministe)', () => {
    const state = join(tmp, 'faults');
    mkdirSync(state, { recursive: true });
    const faults = createStatisticsFaults(state);
    assert.doesNotThrow(() => faults.callDependency({ eventId: 'e' })); // rien d'armé
    writeFileSync(join(state, STATISTICS_FAULT_FILE), JSON.stringify({ remaining: 2 }));
    assert.throws(() => faults.callDependency({ eventId: 'e' }), TemporaryDependencyError);
    assert.throws(() => faults.callDependency({ eventId: 'e' }), TemporaryDependencyError);
    assert.doesNotThrow(() => faults.callDependency({ eventId: 'e' }));
  });
});

describe('smoke - TP5 isolation des circuits de retry (composant fourni)', () => {
  for (const consumer of ['notifications', 'statistics', 'audit'] as const) {
    it(`${consumer} : un retry part vers business.retry avec SA clé, jamais vers business.events, corps et eventId inchangés`, async () => {
      const published: { exchange: string; key: string; body: string; headers: unknown }[] = [];
      const channel = {
        publish: (exchange: string, key: string, content: Buffer, options: { headers?: unknown }, cb: (err: unknown) => void) => {
          published.push({ exchange, key, body: content.toString('utf8'), headers: options.headers });
          queueMicrotask(() => cb(null)); // confirm positif, sans basic.return
          return true;
        },
        on: () => undefined,
        once: () => undefined,
      };
      const scheduler = new ConfirmedEventRetryScheduler(channel, consumer, () => undefined);
      const body = event('evt-retry-1');
      await scheduler.scheduleRetry(body, { retryCount: 1 });
      assert.equal(published.length, 1);
      assert.equal(published[0]?.exchange, 'business.retry');
      assert.equal(published[0]?.key, `${consumer}.retry`);
      assert.deepEqual(published[0]?.headers, { [EVENT_RETRY_COUNT_HEADER]: 2 });
      assert.deepEqual(JSON.parse(published[0]?.body ?? '{}'), body);
      await assert.rejects(scheduler.scheduleRetry(body, { retryCount: EVENT_MAX_RETRIES }), /retries exhausted/);
      assert.equal(published.length, 1);
    });
  }
});
