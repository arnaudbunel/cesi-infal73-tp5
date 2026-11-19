// Comportement du TP4 corrigé (hérité) sans infrastructure : non-régression, aucun lien avec les zones TODO(TP5).
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { describe, it } from 'node:test';
import { fileURLToPath } from 'node:url';
import { buildApp } from '../../apps/order-service/src/app.js';
import { readRabbitUrl } from '../../apps/order-service/src/config.js';
import { PublishError } from '../../apps/order-service/src/messaging/errors.js';
import type { PreparationPublisher, PublishInput } from '../../apps/order-service/src/messaging/preparation-publisher.js';
import { readWorkerConfig } from '../../apps/preparation-worker/src/config.js';
import { createLabFaults } from '../../apps/preparation-worker/src/lab-faults.js';

const repoRoot = fileURLToPath(new URL('../..', import.meta.url));
const post = (app: ReturnType<typeof buildApp>, orderId: string) => app.inject({ method: 'POST', url: `/api/orders/${orderId}/preparation` });

describe('smoke - TP4 route POST /api/orders/:orderId/preparation', () => {
  it('commande inconnue : 404 ; CANCELLED et SHIPPED : 409 (aucune publication)', async () => {
    const calls: PublishInput[] = [];
    const app = buildApp({ publisher: { publishConfirmed: async (input) => (calls.push(input), { messageId: 'x' }) } });
    try {
      assert.equal((await post(app, 'ORD-999')).statusCode, 404);
      assert.equal((await post(app, 'ORD-104')).statusCode, 409);
      assert.equal((await post(app, 'ORD-103')).statusCode, 409);
      assert.equal(calls.length, 0);
    } finally {
      await app.close();
    }
  });

  it('sans messagerie configurée : 503, jamais 202', async () => {
    const app = buildApp();
    try {
      assert.equal((await post(app, 'ORD-102')).statusCode, 503);
    } finally {
      await app.close();
    }
  });

  it('la route transmet le bon message (mandatory, persistant, operationId stable) et répond 202 sur confirmation', async () => {
    const calls: PublishInput[] = [];
    const publisher: PreparationPublisher = { publishConfirmed: async (input) => (calls.push(input), { messageId: 'm-1' }) };
    const app = buildApp({ publisher });
    try {
      const response = await post(app, 'ORD-102');
      assert.equal(response.statusCode, 202);
      assert.deepEqual(response.json(), { orderId: 'ORD-102', preparationStatus: 'PENDING' });
      assert.equal(calls.length, 1);
      const [call] = calls;
      assert.equal(call?.exchange, 'orders.commands');
      assert.equal(call?.routingKey, 'order.prepare');
      assert.equal(call?.mandatory, true);
      assert.equal(call?.persistent, true);
      assert.equal((call?.payload as { operationId: string }).operationId, 'prepare-ORD-102');
      // aucune transition d'état : la préparation ne contourne pas les règles métier
      const order = (await app.inject({ method: 'GET', url: '/api/orders/ORD-102' })).json() as { status: string; sequence: number };
      assert.deepEqual({ status: order.status, sequence: order.sequence }, { status: 'CONFIRMED', sequence: 0 });
    } finally {
      await app.close();
    }
  });

  it('toute publication non confirmée donne 503 problem+json, jamais 202', async () => {
    for (const kind of ['broker-unavailable', 'unroutable', 'nacked', 'outcome-unknown'] as const) {
      const app = buildApp({ publisher: { publishConfirmed: async () => Promise.reject(new PublishError(kind, 'x')) } });
      try {
        const response = await post(app, 'ORD-102');
        assert.equal(response.statusCode, 503, kind);
        assert.match(String(response.headers['content-type']), /problem\+json/);
        assert.doesNotMatch(response.body, /amqp|127\.0\.0\.1/);
      } finally {
        await app.close();
      }
    }
  });
});

describe('smoke - TP4 worker (configuration et injections)', () => {
  it('configuration : prefetch 2, valeurs de laboratoire par défaut, FAULT inconnue refusée', () => {
    const config = readWorkerConfig({});
    assert.equal(config.prefetch, 2);
    assert.match(config.rabbitUrl, /^amqp:\/\/lab:.*@127\.0\.0\.1:5672$/);
    assert.match(config.databaseUrl, /@127\.0\.0\.1:55432\/infal73_tp4_lab$/);
    assert.equal(config.fault, null);
    assert.equal(readWorkerConfig({ FAULT: 'EXIT_AFTER_COMMIT_BEFORE_ACK' }).fault, 'EXIT_AFTER_COMMIT_BEFORE_ACK');
    assert.throws(() => readWorkerConfig({ FAULT: 'BOOM' }), /unknown FAULT/);
  });

  it('production : FAULT, identifiants de laboratoire et valeurs par défaut sont refusés', () => {
    const prod = { NODE_ENV: 'production' };
    assert.throws(() => readWorkerConfig({ ...prod, FAULT: 'EXIT_AFTER_COMMIT_BEFORE_ACK', RABBITMQ_URL: 'amqp://a:b@h', DATABASE_URL: 'postgres://a:b@h/d' }), /forbidden/);
    assert.throws(() => readWorkerConfig(prod), /required/);
    assert.throws(() => readWorkerConfig({ ...prod, RABBITMQ_URL: 'amqp://lab:lab-only-change-me@h', DATABASE_URL: 'postgres://a:b@h/d' }), /lab credentials/);
    assert.equal(readWorkerConfig({ ...prod, RABBITMQ_URL: 'amqp://a:b@h', DATABASE_URL: 'postgres://a:b@h/d' }).fault, null);
    assert.throws(() => readRabbitUrl(prod), /required/);
    assert.throws(() => readRabbitUrl({ ...prod, RABBITMQ_URL: 'amqp://lab:lab-only-change-me@h' }), /forbidden/);
    assert.equal(readRabbitUrl({ ...prod, RABBITMQ_URL: 'amqp://a:b@h' }), 'amqp://a:b@h');
  });

  it('simulation de dépendance 503 : fenêtre contrôlée par le fichier d\'état, rétablissement automatique', () => {
    const dir = mkdtempSync(join(tmpdir(), 'tp4-state-'));
    try {
      const faults = createLabFaults({ fault: null, labStateDir: dir }, () => undefined);
      assert.doesNotThrow(() => faults.callDependency());
      writeFileSync(join(dir, 'dependency-503-until'), String(Date.now() + 60_000));
      assert.throws(() => faults.callDependency(), /503/);
      writeFileSync(join(dir, 'dependency-503-until'), String(Date.now() - 1));
      assert.doesNotThrow(() => faults.callDependency());
      assert.doesNotThrow(() => faults.afterCommitBeforeAck({ operationId: 'o', orderId: 'ORD-1' })); // sans FAULT : sans effet
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  it('FAULT=EXIT_AFTER_COMMIT_BEFORE_ACK tue réellement le processus (SIGKILL) au point d\'injection', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'tp4-fault-'));
    try {
      const script = join(dir, 'probe.ts');
      const module = fileURLToPath(new URL('../../apps/preparation-worker/src/lab-faults.ts', import.meta.url));
      writeFileSync(script, `import { createLabFaults } from ${JSON.stringify(module)};\ncreateLabFaults({ fault: 'EXIT_AFTER_COMMIT_BEFORE_ACK', labStateDir: ${JSON.stringify(dir)} }, () => undefined).afterCommitBeforeAck({ operationId: 'o', orderId: 'ORD-1' });\nconsole.log('SURVECU');\n`);
      const child = spawn(process.execPath, ['--import', 'tsx', script], { cwd: repoRoot, stdio: ['ignore', 'pipe', 'pipe'] });
      let out = '';
      child.stdout.on('data', (c: Buffer) => (out += String(c)));
      const exit = await new Promise<{ code: number | null; signal: string | null }>((resolve) => child.once('exit', (code, signal) => resolve({ code, signal })));

      if (process.platform === 'win32') {
        // Windows n'a pas de signaux POSIX : process.kill(pid, 'SIGKILL') termine brutalement le processus
        // (TerminateProcess) avec un code de sortie non nul et sans signal.
        assert.equal(exit.signal, null);
        assert.notEqual(exit.code, 0);
      } else {
        assert.equal(exit.signal, 'SIGKILL');
      }
      assert.doesNotMatch(out, /SURVECU/);
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });
});
