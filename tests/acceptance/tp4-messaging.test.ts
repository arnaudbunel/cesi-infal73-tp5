// TP4 — T1 à T7 : messagerie RabbitMQ et gestion des pannes. Prérequis : `npm run infra:up` (Docker).
// Corrigé : ces tests doivent tous passer (infrastructure démarrée).
// Les assertions portent sur l'ÉTAT SQL et les compteurs de queues, pas sur la seule disparition d'un message.
// Exécution séquentielle obligatoire (ressources partagées) : npm run test:acceptance:tp4
import assert from 'node:assert/strict';
import { after, afterEach, before, beforeEach, describe, it } from 'node:test';
import { compose, checkDocker } from '../../scripts/lib/docker.mjs';
import { waitForManagement } from '../../scripts/topology.mjs';
import {
  cleanLabState, closeDb, injectMessage, peekMessages, preparationMessage, preparedFor, processedFor, rows, requireInfra,
  setDependencyDown, startOrders, startWorker, stats, stopAllWorkers, waitDrained, waitFor, waitForQueue,
} from '../helpers/lab.js';

before(async () => {
  await requireInfra();
});
beforeEach(async () => {
  await cleanLabState();
});
afterEach(async () => {
  await stopAllWorkers();
});
after(async () => {
  await stopAllWorkers();
  await cleanLabState().catch(() => undefined);
  await closeDb();
});

const post = (baseUrl: string, orderId: string): Promise<Response> => fetch(`${baseUrl}/api/orders/${orderId}/preparation`, { method: 'POST' });

describe('TP4 — T1 à T7', () => {
  it('T1 nominal : 202 confirmé, une préparation en base, message acquitté', async () => {
    const orders = await startOrders();
    try {
      await startWorker('t1-worker');
      const response = await post(orders.baseUrl, 'ORD-102');
      assert.equal(response.status, 202, `statut ${response.status}`);
      assert.deepEqual(await response.json(), { orderId: 'ORD-102', preparationStatus: 'PENDING' });

      await waitFor('ligne prepared_orders pour ORD-102', async () => (await preparedFor('ORD-102')).length === 1);
      assert.deepEqual(await preparedFor('ORD-102'), [{ order_id: 'ORD-102', operation_id: 'prepare-ORD-102' }]);
      assert.equal((await processedFor('prepare-ORD-102')).length, 1);
      const final = await waitDrained(); // ACK reçu : plus rien en attente ni en cours
      assert.equal(final.unacked, 0);
      assert.equal((await stats('orders.prepare.dlq')).ready, 0);
    } finally {
      await orders.stop();
    }
  });

  it('T2 worker arrêté : le message attend en queue puis est repris au redémarrage', async () => {
    const orders = await startOrders();
    try {
      const response = await post(orders.baseUrl, 'ORD-102');
      assert.equal(response.status, 202, 'le message doit être pris en charge (confirmé) même sans consommateur');
      const waiting = await waitForQueue('orders.prepare', 'ready=1 sans consommateur', (s) => s.ready === 1 && s.consumers === 0);
      assert.equal(waiting.unacked, 0);
      assert.equal((await preparedFor('ORD-102')).length, 0, 'aucune préparation tant que le worker est arrêté');

      await startWorker('t2-worker');
      await waitFor('préparation après redémarrage du worker', async () => (await preparedFor('ORD-102')).length === 1);
      await waitDrained();
    } finally {
      await orders.stop();
    }
  });

  it('T3 crash après COMMIT, avant ACK : redelivery, un seul effet métier', async () => {
    const operationId = 'prepare-ORD-T3';
    const workerA = await startWorker('t3-a', { FAULT: 'EXIT_AFTER_COMMIT_BEFORE_ACK' });
    assert.ok(await injectMessage('orders.commands', 'order.prepare', preparationMessage(operationId, 'ORD-T3')), 'message non routé');

    const exit = await Promise.race([workerA.exited, new Promise<null>((resolve) => setTimeout(() => resolve(null), 20_000).unref())]);
    assert.ok(exit, 'le worker A aurait dû s\'arrêter après le COMMIT');
    assert.ok(exit.signal !== null || exit.code !== 0, `arrêt inattendu : ${JSON.stringify(exit)}`);
    assert.equal((await preparedFor('ORD-T3')).length, 1, 'le COMMIT doit précéder le crash');

    const workerB = await startWorker('t3-b');
    await waitFor('redelivery vers le worker B', () => workerB.deliveries(operationId).some((l) => l['redelivered'] === true));
    await waitDrained();
    assert.equal((await preparedFor('ORD-T3')).length, 1, 'la redelivery ne doit pas créer une seconde préparation');
    assert.equal((await processedFor(operationId)).length, 1);
    assert.equal((await stats('orders.prepare.dlq')).ready, 0);
  });

  it('T4 dépendance 503 : retry différé (~2 s) puis succès, operationId stable', async () => {
    const operationId = 'prepare-ORD-T4';
    const worker = await startWorker('t4-worker');
    setDependencyDown(1_500);
    assert.ok(await injectMessage('orders.commands', 'order.prepare', preparationMessage(operationId, 'ORD-T4')));

    await waitFor('préparation après retry', async () => (await preparedFor('ORD-T4')).length === 1, 25_000);
    const deliveries = worker.deliveries(operationId);
    assert.deepEqual(deliveries.map((l) => l['attempt']), [0, 1], 'attempt doit passer de 0 à 1, operationId inchangé');
    const gap = Date.parse(String(deliveries[1]?.ts)) - Date.parse(String(deliveries[0]?.ts));
    assert.ok(gap >= 1_800 && gap <= 8_000, `délai de retry observé : ${gap} ms (attendu ≈ 2 000 ms, TTL non garanti à la milliseconde)`);
    await waitDrained();
    assert.equal((await stats('orders.prepare.dlq')).ready, 0);
  });

  it('T4b dépendance en panne durable : 3 retries (2 s, 5 s, 10 s) puis DLQ', { timeout: 90_000 }, async () => {
    const operationId = 'prepare-ORD-T4B';
    const worker = await startWorker('t4b-worker');
    setDependencyDown(120_000);
    assert.ok(await injectMessage('orders.commands', 'order.prepare', preparationMessage(operationId, 'ORD-T4B')));

    await waitForQueue('orders.prepare.dlq', 'ready=1 après épuisement des tentatives', (s) => s.ready === 1, 60_000);
    const deliveries = worker.deliveries(operationId);
    assert.deepEqual(deliveries.map((l) => l['attempt']), [0, 1, 2, 3], 'quatre traitements au maximum');
    const gaps = [1, 2, 3].map((i) => Date.parse(String(deliveries[i]?.ts)) - Date.parse(String(deliveries[i - 1]?.ts)));
    assert.ok((gaps[0] ?? 0) >= 1_800 && (gaps[1] ?? 0) >= 4_800 && (gaps[2] ?? 0) >= 9_800, `délais observés : ${gaps.join(' / ')} ms`);
    assert.equal((await preparedFor('ORD-T4B')).length, 0, 'aucune préparation tant que la dépendance est en panne');
    await waitDrained();
  });

  it('T5 JSON invalide : rejet définitif en DLQ, sans retry, sans effet', async () => {
    const worker = await startWorker('t5-worker');
    const truncated = '{"schemaVersion":1,"type":"OrderPreparationRequested","operationId":"prepare-ORD-BAD","orderId":"ORD-B';
    assert.ok(await injectMessage('orders.commands', 'order.prepare', truncated), 'message non routé');

    await waitForQueue('orders.prepare.dlq', 'ready=1', (s) => s.ready === 1);
    await waitDrained();
    const [dead] = (await peekMessages('orders.prepare.dlq', 1)) as { payload: string }[];
    assert.equal(dead?.payload, truncated, 'le message d\'origine doit être conservé tel quel en DLQ');
    assert.equal(worker.deliveries().length, 1, 'aucune nouvelle livraison : une erreur définitive ne se rejoue pas');
    for (const retryQueue of ['orders.retry.2s', 'orders.retry.5s', 'orders.retry.10s']) {
      assert.equal((await stats(retryQueue)).ready, 0, `${retryQueue} doit rester vide`);
    }
    assert.equal((await rows('SELECT 1 FROM prepared_orders')).length, 0);
  });

  it('T6 deux workers, même operationId : un seul effet métier', async () => {
    const operationId = 'prepare-ORD-T6';
    const workerA = await startWorker('t6-a');
    const workerB = await startWorker('t6-b');
    for (let i = 0; i < 2; i += 1) {
      assert.ok(await injectMessage('orders.commands', 'order.prepare', preparationMessage(operationId, 'ORD-T6'), { message_id: `t6-msg-${i}` }));
    }
    await waitDrained();
    assert.equal(workerA.deliveries(operationId).length + workerB.deliveries(operationId).length >= 2, true, 'les deux livraisons doivent avoir eu lieu');
    assert.equal((await preparedFor('ORD-T6')).length, 1, 'un seul effet métier');
    assert.equal((await processedFor(operationId)).length, 1);
    assert.equal((await stats('orders.prepare.dlq')).ready, 0, 'un doublon légitime n\'est pas une erreur');
  });

  it('T6-bonus deux operationId pour un même orderId : une seule préparation, aucune perte silencieuse', async () => {
    const [opA, opB] = ['prepare-ORD-T6B', 'prepare-ORD-T6B-bis'] as const;
    await startWorker('t6b-a');
    await startWorker('t6b-b');
    for (const operationId of [opA, opB]) assert.ok(await injectMessage('orders.commands', 'order.prepare', preparationMessage(operationId, 'ORD-T6B')));
    await waitDrained();
    assert.equal((await preparedFor('ORD-T6B')).length, 1, 'la commande ne doit être préparée qu\'une fois');
    const traced = (await processedFor(opA, opB)).length + (await stats('orders.prepare.dlq')).ready;
    assert.ok(traced >= 2, 'la demande contradictoire doit rester traçable (DLQ ou trace SQL), pas disparaître en silence');
  });

  it('T7 broker indisponible : jamais de faux 202, erreur contrôlée, reprise après redémarrage', { timeout: 150_000 }, async () => {
    const docker = checkDocker();
    assert.ok(docker.ok, `Docker requis pour T7 : ${docker.hint}`);
    const orders = await startOrders();
    try {
      assert.ok(compose(['stop', 'rabbitmq']).ok, 'arrêt de RabbitMQ impossible');
      const started = Date.now();
      const down = await post(orders.baseUrl, 'ORD-102');
      assert.notEqual(down.status, 202, 'FAUX 202 : le broker est arrêté');
      assert.equal(down.status, 503, 'erreur contrôlée attendue (503)');
      assert.match(down.headers.get('content-type') ?? '', /application\/problem\+json/);
      assert.ok(Date.now() - started < 15_000, 'la requête ne doit pas rester suspendue');
    } finally {
      assert.ok(compose(['start', 'rabbitmq']).ok, 'redémarrage de RabbitMQ impossible');
      await waitForManagement(120_000);
    }
    try {
      // Les connexions ont été coupées : les premières requêtes peuvent échouer, JAMAIS par un faux 202 ni un 5xx autre que 503.
      let status = 0;
      await waitFor('202 après redémarrage du broker', async () => {
        status = (await post(orders.baseUrl, 'ORD-102')).status;
        assert.ok(status === 202 || status === 503, `statut inattendu pendant la reprise : ${status}`);
        return status === 202;
      }, 60_000, 1_000);
      assert.equal(status, 202);
      await waitForQueue('orders.prepare', 'ready=1 (le 202 correspond à un message réellement en queue)', (s) => s.ready === 1);
    } finally {
      await orders.stop();
    }
  });
});
