import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { demoLogin, getOrder, patchStatus, startFullStack, WsClient } from '../helpers/stack.js';
import type { FullStack } from '../helpers/stack.js';

async function withStack(fn: (stack: FullStack) => Promise<void>): Promise<void> {
  const stack = await startFullStack();
  try {
    await fn(stack);
  } finally {
    await stack.stop();
  }
}

const connectAs = async (stack: FullStack, user: 'alice' | 'bob') => WsClient.connect(stack.wsUrl, await demoLogin(stack.baseUrl, user));

describe('TP3 WebSocket - T4 : abonné autorisé', () => {
  it('T4 - Alice abonnée à ORD-102 : SUBSCRIBED puis UN SEUL ORDER_STATUS_CHANGED', async () => {
    await withStack(async (stack) => {
      const alice = await connectAs(stack, 'alice');
      assert.deepEqual(await alice.request({ type: 'SUBSCRIBE', orderId: 'ORD-102' }), { type: 'SUBSCRIBED', orderId: 'ORD-102' });
      assert.equal((await patchStatus(stack.baseUrl, 'ORD-102', 'SHIPPED')).status, 200);
      const events = await alice.drain();
      assert.deepEqual(events, [{ type: 'ORDER_STATUS_CHANGED', orderId: 'ORD-102', sequence: 1, status: 'SHIPPED' }]);
      await alice.close();
    });
  });

  it("T4b - la diffusion est ciblée : pas d'événement pour une commande non suivie", async () => {
    await withStack(async (stack) => {
      const alice = await connectAs(stack, 'alice');
      assert.deepEqual(await alice.request({ type: 'SUBSCRIBE', orderId: 'ORD-102' }), { type: 'SUBSCRIBED', orderId: 'ORD-102' });
      assert.equal((await patchStatus(stack.baseUrl, 'ORD-101', 'CONFIRMED')).status, 200);
      assert.deepEqual(await alice.drain(), []);
      await alice.close();
    });
  });

  it('T4c - après fermeture de la socket, plus aucun envoi (nettoyage) et le serveur reste sain', async () => {
    await withStack(async (stack) => {
      const alice = await connectAs(stack, 'alice');
      const other = await connectAs(stack, 'alice');
      await alice.request({ type: 'SUBSCRIBE', orderId: 'ORD-102' });
      assert.deepEqual(await other.request({ type: 'SUBSCRIBE', orderId: 'ORD-102' }), { type: 'SUBSCRIBED', orderId: 'ORD-102' });
      await alice.close();
      assert.equal((await patchStatus(stack.baseUrl, 'ORD-102', 'SHIPPED')).status, 200);
      assert.equal((await other.drain()).length, 1);
      await other.close();
    });
  });
});

describe('TP3 WebSocket - T5 : abonné non autorisé', () => {
  it('T5 - Bob : FORBIDDEN_SUBSCRIPTION, aucune notification, aucune donnée de la commande', async () => {
    await withStack(async (stack) => {
      const bob = await connectAs(stack, 'bob');
      const refusal = await bob.request({ type: 'SUBSCRIBE', orderId: 'ORD-102' });
      assert.deepEqual(refusal, { type: 'ERROR', code: 'FORBIDDEN_SUBSCRIPTION' });
      assert.equal((await patchStatus(stack.baseUrl, 'ORD-102', 'SHIPPED')).status, 200);
      assert.deepEqual(await bob.drain(), []);
      await bob.close();
    });
  });

  it("T5b - commande inexistante : même réponse que non autorisée (aucune révélation d'existence)", async () => {
    await withStack(async (stack) => {
      const bob = await connectAs(stack, 'bob');
      const unknown = await bob.request({ type: 'SUBSCRIBE', orderId: 'ORD-999' });
      const forbidden = await bob.request({ type: 'SUBSCRIBE', orderId: 'ORD-102' });
      assert.deepEqual(unknown, forbidden);
      assert.deepEqual(unknown, { type: 'ERROR', code: 'FORBIDDEN_SUBSCRIPTION' });
      await bob.close();
    });
  });

  it('T5c - les droits sont revérifiés à chaque SUBSCRIBE (Bob : sa propre commande ORD-104 est acceptée)', async () => {
    await withStack(async (stack) => {
      const bob = await connectAs(stack, 'bob');
      assert.deepEqual(await bob.request({ type: 'SUBSCRIBE', orderId: 'ORD-104' }), { type: 'SUBSCRIBED', orderId: 'ORD-104' });
      assert.deepEqual(await bob.request({ type: 'SUBSCRIBE', orderId: 'ORD-102' }), { type: 'ERROR', code: 'FORBIDDEN_SUBSCRIPTION' });
      await bob.close();
    });
  });
});

describe('TP3 WebSocket - T6 : transition refusée', () => {
  it('T6-REST - ORD-103 SHIPPED -> CANCELLED : 409 et sequence inchangée (règle héritée du TP1, sans WebSocket)', async () => {
    await withStack(async (stack) => {
      const res = await fetch(`${stack.baseUrl}/api/orders/ORD-103/cancellations`, { method: 'POST' });
      assert.equal(res.status, 409);
      const order = await getOrder(stack.baseUrl, 'ORD-103');
      assert.deepEqual([order.status, order.sequence], ['SHIPPED', 0]);
    });
  });

  it('T6 - ORD-103 SHIPPED -> CANCELLED : 409 et zéro ORDER_STATUS_CHANGED', async () => {
    await withStack(async (stack) => {
      const alice = await connectAs(stack, 'alice');
      assert.deepEqual(await alice.request({ type: 'SUBSCRIBE', orderId: 'ORD-103' }), { type: 'SUBSCRIBED', orderId: 'ORD-103' });
      const res = await fetch(`${stack.baseUrl}/api/orders/ORD-103/cancellations`, { method: 'POST' });
      assert.equal(res.status, 409);
      assert.deepEqual(await alice.drain(), []);
      assert.equal((await getOrder(stack.baseUrl, 'ORD-103')).sequence, 0);
      await alice.close();
    });
  });
});

describe('TP3 WebSocket - T7 (serveur) : coupure puis reconnexion', () => {
  it('T7a - transition pendant la coupure ; après reconnexion et nouveau SUBSCRIBE, REST donne l\'état courant', async () => {
    await withStack(async (stack) => {
      const first = await connectAs(stack, 'alice');
      await first.request({ type: 'SUBSCRIBE', orderId: 'ORD-101' });
      await first.close();

      assert.equal((await patchStatus(stack.baseUrl, 'ORD-101', 'CONFIRMED')).status, 200);

      const second = await connectAs(stack, 'alice');
      assert.deepEqual(await second.request({ type: 'SUBSCRIBE', orderId: 'ORD-101' }), { type: 'SUBSCRIBED', orderId: 'ORD-101' });
      const snapshot = await getOrder(stack.baseUrl, 'ORD-101');
      assert.deepEqual([snapshot.status, snapshot.sequence], ['CONFIRMED', 1]);

      assert.equal((await patchStatus(stack.baseUrl, 'ORD-101', 'SHIPPED')).status, 200);
      assert.deepEqual(await second.drain(), [{ type: 'ORDER_STATUS_CHANGED', orderId: 'ORD-101', sequence: 2, status: 'SHIPPED' }]);
      await second.close();
    });
  });
});
