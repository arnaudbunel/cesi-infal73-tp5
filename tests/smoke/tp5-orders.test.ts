// Service Commandes (TP5) SANS broker : le comportement REST acquis est préservé, le failpoint T7 et la remise à zéro de laboratoire fonctionnent.
// Ces tests ne dépendent PAS de la zone B (publication) : ils restent valides avant et après la réalisation étudiante.
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { after, describe, it } from 'node:test';
import { buildApp } from '../../apps/order-service/src/app.js';
import type { OrderShippedPublisher } from '../../apps/order-service/src/events/order-shipped-publisher.js';
import { createFileFailpoints, FAILPOINT_FILE } from '../../apps/order-service/src/lab-failpoints.js';

const tmp = mkdtempSync(join(tmpdir(), 'tp5-orders-'));
after(() => rmSync(tmp, { recursive: true, force: true }));

describe('smoke - TP5 Commandes : REST acquis, failpoint T7, remise à zéro', () => {
  it('fixtures : ORD-102 CONFIRMED -> SHIPPED autorisé (version métier +1) ; ORD-103 SHIPPED -> CANCELLED refusé', async () => {
    const app = buildApp();
    const shipped = await app.inject({ method: 'PATCH', url: '/api/orders/ORD-102', payload: { status: 'SHIPPED' } });
    assert.equal(shipped.statusCode, 200);
    assert.equal(shipped.json().status, 'SHIPPED');
    assert.equal(shipped.json().sequence, 1); // aggregateVersion de l'événement : la version métier, pas un compteur de livraisons
    const refused = await app.inject({ method: 'POST', url: '/api/orders/ORD-103/cancellations' });
    assert.equal(refused.statusCode, 409);
    const again = await app.inject({ method: 'PATCH', url: '/api/orders/ORD-102', payload: { status: 'SHIPPED' } });
    assert.equal(again.statusCode, 409); // ORD-102 n'est expédiable qu'une fois : reset ou commande distincte pour rejouer
    await app.close();
  });

  it('failpoint AFTER_SHIP_COMMIT_BEFORE_EVENT_PUBLISH : 500, commande SHIPPED, aucune publication tentée, une seule fois', async () => {
    const calls: string[] = [];
    const spy: OrderShippedPublisher = { publishShipped: async (order) => void calls.push(order.id) };
    const dir = join(tmp, 'fp');
    const app = buildApp({ labMode: true, shippedPublisher: spy, failpoints: createFileFailpoints(dir, () => undefined) });
    writeFileSync(join(dir, FAILPOINT_FILE), JSON.stringify({ name: 'AFTER_SHIP_COMMIT_BEFORE_EVENT_PUBLISH', mode: 'throw', once: true }));
    const crashed = await app.inject({ method: 'PATCH', url: '/api/orders/ORD-102', payload: { status: 'SHIPPED' } });
    assert.equal(crashed.statusCode, 500);
    assert.deepEqual(calls, []); // le failpoint précède toute publication
    const state = await app.inject({ method: 'GET', url: '/api/orders/ORD-102' });
    assert.equal(state.json().status, 'SHIPPED'); // transition enregistrée malgré l'erreur : c'est la fenêtre de perte
    // Désarmé après un déclenchement : une autre expédition n'est plus perturbée par le failpoint.
    const created = await app.inject({ method: 'POST', url: '/api/orders', headers: { 'idempotency-key': 'k-fp' }, payload: { customerId: 'C1', items: [{ productId: 'P1', quantity: 1 }] } });
    const id = created.json().id as string;
    await app.inject({ method: 'PATCH', url: `/api/orders/${id}`, payload: { status: 'CONFIRMED' } });
    const ok = await app.inject({ method: 'PATCH', url: `/api/orders/${id}`, payload: { status: 'SHIPPED' } });
    assert.equal(ok.statusCode, 200);
    await app.close();
  });

  it('failpoints : exigent labMode, interdits en production', () => {
    assert.throws(() => buildApp({ failpoints: createFileFailpoints(join(tmp, 'x'), () => undefined) }), /require buildApp\(\{ labMode: true \}\)/);
    const previous = process.env['NODE_ENV'];
    process.env['NODE_ENV'] = 'production';
    try {
      assert.throws(() => createFileFailpoints(join(tmp, 'y'), () => undefined), /forbidden when NODE_ENV=production/);
    } finally {
      if (previous === undefined) delete process.env['NODE_ENV'];
      else process.env['NODE_ENV'] = previous;
    }
  });

  it('remise à zéro des commandes : uniquement en LAB_MODE ; ORD-102 redevient CONFIRMED', async () => {
    const plain = buildApp();
    assert.equal((await plain.inject({ method: 'POST', url: '/api/lab/reset-orders' })).statusCode, 404);
    await plain.close();

    const app = buildApp({ labMode: true });
    await app.inject({ method: 'PATCH', url: '/api/orders/ORD-102', payload: { status: 'SHIPPED' } });
    const reset = await app.inject({ method: 'POST', url: '/api/lab/reset-orders' });
    assert.equal(reset.statusCode, 200);
    const order = await app.inject({ method: 'GET', url: '/api/orders/ORD-102' });
    assert.equal(order.json().status, 'CONFIRMED');
    assert.equal(order.json().sequence, 0);
    assert.equal((await app.inject({ method: 'GET', url: '/api/orders/ORD-103' })).json().status, 'SHIPPED');
    await app.close();
  });
});
