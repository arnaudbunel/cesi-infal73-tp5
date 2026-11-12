import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { describe, it } from 'node:test';
import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';
import { buildApp } from '../../apps/order-service/src/app.js';
import { STOCK_PROTO_PATH } from '../../apps/order-service/src/grpc/stock-proto.js';
import { callGetStock, demoLogin, getOrder, patchStatus, startFullStack, WsClient } from '../helpers/stack.js';

/** Exécute `fn` avec NODE_ENV=production puis restaure l'environnement. */
async function inProduction(fn: () => Promise<void>): Promise<void> {
  const previous = process.env['NODE_ENV'];
  process.env['NODE_ENV'] = 'production';
  try {
    await fn();
  } finally {
    if (previous === undefined) delete process.env['NODE_ENV'];
    else process.env['NODE_ENV'] = previous;
  }
}

describe('smoke - TP3 contrat gRPC', () => {
  it('le .proto se charge et expose inventory.v1.StockService/GetStock', () => {
    const definition = protoLoader.loadSync(STOCK_PROTO_PATH, { keepCase: false });
    const service = definition['inventory.v1.StockService'];
    assert.ok(service, 'service inventory.v1.StockService absent');
    assert.ok('GetStock' in service);
    assert.match(readFileSync(STOCK_PROTO_PATH, 'utf8'), /string product_id = 1;/);
  });

  it('le serveur gRPC Stocks est joignable (réponse OK ou UNIMPLEMENTED, jamais UNAVAILABLE)', async () => {
    const stack = await startFullStack();
    try {
      const { error } = await callGetStock(stack.grpcTarget, 'P12');
      const code = error?.code ?? grpc.status.OK;
      assert.ok(code === grpc.status.OK || code === grpc.status.UNIMPLEMENTED, `statut inattendu : ${code}`);
    } finally {
      await stack.stop();
    }
  });
});

describe('smoke - TP3 Stocks HTTP : catalogue partagé', () => {
  it('P12 = 12, P13 = 0 (stock nul valide), produit inconnu = 404', async () => {
    const stack = await startFullStack();
    try {
      const stockUrl = `http://127.0.0.1:${stack.stockPort}`;
      assert.deepEqual(await (await fetch(`${stockUrl}/stock/P12`)).json(), { productId: 'P12', available: 12 });
      assert.deepEqual(await (await fetch(`${stockUrl}/stock/P13`)).json(), { productId: 'P13', available: 0 });
      assert.equal((await fetch(`${stockUrl}/stock/P999`)).status, 404);
    } finally {
      await stack.stop();
    }
  });
});

describe('smoke - TP3 REST : sequence et identités', () => {
  it('les données initiales sont déterministes et exposent sequence = 0', async () => {
    const app = buildApp();
    for (const [id, status] of [['ORD-101', 'CREATED'], ['ORD-102', 'CONFIRMED'], ['ORD-103', 'SHIPPED'], ['ORD-104', 'CANCELLED']] as const) {
      const res = await app.inject({ method: 'GET', url: `/api/orders/${id}` });
      const body = res.json<{ status: string; sequence: number; ownerId?: unknown }>();
      assert.equal(body.status, status);
      assert.equal(body.sequence, 0);
      assert.equal(body.ownerId, undefined, "le propriétaire n'est pas exposé par REST");
    }
    await app.close();
  });

  it('sequence : +1 par transition enregistrée, inchangée sur refus', async () => {
    const stack = await startFullStack();
    try {
      assert.equal((await patchStatus(stack.baseUrl, 'ORD-101', 'CONFIRMED')).status, 200);
      const confirmed = await getOrder(stack.baseUrl, 'ORD-101');
      assert.deepEqual([confirmed.status, confirmed.sequence], ['CONFIRMED', 1]);
      assert.equal((await patchStatus(stack.baseUrl, 'ORD-101', 'CONFIRMED')).status, 409); // identique : refusée
      assert.equal((await patchStatus(stack.baseUrl, 'ORD-103', 'CONFIRMED')).status, 409); // SHIPPED : refusée
      assert.equal((await getOrder(stack.baseUrl, 'ORD-101')).sequence, 1);
      assert.equal((await getOrder(stack.baseUrl, 'ORD-103')).sequence, 0);
      assert.equal((await patchStatus(stack.baseUrl, 'ORD-101', 'SHIPPED')).status, 200);
      assert.equal((await getOrder(stack.baseUrl, 'ORD-101')).sequence, 2);
    } finally {
      await stack.stop();
    }
  });

  it('reset : une nouvelle application repart des mêmes fixtures', async () => {
    const first = buildApp();
    await first.inject({ method: 'PATCH', url: '/api/orders/ORD-102', payload: { status: 'SHIPPED' } });
    const second = buildApp();
    const res = await second.inject({ method: 'GET', url: '/api/orders/ORD-102' });
    assert.deepEqual([res.json<{ status: string }>().status, res.json<{ sequence: number }>().sequence], ['CONFIRMED', 0]);
    await first.close();
    await second.close();
  });

  it('/availability reste via HTTP par défaut (STOCK_BACKEND=http) et inchangé', async () => {
    const stack = await startFullStack();
    try {
      const res = await fetch(`${stack.baseUrl}/api/orders/ORD-102/availability`);
      assert.equal(res.status, 200);
      assert.deepEqual(await res.json(), { orderId: 'ORD-102', productId: 'P12', requestedQuantity: 2, available: 12, canFulfill: true });
    } finally {
      await stack.stop();
    }
  });
});

describe('smoke - TP3 WebSocket et session démo', () => {
  it('handshake accepté avec une identité valide, refusé sans session ni avec un cookie falsifié', async () => {
    const stack = await startFullStack();
    try {
      const cookie = await demoLogin(stack.baseUrl, 'alice');
      const client = await WsClient.connect(stack.wsUrl, cookie);
      await client.close();
      await assert.rejects(WsClient.connect(stack.wsUrl), /401/);
      await assert.rejects(WsClient.connect(stack.wsUrl, 'demo_session=bob'), /401/); // non signé
      await assert.rejects(WsClient.connect(stack.wsUrl, 'demo_session=s:bob.AAAA'), /401/); // signature invalide
    } finally {
      await stack.stop();
    }
  });

  it("le serveur ignore toute identité fournie dans le JSON (champs refusés) et répond INVALID_MESSAGE", async () => {
    const stack = await startFullStack();
    try {
      const client = await WsClient.connect(stack.wsUrl, await demoLogin(stack.baseUrl, 'bob'));
      const replies = [
        await client.request('pas du json'),
        await client.request({ type: 'INCONNU' }),
        await client.request({ type: 'SUBSCRIBE' }),
        await client.request({ type: 'SUBSCRIBE', orderId: 'ORD-102', userId: 'alice' }),
        await client.request('[]'),
      ];
      for (const reply of replies) assert.deepEqual(reply, { type: 'ERROR', code: 'INVALID_MESSAGE' });
      await client.close();
    } finally {
      await stack.stop();
    }
  });

  it('un message trop volumineux ferme la connexion (code 1009)', async () => {
    const stack = await startFullStack();
    try {
      const client = await WsClient.connect(stack.wsUrl, await demoLogin(stack.baseUrl, 'alice'));
      client.send('x'.repeat(5000));
      assert.equal(await client.closed, 1009);
    } finally {
      await stack.stop();
    }
  });

  it('une origine étrangère est refusée (403)', async () => {
    const stack = await startFullStack();
    try {
      const { default: WebSocket } = await import('ws');
      const cookie = await demoLogin(stack.baseUrl, 'alice');
      const socket = new WebSocket(stack.wsUrl, { headers: { cookie, origin: 'http://evil.example' } });
      const status = await new Promise<number>((resolve) => socket.once('unexpected-response', (_req, res) => resolve(res.statusCode ?? 0)));
      assert.equal(status, 403);
    } finally {
      await stack.stop();
    }
  });

  it('en production : connexion démo, interface démo et diagnostics absentes', async () => {
    await inProduction(async () => {
      const app = buildApp();
      for (const [method, url] of [['POST', '/api/demo/login'], ['GET', '/demo/'], ['GET', '/api/diagnostics/blocking']] as const) {
        const res = await app.inject({ method, url, payload: method === 'POST' ? { user: 'alice' } : undefined });
        assert.equal(res.statusCode, 404, `${method} ${url}`);
      }
      await app.close();
    });
  });

  it('la connexion démo refuse une identité inconnue (400)', async () => {
    const app = buildApp();
    const res = await app.inject({ method: 'POST', url: '/api/demo/login', payload: { user: 'mallory' } });
    assert.equal(res.statusCode, 400);
    await app.close();
  });
});

describe('smoke - TP3 client web', () => {
  it("l'interface est servie, avec ses scripts, sur /demo/", async () => {
    const stack = await startFullStack();
    try {
      const page = await fetch(`${stack.baseUrl}/demo/`);
      assert.equal(page.status, 200);
      assert.match(await page.text(), /data-app="infal73-tp3-demo"/);
      for (const file of ['app.js', 'sync.js', 'style.css']) {
        assert.equal((await fetch(`${stack.baseUrl}/demo/${file}`)).status, 200, file);
      }
      assert.equal((await fetch(`${stack.baseUrl}/demo/../package.json`)).status, 404);
    } finally {
      await stack.stop();
    }
  });
});
