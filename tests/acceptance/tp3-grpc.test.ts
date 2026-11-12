import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import * as grpc from '@grpc/grpc-js';
import { callGetStock, startFullStack } from '../helpers/stack.js';

const TP2_ORD102 = { orderId: 'ORD-102', productId: 'P12', requestedQuantity: 2, available: 12, canFulfill: true };

describe('TP3 gRPC - T1 : P12 / P13', () => {
  it('T1a - GetStock(P12) : available 12, reservable true', async () => {
    const stack = await startFullStack();
    try {
      const { error, response } = await callGetStock(stack.grpcTarget, 'P12');
      assert.equal(error, null);
      assert.deepEqual(response, { productId: 'P12', available: 12, reservable: true });
    } finally {
      await stack.stop();
    }
  });

  it('T1b - GetStock(P13) : available 0, reservable false (stock nul ≠ produit inconnu)', async () => {
    const stack = await startFullStack();
    try {
      const { error, response } = await callGetStock(stack.grpcTarget, 'P13');
      assert.equal(error, null);
      assert.deepEqual(response, { productId: 'P13', available: 0, reservable: false });
    } finally {
      await stack.stop();
    }
  });

  it('T1c - REST /availability identique au TP2 lorsque Commandes utilise gRPC', async () => {
    const stack = await startFullStack({ orders: { stockBackend: 'grpc' } });
    try {
      const res = await fetch(`${stack.baseUrl}/api/orders/ORD-102/availability`);
      assert.equal(res.status, 200);
      assert.deepEqual(await res.json(), TP2_ORD102);
    } finally {
      await stack.stop();
    }
  });
});

describe('TP3 gRPC - T2 : produit inconnu / argument invalide', () => {
  it('T2a - produit inconnu : NOT_FOUND', async () => {
    const stack = await startFullStack();
    try {
      const { error } = await callGetStock(stack.grpcTarget, 'P999');
      assert.equal(error?.code, grpc.status.NOT_FOUND);
    } finally {
      await stack.stop();
    }
  });

  it('T2b - identifiant absent ou vide : INVALID_ARGUMENT', async () => {
    const stack = await startFullStack();
    try {
      for (const id of ['', '   ']) {
        const { error } = await callGetStock(stack.grpcTarget, id);
        assert.equal(error?.code, grpc.status.INVALID_ARGUMENT, `id="${id}"`);
      }
    } finally {
      await stack.stop();
    }
  });

  it('T2c - REST : produit inconnu -> 404 problem+json (ORD-101 contient P1, inconnu de Stocks)', async () => {
    const stack = await startFullStack({ orders: { stockBackend: 'grpc' } });
    try {
      const res = await fetch(`${stack.baseUrl}/api/orders/ORD-101/availability`);
      assert.equal(res.status, 404);
      assert.match(res.headers.get('content-type') ?? '', /application\/problem\+json/);
    } finally {
      await stack.stop();
    }
  });
});

describe('TP3 gRPC - T3 : Stocks arrêté / deadline', () => {
  it('T3a - Stocks gRPC arrêté : 502 ou 503 contrôlé, sans fuite interne', async () => {
    const stack = await startFullStack({ orders: { stockBackend: 'grpc' } });
    try {
      await stack.stopGrpc();
      const res = await fetch(`${stack.baseUrl}/api/orders/ORD-102/availability`);
      const text = await res.text();
      assert.ok([502, 503].includes(res.status), `statut ${res.status}`);
      assert.match(res.headers.get('content-type') ?? '', /application\/problem\+json/);
      assert.doesNotMatch(text, /127\.0\.0\.1|grpc|ECONN|No connection established|\d+ UNAVAILABLE/i);
    } finally {
      await stack.stop();
    }
  });

  it('T3b - latence 800 ms, deadline 500 ms : 504 problem+json', async () => {
    const stack = await startFullStack({ grpc: { latencyMs: 800 }, orders: { stockBackend: 'grpc', stockGrpcDeadlineMs: 500 } });
    try {
      const res = await fetch(`${stack.baseUrl}/api/orders/ORD-102/availability`);
      assert.equal(res.status, 504);
      assert.match(res.headers.get('content-type') ?? '', /application\/problem\+json/);
    } finally {
      await stack.stop();
    }
  });
});
