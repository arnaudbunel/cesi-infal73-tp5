import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { OrderSynchronizer } from '../../apps/demo-web/public/sync.js';

interface Snapshot {
  status: string;
  sequence: number;
}

/** Instantané REST dont la résolution est contrôlée par le test (aucune temporisation). */
function controlledFetch() {
  const pending: Array<(snapshot: Snapshot) => void> = [];
  let calls = 0;
  return {
    fetchOrder: () => {
      calls += 1;
      return new Promise<Snapshot>((resolve) => pending.push(resolve));
    },
    resolveNext: (snapshot: Snapshot) => pending.shift()?.(snapshot),
    calls: () => calls,
  };
}

/** Garde-fou (pas une synchronisation) : une promesse jamais résolue devient un échec lisible, pas un blocage. */
function within<T>(promise: Promise<T>, ms = 2000): Promise<T> {
  let timer: NodeJS.Timeout;
  const watchdog = new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(new Error(`pas de résultat après ${ms} ms (resync incomplet ?)`)), ms);
  });
  return Promise.race([promise, watchdog]).finally(() => clearTimeout(timer));
}

const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

function build(fetcher: { fetchOrder: () => Promise<Snapshot> }) {
  const shown: Snapshot[] = [];
  const sync = new OrderSynchronizer({ fetchOrder: fetcher.fetchOrder, onChange: (state: Snapshot) => shown.push({ ...state }) });
  return { sync, shown };
}

describe('TP3 reconnexion - T7 (client) : resynchronisation', () => {
  it('T7b - un événement reçu pendant le GET REST est tamponné puis rejoué si sa sequence est plus élevée', async () => {
    const rest = controlledFetch();
    const { sync, shown } = build(rest);
    const done = sync.resync();
    sync.handleEvent({ orderId: 'ORD-101', sequence: 3, status: 'SHIPPED' });
    rest.resolveNext({ status: 'CONFIRMED', sequence: 2 });
    await within(done);
    assert.deepEqual(sync.state, { status: 'SHIPPED', sequence: 3 });
    assert.deepEqual(shown.at(-1), { status: 'SHIPPED', sequence: 3 });
  });

  it('T7c - un événement tamponné de sequence <= instantané REST est rejeté (jamais de régression)', async () => {
    const rest = controlledFetch();
    const { sync, shown } = build(rest);
    const done = sync.resync();
    sync.handleEvent({ orderId: 'ORD-101', sequence: 2, status: 'CONFIRMED' });
    rest.resolveNext({ status: 'SHIPPED', sequence: 3 });
    await within(done);
    assert.deepEqual(sync.state, { status: 'SHIPPED', sequence: 3 });
    assert.ok(shown.every((s) => s.sequence >= 3), 'aucun affichage régressif');
  });

  it('T7d - un événement périmé reçu hors synchronisation est ignoré', async () => {
    const rest = controlledFetch();
    const { sync } = build(rest);
    const done = sync.resync();
    rest.resolveNext({ status: 'SHIPPED', sequence: 5 });
    await within(done);
    sync.handleEvent({ orderId: 'ORD-101', sequence: 4, status: 'CONFIRMED' });
    sync.handleEvent({ orderId: 'ORD-101', sequence: 5, status: 'SHIPPED' });
    assert.deepEqual(sync.state, { status: 'SHIPPED', sequence: 5 });
  });

  it('T7e - un événement qui saute une sequence (trou) provoque une nouvelle lecture REST', async () => {
    const rest = controlledFetch();
    const { sync } = build(rest);
    const first = sync.resync();
    rest.resolveNext({ status: 'CONFIRMED', sequence: 1 });
    await within(first);
    sync.handleEvent({ orderId: 'ORD-101', sequence: 4, status: 'SHIPPED' });
    await flush();
    assert.equal(rest.calls(), 2, 'une nouvelle synchronisation doit être demandée');
    rest.resolveNext({ status: 'SHIPPED', sequence: 4 });
    await flush();
    assert.deepEqual(sync.state, { status: 'SHIPPED', sequence: 4 });
  });

  it('T7f - deux resync() simultanés ne lancent qu\'une seule lecture REST', async () => {
    const rest = controlledFetch();
    const { sync } = build(rest);
    const a = sync.resync();
    const b = sync.resync();
    rest.resolveNext({ status: 'CONFIRMED', sequence: 1 });
    await within(Promise.all([a, b]));
    assert.equal(rest.calls(), 1);
  });

  it('T7g - une réponse REST plus ancienne que l\'état affiché est ignorée', async () => {
    const rest = controlledFetch();
    const { sync } = build(rest);
    const first = sync.resync();
    rest.resolveNext({ status: 'SHIPPED', sequence: 5 });
    await within(first);
    const second = sync.resync();
    rest.resolveNext({ status: 'CONFIRMED', sequence: 2 });
    await within(second);
    assert.deepEqual(sync.state, { status: 'SHIPPED', sequence: 5 });
  });
});
