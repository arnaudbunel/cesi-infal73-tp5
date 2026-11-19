// Audit de ROUTAGE (aucun producteur étudiant requis, aucune pollution des vraies queues).
import '../lib/lab-config.mjs'; // refuse NODE_ENV=production
// Lit les bindings réels de `business.events`, en recrée les motifs sur un exchange topic TEMPORAIRE `probe.<id>` (même type, mêmes motifs,
// queues temporaires), y publie des clés de test via l'API Management et compare les copies reçues au comportement attendu des motifs AMQP.
// Vérifie aussi que l'exchange réel est un topic durable et que ses seuls bindings sont les trois attendus.
// Usage : npm run lab:routing-check        (RabbitMQ démarré ; refusé si NODE_ENV=production)
import { randomBytes } from 'node:crypto';
import { mgmt } from '../lib/mgmt.mjs';

const V = '%2F';
const real = await mgmt('GET', `/exchanges/${V}/business.events`);
const bindings = await mgmt('GET', `/exchanges/${V}/business.events/bindings/source`);
let failures = 0;
const check = (ok, label) => {
  console.log(`${ok ? 'PASS' : 'FAIL'}  ${label}`);
  if (!ok) failures++;
};
check(real.type === 'topic' && real.durable === true, `business.events : topic durable (observé : ${real.type}, durable=${real.durable})`);
const actual = new Set(bindings.map((b) => `${b.destination}|${b.routing_key}`));
const expected = ['notifications.events|order.shipped', 'statistics.events|order.*', 'audit.events|#'];
check(expected.every((e) => actual.has(e)) && actual.size === 3, `bindings réels de business.events = ${expected.join(' ; ')} (observé : ${[...actual].join(' ; ')})`);

// Motifs à tester : ceux des bindings réels + `order.#` (pour montrer la différence avec `order.*`).
const patterns = {
  notifications: 'order.shipped',
  statistics: 'order.*',
  audit: '#',
  'order-hash': 'order.#',
};
const id = randomBytes(4).toString('hex');
const exchange = `probe.${id}`;
const queueOf = (name) => `probe.${id}.${name}`;
await mgmt('PUT', `/exchanges/${V}/${exchange}`, { type: 'topic', durable: false, auto_delete: false });
try {
  for (const [name, pattern] of Object.entries(patterns)) {
    // Durable (supprimée dans `finally`) : RabbitMQ 4 déprécie les queues non durables et non exclusives (transient_nonexcl_queues).
    await mgmt('PUT', `/queues/${V}/${queueOf(name)}`, { durable: true, auto_delete: false, arguments: { 'x-queue-type': 'classic' } });
    await mgmt('POST', `/bindings/${V}/e/${exchange}/q/${queueOf(name)}`, { routing_key: pattern });
  }
  // clé de test -> files attendues (sémantique AMQP : `*` = exactement un mot ; `#` = zéro ou plusieurs mots)
  const matrix = {
    'order.shipped': ['notifications', 'statistics', 'audit', 'order-hash'],
    'order.cancelled': ['statistics', 'audit', 'order-hash'],
    'payment.succeeded': ['audit'],
    'order.shipped.eu': ['audit', 'order-hash'], // deux mots après `order` : `order.*` NE correspond PAS, `order.#` oui
    order: ['audit', 'order-hash'], // `order.#` correspond aussi à `order` seul ; `order.*` non
  };
  for (const key of Object.keys(matrix)) {
    const routed = (await mgmt('POST', `/exchanges/${V}/${exchange}/publish`, { properties: { delivery_mode: 1 }, routing_key: key, payload: key, payload_encoding: 'string' })).routed;
    check(routed === true, `${key} : routé (au moins une queue)`);
  }
  for (const [key, want] of Object.entries(matrix)) {
    const got = [];
    for (const name of Object.keys(patterns)) {
      const messages = await mgmt('POST', `/queues/${V}/${queueOf(name)}/get`, { count: 20, ackmode: 'ack_requeue_true', encoding: 'auto' });
      if (messages.some((m) => m.routing_key === key)) got.push(name);
    }
    check(JSON.stringify([...got].sort()) === JSON.stringify([...want].sort()), `${key} -> ${want.join(', ')} (observé : ${got.join(', ') || 'aucune'})`);
  }
} finally {
  for (const name of Object.keys(patterns)) await mgmt('DELETE', `/queues/${V}/${queueOf(name)}`).catch(() => undefined);
  await mgmt('DELETE', `/exchanges/${V}/${exchange}`).catch(() => undefined);
}
console.log(failures === 0 ? '\nRoutage conforme.' : `\n${failures} écart(s).`);
process.exit(failures === 0 ? 0 : 1);
