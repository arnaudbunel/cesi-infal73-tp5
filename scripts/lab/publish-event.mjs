// Injections d'événements OrderShipped de LABORATOIRE, via l'API Management (SANS passer par le producteur ni sa validation).
// Chaque commande publie vers business.events / order.shipped (sauf mention contraire) et affiche si le message a été routé.
//
//   node scripts/lab/publish-event.mjs duplicate [--event-id evt-lab-dup-1] [--order ORD-102] [--count 2]
//       T5 : le MÊME événement (même eventId, même corps) livré plusieurs fois.
//   node scripts/lab/publish-event.mjs republish --event-id <eventId>
//       T5 : republie à l'identique un événement déjà vu par Audit (lu dans .lab-state/audit/<eventId>.json).
//   node scripts/lab/publish-event.mjs bad-version [--version 99]
//       T6 : un événement avec une schemaVersion inconnue.
//
// L'événement injecté est un message de TEST (eventId préfixé « evt-lab- »), indépendant des commandes du service Commandes.
import { randomUUID } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import { lab } from '../lib/lab-config.mjs';
import { injectMessage } from '../lib/mgmt.mjs';

const [command, ...rest] = process.argv.slice(2);
const option = (name, fallback) => {
  const i = rest.indexOf(`--${name}`);
  return i >= 0 && rest[i + 1] !== undefined ? rest[i + 1] : fallback;
};

const EXCHANGE = 'business.events';
const KEY = 'order.shipped';
const sample = (eventId, orderId, schemaVersion = 1) => ({
  eventId,
  eventType: 'OrderShipped',
  schemaVersion,
  occurredAt: new Date().toISOString(),
  source: 'orders-service',
  aggregateId: orderId,
  aggregateVersion: 1,
  data: { orderId, status: 'SHIPPED' },
});

async function send(body, label) {
  const routed = await injectMessage(EXCHANGE, KEY, body, { message_id: randomUUID(), correlation_id: `lab-${randomUUID()}` });
  console.log(`${label} : ${routed ? 'routé' : '✖ non routé (npm run infra:topology)'}`);
  if (!routed) process.exit(1);
}

if (command === 'duplicate') {
  const eventId = option('event-id', 'evt-lab-dup-1');
  const orderId = option('order', 'ORD-102');
  const count = Number(option('count', '2'));
  if (!Number.isInteger(count) || count < 2 || count > 10) throw new Error('--count doit être un entier entre 2 et 10');
  const body = sample(eventId, orderId); // un seul corps, publié `count` fois : même eventId, même occurredAt
  for (let i = 1; i <= count; i++) await send(body, `${eventId} (copie ${i}/${count}, ${orderId})`);
} else if (command === 'republish') {
  const eventId = option('event-id');
  if (!eventId) throw new Error('--event-id est obligatoire');
  const file = join(lab.stateDir, 'audit', `${encodeURIComponent(eventId)}.json`);
  let entry;
  try {
    entry = JSON.parse(readFileSync(file, 'utf8'));
  } catch {
    console.error(`✖ événement ${eventId} inconnu d'Audit (${file}) : Audit l'a-t-il reçu ? (npm run start)`);
    process.exit(1);
  }
  await send(entry.event, `${eventId} republié à l'identique`);
} else if (command === 'bad-version') {
  const version = Number(option('version', '99'));
  await send(sample(`evt-lab-v${version}-${Date.now()}`, 'ORD-102', version), `schemaVersion=${version}`);
} else {
  console.error('usage : publish-event.mjs duplicate | republish --event-id X | bad-version [--version N]');
  process.exit(1);
}
