// T6 — injecte deux messages valides de la MÊME opération métier (même operationId et même orderId, messageId différents).
// Usage : node scripts/lab/publish-duplicates.mjs [orderId]          (défaut : ORD-102)
//         node scripts/lab/publish-duplicates.mjs ORD-102 --conflict  (bonus : deux operationId différents pour le même orderId)
// Démarrez ensuite deux workers sur la même base : npm run dev:worker (deux terminaux, WORKER_NAME différents).
import { randomUUID } from 'node:crypto';
import { injectMessage } from '../lib/mgmt.mjs';

const orderId = process.argv[2] && !process.argv[2].startsWith('--') ? process.argv[2] : 'ORD-102';
const conflict = process.argv.includes('--conflict');
const operationIds = conflict ? [`prepare-${orderId}`, `prepare-${orderId}-bis`] : [`prepare-${orderId}`, `prepare-${orderId}`];

for (const operationId of operationIds) {
  const command = { schemaVersion: 1, type: 'OrderPreparationRequested', operationId, orderId, attempt: 0, requestedAt: new Date().toISOString() };
  const routed = await injectMessage('orders.commands', 'order.prepare', command, { message_id: randomUUID(), correlation_id: `lab-${randomUUID()}` });
  console.log(`${operationId} / ${orderId} : ${routed ? 'routé' : '✖ non routé (npm run infra:topology)'}`);
  if (!routed) process.exit(1);
}
