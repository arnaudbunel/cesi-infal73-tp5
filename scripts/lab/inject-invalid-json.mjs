// T5 — injecte un corps JSON tronqué dans orders.commands / order.prepare (via l'API Management, donc SANS passer par le
// producteur ni contourner la validation du worker). Usage : node scripts/lab/inject-invalid-json.mjs
import { injectMessage } from '../lib/mgmt.mjs';

const body = '{"schemaVersion":1,"type":"OrderPreparationRequested","operationId":"prepare-ORD-BAD","orderId":"ORD-B';
const routed = await injectMessage('orders.commands', 'order.prepare', body);
console.log(routed ? 'JSON invalide injecté (routé vers orders.prepare)' : '✖ message non routé : la topologie est-elle appliquée ? npm run infra:topology');
process.exit(routed ? 0 : 1);
