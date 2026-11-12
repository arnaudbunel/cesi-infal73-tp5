// Remise à zéro CIBLÉE du laboratoire TP5. Ne touche à rien d'étranger :
//   - supprime les dist/ du dépôt ;
//   - vide uniquement les queues du laboratoire (TP4 : orders.*, TP5 : *.events, *.events.retry, *.events.dlq) ;
//   - vide uniquement les tables du laboratoire (TP4 + processed_events, notification_records) et remet shipment_stats.shipped à 0 ;
//   - supprime l'état d'injection local (.lab-state/ : pannes, failpoint, journal d'audit, pids/logs des workers lancés par `npm run start`) ;
//   - remet les commandes ORD-101..104 à l'état initial SI Commandes tourne avec LAB_MODE=1 (sinon : redémarrer Commandes) ;
//   - signale les ports occupés.
// Garde-fous : refusé si NODE_ENV=production ; base dédiée (*_lab) sur hôte local uniquement.
// Options : --topology  supprime puis recrée les queues du laboratoire (utile si leurs arguments ont été modifiés).
import { rmSync } from 'node:fs';
import { createConnection } from 'node:net';
import pg from 'pg';
import { LAB_QUEUES, LAB_TABLES, ORDERS_URL, destructiveGuard, lab } from './lib/lab-config.mjs';
import { mgmt, mgmtReachable, purgeQueue, queueName } from './lib/mgmt.mjs';
import { applyTopology } from './topology.mjs';

const refusal = destructiveGuard();
if (refusal) {
  console.error(`Refus de remise à zéro : ${refusal}.`);
  process.exit(1);
}

// Si des workers lancés par `npm run start` tournent, ils survivraient à la remise à zéro avec un état incohérent : on le dit.
let running = [];
try {
  const { readdirSync } = await import('node:fs');
  const { join } = await import('node:path');
  running = readdirSync(join(lab.stateDir, 'pids')).filter((f) => f.endsWith('.pid'));
} catch {
  /* aucun pid : rien ne tourne via npm run start */
}
if (running.length > 0) console.log(`⚠ processus lancés par « npm run start » encore présents (${running.join(', ')}) : « npm run stop » conseillé avant un reset.`);

for (const dir of ['apps/order-service/dist', 'apps/stock-service/dist', 'apps/preparation-worker/dist', 'apps/event-workers/dist']) {
  rmSync(new URL(`../${dir}`, import.meta.url), { recursive: true, force: true });
  console.log(`removed ${dir}`);
}
// On conserve pids/ et logs/ si des processus tournent encore (stack.mjs en a besoin pour les arrêter).
for (const entry of ['audit', 'failpoint.json', 'statistics-fail-attempts.json', 'dependency-503-until']) rmSync(`${lab.stateDir}/${entry}`, { recursive: true, force: true });
if (running.length === 0) rmSync(lab.stateDir, { recursive: true, force: true });
console.log('removed .lab-state (injections de panne, journal d’audit)');

if (await mgmtReachable()) {
  if (process.argv.includes('--topology')) {
    for (const name of LAB_QUEUES) await mgmt('DELETE', `/queues/%2F/${queueName(name)}`).catch(() => undefined);
    await applyTopology();
  } else {
    for (const name of LAB_QUEUES) {
      try {
        await purgeQueue(name);
        console.log(`queue vidée : ${name}`);
      } catch (error) {
        console.log(`queue ${name} non vidée (${error.message.split('\n')[0]}) — topologie absente ? npm run infra:topology`);
      }
    }
  }
} else {
  console.log(`RabbitMQ Management injoignable (${lab.mgmtUrl}) : queues non vidées (npm run setup).`);
}

const client = new pg.Client({ connectionString: lab.databaseUrl });
try {
  await client.connect();
  await client.query(`TRUNCATE TABLE ${LAB_TABLES.filter((t) => t !== 'shipment_stats').join(', ')}`);
  await client.query("INSERT INTO shipment_stats(metric, counter) VALUES ('shipped', 0) ON CONFLICT (metric) DO UPDATE SET counter = 0, updated_at = now()");
  console.log(`tables vidées : ${LAB_TABLES.join(', ')} (shipment_stats.shipped = 0)`);
} catch (error) {
  console.log(`PostgreSQL non réinitialisé (${error.message.split('\n')[0]}) — npm run setup puis npm run db:migrate.`);
} finally {
  await client.end().catch(() => undefined);
}

try {
  const response = await fetch(`${ORDERS_URL}/api/lab/reset-orders`, { method: 'POST', signal: AbortSignal.timeout(2000) });
  console.log(response.ok ? 'commandes ORD-101..104 remises à l’état initial (service Commandes en LAB_MODE)' : `commandes non remises à zéro (HTTP ${response.status}) : redémarrez Commandes avec LAB_MODE=1`);
} catch {
  console.log('Commandes injoignable : au prochain démarrage, ORD-102 sera CONFIRMED et ORD-103 SHIPPED (données en mémoire).');
}

const inUse = (port) =>
  new Promise((resolve) => {
    const socket = createConnection({ port, host: '127.0.0.1' });
    socket.once('connect', () => (socket.destroy(), resolve(true)));
    socket.once('error', () => resolve(false));
  });

for (const [label, port] of [['Commandes', 3000], ['Stocks HTTP', 3001], ['Stocks gRPC', 50051]]) {
  console.log(`port ${port} (${label}): ${(await inUse(port)) ? 'OCCUPÉ (normal si « npm run start » tourne)' : 'libre'}`);
}
