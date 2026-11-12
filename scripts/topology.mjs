// Applique docker/rabbitmq/definitions.json (exchanges, queues, bindings) via l'API Management. Idempotent.
// Si une queue existe déjà avec d'autres arguments, RabbitMQ refuse : `npm run reset -- --topology` la recrée.
import { readFileSync } from 'node:fs';
import { lab } from './lib/lab-config.mjs';
import { mgmt, mgmtReachable } from './lib/mgmt.mjs';

export async function waitForManagement(timeoutMs = 90_000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await mgmtReachable()) return;
    await new Promise((r) => setTimeout(r, 1000));
  }
  throw new Error(`RabbitMQ Management injoignable sur ${lab.mgmtUrl} (identifiants ${lab.rabbitUser}). Lancez : npm run infra:up`);
}

export async function applyTopology() {
  await waitForManagement();
  const definitions = JSON.parse(readFileSync(new URL('../docker/rabbitmq/definitions.json', import.meta.url), 'utf8'));
  await mgmt('POST', '/definitions', definitions);
  console.log(`topologie appliquée : ${definitions.exchanges.length} exchanges, ${definitions.queues.length} queues, ${definitions.bindings.length} bindings`);
}

if (process.argv[1]?.endsWith('topology.mjs')) {
  applyTopology().catch((error) => {
    console.error(`✖ ${error.message}`);
    process.exit(1);
  });
}
