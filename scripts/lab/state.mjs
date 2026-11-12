// Relevé d'état pour vos fiches de test : ready/unacked/consumers des queues TP4 et TP5 + tables SQL + journal d'audit local.
// Usage : node scripts/lab/state.mjs   (aussi : npm run lab:state)
import { readdirSync } from 'node:fs';
import { join } from 'node:path';
import pg from 'pg';
import { LAB_QUEUES, lab } from '../lib/lab-config.mjs';
import { queueStats } from '../lib/mgmt.mjs';

console.log(`# ${new Date().toISOString()}`);
for (const name of LAB_QUEUES) {
  try {
    const s = await queueStats(name);
    console.log(`${name.padEnd(28)} ready=${s.ready} unacked=${s.unacked} consumers=${s.consumers}`);
  } catch (error) {
    console.log(`${name.padEnd(28)} indisponible (${error.message.split('\n')[0]})`);
  }
}
const client = new pg.Client({ connectionString: lab.databaseUrl });
try {
  await client.connect();
  console.table((await client.query('SELECT order_id, operation_id, prepared_at FROM prepared_orders ORDER BY prepared_at')).rows);
  console.table((await client.query('SELECT consumer_name, operation_id, processed_at FROM processed_operations ORDER BY processed_at')).rows);
  console.log('-- TP5');
  console.table((await client.query('SELECT consumer_name, event_id, processed_at FROM processed_events ORDER BY processed_at')).rows);
  console.table((await client.query('SELECT id, event_id, order_id, channel, created_at FROM notification_records ORDER BY id')).rows);
  console.table((await client.query('SELECT metric, counter, updated_at FROM shipment_stats ORDER BY metric')).rows);
} catch (error) {
  console.log(`PostgreSQL indisponible : ${error.message.split('\n')[0]}`);
} finally {
  await client.end().catch(() => undefined);
}
try {
  const files = readdirSync(join(lab.stateDir, 'audit')).filter((f) => f.endsWith('.json'));
  console.log(`audit (journal local) : ${files.length} fait(s) — ${files.map((f) => decodeURIComponent(f.replace(/\.json$/, ''))).join(', ') || 'aucun'}`);
} catch {
  console.log('audit (journal local) : aucun');
}
