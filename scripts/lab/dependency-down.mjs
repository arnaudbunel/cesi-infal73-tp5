// T4 — simule une dépendance du worker en 503 pendant une durée contrôlée (RabbitMQ n'est PAS arrêté).
// Usage : node scripts/lab/dependency-down.mjs --ms 5000     |     node scripts/lab/dependency-down.mjs --clear
// Mécanisme : un fichier d'état local (.lab-state/dependency-503-until) lu par le worker à chaque appel de dépendance.
import { mkdirSync, rmSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { lab } from '../lib/lab-config.mjs';

const file = join(lab.stateDir, 'dependency-503-until');
if (process.argv.includes('--clear')) {
  rmSync(file, { force: true });
  console.log('dépendance rétablie');
} else {
  const index = process.argv.indexOf('--ms');
  const ms = Number(process.argv[index + 1]);
  if (index < 0 || !Number.isInteger(ms) || ms <= 0 || ms > 120_000) {
    console.error('usage : node scripts/lab/dependency-down.mjs --ms <1..120000> | --clear');
    process.exit(1);
  }
  mkdirSync(lab.stateDir, { recursive: true });
  const until = Date.now() + ms;
  writeFileSync(file, String(until));
  console.log(`dépendance en 503 jusqu'à ${new Date(until).toISOString()} (${ms} ms)`);
}
