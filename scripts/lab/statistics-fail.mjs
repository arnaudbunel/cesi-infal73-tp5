// T4 — fait échouer (503 simulé) les N PROCHAINS appels de la dépendance du consommateur Statistiques, puis tout redevient normal.
// Usage : node scripts/lab/statistics-fail.mjs --attempts 2   |   node scripts/lab/statistics-fail.mjs --clear
// Mécanisme : fichier d'état local .lab-state/statistics-fail-attempts.json lu par le worker Statistiques (jamais en production).
// Le scénario compte des appels, pas des secondes : il est déterministe.
import { mkdirSync, rmSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { lab } from '../lib/lab-config.mjs';

const file = join(lab.stateDir, 'statistics-fail-attempts.json');
if (process.argv.includes('--clear')) {
  rmSync(file, { force: true });
  console.log('dépendance de Statistiques rétablie');
} else {
  const index = process.argv.indexOf('--attempts');
  const attempts = Number(process.argv[index + 1]);
  if (index < 0 || !Number.isInteger(attempts) || attempts < 1 || attempts > 10) {
    console.error('usage : node scripts/lab/statistics-fail.mjs --attempts N (1..10)  |  --clear');
    process.exit(1);
  }
  mkdirSync(lab.stateDir, { recursive: true });
  writeFileSync(file, JSON.stringify({ remaining: attempts }));
  console.log(`Statistiques : les ${attempts} prochain(s) appel(s) de dépendance échoueront en 503, puis retour à la normale`);
}
