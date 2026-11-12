// T7 — arme (ou désarme) le failpoint AFTER_SHIP_COMMIT_BEFORE_EVENT_PUBLISH du service Commandes :
// juste APRÈS l'enregistrement de CONFIRMED -> SHIPPED, AVANT toute publication d'événement.
// Usage : node scripts/lab/failpoint.mjs set AFTER_SHIP_COMMIT_BEFORE_EVENT_PUBLISH [--mode throw|exit] [--keep]
//         node scripts/lab/failpoint.mjs clear
// Le service Commandes doit tourner avec LAB_MODE=1 (« npm run start » le fait) ; refusé si NODE_ENV=production.
//   --mode throw (défaut) : erreur levée, le processus reste vivant ; la commande reste SHIPPED, aucun événement n'est publié.
//   --mode exit           : SIGKILL du processus Commandes. Limite : les commandes sont en MÉMOIRE (TP1–TP4), l'état est perdu au redémarrage.
//   --keep                : ne pas se désarmer après le premier déclenchement (par défaut : une seule fois).
import { mkdirSync, rmSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { lab } from '../lib/lab-config.mjs';

const file = join(lab.stateDir, 'failpoint.json');
const [action, name] = process.argv.slice(2);
if (action === 'clear') {
  rmSync(file, { force: true });
  console.log('failpoint désarmé');
} else if (action === 'set' && name === 'AFTER_SHIP_COMMIT_BEFORE_EVENT_PUBLISH') {
  const i = process.argv.indexOf('--mode');
  const mode = i >= 0 ? process.argv[i + 1] : 'throw';
  if (mode !== 'throw' && mode !== 'exit') {
    console.error('--mode doit valoir throw ou exit');
    process.exit(1);
  }
  mkdirSync(lab.stateDir, { recursive: true });
  writeFileSync(file, JSON.stringify({ name, mode, once: !process.argv.includes('--keep') }));
  console.log(`failpoint armé : ${name} (mode ${mode}, ${process.argv.includes('--keep') ? 'permanent' : 'une seule fois'})`);
} else {
  console.error('usage : failpoint.mjs set AFTER_SHIP_COMMIT_BEFORE_EVENT_PUBLISH [--mode throw|exit] [--keep]  |  clear');
  process.exit(1);
}
