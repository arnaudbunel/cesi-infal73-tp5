import { mkdirSync, readFileSync, rmSync } from 'node:fs';
import { join } from 'node:path';

/**
 * Failpoints de LABORATOIRE du service Commandes (TP5, test T7).
 *
 *   AFTER_SHIP_COMMIT_BEFORE_EVENT_PUBLISH : juste APRÈS l'enregistrement de CONFIRMED -> SHIPPED, AVANT toute publication.
 *
 * Activation explicite et locale : `npm run lab:failpoint -- set AFTER_SHIP_COMMIT_BEFORE_EVENT_PUBLISH` écrit
 * `.lab-state/failpoint.json` ; le service ne le lit que s'il est lancé avec LAB_MODE=1 (jamais en production).
 * Par défaut le failpoint se déclenche UNE fois puis se désarme (déterminisme).
 *   mode `throw` (défaut) : lève une erreur ; le processus reste vivant, la commande reste SHIPPED en mémoire.
 *   mode `exit`           : SIGKILL du processus (pas de fermeture propre) ; l'état en mémoire est perdu au redémarrage
 *                           (les commandes du fil rouge ne sont pas en base : limite documentée).
 */
export const KNOWN_FAILPOINTS = ['AFTER_SHIP_COMMIT_BEFORE_EVENT_PUBLISH'] as const;
export type FailpointName = (typeof KNOWN_FAILPOINTS)[number];
export type FailpointMode = 'throw' | 'exit';

export interface Failpoints {
  hit(name: FailpointName, context: Record<string, unknown>): void;
}

export class FailpointTriggered extends Error {
  constructor(readonly failpoint: FailpointName) {
    super(`failpoint ${failpoint} triggered (laboratory fault injection)`);
    this.name = 'FailpointTriggered';
  }
}

interface FailpointState {
  name: FailpointName;
  mode: FailpointMode;
  once: boolean;
}

export const FAILPOINT_FILE = 'failpoint.json';

/** Failpoints pilotés par un fichier d'état local. Refuse NODE_ENV=production. */
export function createFileFailpoints(stateDir: string, log: (event: string, fields?: Record<string, unknown>) => void): Failpoints {
  if (process.env['NODE_ENV'] === 'production') throw new Error('failpoints are forbidden when NODE_ENV=production');
  mkdirSync(stateDir, { recursive: true });
  const file = join(stateDir, FAILPOINT_FILE);
  return {
    hit(name, context) {
      let state: FailpointState;
      try {
        state = JSON.parse(readFileSync(file, 'utf8')) as FailpointState;
      } catch {
        return; // pas de fichier (ou illisible) : aucun failpoint armé
      }
      if (state.name !== name) return;
      if (state.once) rmSync(file, { force: true });
      log('failpoint.triggered', { failpoint: name, mode: state.mode, ...context });
      if (state.mode === 'exit') process.kill(process.pid, 'SIGKILL');
      throw new FailpointTriggered(name);
    },
  };
}
