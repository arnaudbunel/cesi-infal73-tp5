import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import type { WorkerConfig } from './config.js';
import { TemporaryDependencyError } from './errors.js';
import type { Logger } from './logger.js';

/**
 * Points d'injection de panne de LABORATOIRE (fournis). Ils ne font rien sans configuration explicite
 * et ne sont pas disponibles en production (la configuration les refuse).
 */
export interface LabFaults {
  /**
   * À appeler APRÈS le COMMIT confirmé et AVANT `channel.ack()`.
   * Avec FAULT=EXIT_AFTER_COMMIT_BEFORE_ACK : le processus est tué (SIGKILL) ici, sans fermeture propre.
   */
  afterCommitBeforeAck(context: { operationId: string; orderId: string }): void;
  /**
   * Simule l'appel à une dépendance externe. Lève `TemporaryDependencyError` (503) tant que la fenêtre
   * ouverte par `node scripts/lab/dependency-down.mjs --ms N` n'est pas écoulée.
   * Intégrer cet appel dans le traitement métier, AVANT l'écriture en base.
   */
  callDependency(): void;
}

export function createLabFaults(config: Pick<WorkerConfig, 'fault' | 'labStateDir'>, log: Logger): LabFaults {
  const stateFile = join(config.labStateDir, 'dependency-503-until');
  return {
    afterCommitBeforeAck(context) {
      if (config.fault !== 'EXIT_AFTER_COMMIT_BEFORE_ACK') return;
      log('fault.exit-after-commit-before-ack', context);
      process.kill(process.pid, 'SIGKILL');
    },
    callDependency() {
      let until = 0;
      try {
        until = Number(readFileSync(stateFile, 'utf8').trim());
      } catch {
        return; // pas de fichier : dépendance disponible
      }
      if (Number.isFinite(until) && Date.now() < until) throw new TemporaryDependencyError();
    },
  };
}
