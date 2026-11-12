import { mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { TemporaryDependencyError } from './errors.js';

/**
 * Injection de panne de LABORATOIRE pour le consommateur Statistiques (T4). Ne fait rien sans activation explicite ;
 * refusée si NODE_ENV=production.
 *
 *   npm run lab:statistics-fail -- --attempts 2     les 2 prochains appels de la dépendance échouent (503), puis tout redevient normal
 *   npm run lab:statistics-fail -- --clear
 *
 * Le compteur compte des APPELS, pas des secondes : le scénario est déterministe (pas de fenêtre de temps à viser).
 */
export interface StatisticsFaults {
  /**
   * Simule l'appel à une dépendance externe de Statistiques. Lève `TemporaryDependencyError` (503) tant que le nombre
   * d'échecs demandé n'est pas épuisé. À placer AVANT l'écriture en base : c'est à vous de décider où exactement.
   */
  callDependency(context: { eventId: string }): void;
}

export const STATISTICS_FAULT_FILE = 'statistics-fail-attempts.json';

export function createStatisticsFaults(labStateDir: string): StatisticsFaults {
  if (process.env['NODE_ENV'] === 'production') return { callDependency: () => undefined };
  const file = join(labStateDir, STATISTICS_FAULT_FILE);
  return {
    callDependency() {
      let remaining = 0;
      try {
        remaining = Number((JSON.parse(readFileSync(file, 'utf8')) as { remaining?: unknown }).remaining);
      } catch {
        return; // pas de fichier : dépendance disponible
      }
      if (!Number.isInteger(remaining) || remaining <= 0) return;
      mkdirSync(labStateDir, { recursive: true });
      writeFileSync(file, JSON.stringify({ remaining: remaining - 1 }));
      throw new TemporaryDependencyError();
    },
  };
}
