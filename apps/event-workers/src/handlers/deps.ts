import type pg from 'pg';
import type { Logger } from '../logger.js';
import type { StatisticsFaults } from '../lab-faults.js';

/** Dépendances communes des handlers (fournies par server.ts). */
export interface HandlerDeps {
  pool: pg.Pool;
  log: Logger;
  faults: StatisticsFaults;
  /** Dossier d'état de laboratoire (journal d'audit, injections). */
  labStateDir: string;
}
