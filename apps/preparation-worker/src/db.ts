import pg from 'pg';
import type { Logger } from './logger.js';

/** Pool PostgreSQL FOURNI. La transaction (un seul client du pool) est ouverte par PgPreparationStore. */
export function createPool(databaseUrl: string, log: Logger): pg.Pool {
  const pool = new pg.Pool({ connectionString: databaseUrl, max: 5, connectionTimeoutMillis: 5_000 });
  // Une connexion inactive coupée par le serveur ne doit pas arrêter le processus.
  pool.on('error', (error) => log('db.pool.error', { message: error.message }));
  return pool;
}
