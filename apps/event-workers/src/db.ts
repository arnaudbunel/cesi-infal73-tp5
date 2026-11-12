import pg from 'pg';
import type { Logger } from './logger.js';

/** Pool PostgreSQL FOURNI. */
export function createPool(databaseUrl: string, log: Logger): pg.Pool {
  const pool = new pg.Pool({ connectionString: databaseUrl, max: 5, connectionTimeoutMillis: 5_000 });
  pool.on('error', (error) => log('db.pool.error', { message: error.message }));
  return pool;
}

/**
 * Exécute `work` dans UNE transaction (BEGIN ... COMMIT ; ROLLBACK si `work` lève). FOURNI.
 * Elle ne se résout qu'APRÈS le COMMIT : c'est à l'appelant de n'acquitter le message RabbitMQ qu'ensuite.
 * L'ACK n'est pas dans la transaction (RabbitMQ et PostgreSQL ne partagent aucune transaction).
 */
export async function withTransaction<T>(pool: pg.Pool, work: (client: pg.PoolClient) => Promise<T>): Promise<T> {
  const client = await pool.connect();
  let broken = false;
  try {
    await client.query('BEGIN');
    const result = await work(client);
    await client.query('COMMIT');
    return result;
  } catch (error) {
    try {
      await client.query('ROLLBACK');
    } catch {
      broken = true; // connexion inutilisable : on la détruit plutôt que de la remettre dans le pool
    }
    throw error;
  } finally {
    client.release(broken ? true : undefined);
  }
}
