import { WORKER_CONSUMER_NAME } from '@infal73/contracts';
import type { PreparationCommand } from '@infal73/contracts';
import type pg from 'pg';

/**
 * Résultat d'une tentative de préparation.
 *  - `prepared`  : effet métier écrit et validé par COMMIT ;
 *  - `duplicate` : même opération (même operationId ET même orderId) déjà traitée : aucun nouvel effet ;
 *  - `conflict`  : demande CONTRADICTOIRE, jamais un faux succès, aucune écriture :
 *      · `operation-order-mismatch` : l'operationId est déjà associé à une AUTRE commande ;
 *      · `order-already-prepared`   : la commande a déjà été préparée par une AUTRE opération ;
 *      · `inconsistent-state`       : opération marquée traitée sans effet métier (anomalie à investiguer).
 */
export type ConflictReason = 'operation-order-mismatch' | 'order-already-prepared' | 'inconsistent-state';
export type PrepareOutcome = { kind: 'prepared' } | { kind: 'duplicate' } | { kind: 'conflict'; reason: ConflictReason; existing?: string };

export interface PreparationStore {
  prepareIdempotently(command: PreparationCommand): Promise<PrepareOutcome>;
}

export class PgPreparationStore implements PreparationStore {
  constructor(
    private readonly pool: pg.Pool,
    readonly consumerName: string = WORKER_CONSUMER_NAME,
  ) {}

  /**
   * Déduplication et effet métier dans UNE transaction ; les contraintes (PK / UNIQUE) arbitrent la concurrence.
   * L'ACK RabbitMQ n'est PAS dans cette transaction : l'appelant n'acquitte qu'après le retour (donc après COMMIT).
   * Un SELECT préalable ne protégerait pas : deux transactions concurrentes verraient toutes deux « absent ».
   */
  async prepareIdempotently(command: PreparationCommand): Promise<PrepareOutcome> {
    const client = await this.pool.connect();
    let broken = false;
    try {
      await client.query('BEGIN');
      // 1. Clé de déduplication. Sous concurrence, la 2e transaction ATTEND la 1re (index unique) puis voit le conflit.
      const claimed = await client.query(
        'INSERT INTO processed_operations(consumer_name, operation_id) VALUES ($1, $2) ON CONFLICT DO NOTHING RETURNING operation_id',
        [this.consumerName, command.operationId],
      );
      if (claimed.rowCount === 0) {
        // 2. Déjà traitée : même commande ? (la ligne métier est validée puisque la clé l'est : même transaction)
        const existing = await client.query<{ order_id: string }>('SELECT order_id FROM prepared_orders WHERE operation_id = $1', [command.operationId]);
        await client.query('COMMIT');
        const row = existing.rows[0];
        if (!row) return { kind: 'conflict', reason: 'inconsistent-state' };
        if (row.order_id !== command.orderId) return { kind: 'conflict', reason: 'operation-order-mismatch', existing: row.order_id };
        return { kind: 'duplicate' };
      }
      // 3. Effet métier dans la même transaction. Conflit sur order_id : autre opération déjà passée par là.
      const inserted = await client.query(
        'INSERT INTO prepared_orders(order_id, operation_id) VALUES ($1, $2) ON CONFLICT DO NOTHING RETURNING order_id',
        [command.orderId, command.operationId],
      );
      if (inserted.rowCount === 0) {
        await client.query('ROLLBACK'); // annule aussi processed_operations : aucune trace « traité » sans effet
        const other = await this.pool.query<{ operation_id: string }>('SELECT operation_id FROM prepared_orders WHERE order_id = $1', [command.orderId]);
        const existing = other.rows[0]?.operation_id;
        return existing === undefined ? { kind: 'conflict', reason: 'order-already-prepared' } : { kind: 'conflict', reason: 'order-already-prepared', existing };
      }
      await client.query('COMMIT');
      return { kind: 'prepared' };
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
}
