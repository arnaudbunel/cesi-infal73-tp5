/**
 * Journal JSON (une ligne par événement, horodatage ISO 8601) sur la sortie standard.
 * Événements fournis : `delivery.received` (livraison reçue). À vous d'émettre les autres
 * (ex. `operation.completed`, `retry.scheduled`, `message.dead-lettered`) pour distinguer
 * « message publié », « livraison reçue » et « opération métier terminée ».
 */
export type LogFields = Record<string, unknown>;
export type Logger = (event: string, fields?: LogFields) => void;

export function createLogger(worker: string): Logger {
  return (event, fields = {}) => {
    console.log(JSON.stringify({ ts: new Date().toISOString(), worker, event, ...fields }));
  };
}
