import { TodoNotImplementedError } from '../errors.js';
import type { DeliveryContext } from '../runtime/delivery.js';
import type { EventHandler } from '../runtime/consumer-runtime.js';
import type { HandlerDeps } from './deps.js';

/**
 * Consommateur STATISTIQUES (exercice C2) — queue `statistics.events` (binding `order.*`), consumer_name `statistics-worker`.
 * Effet métier : incrémenter le compteur `shipped` de `shipment_stats`.
 *
 * Fourni : le runtime (ACK manuel, circuit de retry/DLQ propre à Statistiques), `withTransaction`, le validateur de contrat,
 * et l'injection de panne `deps.faults.callDependency(...)` (503 contrôlé : `npm run lab:statistics-fail -- --attempts N`).
 */
export class StatisticsHandler implements EventHandler {
  constructor(protected readonly deps: HandlerDeps) {}

  async handle(ctx: DeliveryContext): Promise<void> {
    // TODO(TP5) [C2.1] Valider le contrat (même discipline que Notifications : aucune issue silencieuse).
    // TODO(TP5) [C2.2] Incrémenter le compteur `shipped` ET marquer l'événement traité pour ce consommateur dans UNE transaction.
    //   Le doublon (même eventId) ne doit jamais incrémenter deux fois, même si deux livraisons arrivent presque en même temps.
    // TODO(TP5) [C2.3] Placer l'appel à `this.deps.faults.callDependency({ eventId })` là où une dépendance externe pourrait échouer,
    //   puis articuler : erreur temporaire -> retry différé de CE circuit ; erreur définitive -> DLQ ; succès -> ACK après COMMIT.
    void this.deps;
    void ctx;
    throw new TodoNotImplementedError('C2', 'StatisticsHandler.handle');
  }
}
