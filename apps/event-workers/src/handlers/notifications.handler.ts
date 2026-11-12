import { TodoNotImplementedError } from '../errors.js';
import type { DeliveryContext } from '../runtime/delivery.js';
import type { EventHandler } from '../runtime/consumer-runtime.js';
import type { HandlerDeps } from './deps.js';

/**
 * Consommateur NOTIFICATIONS (exercice C1) — queue `notifications.events`, consumer_name `notifications-worker`.
 * Effet métier : UNE notification SIMULÉE dans `notification_records` (aucun e-mail réel).
 *
 * Fourni : la connexion/ACK manuel (runtime), `withTransaction` (src/db.ts, se résout après COMMIT), le validateur
 * `validateOrderShippedMessage` et `describeViolation` (@infal73/contracts), le schéma SQL (database/migrations/002_tp5_events.sql).
 */
export class NotificationsHandler implements EventHandler {
  constructor(protected readonly deps: HandlerDeps) {}

  async handle(ctx: DeliveryContext): Promise<void> {
    // TODO(TP5) [C1.1] Valider le contrat de l'événement. Contrat invalide ou version inconnue : quelle issue ?
    //   (ni ACK silencieux, ni retry : le résultat serait identique à chaque tentative).
    // TODO(TP5) [C1.2] Dans UNE SEULE transaction PostgreSQL : enregistrer la notification simulée ET l'identifiant traité
    //   dans `processed_events` pour ce consommateur. Une réception dupliquée ne doit produire aucun nouvel effet.
    // TODO(TP5) [C1.3] Acquitter APRÈS le COMMIT. Que doit-il se passer si l'événement a déjà été traité ?
    // TODO(TP5) [C1.4] Erreur temporaire (base indisponible...) ou définitive : choisir `retryLater` ou `deadLetter`.
    void this.deps;
    void ctx;
    throw new TodoNotImplementedError('C1', 'NotificationsHandler.handle');
  }
}
