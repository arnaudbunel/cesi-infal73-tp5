import { mkdirSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { describeViolation, EVENT_CONSUMERS, validateOrderShippedMessage } from '@infal73/contracts';
import type { DeliveryContext } from '../runtime/delivery.js';
import type { EventHandler } from '../runtime/consumer-runtime.js';
import type { HandlerDeps } from './deps.js';

/**
 * Worker AUDIT — consommateur de RÉFÉRENCE, opérationnel dès le départ, pour vérifier le routage (binding `#`) :
 * « cet événement est-il bien arrivé dans ma queue, avec quel eventId, par quelle clé de routage ? »
 *
 * Il ne dépend ni de PostgreSQL ni de `processed_events` : son effet est un fichier de journal par fait métier
 * (`.lab-state/audit/<eventId>.json`), écrit en création exclusive. Un second événement de même eventId trouve le fichier
 * et ne change rien : c'est une autre technique d'idempotence que celle demandée pour Notifications et Statistiques
 * (déduplication transactionnelle en base), qui ne se transpose pas telle quelle.
 */
export class AuditHandler implements EventHandler {
  private readonly dir: string;

  constructor(private readonly deps: HandlerDeps) {
    this.dir = join(deps.labStateDir, 'audit');
  }

  async handle(ctx: DeliveryContext): Promise<void> {
    const validation = validateOrderShippedMessage(ctx.body);
    if (!validation.ok) {
      const diagnostic = describeViolation(validation.violation);
      ctx.log('contract.rejected', { code: validation.violation.code, diagnostic });
      ctx.deadLetter(`invalid contract: ${diagnostic}`); // erreur définitive : jamais de retry, jamais d'ACK silencieux
      return;
    }
    const { event } = validation;
    const created = this.record(ctx, event);
    ctx.log(created ? 'audit.recorded' : 'audit.duplicate-ignored', { eventId: event.eventId, aggregateId: event.aggregateId, routingKey: ctx.routingKey });
    ctx.ack();
  }

  /** true si le fait est enregistré pour la première fois, false s'il l'était déjà. */
  private record(ctx: DeliveryContext, event: { eventId: string }): boolean {
    mkdirSync(this.dir, { recursive: true });
    const file = join(this.dir, `${encodeURIComponent(event.eventId)}.json`);
    const entry = {
      consumer: EVENT_CONSUMERS.audit,
      receivedAt: new Date().toISOString(),
      exchange: ctx.exchange,
      routingKey: ctx.routingKey,
      messageId: ctx.messageId,
      redelivered: ctx.redelivered,
      event,
    };
    try {
      writeFileSync(file, `${JSON.stringify(entry, null, 2)}\n`, { flag: 'wx' }); // 'wx' : échoue si le fichier existe
      return true;
    } catch (error) {
      if ((error as { code?: unknown }).code === 'EEXIST') return false;
      throw error;
    }
  }
}
