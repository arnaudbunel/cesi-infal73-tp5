import { MAX_RETRIES, validatePreparationMessage } from '@infal73/contracts';
import type { PreparationCommand } from '@infal73/contracts';
import type { Channel, ConsumeMessage } from 'amqplib';
import type { WorkerConfig } from './config.js';
import { isTemporary } from './errors.js';
import type { LabFaults } from './lab-faults.js';
import type { Logger } from './logger.js';
import type { RetryScheduler } from './retry.js';
import type { PrepareOutcome, PreparationStore } from './store.js';

export interface ConsumerDeps {
  /** Canal de consommation (prefetch réglé, `noAck: false`). */
  channel: Pick<Channel, 'ack' | 'nack'>;
  config: Pick<WorkerConfig, 'workerName'>;
  store: PreparationStore;
  retries: RetryScheduler;
  faults: LabFaults;
  log: Logger;
  /**
   * Ferme la session AMQP (connexion) : tous les messages non acquittés sont remis en queue et redélivrés.
   * Utilisé quand l'état d'un message est indéterminé (échec de republication du retry) : on n'ACK pas l'original.
   */
  abortSession: (reason: string) => void;
}

/**
 * Traite UNE livraison de `orders.prepare`.
 *
 *   contrat invalide                      -> nack(requeue=false)  -> DLX -> DLQ (jamais de retry)
 *   préparée / doublon identique          -> COMMIT PostgreSQL déjà fait -> ack
 *   conflit (demande contradictoire)      -> nack(requeue=false)  -> DLQ (erreur définitive)
 *   erreur temporaire et attempt < 3      -> republication CONFIRMÉE vers orders.retry -> ack de l'original
 *   erreur définitive ou attempt >= 3     -> nack(requeue=false)  -> DLQ
 *   republication non confirmée           -> PAS d'ack ; fermeture de session => redelivery contrôlée
 */
export async function handleDelivery(deps: ConsumerDeps, msg: ConsumeMessage): Promise<void> {
  const { channel, log } = deps;
  const correlationId = typeof msg.properties.correlationId === 'string' ? msg.properties.correlationId : undefined;
  const messageId = typeof msg.properties.messageId === 'string' ? msg.properties.messageId : undefined;
  const base = { messageId, correlationId, deliveryTag: msg.fields.deliveryTag };

  const validation = validatePreparationMessage(msg.content);
  if (!validation.ok) {
    log('dead_lettered', { ...base, reason: `invalid contract: ${validation.reason}`, retriable: false });
    channel.nack(msg, false, false);
    return;
  }
  const command = validation.command;
  const ids = { ...base, operationId: command.operationId, orderId: command.orderId, attempt: command.attempt };

  let outcome: PrepareOutcome;
  try {
    deps.faults.callDependency(); // dépendance simulée (503 temporaire) : AVANT toute écriture
    outcome = await deps.store.prepareIdempotently(command);
  } catch (error) {
    const reason = error instanceof Error ? `${error.name}: ${error.message}` : String(error);
    if (!isTemporary(error) || command.attempt >= MAX_RETRIES) {
      log('dead_lettered', { ...ids, reason, retriable: isTemporary(error), exhausted: command.attempt >= MAX_RETRIES });
      channel.nack(msg, false, false);
      return;
    }
    log('failed_temporarily', { ...ids, reason });
    await retryThenAck(deps, msg, command, ids, correlationId);
    return;
  }

  if (outcome.kind === 'conflict') {
    log('dead_lettered', { ...ids, reason: `conflict: ${outcome.reason}`, existing: outcome.existing, retriable: false });
    channel.nack(msg, false, false);
    return;
  }
  log('committed', { ...ids, outcome: outcome.kind });
  // Point d'injection : crash après COMMIT confirmé, AVANT l'ACK (laboratoire uniquement, désactivé en production).
  if (outcome.kind === 'prepared') deps.faults.afterCommitBeforeAck({ operationId: command.operationId, orderId: command.orderId });
  channel.ack(msg);
  log('acked', { ...ids, outcome: outcome.kind });
}

/** Erreur temporaire, tentatives restantes : republier (confirmé) PUIS acquitter l'original, jamais l'inverse. */
async function retryThenAck(
  deps: ConsumerDeps,
  msg: ConsumeMessage,
  command: PreparationCommand,
  ids: Record<string, unknown>,
  correlationId: string | undefined,
): Promise<void> {
  try {
    await deps.retries.scheduleRetry(command, correlationId === undefined ? {} : { correlationId });
  } catch (error) {
    // Retry non confirmé / incertain : on n'ACK PAS l'original. On ferme la session pour forcer une redelivery
    // contrôlée (et non un message « oublié » unacked sur une connexion vivante).
    deps.log('retry_publish_failed', { ...ids, reason: error instanceof Error ? error.message : String(error) });
    deps.abortSession('retry publication not confirmed');
    return;
  }
  deps.channel.ack(msg);
  deps.log('acked', { ...ids, outcome: 'retried' });
}
