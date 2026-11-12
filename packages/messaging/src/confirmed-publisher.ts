import { randomUUID } from 'node:crypto';
import type { Options } from 'amqplib';
import { PublishError } from './errors.js';

export interface PublishInput {
  exchange: string;
  routingKey: string;
  /** Sérialisé en JSON. */
  payload: unknown;
  /** Doit valoir true pour que l'absence de routage soit détectée (basic.return). */
  mandatory: boolean;
  /** Doit valoir true (delivery mode 2). */
  persistent: boolean;
  correlationId?: string;
  /** Identifiant de transport ; généré (UUID) s'il est absent. Doit être unique par publication. */
  messageId?: string;
  /** En-têtes AMQP applicatifs (TP5 : compteur de retry). Optionnel : sans effet pour les appelants du TP4. */
  headers?: Record<string, string | number>;
}

export interface PublishReceipt {
  /** Identifiant de transport de CE message (change à chaque republication, contrairement à l'operationId). */
  messageId: string;
}

/** Sous-ensemble de `amqplib.ConfirmChannel` utilisé ici (permet de tester avec un canal simulé). */
export interface ConfirmChannelLike {
  publish(exchange: string, routingKey: string, content: Buffer, options: Options.Publish, callback: (err: unknown, ok?: unknown) => void): boolean;
  on(event: string, listener: (arg: unknown) => void): unknown;
  once(event: string, listener: (arg: unknown) => void): unknown;
}

export type PublisherLog = (event: string, fields?: Record<string, unknown>) => void;

export interface ConfirmedPublisherOptions {
  /** Délai maximal d'attente du confirm ; au-delà : résultat incertain. */
  confirmTimeoutMs?: number;
  log?: PublisherLog;
}

interface Pending {
  returned: { replyCode?: unknown; replyText?: unknown } | null;
  /** Rejette (résultat incertain) lorsque le canal se ferme avant le confirm. */
  abort: (error: PublishError) => void;
}

interface ChannelState {
  pending: Map<string, Pending>;
  /** Promesse de vidange du tampon (back-pressure) : les publications suivantes l'attendent. */
  drain: Promise<void> | null;
  releaseDrain: (() => void) | null;
  closed: boolean;
}

const DEFAULT_CONFIRM_TIMEOUT_MS = 5_000;

/**
 * Publication confirmée.
 *
 * Résolution : UNIQUEMENT après confirm positif du broker ET absence de `basic.return` pour CE message.
 *  - Le broker émet `basic.return` AVANT le `basic.ack` d'un message non routable : un confirm positif seul ne prouve donc
 *    pas le routage. Chaque publication a un `messageId` unique ; la table `pending` associe un return à la bonne publication
 *    même avec plusieurs publications concurrentes sur le même canal.
 *  - Le booléen retourné par `channel.publish()` (tampon plein) n'est PAS un confirm : s'il vaut false, les publications
 *    suivantes attendent `drain`, mais la publication courante n'est résolue que par son confirm.
 *  - Canal fermé / erreur de canal / confirm manquant : résultat INCERTAIN (`outcome-unknown`), pas « échec certain ».
 *  - Exception synchrone de publish (canal déjà fermé) : rien n'est parti (`broker-unavailable`).
 */
export class ConfirmedPublisher {
  private readonly states = new WeakMap<object, ChannelState>();
  private readonly confirmTimeoutMs: number;
  private readonly log: PublisherLog;

  constructor(
    private readonly getChannel: () => Promise<ConfirmChannelLike>,
    options: ConfirmedPublisherOptions = {},
  ) {
    this.confirmTimeoutMs = options.confirmTimeoutMs ?? DEFAULT_CONFIRM_TIMEOUT_MS;
    this.log = options.log ?? (() => undefined);
  }

  private stateFor(channel: ConfirmChannelLike): ChannelState {
    let state = this.states.get(channel);
    if (state) return state;
    const created: ChannelState = { pending: new Map(), drain: null, releaseDrain: null, closed: false };
    state = created;
    this.states.set(channel, created);
    channel.on('return', (message) => {
      const properties = (message as { properties?: { messageId?: unknown }; fields?: { replyCode?: unknown; replyText?: unknown } } | null) ?? {};
      const messageId = properties.properties?.messageId;
      const entry = typeof messageId === 'string' ? created.pending.get(messageId) : undefined;
      if (!entry) {
        this.log('return.unmatched', { messageId });
        return;
      }
      entry.returned = { replyCode: properties.fields?.replyCode, replyText: properties.fields?.replyText };
    });
    const abortAll = (reason: string): void => {
      created.closed = true;
      created.releaseDrain?.();
      for (const entry of [...created.pending.values()]) entry.abort(new PublishError('outcome-unknown', reason));
    };
    channel.on('close', () => abortAll('channel closed before the broker confirmed the publication'));
    channel.on('error', (error) => abortAll(`channel error before confirmation: ${error instanceof Error ? error.message : String(error)}`));
    return created;
  }

  async publishConfirmed(input: PublishInput): Promise<PublishReceipt> {
    if (!input.mandatory || !input.persistent) {
      throw new Error('publishConfirmed requires mandatory=true and persistent=true');
    }
    let channel: ConfirmChannelLike;
    try {
      channel = await this.getChannel();
    } catch (error) {
      throw error instanceof PublishError ? error : new PublishError('broker-unavailable', 'cannot obtain a confirm channel', { cause: error });
    }
    const state = this.stateFor(channel);
    if (state.closed) throw new PublishError('broker-unavailable', 'channel is closed');
    if (state.drain) await state.drain; // back-pressure : on attend la vidange du tampon avant d'écrire
    if (state.closed) throw new PublishError('broker-unavailable', 'channel closed while waiting for drain');

    const messageId = input.messageId ?? randomUUID();
    const base = { messageId, correlationId: input.correlationId, exchange: input.exchange, routingKey: input.routingKey };
    const body = Buffer.from(JSON.stringify(input.payload));

    return new Promise<PublishReceipt>((resolve, reject) => {
      let settled = false;
      let timer: NodeJS.Timeout | undefined;
      const finish = (outcome: () => void): void => {
        if (settled) return;
        settled = true;
        if (timer) clearTimeout(timer);
        state.pending.delete(messageId);
        outcome();
      };
      const entry: Pending = {
        returned: null,
        abort: (error) =>
          finish(() => {
            this.log('outcome_unknown', { ...base, reason: error.message });
            reject(new PublishError('outcome-unknown', error.message, { messageId }));
          }),
      };
      state.pending.set(messageId, entry);
      timer = setTimeout(() => entry.abort(new PublishError('outcome-unknown', `no confirm within ${this.confirmTimeoutMs} ms`)), this.confirmTimeoutMs);

      const options: Options.Publish = {
        persistent: input.persistent,
        mandatory: input.mandatory,
        contentType: 'application/json',
        messageId,
        timestamp: Math.floor(Date.now() / 1000),
        ...(input.correlationId === undefined ? {} : { correlationId: input.correlationId }),
        ...(input.headers === undefined ? {} : { headers: input.headers }),
      };
      try {
        const accepted = channel.publish(input.exchange, input.routingKey, body, options, (error) => {
          if (error) {
            const nacked = error instanceof Error && /nack/i.test(error.message);
            finish(() => {
              this.log(nacked ? 'nacked' : 'outcome_unknown', { ...base, reason: error instanceof Error ? error.message : String(error) });
              reject(
                nacked
                  ? new PublishError('nacked', 'broker refused the publication (basic.nack)', { cause: error, messageId })
                  : new PublishError('outcome-unknown', 'publication failed before confirmation', { cause: error, messageId }),
              );
            });
            return;
          }
          if (entry.returned) {
            finish(() => {
              this.log('returned', { ...base, ...entry.returned });
              reject(new PublishError('unroutable', 'message returned by the broker (basic.return): no queue matched', { messageId }));
            });
            return;
          }
          finish(() => {
            this.log('confirmed', base);
            resolve({ messageId });
          });
        });
        this.log('published', { ...base, bufferFull: accepted === false });
        if (accepted === false && !state.drain) {
          // Tampon plein : les publications suivantes attendent `drain`. Ce n'est PAS un confirm.
          state.drain = new Promise<void>((done) => {
            const release = (): void => {
              state.drain = null;
              state.releaseDrain = null;
              done();
            };
            state.releaseDrain = release;
            channel.once('drain', release);
          });
        }
      } catch (error) {
        // publish a levé de façon synchrone : le message n'a pas été écrit.
        finish(() => reject(new PublishError('broker-unavailable', 'publish failed synchronously (channel closed?)', { cause: error, messageId })));
      }
    });
  }
}
