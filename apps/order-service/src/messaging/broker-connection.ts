import amqp from 'amqplib';
import type { ChannelModel, ConfirmChannel } from 'amqplib';
import { PublishError } from './errors.js';

const CONNECT_TIMEOUT_MS = 3_000;

/**
 * Connexion AMQP FOURNIE (pas à écrire) : une connexion partagée, un Confirm Channel, ouverture paresseuse et
 * reprise automatique après fermeture (redémarrage de RabbitMQ, coupure). Les échecs d'ouverture sont remontés
 * en `PublishError('broker-unavailable')` ; aucune tentative en arrière-plan : la prochaine requête retente.
 */
export class BrokerConnection {
  private connection: ChannelModel | null = null;
  private channel: ConfirmChannel | null = null;
  private opening: Promise<ConfirmChannel> | null = null;
  private closed = false;

  constructor(
    private readonly url: string,
    private readonly log: (event: string, fields?: Record<string, unknown>) => void = () => undefined,
  ) {}

  /** Retourne un Confirm Channel ouvert (en le créant si nécessaire). */
  getConfirmChannel(): Promise<ConfirmChannel> {
    if (this.closed) return Promise.reject(new PublishError('broker-unavailable', 'broker connection was closed'));
    if (this.channel) return Promise.resolve(this.channel);
    this.opening ??= this.open().finally(() => {
      this.opening = null;
    });
    return this.opening;
  }

  private async open(): Promise<ConfirmChannel> {
    try {
      const connection = await amqp.connect(this.url, { timeout: CONNECT_TIMEOUT_MS });
      // Les événements `error` doivent avoir un écouteur : sinon Node arrête le processus.
      connection.on('error', (error: Error) => this.log('broker.connection.error', { message: error.message }));
      const channel = await connection.createConfirmChannel();
      channel.on('error', (error: Error) => this.log('broker.channel.error', { message: error.message }));
      const reset = (): void => {
        if (this.channel === channel) this.channel = null;
        if (this.connection === connection) this.connection = null;
      };
      channel.on('close', reset);
      connection.on('close', reset);
      this.connection = connection;
      this.channel = channel;
      return channel;
    } catch (error) {
      throw new PublishError('broker-unavailable', 'cannot open a channel to the message broker', { cause: error });
    }
  }

  async close(): Promise<void> {
    this.closed = true;
    const connection = this.connection;
    this.channel = null;
    this.connection = null;
    await connection?.close().catch(() => undefined);
  }
}
