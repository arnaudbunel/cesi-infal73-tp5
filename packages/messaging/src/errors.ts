/**
 * Issue d'une publication qui n'est PAS une prise en charge confirmée et routée.
 * - `broker-unavailable` : rien n'a été envoyé (connexion/canal indisponible ou fermé avant l'envoi) ;
 * - `unroutable`         : le broker a renvoyé le message (`basic.return`) : aucune queue ne l'a reçu ;
 * - `nacked`             : le broker a refusé la publication (`basic.nack` du confirm) ;
 * - `outcome-unknown`    : canal/connexion fermés ou confirm non reçu dans le délai : le message est PEUT-ÊTRE en queue.
 *                          Republier est possible mais peut créer un doublon (l'idempotence `operationId` le neutralise).
 */
export type PublishFailureKind = 'broker-unavailable' | 'unroutable' | 'nacked' | 'outcome-unknown';

export class PublishError extends Error {
  constructor(
    readonly kind: PublishFailureKind,
    message: string,
    options?: { cause?: unknown; messageId?: string },
  ) {
    super(message, options?.cause === undefined ? undefined : { cause: options.cause });
    this.name = 'PublishError';
    this.messageId = options?.messageId;
  }
  readonly messageId: string | undefined;
}
