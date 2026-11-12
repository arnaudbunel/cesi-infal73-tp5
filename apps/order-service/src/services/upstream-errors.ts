import { PROBLEM_TYPES } from '../problems.js';

/** Cause précise d'un échec amont (journalisée côté serveur, jamais exposée telle quelle au client). */
export type UpstreamFailureKind =
  | 'timeout' //            budget total dépassé (en-têtes OU corps)
  | 'status-503' //         Stocks a répondu 503
  | 'status-unexpected' //  autre statut non 2xx
  | 'connect' //            échec avant réception des en-têtes (connexion refusée, reset, DNS...)
  | 'body-interrupted' //   en-têtes reçus, corps interrompu (socket fermé, flux avorté)
  | 'invalid-json' //       corps reçu en entier mais syntaxiquement invalide
  | 'invalid-payload' //    JSON valide mais non conforme à la représentation attendue
  | 'product-not-found' //  le produit n'existe pas côté Stocks (≠ stock nul)
  | 'bad-request' //        la requête transmise à Stocks est invalide
  | 'not-implemented'; //   fonctionnalité non réalisée (réservé au socle étudiant)

const PUBLIC: Record<UpstreamFailureKind, { status: 400 | 404 | 501 | 502 | 503 | 504; type: string; detail: string }> = {
  timeout: { status: 504, type: PROBLEM_TYPES.upstreamTimeout, detail: 'The stock service did not answer in time.' },
  'status-503': { status: 503, type: PROBLEM_TYPES.upstreamUnavailable, detail: 'The stock service is temporarily unavailable.' },
  'status-unexpected': { status: 502, type: PROBLEM_TYPES.upstreamBadGateway, detail: 'The stock service returned an unexpected response.' },
  connect: { status: 502, type: PROBLEM_TYPES.upstreamBadGateway, detail: 'The stock service could not be reached.' },
  'body-interrupted': { status: 502, type: PROBLEM_TYPES.upstreamBadGateway, detail: 'The stock service response was interrupted.' },
  'invalid-json': { status: 502, type: PROBLEM_TYPES.upstreamBadGateway, detail: 'The stock service returned an invalid response.' },
  'product-not-found': { status: 404, type: PROBLEM_TYPES.productNotFound, detail: 'The product does not exist.' },
  'bad-request': { status: 400, type: PROBLEM_TYPES.badRequest, detail: 'The request could not be processed.' },
  'not-implemented': { status: 501, type: PROBLEM_TYPES.notImplemented, detail: 'This capability is not implemented yet.' },
  'invalid-payload': { status: 502, type: PROBLEM_TYPES.upstreamBadGateway, detail: 'The stock service returned an invalid response.' },
};

/** Erreur « métier » du dialogue avec Stocks : distincte d'une erreur interne de Commandes. */
export class UpstreamError extends Error {
  readonly httpStatus: 400 | 404 | 501 | 502 | 503 | 504;
  readonly problemType: string;
  readonly publicDetail: string;

  constructor(
    readonly kind: UpstreamFailureKind,
    readonly elapsedMs: number,
    readonly upstreamStatus?: number,
    options?: { cause?: unknown },
  ) {
    super(`upstream failure: ${kind}`, options);
    this.name = 'UpstreamError';
    const mapping = PUBLIC[kind];
    this.httpStatus = mapping.status;
    this.problemType = mapping.type;
    this.publicDetail = mapping.detail;
  }
}
