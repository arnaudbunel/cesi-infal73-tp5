import * as grpc from '@grpc/grpc-js';
import { UpstreamError } from '../services/upstream-errors.js';

/**
 * Traduction d'une erreur gRPC en UpstreamError (mapping REST public ; le détail reste dans les logs) :
 *   NOT_FOUND -> 404 (produit inexistant, distinct d'un stock nul)
 *   INVALID_ARGUMENT -> 400 (l'identifiant vient de la commande : le client en est à l'origine)
 *   DEADLINE_EXCEEDED -> 504 · UNAVAILABLE -> 503 · tout le reste (INTERNAL, UNKNOWN...) -> 502
 * Il n'y a AUCUN repli vers HTTP : l'échec est propagé tel quel.
 */
export function mapGrpcError(error: grpc.ServiceError, elapsedMs: number): UpstreamError {
  const options = { cause: error };
  switch (error.code) {
    case grpc.status.NOT_FOUND:
      return new UpstreamError('product-not-found', elapsedMs, undefined, options);
    case grpc.status.INVALID_ARGUMENT:
      return new UpstreamError('bad-request', elapsedMs, undefined, options);
    case grpc.status.DEADLINE_EXCEEDED:
      return new UpstreamError('timeout', elapsedMs, undefined, options);
    case grpc.status.UNAVAILABLE:
      return new UpstreamError('status-503', elapsedMs, undefined, options);
    default:
      return new UpstreamError('status-unexpected', elapsedMs, undefined, options);
  }
}
