import type { StockGateway } from './stock-gateway.js';
import type { OrderServiceConfig } from '../config.js';
import { UpstreamError } from './upstream-errors.js';

export interface StockInfo {
  productId: string;
  available: number;
}

export interface StockCallContext {
  /** Identifiant de corrélation propagé à Stocks (en-tête X-Request-Id). */
  requestId?: string;
}

/**
 * Validation runtime de la représentation Stocks. `as StockInfo` ne valide rien :
 * ici on vérifie réellement la forme et les valeurs.
 */
export function parseStockPayload(body: unknown, expectedProductId: string): StockInfo | null {
  if (typeof body !== 'object' || body === null || Array.isArray(body)) return null;
  const { productId, available } = body as Record<string, unknown>;
  if (productId !== expectedProductId) return null; // typeof string implicite : égalité stricte avec une chaîne
  if (typeof available !== 'number' || !Number.isSafeInteger(available) || available < 0) return null;
  return { productId, available };
}

/** Client HTTP du service Stocks (consultation uniquement, aucun retry). */
export class StockClient implements StockGateway {
  constructor(private readonly config: OrderServiceConfig) {}

  async getStock(productId: string, context: StockCallContext = {}): Promise<StockInfo> {
    const startedAt = performance.now();
    const elapsed = () => Math.round(performance.now() - startedAt);
    // UN SEUL signal pour tout le cycle : attente des en-têtes ET lecture du corps.
    const signal = AbortSignal.timeout(this.config.stockTimeoutMs);
    const url = `${this.config.stockBaseUrl}/stock/${encodeURIComponent(productId)}`;
    const headers: Record<string, string> = { accept: 'application/json' };
    if (context.requestId) headers['x-request-id'] = context.requestId;

    // Phase 1 : obtention des en-têtes. Un échec ici n'est PAS un statut HTTP.
    let response: Response;
    try {
      response = await fetch(url, { signal, headers });
    } catch (error) {
      throw new UpstreamError(signal.aborted ? 'timeout' : 'connect', elapsed(), undefined, { cause: error });
    }

    // Phase 2 : un statut HTTP est une réponse, pas un rejet de fetch().
    if (!response.ok) {
      await response.body?.cancel().catch(() => undefined); // libère la connexion
      throw new UpstreamError(response.status === 503 ? 'status-503' : 'status-unexpected', elapsed(), response.status);
    }

    // Phase 3 : lecture du corps, sous le MÊME signal. Séparée du parsing pour ne pas confondre
    // « corps interrompu » (transport) et « corps invalide » (contenu).
    let text: string;
    try {
      text = await response.text();
    } catch (error) {
      throw new UpstreamError(signal.aborted ? 'timeout' : 'body-interrupted', elapsed(), response.status, { cause: error });
    }

    // Phase 4 : syntaxe puis sémantique.
    let body: unknown;
    try {
      body = JSON.parse(text);
    } catch (error) {
      throw new UpstreamError('invalid-json', elapsed(), response.status, { cause: error });
    }
    const stock = parseStockPayload(body, productId);
    if (!stock) throw new UpstreamError('invalid-payload', elapsed(), response.status);
    return stock;
  }
}
