/** Dépendance externe simulée indisponible (HTTP 503). Erreur TEMPORAIRE : une nouvelle tentative peut réussir. */
export class TemporaryDependencyError extends Error {
  readonly status = 503;
  constructor(message = 'dependency answered 503') {
    super(message);
    this.name = 'TemporaryDependencyError';
  }
}

/** Erreur DÉFINITIVE : la rejouer donnerait le même résultat (contrat invalide, demande contradictoire...). */
export class PermanentError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'PermanentError';
  }
}

/** Codes SQLSTATE réessayables : classe 08 (connexion), 40001/40P01 (sérialisation, deadlock), 53 (ressources), 57P01-3 (arrêt serveur). */
const TRANSIENT_SQLSTATE = /^(08|53|57P0[123]|40001|40P01)/;
const TRANSIENT_NODE_CODES = new Set(['ECONNREFUSED', 'ECONNRESET', 'ETIMEDOUT', 'EPIPE', 'ENOTFOUND', 'EAI_AGAIN']);

/**
 * Classification : true = temporaire (retry différé possible), false = définitive (DLQ).
 * Par défaut une erreur INCONNUE est définitive : on ne boucle pas sur un bug ; la DLQ la rend visible.
 */
export function isTemporary(error: unknown): boolean {
  if (error instanceof TemporaryDependencyError) return true;
  if (error instanceof PermanentError) return false;
  const code = (error as { code?: unknown } | null)?.code;
  if (typeof code === 'string') return TRANSIENT_SQLSTATE.test(code) || TRANSIENT_NODE_CODES.has(code);
  const message = error instanceof Error ? error.message : '';
  return /Connection terminated|timeout exceeded when trying to connect/i.test(message);
}
