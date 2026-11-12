/**
 * Marqueur pédagogique TP5 : levé par les zones laissées à compléter (`TODO(TP5)`).
 * Les outils de recette distinguent « zone non réalisée » (cette erreur) d'une vraie panne.
 * Supprimez l'appel qui la lève quand vous implémentez la zone.
 */
export class TodoNotImplementedError extends Error {
  constructor(
    readonly zone: string,
    message: string,
  ) {
    super(`TODO(TP5) [${zone}] non réalisé : ${message}`);
    this.name = 'TodoNotImplementedError';
  }
}
