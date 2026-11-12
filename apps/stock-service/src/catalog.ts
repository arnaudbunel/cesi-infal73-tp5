/**
 * Source de données UNIQUE du service Stocks, partagée par l'interface HTTP et l'interface gRPC.
 * P12 = `available` configuré (12 par défaut) ; P13 = 0 ; tout autre produit est INCONNU.
 * Un produit inconnu (`undefined`) n'est pas un stock à zéro.
 */
export function lookupStock(productId: string, p12Available: number): number | undefined {
  if (productId === 'P12') return p12Available;
  if (productId === 'P13') return 0;
  return undefined;
}
