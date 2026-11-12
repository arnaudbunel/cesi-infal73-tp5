import { fileURLToPath } from 'node:url';
import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';

/**
 * Chargement dynamique du contrat `packages/contracts/proto/stock.proto` (@grpc/proto-loader).
 * `keepCase: false` : `product_id` devient `productId`. Chemin identique depuis `src/grpc/` et `dist/grpc/`.
 */
export const STOCK_PROTO_PATH = fileURLToPath(new URL('../../../../packages/contracts/proto/stock.proto', import.meta.url));

export interface StockRequest {
  productId: string;
}

export interface StockResponse {
  productId: string;
  available: number;
  reservable: boolean;
}

export interface StockServiceDefinition {
  service: grpc.ServiceDefinition;
}

export function loadStockService(): grpc.ServiceDefinition {
  const definition = protoLoader.loadSync(STOCK_PROTO_PATH, {
    keepCase: false,
    longs: Number,
    enums: String,
    defaults: true, // product_id absent -> '' ; available absent -> 0 (valeurs par défaut proto3)
    oneofs: true,
  });
  const loaded = grpc.loadPackageDefinition(definition) as unknown as {
    inventory: { v1: { StockService: { service: grpc.ServiceDefinition } } };
  };
  return loaded.inventory.v1.StockService.service;
}
