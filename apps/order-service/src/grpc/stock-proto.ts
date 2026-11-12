import { fileURLToPath } from 'node:url';
import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';

/**
 * Chargement dynamique du contrat `packages/contracts/proto/stock.proto` (@grpc/proto-loader).
 * Option `keepCase: false` : `product_id` du .proto devient `productId` en TypeScript.
 * Le chemin relatif est identique depuis `src/grpc/` et `dist/grpc/`.
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

export interface StockServiceClient extends grpc.Client {
  getStock(
    request: StockRequest,
    options: grpc.CallOptions,
    callback: (error: grpc.ServiceError | null, response?: StockResponse) => void,
  ): grpc.ClientUnaryCall;
}

export type StockServiceClientConstructor = new (
  address: string,
  credentials: grpc.ChannelCredentials,
  options?: Partial<grpc.ClientOptions>,
) => StockServiceClient;

export function loadStockServiceClient(): StockServiceClientConstructor {
  const definition = protoLoader.loadSync(STOCK_PROTO_PATH, {
    keepCase: false,
    longs: Number,
    enums: String,
    defaults: true, // available = 0 et reservable = false sont des valeurs présentes, jamais « absentes »
    oneofs: true,
  });
  const loaded = grpc.loadPackageDefinition(definition) as unknown as {
    inventory: { v1: { StockService: StockServiceClientConstructor } };
  };
  return loaded.inventory.v1.StockService;
}
