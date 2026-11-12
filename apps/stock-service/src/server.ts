import { buildStockApp } from './app.js';
import { readGrpcStockConfig, readStockConfig } from './config.js';
import { buildGrpcStockServer } from './grpc/server.js';

let config;
let grpcConfig;
try {
  config = readStockConfig();
  grpcConfig = readGrpcStockConfig();
} catch (error) {
  console.error(`[stock-service] invalid configuration: ${(error as Error).message}`);
  process.exit(1);
}

// Simulateur de pannes : usage laboratoire uniquement.
if (process.env['NODE_ENV'] === 'production') {
  console.error('[stock-service] refusing to start: the fault simulator is forbidden when NODE_ENV=production');
  process.exit(1);
}

const server = buildStockApp(config);
const grpcServer = buildGrpcStockServer(grpcConfig);

server.listen(config.port, config.host, () => {
  console.log(
    `[stock-service] listening on http://${config.host}:${config.port} ` +
      `mode=${config.mode} latencyMs=${config.latencyMs} available=${config.available}`,
  );
  if (config.mode !== 'normal') console.warn(`[stock-service] FAULT MODE ACTIVE: ${config.mode}`);
});

server.on('error', (error) => {
  console.error(`[stock-service] ${error.message}`);
  process.exit(1);
});

try {
  const port = await grpcServer.start();
  console.log(`[stock-service] gRPC listening on ${grpcConfig.host}:${port} latencyMs=${grpcConfig.latencyMs}`);
} catch (error) {
  console.error(`[stock-service] gRPC server failed to start: ${(error as Error).message}`);
  process.exit(1);
}

async function shutdown(): Promise<void> {
  server.closeAllConnections();
  server.close();
  await grpcServer.stop();
  process.exit(0);
}
process.on('SIGINT', () => void shutdown());
process.on('SIGTERM', () => void shutdown());
