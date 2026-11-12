import { CONSUMER_KEYS, readEventWorkerConfig } from './config.js';
import type { EventWorkerConfig } from './config.js';
import { createPool } from './db.js';
import { AuditHandler } from './handlers/audit.handler.js';
import type { HandlerDeps } from './handlers/deps.js';
import { NotificationsHandler } from './handlers/notifications.handler.js';
import { StatisticsHandler } from './handlers/statistics.handler.js';
import { createStatisticsFaults } from './lab-faults.js';
import { createLogger } from './logger.js';
import { ConsumerRuntime } from './runtime/consumer-runtime.js';
import type { EventHandler } from './runtime/consumer-runtime.js';

// Usage : tsx src/server.ts <notifications|statistics|audit>   (ou CONSUMER=...). Un processus = un consommateur = une queue.
let config: EventWorkerConfig;
try {
  config = readEventWorkerConfig(process.argv[2]);
} catch (error) {
  console.error(`[event-workers] invalid configuration: ${(error as Error).message} (known: ${CONSUMER_KEYS.join(', ')})`);
  process.exit(1);
}

const log = createLogger(config.workerName);
const pool = createPool(config.databaseUrl, log);
const deps: HandlerDeps = { pool, log, faults: createStatisticsFaults(config.labStateDir), labStateDir: config.labStateDir };

const handlers: Record<typeof config.consumer, EventHandler> = {
  notifications: new NotificationsHandler(deps),
  statistics: new StatisticsHandler(deps),
  audit: new AuditHandler(deps),
};

await new ConsumerRuntime(config, handlers[config.consumer], log, () => pool.end()).run();
