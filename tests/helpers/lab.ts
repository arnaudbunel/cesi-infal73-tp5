// Aides de test du laboratoire TP4 : état RabbitMQ (API Management), état SQL, workers pilotés par processus.
// Aucune publication AMQP ici : les messages sont injectés par l'API Management (scripts publics de laboratoire).
import { spawn } from 'node:child_process';
import type { ChildProcess } from 'node:child_process';
import { mkdirSync, rmSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import pg from 'pg';
import { buildApp } from '../../apps/order-service/src/app.js';
import { readRabbitUrl } from '../../apps/order-service/src/config.js';
import { BrokerConnection } from '../../apps/order-service/src/messaging/broker-connection.js';
import { RabbitPreparationPublisher } from '../../apps/order-service/src/messaging/preparation-publisher.js';
import { lab, TP4_QUEUES as LAB_QUEUES, TP4_TABLES as LAB_TABLES } from '../../scripts/lib/lab-config.mjs';
import { injectMessage, mgmtReachable, peekMessages, purgeQueue, queueStats } from '../../scripts/lib/mgmt.mjs';

export { lab, LAB_QUEUES, injectMessage, peekMessages, queueStats };

export interface QueueStats {
  ready: number;
  unacked: number;
  consumers: number;
}
export const stats = (name: string): Promise<QueueStats> => queueStats(name) as Promise<QueueStats>;

const repoRoot = fileURLToPath(new URL('../..', import.meta.url));
export const sleep = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));

/** Attend qu'une condition (état lu, jamais un délai fixe) devienne vraie ; échoue avec un message lisible. */
export async function waitFor<T>(what: string, probe: () => Promise<T | false | null | undefined> | T | false | null | undefined, timeoutMs = 15_000, intervalMs = 250): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  let lastError: unknown;
  for (;;) {
    try {
      const value = await probe();
      if (value) return value;
    } catch (error) {
      lastError = error;
    }
    if (Date.now() > deadline) throw new Error(`timeout (${timeoutMs} ms) en attendant : ${what}${lastError ? ` — dernière erreur : ${String(lastError)}` : ''}`);
    await sleep(intervalMs);
  }
}

/** Attend qu'une condition sur une queue soit vraie sur deux relevés espacés (évite un compteur transitoire ou périmé). */
export async function waitForQueue(name: string, what: string, predicate: (s: QueueStats) => boolean, timeoutMs = 20_000): Promise<QueueStats> {
  return waitFor(`${name} : ${what}`, async () => {
    const first = await stats(name);
    if (!predicate(first)) return false;
    await sleep(700);
    const second = await stats(name);
    return predicate(second) ? second : false;
  }, timeoutMs, 100);
}

export const waitDrained = (): Promise<QueueStats> => waitForQueue('orders.prepare', 'ready=0 et unacked=0', (s) => s.ready === 0 && s.unacked === 0);

// ─────────── PostgreSQL ───────────
let pool: pg.Pool | null = null;
export function db(): pg.Pool {
  pool ??= new pg.Pool({ connectionString: lab.databaseUrl, max: 3 });
  return pool;
}
export async function closeDb(): Promise<void> {
  await pool?.end();
  pool = null;
}
export async function rows<T extends pg.QueryResultRow = pg.QueryResultRow>(text: string, params: unknown[] = []): Promise<T[]> {
  return (await db().query<T>(text, params)).rows;
}
export const preparedFor = (orderId: string) => rows<{ order_id: string; operation_id: string }>('SELECT order_id, operation_id FROM prepared_orders WHERE order_id = $1', [orderId]);
export const processedFor = (...operationIds: string[]) =>
  rows<{ operation_id: string }>("SELECT operation_id FROM processed_operations WHERE consumer_name = 'preparation-worker' AND operation_id = ANY($1)", [operationIds]);

// ─────────── Garde-fous et nettoyage ciblé ───────────
/** Échoue clairement si l'infrastructure n'est pas démarrée, ou si un worker étranger consommerait les messages du test. */
export async function requireInfra(): Promise<void> {
  if (!(await mgmtReachable())) throw new Error(`RabbitMQ Management injoignable (${lab.mgmtUrl}) : lancez « npm run infra:up » (Docker requis).`);
  try {
    await db().query('SELECT 1 FROM prepared_orders LIMIT 1');
  } catch (error) {
    throw new Error(`PostgreSQL ou migrations indisponibles (${String(error)}) : « npm run infra:up » puis « npm run db:migrate ».`);
  }
  const { consumers } = await stats('orders.prepare');
  if (consumers > 0) throw new Error(`${consumers} consommateur(s) déjà actif(s) sur orders.prepare : arrêtez votre « npm run dev » / worker avant de lancer les tests.`);
}

/** Vide UNIQUEMENT les 5 queues et les 2 tables du laboratoire, et l'état de panne local. */
export async function cleanLabState(): Promise<void> {
  if (!/_lab$/.test(lab.pgDatabase)) throw new Error(`refus de vider la base « ${lab.pgDatabase} » (nom attendu : *_lab)`);
  for (const queue of LAB_QUEUES) await purgeQueue(queue);
  await db().query(`TRUNCATE TABLE ${LAB_TABLES.join(', ')}`);
  rmSync(join(lab.stateDir, 'dependency-503-until'), { force: true }); // uniquement l'état TP4 : ne pas toucher aux pids/journaux TP5
}

export function setDependencyDown(ms: number): void {
  mkdirSync(lab.stateDir, { recursive: true });
  writeFileSync(join(lab.stateDir, 'dependency-503-until'), String(Date.now() + ms));
}

// ─────────── Workers (processus réels) ───────────
export interface LogLine {
  ts?: string;
  worker?: string;
  event?: string;
  [key: string]: unknown;
}

export class WorkerHandle {
  readonly lines: LogLine[] = [];
  readonly exited: Promise<{ code: number | null; signal: NodeJS.Signals | null }>;
  hasExited = false;
  private readonly child: ChildProcess;

  constructor(readonly name: string, env: Record<string, string>) {
    this.child = spawn(process.execPath, ['--import', 'tsx', 'apps/preparation-worker/src/server.ts'], {
      cwd: repoRoot,
      env: { ...process.env, WORKER_NAME: name, ...env },
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    let buffer = '';
    this.child.stdout?.on('data', (chunk: Buffer) => {
      buffer += String(chunk);
      let index = buffer.indexOf('\n');
      while (index >= 0) {
        const text = buffer.slice(0, index).trim();
        buffer = buffer.slice(index + 1);
        index = buffer.indexOf('\n');
        if (!text) continue;
        try {
          this.lines.push(JSON.parse(text) as LogLine);
        } catch {
          this.lines.push({ event: 'raw', text });
        }
      }
    });
    this.child.stderr?.on('data', (chunk: Buffer) => this.lines.push({ event: 'stderr', text: String(chunk).trim() }));
    this.exited = new Promise((resolve) =>
      this.child.once('exit', (code, signal) => {
        this.hasExited = true;
        resolve({ code, signal });
      }),
    );
  }

  /** Livraisons reçues (événement `delivery.received`, fourni), éventuellement filtrées par operationId. */
  deliveries(operationId?: string): LogLine[] {
    return this.lines.filter((l) => l.event === 'delivery.received' && (operationId === undefined || l['operationId'] === operationId));
  }

  async stop(): Promise<void> {
    if (this.hasExited) return;
    this.child.kill('SIGTERM');
    const timer = setTimeout(() => this.child.kill('SIGKILL'), 8_000);
    await this.exited;
    clearTimeout(timer);
  }
}

const started: WorkerHandle[] = [];

/** Démarre un worker et attend qu'il soit consommateur de orders.prepare (lecture d'état, pas de sleep). */
export async function startWorker(name: string, env: Record<string, string> = {}): Promise<WorkerHandle> {
  const before = (await stats('orders.prepare')).consumers;
  const worker = new WorkerHandle(name, env);
  started.push(worker);
  await waitFor(`worker ${name} consommateur de orders.prepare`, async () => {
    if (worker.hasExited) throw new Error(`le worker ${name} s'est arrêté au démarrage : ${JSON.stringify(worker.lines.slice(-3))}`);
    return (await stats('orders.prepare')).consumers > before;
  }, 30_000, 200);
  return worker;
}

export async function stopAllWorkers(): Promise<void> {
  await Promise.all(started.splice(0).map((w) => w.stop()));
}

// ─────────── Service Commandes avec le vrai publisher ───────────
export async function startOrders(): Promise<{ baseUrl: string; stop: () => Promise<void> }> {
  const broker = new BrokerConnection(readRabbitUrl());
  const app = buildApp({ publisher: new RabbitPreparationPublisher(broker) });
  await app.listen({ port: 0, host: '127.0.0.1' });
  const address = app.server.address();
  const port = typeof address === 'object' && address ? address.port : 0;
  return {
    baseUrl: `http://127.0.0.1:${port}`,
    stop: async () => {
      await app.close();
      await broker.close();
    },
  };
}

export function preparationMessage(operationId: string, orderId: string, attempt = 0): Record<string, unknown> {
  return { schemaVersion: 1, type: 'OrderPreparationRequested', operationId, orderId, attempt, requestedAt: new Date().toISOString() };
}
