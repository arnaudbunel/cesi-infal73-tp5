// Aides de la recette TP5 : processus réels (consommateurs d'événements), service Commandes avec le vrai publisher,
// observation RabbitMQ (Management) / PostgreSQL / journal d'audit. Aucune donnée simulée : on observe l'état réel.
import { spawn } from 'node:child_process';
import type { ChildProcess } from 'node:child_process';
import { existsSync, mkdirSync, readdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { buildApp } from '../../apps/order-service/src/app.js';
import { BrokerOrderShippedPublisher } from '../../apps/order-service/src/events/order-shipped-publisher.js';
import { createFileFailpoints, FAILPOINT_FILE } from '../../apps/order-service/src/lab-failpoints.js';
import { readRabbitUrl } from '../../apps/order-service/src/config.js';
import { BrokerConnection } from '../../apps/order-service/src/messaging/broker-connection.js';
import { RabbitPreparationPublisher } from '../../apps/order-service/src/messaging/preparation-publisher.js';
import { TP5_CONSUMERS, TP5_QUEUES, destructiveGuard, lab } from '../../scripts/lib/lab-config.mjs';
import { exchangeStats, injectMessage, purgeQueue, queueStats } from '../../scripts/lib/mgmt.mjs';
import { closeDb, db, rows } from './lab.js';

export { TP5_CONSUMERS, TP5_QUEUES, exchangeStats, injectMessage, queueStats, closeDb, db, rows, lab };
export type ConsumerKey = 'notifications' | 'statistics' | 'audit';

const repoRoot = fileURLToPath(new URL('../..', import.meta.url));
export const sleep = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));

// ─────────── Zones TODO(TP5) restantes ───────────
export type Zone = 'B' | 'C1' | 'C2';
const ZONE_FILES: Record<Zone, string[]> = {
  B: ['apps/order-service/src/events/order-shipped-publisher.ts', 'apps/order-service/src/services/orders.service.ts'],
  C1: ['apps/event-workers/src/handlers/notifications.handler.ts'],
  C2: ['apps/event-workers/src/handlers/statistics.handler.ts'],
};
/** Zones dont le code source contient encore un marqueur `TODO(TP5) [Zone...]`. Sert UNIQUEMENT à qualifier un échec observé. */
export function pendingZones(): Zone[] {
  return (Object.keys(ZONE_FILES) as Zone[]).filter((zone) =>
    ZONE_FILES[zone].some((file) => new RegExp(`TODO\\(TP5\\) \\[${zone}[\\].\\d-]`).test(readFileSync(join(repoRoot, file), 'utf8'))),
  );
}

// ─────────── Attentes bornées ───────────
/** Attend une condition lue (jamais un délai fixe) jusqu'à `timeoutMs`. `giveUp` permet de renoncer tôt si l'attente est vaine. */
export async function waitUntil(probe: () => Promise<boolean> | boolean, timeoutMs = 12_000, giveUp?: () => boolean, intervalMs = 200): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    try {
      if (await probe()) return true;
    } catch {
      /* observation momentanément impossible : on réessaie jusqu'au délai */
    }
    if (Date.now() > deadline || giveUp?.()) return false;
    await sleep(intervalMs);
  }
}

// ─────────── État de laboratoire ───────────
/** Remet à zéro UNIQUEMENT le laboratoire TP5 (queues TP5, tables TP5, journal d'audit, injections). */
export async function resetTp5State(): Promise<void> {
  const refusal = destructiveGuard();
  if (refusal) throw new Error(`refus de réinitialiser : ${refusal}`);
  for (const queue of TP5_QUEUES as string[]) await purgeQueue(queue);
  await db().query('TRUNCATE TABLE processed_events, notification_records');
  await db().query("INSERT INTO shipment_stats(metric, counter) VALUES ('shipped', 0) ON CONFLICT (metric) DO UPDATE SET counter = 0, updated_at = now()");
  for (const entry of ['audit', FAILPOINT_FILE, 'statistics-fail-attempts.json']) rmSync(join(lab.stateDir, entry), { recursive: true, force: true });
}

export function armStatisticsFailures(attempts: number): void {
  mkdirSync(lab.stateDir, { recursive: true });
  writeFileSync(join(lab.stateDir, 'statistics-fail-attempts.json'), JSON.stringify({ remaining: attempts }));
}
export function armFailpoint(mode: 'throw' | 'exit' = 'throw'): void {
  mkdirSync(lab.stateDir, { recursive: true });
  writeFileSync(join(lab.stateDir, FAILPOINT_FILE), JSON.stringify({ name: 'AFTER_SHIP_COMMIT_BEFORE_EVENT_PUBLISH', mode, once: true }));
}

// ─────────── Observations ───────────
export interface AuditedEvent {
  eventId: string;
  aggregateId: string;
  routingKey: string;
}
/** Faits enregistrés par le worker Audit de référence (un fichier par eventId). */
export function auditedEvents(): AuditedEvent[] {
  const dir = join(lab.stateDir, 'audit');
  if (!existsSync(dir)) return [];
  return readdirSync(dir)
    .filter((f) => f.endsWith('.json'))
    .map((f) => {
      const entry = JSON.parse(readFileSync(join(dir, f), 'utf8')) as { routingKey: string; event: { eventId: string; aggregateId: string } };
      return { eventId: entry.event.eventId, aggregateId: entry.event.aggregateId, routingKey: entry.routingKey };
    });
}
export const notificationCount = async (): Promise<number> => Number((await rows<{ n: string }>('SELECT count(*) AS n FROM notification_records'))[0]?.n ?? 0);
export const shippedCounter = async (): Promise<number> => Number((await rows<{ counter: string }>("SELECT counter FROM shipment_stats WHERE metric = 'shipped'"))[0]?.counter ?? 0);
export const processedCount = async (consumerName: string): Promise<number> =>
  Number((await rows<{ n: string }>('SELECT count(*) AS n FROM processed_events WHERE consumer_name = $1', [consumerName]))[0]?.n ?? 0);

export async function summary(): Promise<string> {
  const [n, s, a] = [await notificationCount(), await shippedCounter(), auditedEvents().length];
  return `notifications=${n} stats.shipped=${s} audit=${a}`;
}

/** Les trois queues d'abonnement sont vides (ready=0, unacked=0) sur deux relevés espacés. */
export async function eventQueuesDrained(keys: ConsumerKey[] = ['notifications', 'statistics', 'audit']): Promise<boolean> {
  const read = async (): Promise<boolean> => {
    for (const key of keys) {
      const s = await queueStats(`${key}.events`);
      if (s.ready !== 0 || s.unacked !== 0) return false;
    }
    return true;
  };
  if (!(await read())) return false;
  await sleep(700);
  return read();
}

// ─────────── Consommateurs d'événements : processus réels ───────────
export interface LogLine {
  event?: string;
  [key: string]: unknown;
}

export class EventWorker {
  readonly lines: LogLine[] = [];
  hasExited = false;
  exitCode: number | null = null;
  readonly exited: Promise<number | null>;
  private readonly child: ChildProcess;

  constructor(readonly key: ConsumerKey) {
    this.child = spawn(process.execPath, ['--import', 'tsx', 'apps/event-workers/src/server.ts', key], {
      cwd: repoRoot,
      env: { ...process.env, WORKER_NAME: `${key}-test`, LAB_STATE_DIR: lab.stateDir },
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    let buffer = '';
    this.child.stdout?.on('data', (chunk: Buffer) => {
      buffer += String(chunk);
      for (let i = buffer.indexOf('\n'); i >= 0; i = buffer.indexOf('\n')) {
        const text = buffer.slice(0, i).trim();
        buffer = buffer.slice(i + 1);
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
      this.child.once('exit', (code) => {
        this.hasExited = true;
        this.exitCode = code;
        resolve(code);
      }),
    );
  }

  /** Arrêté parce que sa zone TODO(TP5) n'est pas réalisée (code de sortie 3, message laissé dans la queue). */
  get stoppedByTodo(): boolean {
    return this.hasExited && this.exitCode === 3;
  }
  /** Arrêté pour une autre raison qu'un arrêt demandé ou qu'un TODO : vraie panne. */
  get crashed(): boolean {
    return this.hasExited && this.exitCode !== 3 && this.exitCode !== 0;
  }
  events(name: string, eventId?: string): LogLine[] {
    return this.lines.filter((l) => l.event === name && (eventId === undefined || l['eventId'] === eventId));
  }
  async stop(): Promise<void> {
    if (this.hasExited) return;
    this.child.kill('SIGTERM');
    const timer = setTimeout(() => this.child.kill('SIGKILL'), 8_000);
    await this.exited;
    clearTimeout(timer);
  }
}

/** Démarre un consommateur et attend qu'il consomme sa queue. Un arrêt par TODO est toléré ; tout autre arrêt est une panne. */
export async function startEventWorker(key: ConsumerKey): Promise<EventWorker> {
  const worker = new EventWorker(key);
  const ready = await waitUntil(async () => worker.hasExited || (await queueStats(`${key}.events`)).consumers > 0, 30_000, undefined, 200);
  if (!ready) {
    await worker.stop();
    throw new Error(`le worker ${key} n'est pas devenu consommateur de ${key}.events en 30 s : ${JSON.stringify(worker.lines.slice(-3))}`);
  }
  if (worker.crashed) throw new Error(`le worker ${key} s'est arrêté au démarrage (code ${worker.exitCode}) : ${JSON.stringify(worker.lines.slice(-3))}`);
  return worker;
}

// ─────────── Service Commandes avec le vrai publisher (code étudiant inclus) ───────────
export interface OrdersHandle {
  baseUrl: string;
  stop: () => Promise<void>;
}
export async function startOrders(): Promise<OrdersHandle> {
  const broker = new BrokerConnection(readRabbitUrl());
  const confirmed = new RabbitPreparationPublisher(broker);
  const app = buildApp({
    publisher: confirmed,
    shippedPublisher: new BrokerOrderShippedPublisher(confirmed),
    labMode: true,
    failpoints: createFileFailpoints(lab.stateDir, () => undefined),
  });
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

export async function api(baseUrl: string, method: string, path: string, body?: unknown, headers: Record<string, string> = {}): Promise<{ status: number; json: unknown }> {
  const response = await fetch(`${baseUrl}${path}`, {
    method,
    headers: { ...(body === undefined ? {} : { 'content-type': 'application/json' }), ...headers },
    body: body === undefined ? undefined : JSON.stringify(body),
    signal: AbortSignal.timeout(15_000),
  });
  const text = await response.text();
  return { status: response.status, json: text ? (JSON.parse(text) as unknown) : null };
}
export const shipOrder = (baseUrl: string, id: string) => api(baseUrl, 'PATCH', `/api/orders/${id}`, { status: 'SHIPPED' });

/** Crée une NOUVELLE commande CONFIRMED (commande de test distincte : ORD-102 n'est expédiable qu'une fois par remise à zéro). */
export async function createConfirmedOrder(baseUrl: string): Promise<string> {
  const created = await api(baseUrl, 'POST', '/api/orders', { customerId: 'C-TP5', items: [{ productId: 'P1', quantity: 1 }] }, { 'idempotency-key': `tp5-${Date.now()}-${Math.random()}` });
  const id = (created.json as { id?: string } | null)?.id;
  if (created.status !== 201 || !id) throw new Error(`création de commande de test impossible (HTTP ${created.status})`);
  const confirmed = await api(baseUrl, 'PATCH', `/api/orders/${id}`, { status: 'CONFIRMED' });
  if (confirmed.status !== 200) throw new Error(`confirmation de ${id} impossible (HTTP ${confirmed.status})`);
  return id;
}
