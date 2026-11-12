// Recette TP5 (T1–T7) : scénarios RÉELS contre RabbitMQ + PostgreSQL + processus applicatifs. Aucun mock, aucune donnée simulée.
//
//   npm run test:acceptance                  tous les tests
//   npm run test:acceptance -- --only T1,T5  sous-ensemble
//   npm run test:acceptance:tp5:strict       un TODO restant devient un échec (rendu final)
//
// Statuts :
//   PASS         le scénario a produit le comportement attendu, observé en base / RabbitMQ / logs ;
//   FAIL         comportement faux, ou comportement ACQUIS (REST, infra, failpoint fourni) cassé ;
//   TODO ATTENDU le scénario échoue ET une zone TODO(TP5) qui le bloque est encore présente dans le code (état normal du starter) ;
//   NON TESTÉ    prérequis absent (Docker, migration, topologie, consommateur étranger) : aucun scénario n'a été exécuté.
// Un échec n'est « attendu » que si le marqueur TODO(TP5) de la zone bloquante existe encore : retirer le marqueur sans réussir
// donne FAIL. Codes de sortie : 0 (aucun FAIL ni NON TESTÉ), 1 (FAIL, ou TODO en --strict), 2 (NON TESTÉ).
import { auditedEvents, api, armFailpoint, armStatisticsFailures, closeDb, createConfirmedOrder, db, eventQueuesDrained, exchangeStats, injectMessage, lab, notificationCount, pendingZones, processedCount, queueStats, resetTp5State, shipOrder, shippedCounter, sleep, startEventWorker, startOrders, summary, waitUntil } from '../../helpers/tp5.js';
import type { ConsumerKey, EventWorker, OrdersHandle, Zone } from '../../helpers/tp5.js';
import { destructiveGuard } from '../../../scripts/lib/lab-config.mjs';
import { mgmt, mgmtReachable } from '../../../scripts/lib/mgmt.mjs';

type Status = 'PASS' | 'FAIL' | 'TODO ATTENDU' | 'NON TESTÉ';
interface Result {
  id: string;
  title: string;
  status: Status;
  detail: string;
}

/** Échec d'une étape qui dépend d'une zone étudiante. Qualifié « attendu » seulement si la zone est encore marquée TODO. */
class StepFailed extends Error {
  constructor(
    readonly step: string,
    readonly blockers: Zone[],
    detail: string,
  ) {
    super(`${step} — ${detail}`);
  }
}
/** Échec d'un comportement ACQUIS ou de l'outillage : jamais « attendu ». */
class HardFailure extends Error {}

const args = process.argv.slice(2);
const strict = args.includes('--strict');
const onlyArg = args.indexOf('--only');
const only = onlyArg >= 0 ? new Set((args[onlyArg + 1] ?? '').split(',').map((s) => s.trim().toUpperCase())) : null;

async function expectStep(step: string, blockers: Zone[], probe: () => Promise<boolean> | boolean, describeState: () => Promise<string> | string, timeoutMs = 12_000, giveUp?: () => boolean): Promise<void> {
  if (!(await waitUntil(probe, timeoutMs, giveUp))) throw new StepFailed(step, blockers, `non observé en ${timeoutMs / 1000} s max (${await describeState()})`);
}

// ─────────── Environnement d'un scénario : état propre + Commandes + 3 consommateurs ───────────
interface Env {
  orders: OrdersHandle;
  workers: Record<ConsumerKey, EventWorker>;
  stuck: () => boolean;
}
async function withEnv(work: (env: Env) => Promise<void>, options: { startNotifications?: boolean } = {}): Promise<void> {
  await resetTp5State();
  const orders = await startOrders();
  const started: EventWorker[] = [];
  try {
    const workers = {} as Record<ConsumerKey, EventWorker>;
    for (const key of ['notifications', 'statistics', 'audit'] as ConsumerKey[]) {
      if (key === 'notifications' && options.startNotifications === false) continue;
      workers[key] = await startEventWorker(key);
      started.push(workers[key]);
    }
    // Renoncer tôt à une attente est légitime si un consommateur s'est arrêté sur un TODO : il ne traitera jamais rien.
    await work({ orders, workers, stuck: () => started.some((w) => w.stoppedByTodo) });
  } finally {
    await Promise.all(started.map((w) => w.stop()));
    await orders.stop();
    await resetTp5State();
  }
}

const publishDelta = async (before: { publishIn: number; publishOut: number }): Promise<{ publishIn: number; publishOut: number }> => {
  const now = await exchangeStats('business.events');
  return { publishIn: now.publishIn - before.publishIn, publishOut: now.publishOut - before.publishOut };
};

async function shipOrFail(env: Env, id: string): Promise<void> {
  const response = await shipOrder(env.orders.baseUrl, id);
  if (response.status !== 200) throw new HardFailure(`PATCH ${id} -> SHIPPED a répondu HTTP ${response.status} (200 attendu : comportement REST acquis)`);
}

const eventFor = (orderId: string) => auditedEvents().find((e) => e.aggregateId === orderId);

// ─────────── Scénarios ───────────
const scenarios: { id: string; title: string; run: () => Promise<string> }[] = [
  {
    id: 'T1',
    title: 'Expédition valide ORD-102 : une publication, trois copies, traitement isolé',
    run: async () => {
      let detail = '';
      await withEnv(async (env) => {
        const before = await exchangeStats('business.events');
        await shipOrFail(env, 'ORD-102');
        await expectStep('copie vers Audit', ['B'], () => eventFor('ORD-102') !== undefined, summary, 12_000);
        await expectStep('effet Statistiques (compteur = 1)', ['B', 'C2'], async () => (await shippedCounter()) === 1, summary, 12_000, env.stuck);
        await expectStep('effet Notifications (1 notification)', ['B', 'C1'], async () => (await notificationCount()) === 1, summary, 12_000, env.stuck);
        await expectStep('files vides', ['B', 'C1', 'C2'], () => eventQueuesDrained(), summary, 12_000);
        let delta = await publishDelta(before);
        await waitUntil(async () => ((delta = await publishDelta(before)), delta.publishIn >= 1 && delta.publishOut >= 3), 5_000);
        if (delta.publishIn !== 1 || delta.publishOut !== 3) throw new StepFailed('une publication logique, trois copies routées', ['B'], `business.events : publish_in=${delta.publishIn}, publish_out=${delta.publishOut} (attendu 1 et 3)`);
        const eventId = eventFor('ORD-102')?.eventId ?? '?';
        detail = `eventId=${eventId} ; ${await summary()} ; processed_events : notifications=${await processedCount('notifications-worker')}, statistics=${await processedCount('statistics-worker')}`;
      });
      return detail;
    },
  },
  {
    id: 'T2',
    title: 'Transition refusée sur ORD-103 : aucun OrderShipped publié',
    run: async () => {
      let detail = '';
      await withEnv(async (env) => {
        const before = await exchangeStats('business.events');
        const refused = await api(env.orders.baseUrl, 'POST', '/api/orders/ORD-103/cancellations');
        if (refused.status !== 409) throw new HardFailure(`SHIPPED -> CANCELLED sur ORD-103 a répondu HTTP ${refused.status} (409 attendu : règle TP3 acquise)`);
        // Contrôle positif : sans lui, « rien publié » ne prouverait rien (un producteur qui ne publie jamais passerait).
        const control = await createConfirmedOrder(env.orders.baseUrl);
        await shipOrFail(env, control);
        await expectStep(`contrôle positif : l'expédition valide de ${control} est publiée`, ['B'], () => eventFor(control) !== undefined, summary);
        if (eventFor('ORD-103')) throw new StepFailed('aucun événement pour ORD-103', ['B'], 'un événement ORD-103 a été journalisé par Audit');
        await waitUntil(async () => (await publishDelta(before)).publishIn >= 1, 5_000);
        const delta = await publishDelta(before);
        if (delta.publishIn !== 1) throw new StepFailed('une seule publication (le contrôle positif)', ['B'], `publish_in=${delta.publishIn}`);
        detail = `refus 409 sur ORD-103 ; seul ${control} publié (publish_in=1)`;
      });
      return detail;
    },
  },
  {
    id: 'T3',
    title: 'Notifications arrêté avant publication : Statistiques et Audit continuent, backlog conservé',
    run: async () => {
      let detail = '';
      await withEnv(async (env) => {
        await env.workers.notifications.stop();
        await expectStep('Notifications sans consommateur', [], async () => (await queueStats('notifications.events')).consumers === 0, () => 'consumers>0');
        await shipOrFail(env, 'ORD-102');
        await expectStep('Audit continue', ['B'], () => eventFor('ORD-102') !== undefined, summary);
        await expectStep('Statistiques continue (compteur = 1)', ['B', 'C2'], async () => (await shippedCounter()) === 1, summary, 12_000, () => env.workers.statistics.stoppedByTodo);
        const backlog = await queueStats('notifications.events');
        if (backlog.ready < 1 || backlog.consumers !== 0) throw new StepFailed('backlog Notifications conservé', ['B'], `ready=${backlog.ready} consumers=${backlog.consumers}`);
        if ((await notificationCount()) !== 0) throw new StepFailed('aucune notification tant que Notifications est arrêté', ['B', 'C1'], `notification_records=${await notificationCount()}`);
        // Reprise : le backlog est consommé.
        const revived = await startEventWorker('notifications');
        try {
          await expectStep('reprise de Notifications : backlog traité (1 notification)', ['C1'], async () => (await notificationCount()) === 1, summary, 12_000, () => revived.stoppedByTodo);
          await expectStep('files vides après reprise', ['C1'], () => eventQueuesDrained(), summary);
        } finally {
          await revived.stop();
        }
        detail = `backlog notifications.events ready=${backlog.ready} (consumers=0) conservé puis consommé ; ${await summary()}`;
      }, { startNotifications: true });
      return detail;
    },
  },
  {
    id: 'T4',
    title: 'Statistiques en erreur temporaire (503) : retry différé puis exactement un incrément',
    run: async () => {
      let detail = '';
      await withEnv(async (env) => {
        armStatisticsFailures(2);
        await shipOrFail(env, 'ORD-102');
        await expectStep('Audit reçoit sa copie', ['B'], () => eventFor('ORD-102') !== undefined, summary);
        const stats = env.workers.statistics;
        await expectStep('Statistiques : 2 retries planifiés dans son circuit', ['B', 'C2'], () => stats.events('retry_scheduled').length >= 2, () => `retry_scheduled=${stats.events('retry_scheduled').length}`, 30_000, () => stats.stoppedByTodo);
        await expectStep('Statistiques converge : compteur = 1', ['C2'], async () => (await shippedCounter()) === 1, summary, 20_000, () => stats.stoppedByTodo);
        await expectStep('Notifications traite normalement (1 notification)', ['B', 'C1'], async () => (await notificationCount()) === 1, summary, 12_000, () => env.workers.notifications.stoppedByTodo);
        await expectStep('files vides', ['C1', 'C2'], () => eventQueuesDrained(), summary, 20_000);
        const dlq = await queueStats('statistics.events.dlq');
        const leaks = env.workers.notifications.events('retry_scheduled').length + env.workers.audit.events('retry_scheduled').length;
        if (leaks > 0 || dlq.ready > 0) throw new StepFailed('isolation des circuits', ['C1', 'C2'], `retry chez les autres=${leaks}, statistics.events.dlq ready=${dlq.ready}`);
        const retries = stats.events('retry_scheduled').length;
        if ((await shippedCounter()) !== 1 || retries !== 2) throw new StepFailed('exactement 2 retries puis un incrément', ['C2'], `retries=${retries}, compteur=${await shippedCounter()}`);
        detail = `retries Statistiques=${retries}, retries ailleurs=0, DLQ vide ; ${await summary()}`;
      });
      return detail;
    },
  },
  {
    id: 'T5',
    title: 'Même eventId livré deux fois : un seul effet par consommateur',
    run: async () => {
      let detail = '';
      await withEnv(async (env) => {
        const eventId = 'evt-lab-t5';
        const body = { eventId, eventType: 'OrderShipped', schemaVersion: 1, occurredAt: new Date().toISOString(), source: 'orders-service', aggregateId: 'ORD-102', aggregateVersion: 1, data: { orderId: 'ORD-102', status: 'SHIPPED' } };
        for (const copy of [1, 2]) if (!(await injectMessage('business.events', 'order.shipped', body, { message_id: `lab-t5-${copy}` }))) throw new HardFailure('message non routé : topologie absente ? (npm run infra:topology)');
        const w = env.workers;
        await expectStep('Audit : 2 livraisons reçues, 1 fait enregistré', [], () => w.audit.events('delivery.received', eventId).length === 2 && auditedEvents().length === 1, summary);
        await expectStep('Notifications : 2 livraisons reçues', ['C1'], () => w.notifications.events('delivery.received', eventId).length === 2, () => `livraisons=${w.notifications.events('delivery.received', eventId).length}`, 12_000, () => w.notifications.stoppedByTodo);
        await expectStep('Statistiques : 2 livraisons reçues', ['C2'], () => w.statistics.events('delivery.received', eventId).length === 2, () => `livraisons=${w.statistics.events('delivery.received', eventId).length}`, 12_000, () => w.statistics.stoppedByTodo);
        await expectStep('files vides', ['C1', 'C2'], () => eventQueuesDrained(), summary);
        const [n, s, pn, ps] = [await notificationCount(), await shippedCounter(), await processedCount('notifications-worker'), await processedCount('statistics-worker')];
        if (n !== 1 || s !== 1 || pn !== 1 || ps !== 1 || auditedEvents().length !== 1) throw new StepFailed('un seul effet par consommateur', ['C1', 'C2'], `notifications=${n} compteur=${s} processed(notif)=${pn} processed(stats)=${ps} audit=${auditedEvents().length}`);
        detail = `2 livraisons de ${eventId} par consommateur → notifications=1, compteur=1, processed_events=1 chacun, audit=1`;
      });
      return detail;
    },
  },
  {
    id: 'T6',
    title: 'schemaVersion=99 : rejet contrôlé, diagnostic, DLQ, aucune boucle',
    run: async () => {
      let detail = '';
      await withEnv(async (env) => {
        const eventId = 'evt-lab-t6';
        const body = { eventId, eventType: 'OrderShipped', schemaVersion: 99, occurredAt: new Date().toISOString(), source: 'orders-service', aggregateId: 'ORD-102', aggregateVersion: 1, data: { orderId: 'ORD-102', status: 'SHIPPED' } };
        if (!(await injectMessage('business.events', 'order.shipped', body))) throw new HardFailure('message non routé : topologie absente ? (npm run infra:topology)');
        const w = env.workers;
        const dlq = async (key: ConsumerKey): Promise<number> => (await queueStats(`${key}.events.dlq`)).ready;
        await expectStep('Audit : message en DLQ', [], async () => (await dlq('audit')) === 1, async () => `audit.dlq=${await dlq('audit')}`);
        await expectStep('Notifications : message en DLQ', ['C1'], async () => (await dlq('notifications')) === 1, async () => `notifications.dlq=${await dlq('notifications')}`, 12_000, () => w.notifications.stoppedByTodo);
        await expectStep('Statistiques : message en DLQ', ['C2'], async () => (await dlq('statistics')) === 1, async () => `statistics.dlq=${await dlq('statistics')}`, 12_000, () => w.statistics.stoppedByTodo);
        await expectStep('files principales vides', ['C1', 'C2'], () => eventQueuesDrained(), summary);
        for (const key of ['notifications', 'statistics', 'audit'] as ConsumerKey[]) {
          const received = w[key].events('delivery.received', eventId).length;
          const dead = w[key].events('dead_lettered');
          if (received !== 1) throw new StepFailed(`${key} : une seule livraison (pas de boucle)`, ['C1', 'C2'], `livraisons=${received}`);
          if (dead.length !== 1 || typeof dead[0]?.['reason'] !== 'string' || String(dead[0]?.['reason']).length === 0) throw new StepFailed(`${key} : diagnostic journalisé`, ['C1', 'C2'], `dead_lettered=${JSON.stringify(dead)}`);
        }
        if ((await notificationCount()) !== 0 || (await shippedCounter()) !== 0) throw new StepFailed('aucun effet métier', ['C1', 'C2'], await summary());
        detail = `1 livraison + 1 message en DLQ par consommateur ; diagnostic : ${JSON.stringify(w.audit.events('dead_lettered')[0]?.['reason'])}`;
      });
      return detail;
    },
  },
  {
    id: 'T7',
    title: 'Failpoint après COMMIT, avant publish : fenêtre de perte démontrée (analyse Outbox à rédiger)',
    run: async () => {
      let detail = '';
      await withEnv(async (env) => {
        const before = await exchangeStats('business.events');
        armFailpoint('throw');
        const crashed = await shipOrder(env.orders.baseUrl, 'ORD-102');
        if (crashed.status !== 500) throw new HardFailure(`failpoint : HTTP ${crashed.status} reçu, 500 attendu (failpoint fourni inopérant ?)`);
        const state = await api(env.orders.baseUrl, 'GET', '/api/orders/ORD-102');
        if ((state.json as { status?: string } | null)?.status !== 'SHIPPED') throw new HardFailure('la transition enregistrée avant le failpoint doit rester SHIPPED');
        await sleep(1_200); // laisse les compteurs du Management (500 ms) se rafraîchir avant de constater l'ABSENCE de publication
        const lost = await publishDelta(before);
        if (lost.publishIn !== 0 || eventFor('ORD-102')) throw new StepFailed('aucun événement après le failpoint', ['B'], `publish_in=${lost.publishIn}`);
        // Contrôle positif : le failpoint est désarmé (une seule fois) ; une expédition normale DOIT publier, sinon l'absence ci-dessus ne prouverait rien.
        const control = await createConfirmedOrder(env.orders.baseUrl);
        await shipOrFail(env, control);
        await expectStep(`contrôle positif : ${control} est publié quand aucun failpoint n'est armé`, ['B'], () => eventFor(control) !== undefined, summary);
        detail = `ORD-102 SHIPPED (HTTP 500 au client) mais publish_in=0 : fenêtre de perte observée ; contrôle positif ${control} publié. Proposition Outbox : à rédiger dans la fiche (non évaluée ici)`;
      });
      return detail;
    },
  },
];

// ─────────── Prérequis : sans eux, rien n'est exécuté (NON TESTÉ), et surtout rien n'est qualifié de « TODO attendu » ───────────
async function prerequisites(): Promise<string | null> {
  const guard = destructiveGuard();
  if (guard) return `garde-fou de laboratoire : ${guard}`;
  if (!(await mgmtReachable())) return `RabbitMQ Management injoignable (${lab.mgmtUrl}) : « npm run setup » (Docker requis)`;
  try {
    await db().query('SELECT 1 FROM processed_events LIMIT 1');
    await db().query('SELECT 1 FROM notification_records LIMIT 1');
    await db().query('SELECT 1 FROM shipment_stats LIMIT 1');
  } catch (error) {
    return `PostgreSQL ou migration 002 indisponible (${String(error).split('\n')[0]}) : « npm run setup »`;
  }
  const bindings = (await mgmt('GET', '/exchanges/%2F/business.events/bindings/source')) as { destination: string; routing_key: string }[];
  const have = new Set(bindings.map((b) => `${b.destination}|${b.routing_key}`));
  for (const expected of ['notifications.events|order.shipped', 'statistics.events|order.*', 'audit.events|#']) {
    if (!have.has(expected)) return `binding manquant sur business.events : ${expected} (npm run infra:topology)`;
  }
  for (const key of ['notifications', 'statistics', 'audit']) {
    const { consumers } = await queueStats(`${key}.events`);
    if (consumers > 0) return `${consumers} consommateur(s) déjà actif(s) sur ${key}.events : « npm run stop » (ou Ctrl+C de npm run dev) avant la recette`;
  }
  return null;
}

async function main(): Promise<number> {
  const selected = scenarios.filter((s) => !only || only.has(s.id));
  const results: Result[] = [];
  let blocked: string | null = null;
  try {
    blocked = await prerequisites();
  } catch (error) {
    blocked = `prérequis illisibles : ${error instanceof Error ? error.message : String(error)}`;
  }
  if (blocked) {
    for (const s of selected) results.push({ id: s.id, title: s.title, status: 'NON TESTÉ', detail: blocked });
  } else {
    for (const s of selected) {
      process.stdout.write(`… ${s.id} ${s.title}\n`);
      try {
        const detail = await s.run();
        results.push({ id: s.id, title: s.title, status: 'PASS', detail });
      } catch (error) {
        if (error instanceof StepFailed) {
          const pending = error.blockers.filter((zone) => pendingZones().includes(zone));
          results.push(
            pending.length > 0
              ? { id: s.id, title: s.title, status: 'TODO ATTENDU', detail: `bloqué par la zone ${pending.join('+')} (TODO(TP5) présent) — ${error.message}` }
              : { id: s.id, title: s.title, status: 'FAIL', detail: `${error.message}${error.blockers.length === 0 ? '' : ' — aucun marqueur TODO(TP5) ne subsiste dans les zones concernées'}` },
          );
        } else {
          results.push({ id: s.id, title: s.title, status: 'FAIL', detail: error instanceof Error ? error.message : String(error) });
        }
      }
    }
  }
  await closeDb().catch(() => undefined);

  console.log('\n=== Recette TP5 ===');
  for (const r of results) console.log(`${r.status.padEnd(13)} ${r.id}  ${r.title}\n${' '.repeat(14)}${r.detail}`);
  const count = (status: Status): number => results.filter((r) => r.status === status).length;
  console.log(`\nPASS ${count('PASS')} · FAIL ${count('FAIL')} · TODO ATTENDU ${count('TODO ATTENDU')} · NON TESTÉ ${count('NON TESTÉ')}`);
  if (count('NON TESTÉ') > 0) return 2;
  if (count('FAIL') > 0 || (strict && count('TODO ATTENDU') > 0)) return 1;
  return 0;
}

process.exitCode = await main();
