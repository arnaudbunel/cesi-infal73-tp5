// Démarrage / arrêt REPRODUCTIBLES des processus applicatifs, en arrière-plan, sans shell (portable Windows/Linux).
//   node scripts/stack.mjs start  [service...]   (par défaut : tous)
//   node scripts/stack.mjs stop   [service...]
//   node scripts/stack.mjs status [service...]
//   node scripts/stack.mjs logs   <service>      (dernières lignes du journal)
// Services : stocks, orders, preparation, notifications, statistics, audit.
// Pids dans .lab-state/pids/, journaux dans .lab-state/logs/ (un fichier par service). RabbitMQ/PostgreSQL : npm run setup.
// Commandes est lancé avec LAB_MODE=1 (failpoint T7, remise à zéro des commandes) ; refusé si NODE_ENV=production (lab-config).
import { spawn } from 'node:child_process';
import { closeSync, existsSync, mkdirSync, openSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { createConnection } from 'node:net';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { lab } from './lib/lab-config.mjs';
import { queueStats } from './lib/mgmt.mjs';

const root = fileURLToPath(new URL('..', import.meta.url));
const pidDir = join(lab.stateDir, 'pids');
const logDir = join(lab.stateDir, 'logs');

export const SERVICES = {
  stocks: { entry: ['apps/stock-service/src/server.ts'], port: Number(process.env.STOCK_PORT ?? 3001) },
  orders: { entry: ['apps/order-service/src/server.ts'], port: Number(process.env.PORT ?? 3000), env: { LAB_MODE: '1' } },
  preparation: { entry: ['apps/preparation-worker/src/server.ts'], queue: 'orders.prepare' },
  notifications: { entry: ['apps/event-workers/src/server.ts', 'notifications'], queue: 'notifications.events' },
  statistics: { entry: ['apps/event-workers/src/server.ts', 'statistics'], queue: 'statistics.events' },
  audit: { entry: ['apps/event-workers/src/server.ts', 'audit'], queue: 'audit.events' },
};

const pidFile = (name) => join(pidDir, `${name}.pid`);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function alive(pid) {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}
function readPid(name) {
  try {
    const pid = Number(readFileSync(pidFile(name), 'utf8').trim());
    return Number.isInteger(pid) && pid > 0 && alive(pid) ? pid : null;
  } catch {
    return null;
  }
}
const portOpen = (port) =>
  new Promise((resolve) => {
    const socket = createConnection({ port, host: '127.0.0.1' });
    socket.once('connect', () => (socket.destroy(), resolve(true)));
    socket.once('error', () => resolve(false));
  });

async function ready(name, pid, timeoutMs = 30_000) {
  const spec = SERVICES[name];
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (!alive(pid)) return { ok: false, why: `le processus s'est arrêté au démarrage (voir : node scripts/stack.mjs logs ${name})` };
    try {
      if (spec.port && (await portOpen(spec.port))) return { ok: true };
      if (spec.queue && (await queueStats(spec.queue)).consumers > 0) return { ok: true };
    } catch {
      /* RabbitMQ pas encore prêt : on réessaie */
    }
    await sleep(300);
  }
  return { ok: false, why: `pas prêt après ${timeoutMs / 1000} s (voir : node scripts/stack.mjs logs ${name})` };
}

function pick(names) {
  const list = names.length > 0 ? names : Object.keys(SERVICES);
  for (const name of list) if (!SERVICES[name]) throw new Error(`service inconnu « ${name} » (connus : ${Object.keys(SERVICES).join(', ')})`);
  return list;
}

async function start(name) {
  const running = readPid(name);
  if (running) return console.log(`${name.padEnd(14)} déjà démarré (pid ${running})`);
  const spec = SERVICES[name];
  if (spec.port && (await portOpen(spec.port))) return console.log(`${name.padEnd(14)} ✖ le port ${spec.port} est déjà occupé par un autre processus (npm run reset signale les ports)`);
  mkdirSync(pidDir, { recursive: true });
  mkdirSync(logDir, { recursive: true });
  const out = openSync(join(logDir, `${name}.log`), 'a');
  const child = spawn(process.execPath, ['--import', 'tsx', ...spec.entry], {
    cwd: root,
    detached: true,
    stdio: ['ignore', out, out],
    env: { ...process.env, LAB_STATE_DIR: lab.stateDir, WORKER_NAME: process.env.WORKER_NAME ?? name, ...spec.env },
  });
  child.unref();
  closeSync(out);
  writeFileSync(pidFile(name), String(child.pid));
  const state = await ready(name, child.pid);
  // Pas prêt mais encore vivant (ex. RabbitMQ éteint : le worker boucle en reconnexion) : on GARDE le pid pour que `stop` puisse l'arrêter.
  if (!state.ok && !alive(child.pid)) rmSync(pidFile(name), { force: true });
  console.log(`${name.padEnd(14)} ${state.ok ? `démarré (pid ${child.pid})` : `✖ ${state.why}${alive(child.pid) ? ` — processus conservé (pid ${child.pid}), « npm run stop » pour l'arrêter` : ''}`}`);
  return state.ok;
}

async function stop(name) {
  const pid = readPid(name);
  if (!pid) {
    rmSync(pidFile(name), { force: true });
    return console.log(`${name.padEnd(14)} déjà arrêté`);
  }
  process.kill(pid, 'SIGTERM');
  const deadline = Date.now() + 8_000;
  while (alive(pid) && Date.now() < deadline) await sleep(100);
  if (alive(pid)) process.kill(pid, 'SIGKILL');
  rmSync(pidFile(name), { force: true });
  console.log(`${name.padEnd(14)} arrêté`);
}

function status(name) {
  const pid = readPid(name);
  console.log(`${name.padEnd(14)} ${pid ? `en marche (pid ${pid})` : 'arrêté'}`);
}

export async function run(action, names = []) {
  if (action === 'logs') {
    const [name] = pick(names);
    const file = join(logDir, `${name}.log`);
    if (!existsSync(file)) return console.log(`aucun journal pour ${name}`);
    console.log(readFileSync(file, 'utf8').trimEnd().split('\n').slice(-40).join('\n'));
    return;
  }
  const list = pick(names);
  // Ordre : dépendances d'abord au démarrage (stocks, orders, workers) ; inverse à l'arrêt.
  if (action === 'start') {
    let failed = false;
    for (const name of list) if ((await start(name)) === false) failed = true;
    if (failed) process.exitCode = 1;
  } else if (action === 'stop') for (const name of [...list].reverse()) await stop(name);
  else if (action === 'status') list.forEach(status);
  else throw new Error('usage : stack.mjs start|stop|status|logs [service...]');
}

if (process.argv[1]?.endsWith('stack.mjs')) {
  run(process.argv[2], process.argv.slice(3)).catch((error) => {
    console.error(`✖ ${error.message}`);
    process.exit(1);
  });
}
