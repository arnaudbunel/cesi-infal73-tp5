import { OrderLiveClient } from '/demo/live-client.js';

const $ = (id) => document.getElementById(id);
const logEl = $('log');
const log = (line) => {
  logEl.textContent += `${new Date().toLocaleTimeString()} ${line}\n`;
  logEl.scrollTop = logEl.scrollHeight;
};
const orderId = () => $('orderId').value.trim();

const STATE_LABELS = {
  idle: 'fermée', connecting: 'connexion...', open: 'ouverte', subscribed: 'ouverte (abonnée)',
  waiting: 'coupée, reconnexion planifiée', forbidden: 'abonnement refusé', unauthenticated: 'session invalide',
  failed: 'abandon après plusieurs tentatives', stopped: 'fermée (volontaire)',
};

let live = null;

/** Lecture REST authentifiée (façade réservée au propriétaire de la commande). */
async function fetchOrder(id) {
  const res = await fetch(`/api/demo/orders/${encodeURIComponent(id)}`);
  if (!res.ok) throw Object.assign(new Error(`REST ${res.status}`), { status: res.status });
  const order = await res.json();
  return { status: order.status, sequence: order.sequence };
}

function newClient() {
  live?.stop();
  const id = orderId();
  live = new OrderLiveClient({
    orderId: id,
    createSocket: () => new WebSocket(`${location.protocol === 'https:' ? 'wss' : 'ws'}://${location.host}/ws/orders`),
    fetchOrder: () => fetchOrder(id),
    onChange: (state) => {
      $('status').textContent = state.status;
      $('sequence').textContent = String(state.sequence);
    },
    onState: (state) => {
      $('socketState').textContent = STATE_LABELS[state] ?? state;
      $('subState').textContent = state === 'subscribed' ? `abonné à ${id}` : state === 'forbidden' ? 'refusé (FORBIDDEN_SUBSCRIPTION)' : 'aucun';
    },
    checkSession: async () => (await fetch('/api/demo/me')).ok,
    log,
  });
  return live;
}

const guarded = (fn) => async () => {
  try {
    await fn();
  } catch (error) {
    log(`erreur: ${error.message}`);
  }
};

$('login').addEventListener('click', guarded(async () => {
  const user = $('user').value;
  const res = await fetch('/api/demo/login', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ user }),
  });
  if (!res.ok) throw new Error(`login ${res.status}`);
  $('session').textContent = `session : ${user}`;
  log(`session ouverte pour ${user}`);
}));
$('connect').addEventListener('click', () => (newClient().start(), log('connexion + abonnement automatique')));
$('subscribe').addEventListener('click', () => (live ?? newClient()).subscribe());
$('disconnect').addEventListener('click', () => {
  live?.stop();
  log('déconnexion volontaire : aucune reconnexion automatique');
});
$('reconnect').addEventListener('click', () => (newClient().start(), log('reconnexion manuelle')));
$('drop').addEventListener('click', () => {
  // Coupure « réseau » simulée : fermeture non volontaire côté client -> la reconnexion automatique s'applique.
  live?.socket?.close();
  log('coupure simulée (reconnexion automatique à délai borné)');
});
$('refresh').addEventListener('click', guarded(() => (live ?? newClient()).sync.resync()));

async function callTransition(request) {
  const res = await request;
  const body = await res.json();
  log(`REST ${res.status}: ${body.status ?? body.title} ${body.sequence !== undefined ? `seq=${body.sequence}` : ''}`);
}
for (const button of document.querySelectorAll('.transition')) {
  button.addEventListener('click', guarded(() =>
    callTransition(fetch(`/api/orders/${encodeURIComponent(orderId())}`, {
      method: 'PATCH',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ status: button.dataset.status }),
    })),
  ));
}
$('cancel').addEventListener('click', guarded(() =>
  callTransition(fetch(`/api/orders/${encodeURIComponent(orderId())}/cancellations`, { method: 'POST' })),
));

fetch('/api/demo/me').then(async (res) => {
  if (res.ok) $('session').textContent = `session : ${(await res.json()).user}`;
});
log('interface prête : 1) session  2) connexion WS (abonnement automatique)');
