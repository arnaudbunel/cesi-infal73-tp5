// Client minimal de l'API HTTP RabbitMQ Management (vhost « / »). Aucune dépendance, aucune publication AMQP.
import { lab } from './lab-config.mjs';

const auth = 'Basic ' + Buffer.from(`${lab.rabbitUser}:${lab.rabbitPassword}`).toString('base64');
const VHOST = '%2F';

export async function mgmt(method, path, body) {
  const response = await fetch(`${lab.mgmtUrl}/api${path}`, {
    method,
    headers: { authorization: auth, ...(body === undefined ? {} : { 'content-type': 'application/json' }) },
    body: body === undefined ? undefined : JSON.stringify(body),
    signal: AbortSignal.timeout(5000),
  });
  const text = await response.text();
  if (!response.ok) throw new Error(`Management ${method} ${path} -> ${response.status} ${text.slice(0, 200)}`);
  return text ? JSON.parse(text) : null;
}

export async function mgmtReachable() {
  try {
    await mgmt('GET', '/overview');
    return true;
  } catch {
    return false;
  }
}

export const queueName = (name) => encodeURIComponent(name);

/** Compteurs d'une queue : ready, unacked, consumers (rafraîchis toutes les 500 ms avec la config du laboratoire). */
export async function queueStats(name) {
  const q = await mgmt('GET', `/queues/${VHOST}/${queueName(name)}`);
  return { ready: q.messages_ready ?? 0, unacked: q.messages_unacknowledged ?? 0, consumers: q.consumers ?? 0 };
}

export async function purgeQueue(name) {
  await mgmt('DELETE', `/queues/${VHOST}/${queueName(name)}/contents`);
}

/**
 * Injecte un message via l'API Management (sans client AMQP) : sert aux scénarios de panne.
 * Le corps est envoyé tel quel (`payload_encoding: string`), ce qui permet un JSON invalide.
 * Retourne `routed` (true si au moins une queue a reçu le message).
 */
export async function injectMessage(exchange, routingKey, payload, properties = {}) {
  const result = await mgmt('POST', `/exchanges/${VHOST}/${encodeURIComponent(exchange)}/publish`, {
    properties: { delivery_mode: 2, content_type: 'application/json', ...properties },
    routing_key: routingKey,
    payload: typeof payload === 'string' ? payload : JSON.stringify(payload),
    payload_encoding: 'string',
  });
  return result.routed === true;
}

/** Lit (sans consommer : ack_requeue_true) jusqu'à `count` messages d'une queue. */
export async function peekMessages(name, count = 5) {
  return mgmt('POST', `/queues/${VHOST}/${queueName(name)}/get`, { count, ackmode: 'ack_requeue_true', encoding: 'auto' });
}

/** Compteurs cumulés d'un exchange (publish_in = messages publiés vers lui, publish_out = copies routées). Rafraîchis toutes les 500 ms. */
export async function exchangeStats(name) {
  const e = await mgmt('GET', `/exchanges/${VHOST}/${encodeURIComponent(name)}`);
  return { publishIn: e.message_stats?.publish_in ?? 0, publishOut: e.message_stats?.publish_out ?? 0 };
}
