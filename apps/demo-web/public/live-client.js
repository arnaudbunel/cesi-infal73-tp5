// Client « temps réel » d'une commande : WebSocket + resynchronisation REST + reconnexion à délai borné.
// Sans dépendance au DOM : la socket, le fetch et les timers sont injectés (donc testable avec le client `ws`).
import { OrderSynchronizer } from './sync.js';

/**
 * États : 'idle' | 'connecting' | 'open' | 'subscribed' | 'waiting' (reconnexion planifiée)
 *         | 'forbidden' | 'unauthenticated' | 'failed' | 'stopped'
 *
 * @typedef {{
 *   orderId: string,
 *   createSocket: () => any,
 *   fetchOrder: () => Promise<{ status: string, sequence: number }>,
 *   onChange: (state: { status: string, sequence: number }) => void,
 *   onState?: (state: string) => void,
 *   log?: (line: string) => void,
 *   checkSession?: () => Promise<boolean>,
 *   setTimer?: (fn: () => void, ms: number) => any,
 *   clearTimer?: (handle: any) => void,
 *   baseDelayMs?: number,
 *   maxDelayMs?: number,
 *   maxAttempts?: number,
 * }} LiveOptions
 */
export class OrderLiveClient {
  /** @param {LiveOptions} options */
  constructor(options) {
    this.orderId = options.orderId;
    this.createSocket = options.createSocket;
    this.onState = options.onState ?? (() => {});
    this.log = options.log ?? (() => {});
    this.checkSession = options.checkSession ?? (async () => true);
    this.setTimer = options.setTimer ?? ((fn, ms) => setTimeout(fn, ms));
    this.clearTimer = options.clearTimer ?? ((handle) => clearTimeout(handle));
    this.baseDelayMs = options.baseDelayMs ?? 250;
    this.maxDelayMs = options.maxDelayMs ?? 5000;
    this.maxAttempts = options.maxAttempts ?? 8;
    this.sync = new OrderSynchronizer({
      fetchOrder: async () => {
        try {
          return await options.fetchOrder();
        } catch (error) {
          if (error && error.status === 401) this.#halt('unauthenticated');
          throw error;
        }
      },
      onChange: options.onChange,
      log: this.log,
    });
    this.socket = null;
    this.timer = null;
    this.attempt = 0;
    this.intentionalStop = true;
    this.state = 'idle';
    /** Génération de connexion : les callbacks d'une ancienne socket sont ignorés. */
    this.generation = 0;
  }

  /** Délai de reconnexion : exponentiel, borné (250, 500, 1000, 2000, 4000, 5000, 5000...). */
  nextDelay(attempt = this.attempt) {
    return Math.min(this.maxDelayMs, this.baseDelayMs * 2 ** attempt);
  }

  #setState(state) {
    this.state = state;
    this.onState(state);
  }

  start() {
    this.intentionalStop = false;
    this.attempt = 0;
    this.#open();
  }

  /** Fermeture volontaire : plus aucune reconnexion, timer annulé, réponses en vol ignorées. */
  stop() {
    this.intentionalStop = true;
    this.#cancelTimer();
    this.generation += 1;
    this.sync.reset();
    const socket = this.socket;
    this.socket = null;
    if (socket) {
      try {
        socket.close();
      } catch {
        /* déjà fermée */
      }
    }
    this.#setState('stopped');
  }

  /** Demande un nouvel abonnement sur la socket ouverte (les droits sont revérifiés par le serveur). */
  subscribe() {
    if (this.socket && this.socket.readyState === 1) {
      const message = JSON.stringify({ type: 'SUBSCRIBE', orderId: this.orderId });
      this.log(`→ ${message}`);
      this.socket.send(message);
    }
  }

  #cancelTimer() {
    if (this.timer !== null) {
      this.clearTimer(this.timer);
      this.timer = null;
    }
  }

  #halt(state) {
    this.intentionalStop = true;
    this.#cancelTimer();
    this.#setState(state);
  }

  #open() {
    this.#cancelTimer();
    // Une seule socket vivante : toute ancienne socket est détachée avant d'en ouvrir une nouvelle.
    const previous = this.socket;
    this.generation += 1;
    const generation = this.generation;
    this.sync.reset();
    if (previous) {
      try {
        previous.close();
      } catch {
        /* ignore */
      }
    }
    this.#setState('connecting');
    const socket = this.createSocket();
    this.socket = socket;
    // Listeners installés immédiatement, avant toute attente.
    socket.addEventListener('open', () => {
      if (generation !== this.generation) return;
      this.#setState('open');
      this.subscribe();
    });
    socket.addEventListener('message', (event) => {
      if (generation !== this.generation) return;
      this.#onMessage(event.data);
    });
    socket.addEventListener('close', () => {
      if (generation !== this.generation) return; // socket remplacée ou arrêtée volontairement
      this.socket = null;
      this.#scheduleReconnect();
    });
    socket.addEventListener('error', () => {
      /* `close` suit toujours une erreur de connexion */
    });
  }

  #onMessage(raw) {
    this.log(`← ${String(raw)}`);
    let message;
    try {
      message = JSON.parse(String(raw));
    } catch {
      return;
    }
    if (message.type === 'SUBSCRIBED' && message.orderId === this.orderId) {
      this.attempt = 0;
      this.#setState('subscribed');
      // Ordre sûr : SUBSCRIBED d'abord (les événements arrivent désormais), PUIS lecture REST ; les
      // événements reçus pendant la lecture sont tamponnés par le synchroniseur.
      this.sync.resync().catch((error) => this.log(`erreur de synchronisation: ${error?.message ?? error}`));
    } else if (message.type === 'ORDER_STATUS_CHANGED' && message.orderId === this.orderId) {
      this.sync.handleEvent(message);
    } else if (message.type === 'ERROR' && message.code === 'FORBIDDEN_SUBSCRIPTION') {
      this.#halt('forbidden'); // inutile de reboucler : le refus ne changera pas
    }
  }

  async #scheduleReconnect() {
    if (this.intentionalStop || this.timer !== null) return; // un seul timer à la fois (doubles fermetures)
    if (this.attempt >= this.maxAttempts) {
      this.#setState('failed');
      return;
    }
    // La session est-elle encore valide ? Sinon se reconnecter en boucle est inutile.
    const generation = this.generation;
    let valid = true;
    try {
      valid = await this.checkSession();
    } catch {
      valid = true; // réseau indisponible : on retente
    }
    if (generation !== this.generation || this.intentionalStop || this.timer !== null) return;
    if (!valid) return this.#halt('unauthenticated');
    const delay = this.nextDelay();
    this.attempt += 1;
    this.#setState('waiting');
    this.log(`reconnexion dans ${delay} ms (tentative ${this.attempt})`);
    this.timer = this.setTimer(() => {
      this.timer = null;
      if (!this.intentionalStop) this.#open();
    }, delay);
  }
}
