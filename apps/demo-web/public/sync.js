// Resynchronisation d'une commande affichée : instantané REST + événements WebSocket.
// Module sans dépendance au DOM : testé automatiquement.
//
// Invariants :
//  - la sequence affichée ne régresse JAMAIS (REST seq=42 puis WS seq=41 : on garde 42) ;
//  - un seul resync() en vol (Promise partagée) ;
//  - les événements reçus pendant le GET REST sont tamponnés, puis rejoués dans l'ordre ;
//  - un trou de sequence (contiguës par commande) déclenche une nouvelle lecture REST, jamais une
//    application « au hasard » ; le nombre de relectures consécutives dues à des trous est borné ;
//  - après reset() (changement d'abonnement, déconnexion), les réponses des anciennes opérations sont ignorées.

/**
 * @typedef {{ orderId: string, sequence: number, status: string }} OrderEvent
 * @typedef {{ status: string, sequence: number }} OrderSnapshot
 * @typedef {{
 *   fetchOrder: () => Promise<OrderSnapshot>,
 *   onChange: (state: OrderSnapshot) => void,
 *   log?: (line: string) => void,
 *   maxGapResyncs?: number,
 * }} SyncOptions
 */

export class OrderSynchronizer {
  /** @param {SyncOptions} options */
  constructor({ fetchOrder, onChange, log = () => {}, maxGapResyncs = 3 }) {
    this.fetchOrder = fetchOrder;
    this.onChange = onChange;
    this.log = log;
    this.maxGapResyncs = maxGapResyncs;
    /** Dernier état affiché (null tant qu'aucun instantané REST n'a été appliqué). @type {OrderSnapshot | null} */
    this.state = null;
    /** @type {Promise<void> | null} */
    this.inflight = null;
    /** @type {OrderEvent[]} */
    this.buffer = [];
    this.epoch = 0;
    this.gapResyncs = 0;
  }

  /** Abandonne toute opération en cours : leurs résultats seront ignorés. L'état affiché est conservé. */
  reset() {
    this.epoch += 1;
    this.inflight = null;
    this.buffer = [];
    this.gapResyncs = 0;
  }

  /** @param {OrderSnapshot} next */
  #show(next) {
    this.state = { status: next.status, sequence: next.sequence };
    this.onChange(this.state);
  }

  /**
   * Lit l'état REST et l'affiche, en tamponnant les événements reçus pendant la lecture.
   * Si une synchronisation est déjà en cours, la même Promise est retournée (aucune lecture parallèle).
   * @returns {Promise<void>}
   */
  resync() {
    if (this.inflight) return this.inflight;
    const epoch = this.epoch;
    this.buffer = [];
    const run = (async () => {
      let snapshot;
      try {
        snapshot = await this.fetchOrder();
      } finally {
        if (this.epoch === epoch) this.inflight = null;
      }
      if (this.epoch !== epoch) return; // abonnement changé / connexion fermée entre-temps
      // Une réponse plus ancienne que l'état déjà affiché est ignorée (jamais de régression).
      if (!this.state || snapshot.sequence > this.state.sequence) {
        this.#show(snapshot);
        this.log(`REST: ${snapshot.status} seq=${snapshot.sequence}`);
      } else {
        this.log(`REST ignoré (seq=${snapshot.sequence} <= affichée ${this.state.sequence})`);
      }
      const buffered = this.buffer.sort((a, b) => a.sequence - b.sequence);
      this.buffer = [];
      for (const event of buffered) this.#apply(event);
    })();
    this.inflight = run;
    return run;
  }

  /**
   * À appeler pour chaque ORDER_STATUS_CHANGED reçu.
   * @param {OrderEvent} event
   */
  handleEvent(event) {
    if (this.inflight) {
      this.buffer.push(event);
      return;
    }
    this.#apply(event);
  }

  /** @param {OrderEvent} event */
  #apply(event) {
    if (!this.state) {
      void this.resync().catch(() => {});
      return;
    }
    if (event.sequence <= this.state.sequence) {
      this.log(`événement périmé ignoré seq=${event.sequence}`);
      return;
    }
    if (event.sequence !== this.state.sequence + 1) {
      if (this.gapResyncs >= this.maxGapResyncs) {
        this.log(`trou de sequence persistant (${this.state.sequence} -> ${event.sequence}) : relecture manuelle requise`);
        return;
      }
      this.gapResyncs += 1;
      this.log(`trou de sequence (${this.state.sequence} -> ${event.sequence}) : relecture REST`);
      void this.resync().catch(() => {});
      return;
    }
    this.gapResyncs = 0;
    this.#show({ status: event.status, sequence: event.sequence });
  }
}
