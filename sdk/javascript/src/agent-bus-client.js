// Agent Bus — JavaScript (ES6) client SDK.
//
// High-level client for the Agent Bus Socket.IO gateway. Start workflows and
// `for await` their events without hand-rolling Socket.IO or cid-correlation.
//
// Wraps the *bus* protocol (envelopes + commands); unrelated to the
// agent_server SDK (the LLM brain). Wire reference: ../../documents/client_sdk.md
//
// Browser (no bundler): map the bare import with an import map ->
//   <script type="importmap">{ "imports": {
//     "socket.io-client": "https://cdn.socket.io/4.7.5/socket.io.esm.min.js" }}</script>
// Node / bundlers: `npm i socket.io-client` resolves it normally.

import { io } from "socket.io-client";

const WORKFLOW_TERMINATED = "workflow.terminated";

/** A friendly view over one event envelope (see client_sdk.md §3). */
export class BusEvent {
  constructor(env) {
    this.raw = env;
    const h = env.header || {};
    this.streamId = h.stream_id;
    this.cid = h.cid;
    this.sid = h.sid ?? 0;
    this.type = h.event_type;
    this.sender = h.sender;
    this.timestamp = h.timestamp;
    this.data = (env.payload && env.payload.data) || {};
  }
  get isTerminal() {
    return this.type === WORKFLOW_TERMINATED;
  }
}

/**
 * One workflow (`cid`). Async-iterable; yields its events until it ends.
 *
 *   const wf = await client.start("summarize X");
 *   for await (const ev of wf) {        // only THIS workflow's events
 *     if (ev.sid > 500) await wf.terminate();
 *   }
 *   await wf.completed;                 // true=terminated, false=disconnected
 *
 * Iteration ends after the terminal event, on disconnect, or — if `idleTimeout`
 * (seconds) is set — after that long without events (a workflow that ended by
 * "going quiet" emits no terminal event).
 */
export class Workflow {
  constructor(client, cid, { idleTimeout = null } = {}) {
    this.client = client;
    this.cid = cid;
    this.idleTimeout = idleTimeout;
    this.sid = 0; // latest step seen = live iteration count
    this._queue = []; // buffered items (BusEvent or null sentinel)
    this._waiters = []; // pending next() resolvers
    this._ended = false;
    this.completed = new Promise((resolve) => {
      this._resolveCompleted = resolve;
    });
  }

  _feed(ev) {
    this.sid = Math.max(this.sid, ev.sid);
    this._push(ev);
    if (ev.isTerminal) this._end(true);
  }
  _disconnected() {
    this._end(false);
  }
  _end(terminated) {
    if (this._ended) return;
    this._ended = true;
    this._resolveCompleted(terminated);
    this._push(null); // sentinel
  }
  _push(item) {
    const waiter = this._waiters.shift();
    if (waiter) waiter(item);
    else this._queue.push(item);
  }

  [Symbol.asyncIterator]() {
    const take = (item) =>
      item === null ? { value: undefined, done: true } : { value: item, done: false };
    return {
      next: () => {
        if (this._queue.length) return Promise.resolve(take(this._queue.shift()));
        if (this._ended) return Promise.resolve({ value: undefined, done: true });
        return new Promise((resolve) => {
          let timer = null;
          const waiter = (item) => {
            if (timer) clearTimeout(timer);
            resolve(take(item));
          };
          this._waiters.push(waiter);
          if (this.idleTimeout != null) {
            timer = setTimeout(() => {
              const i = this._waiters.indexOf(waiter);
              if (i >= 0) this._waiters.splice(i, 1);
              if (!this._ended) {
                this._ended = true;
                this._resolveCompleted(false);
              }
              resolve({ value: undefined, done: true });
            }, this.idleTimeout * 1000);
          }
        });
      },
    };
  }

  /** Eliminate this workflow (the outlier kill switch). */
  terminate() {
    return this.client.terminate(this.cid);
  }
  /** Snapshot this workflow's live step count + state. */
  status() {
    return this.client.status(this.cid);
  }
  /** Drain to completion and return all events. */
  async collect() {
    const out = [];
    for await (const ev of this) out.push(ev);
    return out;
  }
}

/** The connection + thin protocol core. */
export class AgentBusClient {
  constructor(url, options = {}) {
    this.url = url;
    this._options = options;
    this._socket = null;
    this._workflows = new Map();
    this._orphans = new Map(); // events that raced ahead of start()
    this.streamId = null;
  }

  connect(timeout = 10000) {
    return new Promise((resolve, reject) => {
      this._socket = io(this.url, { ...this._options });
      const timer = setTimeout(() => reject(new Error("connect timeout")), timeout);
      this._socket.on("connected", (data) => {
        this.streamId = data && data.stream_id;
        clearTimeout(timer);
        resolve(this);
      });
      this._socket.on("connect_error", (err) => {
        clearTimeout(timer);
        reject(err);
      });
      this._socket.on("event", (env) => this._dispatch(env));
      this._socket.on("disconnect", () => {
        for (const wf of this._workflows.values()) wf._disconnected();
      });
    });
  }

  disconnect() {
    if (this._socket) this._socket.disconnect();
  }

  _dispatch(env) {
    const ev = new BusEvent(env);
    const wf = this._workflows.get(ev.cid);
    if (wf) {
      wf._feed(ev);
    } else {
      if (!this._orphans.has(ev.cid)) this._orphans.set(ev.cid, []);
      this._orphans.get(ev.cid).push(ev);
    }
  }

  _call(event, payload, timeout = 10000) {
    return new Promise((resolve, reject) => {
      this._socket.timeout(timeout).emit(event, payload, (err, ack) => {
        if (err) reject(err);
        else resolve(ack);
      });
    });
  }

  /** Start a workflow; returns a Workflow bound to its cid. */
  async start(text, { idleTimeout = null } = {}) {
    const ack = await this._call("request", { text });
    const cid = ack.cid;
    const wf = new Workflow(this, cid, { idleTimeout });
    this._workflows.set(cid, wf);
    const buffered = this._orphans.get(cid) || [];
    this._orphans.delete(cid);
    for (const ev of buffered) wf._feed(ev);
    return wf;
  }

  // Low-level passthroughs (by cid).
  async request(text) {
    return (await this._call("request", { text })).cid;
  }
  status(cid) {
    return this._call("status", { cid });
  }
  terminate(cid) {
    return this._call("terminate", { cid });
  }
}
