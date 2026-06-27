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

/**
 * A live subscription to a stream — async-iterable over EVERY event published to
 * it (observer semantics; events you did not produce). Unlike a Workflow (one
 * cid, terminates), it spans all cids on the stream and runs until you
 * `unsubscribe()` or the client disconnects.
 *
 *   const sub = await client.subscribe("some-stream-id");
 *   for await (const ev of sub) console.log(ev.cid, ev.type, ev.data);
 *   await sub.unsubscribe();
 *
 * Observer semantics — every subscriber sees every event; NOT consumer-group
 * work distribution (that is a server-side concern, not available in the browser).
 */
export class Subscription {
  constructor(client, streamId) {
    this.client = client;
    this.streamId = streamId;
    this._queue = [];
    this._waiters = [];
    this._closed = false;
  }
  _feed(ev) {
    this._push(ev);
  }
  _close() {
    if (!this._closed) {
      this._closed = true;
      this._push(null); // sentinel
    }
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
        if (this._closed) return Promise.resolve({ value: undefined, done: true });
        return new Promise((resolve) => this._waiters.push((item) => resolve(take(item))));
      },
    };
  }
  /** Stop the subscription (server-side observer too) and end iteration. */
  unsubscribe() {
    return this.client.unsubscribe(this.streamId);
  }
}

/** The connection + thin protocol core. */
export class AgentBusClient {
  constructor(url, options = {}) {
    this.url = url;
    this._options = options;
    this._socket = null;
    this._workflows = new Map();
    this._subscriptions = new Map();
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
        for (const sub of this._subscriptions.values()) sub._close();
      });
    });
  }

  disconnect() {
    if (this._socket) this._socket.disconnect();
  }

  _dispatch(env) {
    const ev = new BusEvent(env);
    // Route by source stream to a Subscription, and/or by cid to a Workflow.
    const sub = this._subscriptions.get(ev.streamId);
    if (sub) sub._feed(ev);
    const wf = this._workflows.get(ev.cid);
    if (wf) {
      wf._feed(ev);
    } else if (!sub) {
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

  /** Publish an event to ANY stream (general producer). Returns {cid, sid, entry_id}. */
  async publish(streamId, eventType, data = {}, { cid = null } = {}) {
    const payload = { stream_id: streamId, event_type: eventType, data };
    if (cid) payload.cid = cid;
    return this._call("publish", payload);
  }

  /** Subscribe to ANY stream — receive every event published to it as a
   * Subscription you `for await` over. (Observer semantics; not a consumer group.) */
  async subscribe(streamId) {
    const sub = new Subscription(this, streamId);
    this._subscriptions.set(streamId, sub); // register before the server emits
    await this._call("subscribe", { stream_id: streamId });
    return sub;
  }

  async unsubscribe(streamId) {
    const sub = this._subscriptions.get(streamId);
    this._subscriptions.delete(streamId);
    try {
      await this._call("unsubscribe", { stream_id: streamId });
    } finally {
      if (sub) sub._close();
    }
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
