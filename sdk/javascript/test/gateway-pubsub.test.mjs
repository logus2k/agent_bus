// Integration test: gateway publish/subscribe across two JS clients.
// Needs the running gateway (default http://127.0.0.1:6815) and socket.io-client
// installed (npm i). Run: node test/gateway-pubsub.test.mjs
import { AgentBusClient } from "../src/agent-bus-client.js";

const URL = process.env.AGENT_BUS_URL || "http://127.0.0.1:6815";
const checks = [];
const check = (n, c) => checks.push([n, !!c]);

const feed = "feed-js-" + Math.floor(performance.now()).toString(36);
const a = new AgentBusClient(URL);
const b = new AgentBusClient(URL);
await a.connect();
await b.connect();

const received = [];
const sub = await a.subscribe(feed);
(async () => { for await (const ev of sub) received.push(ev); })();
await new Promise((r) => setTimeout(r, 200));

for (let i = 0; i < 3; i++) {
  const ack = await b.publish(feed, "feed.item", { n: i });
  check(`publish ${i} acked`, ack && ack.ok && ack.entry_id);
}
for (let k = 0; k < 50 && received.length < 3; k++) await new Promise((r) => setTimeout(r, 50));
check("received all 3 published events", received.length === 3);
check("events carry publisher data", JSON.stringify(received.map((e) => e.data.n)) === "[0,1,2]");
check("events on subscribed stream", received.every((e) => e.streamId === feed));
check("sees events it did NOT produce", received.every((e) => e.sender !== a.streamId));

await sub.unsubscribe();
await new Promise((r) => setTimeout(r, 100));
const before = received.length;
await b.publish(feed, "feed.item", { n: 99 });
await new Promise((r) => setTimeout(r, 400));
check("no delivery after unsubscribe", received.length === before);

a.disconnect();
b.disconnect();

let ok = true;
for (const [n, p] of checks) { console.log(`  [${p ? "PASS" : "FAIL"}] ${n}`); ok = ok && p; }
console.log("\nRESULT:", ok ? "PASS" : "FAIL");
process.exit(ok ? 0 : 1);
