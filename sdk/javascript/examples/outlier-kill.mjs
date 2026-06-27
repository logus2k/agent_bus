// Watch a workflow's live iteration count and kill it if it runs away.
//   ECHO_LOOP=true docker compose up -d agent-bus-app
//   node examples/outlier-kill.mjs
import { AgentBusClient } from "../src/agent-bus-client.js";

const GATEWAY = "http://127.0.0.1:6815";
const STEP_BUDGET = 20;

const client = new AgentBusClient(GATEWAY);
await client.connect();

const wf = await client.start("loop forever please");
console.log("workflow:", wf.cid, "| budget:", STEP_BUDGET);

for await (const ev of wf) {
  console.log(`  sid=${String(ev.sid).padStart(3)}  ${ev.type}`);
  if (ev.sid >= STEP_BUDGET && !ev.isTerminal) {
    console.log("  budget exceeded:", await wf.status(), "-> terminating");
    await wf.terminate();
  }
}

console.log("final status:", await wf.status());
client.disconnect();
