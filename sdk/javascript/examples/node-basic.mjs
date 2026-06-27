// Start a workflow and print its events as they stream back.
//   node examples/node-basic.mjs
import { AgentBusClient } from "../src/agent-bus-client.js";

const GATEWAY = "http://127.0.0.1:6815";

const client = new AgentBusClient(GATEWAY);
await client.connect();
console.log("connected, stream:", client.streamId);

const wf = await client.start("summarize the onboarding doc");
console.log("workflow:", wf.cid);

for await (const ev of wf) {
  console.log(`  sid=${String(ev.sid).padStart(2)}  ${ev.type}`, ev.data);
}

const ended = await wf.completed;
console.log("done:", ended, "after", wf.sid, "steps");
client.disconnect();
