"""Watch a workflow's live iteration count and kill it if it runs away.

Run the gateway in loop mode to see this fire:
    ECHO_LOOP=true docker compose up -d agent-bus-app
"""
import asyncio

from agent_bus_client import AgentBusClient

GATEWAY = "http://127.0.0.1:6815"
STEP_BUDGET = 20


async def main() -> None:
    async with AgentBusClient(GATEWAY) as client:
        wf = await client.start("loop forever please")
        print("workflow:", wf.cid, "| budget:", STEP_BUDGET)

        async for ev in wf:
            print(f"  sid={ev.sid:>3}  {ev.type}")
            if ev.sid >= STEP_BUDGET and ev.type != "workflow.terminated":
                snap = await wf.status()
                print("  budget exceeded:", snap, "-> terminating")
                await wf.terminate()

        print("final status:", await wf.status())


if __name__ == "__main__":
    asyncio.run(main())
