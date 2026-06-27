"""Start a workflow and print its events as they stream back."""
import asyncio

from agent_bus_client import AgentBusClient

GATEWAY = "http://127.0.0.1:6815"


async def main() -> None:
    async with AgentBusClient(GATEWAY) as client:
        print("connected, stream:", client.stream_id)

        wf = await client.start("summarize the onboarding doc")
        print("workflow:", wf.cid)

        async for ev in wf:                 # only this workflow's events
            print(f"  sid={ev.sid:>2}  {ev.type:<20} {ev.data}")

        ended = await wf.completed          # True = terminated, False = disconnected
        print("done:", ended, "after", wf.sid, "steps")


if __name__ == "__main__":
    asyncio.run(main())
