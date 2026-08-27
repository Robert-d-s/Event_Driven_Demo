"""
observer's HTTP + WebSocket surface.

  GET  /health          liveness
  GET  /queues          current queue depths, via RabbitMQ management API
  WS   /ws              live feed: every message seen on the bus

The consumer thread calls broadcast() for each message; connected WebSocket
clients receive it. A ring buffer of recent events is replayed to a client when
it first connects, so a freshly-opened dashboard isn't blank.
"""

import asyncio
import collections
import os

import httpx
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from pydantic import BaseModel

from pyevents import publish_command

app = FastAPI(title="observer")

MGMT_URL = os.environ.get("RABBITMQ_MGMT_URL", "http://rabbitmq:15672")
MGMT_AUTH = (
    os.environ.get("RABBITMQ_USER", "guest"),
    os.environ.get("RABBITMQ_PASS", "guest"),
)

_recent: collections.deque = collections.deque(maxlen=100)
_clients: set[WebSocket] = set()
_loop: asyncio.AbstractEventLoop | None = None


@app.on_event("startup")
async def _capture_loop() -> None:
    # The consumer thread needs a handle to this loop to schedule broadcasts.
    global _loop
    _loop = asyncio.get_running_loop()


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "clients": len(_clients)}


class Command(BaseModel):
    target: str = "all"  # "payment" | "inventory" | "shipping" | "all"
    action: str  # "fail" | "slow_ms" | "duplicate" | "pause_relay"
    value: bool | int


@app.post("/control")
def control(cmd: Command) -> dict:
    """
    Fan a failure-lab command out to every service via the control exchange.
    The dashboard's "Break something" buttons POST here.
    """
    publish_command(cmd.model_dump())
    print(f"[observer] control command: {cmd.model_dump()}", flush=True)
    return {"sent": cmd.model_dump()}


@app.get("/stats")
def stats() -> dict:
    """Cross-service consistency snapshot — see app/stats.py."""
    from .stats import snapshot

    return snapshot()


class KillRequest(BaseModel):
    service: str  # e.g. "order-service", "payment-service", "inventory-service"


# Only these can be targeted, so a stray request can't take down the broker/db.
_KILLABLE = {
    "order-service",
    "payment-service",
    "inventory-service",
    "shipping-service",
}


@app.post("/kill")
def kill(req: KillRequest) -> dict:
    """
    SIGKILL a service's container(s) then start them again — the dashboard's way
    of triggering "process dies mid-flight" without a terminal. Uses the Docker
    Engine API over the socket mounted at /var/run/docker.sock (see compose).
    """
    if req.service not in _KILLABLE:
        return {"error": f"not killable: {req.service}", "allowed": sorted(_KILLABLE)}

    import docker  # docker SDK; imported here so observer starts even without it
    from docker.errors import APIError

    project = os.environ.get("COMPOSE_PROJECT_NAME", "event_driven_demo")
    try:
        client = docker.from_env()
        containers = client.containers.list(
            all=True,
            filters={
                "label": [
                    f"com.docker.compose.project={project}",
                    f"com.docker.compose.service={req.service}",
                ]
            },
        )
        for c in containers:
            try:
                c.kill(signal="SIGKILL")
            except APIError:
                pass  # already stopped
        for c in containers:
            c.start()
        print(
            f"[observer] killed + restarted {req.service} "
            f"({len(containers)} container(s))",
            flush=True,
        )
        return {"killed": req.service, "containers": len(containers)}
    except Exception as err:  # noqa: BLE001
        return {"error": str(err)}


@app.get("/queues")
async def queues() -> list[dict]:
    """Queue depths straight from the broker's management API."""
    async with httpx.AsyncClient(auth=MGMT_AUTH, timeout=5) as client:
        resp = await client.get(f"{MGMT_URL}/api/queues/%2F")
        resp.raise_for_status()
        return [
            {
                "name": q["name"],
                "ready": q.get("messages_ready", 0),
                "unacked": q.get("messages_unacknowledged", 0),
                "consumers": q.get("consumers", 0),
            }
            for q in resp.json()
        ]


@app.websocket("/ws")
async def ws(sock: WebSocket) -> None:
    await sock.accept()
    _clients.add(sock)
    try:
        for item in list(_recent):  # replay history to a new client
            await sock.send_json(item)
        while True:
            await sock.receive_text()  # keepalive; we don't expect real input
    except WebSocketDisconnect:
        pass
    finally:
        _clients.discard(sock)


def broadcast(event: dict) -> None:
    """
    Called from the consumer thread. Hands the event to the asyncio loop, which
    fans it out to WebSocket clients. Thread -> loop handoff via
    run_coroutine_threadsafe, because pika and asyncio are different worlds.
    """
    _recent.append(event)
    if _loop is None:
        return

    async def _fan_out() -> None:
        dead = []
        for client in _clients:
            try:
                await client.send_json(event)
            except Exception:  # noqa: BLE001
                dead.append(client)
        for d in dead:
            _clients.discard(d)

    asyncio.run_coroutine_threadsafe(_fan_out(), _loop)
