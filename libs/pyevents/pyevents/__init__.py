"""
pyevents — the shared *transport* layer. Not shared domain logic.

What lives here: how to open a connection, how to publish to an exchange, how to
run a consume loop. Plumbing that every service does identically.

What does NOT live here: the events themselves. Those are defined in /contracts
as JSON Schema and each service owns its own reading of them. If PaymentCaptured
were a Python class imported from here, payment-service and shipping-service
would be coupled at import time — a shared library is exactly the trap that makes
"decoupled" services silently un-decoupled.
"""

from .connection import connect, BROKER_URL, Channel
from .publisher import publish, Publisher, set_duplicate_mode, duplicate_mode
from .consumer import consume, Message
from .control import listen_for_commands, publish_command
from .db import connect_db, ensure_processed_events, process_once, run_script, DB_URL

__all__ = [
    "connect",
    "BROKER_URL",
    "Channel",
    "publish",
    "Publisher",
    "set_duplicate_mode",
    "duplicate_mode",
    "consume",
    "Message",
    "listen_for_commands",
    "publish_command",
    "connect_db",
    "ensure_processed_events",
    "process_once",
    "run_script",
    "DB_URL",
]
