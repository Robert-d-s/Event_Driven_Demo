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
from .publisher import publish, Publisher
from .consumer import consume, Message
from .control import listen_for_commands, publish_command

__all__ = [
    "connect",
    "BROKER_URL",
    "Channel",
    "publish",
    "Publisher",
    "consume",
    "Message",
    "listen_for_commands",
    "publish_command",
]
