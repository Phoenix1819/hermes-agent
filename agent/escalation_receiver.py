"""A2A escalation receiver for Phoenix.

Subscribes to hermes/escalate/+ on the local MQTT broker and routes
escalations into the unified notification bus.

Usage (inside Phoenix gateway process):
    from agent.escalation_receiver import start_escalation_receiver
    start_escalation_receiver()
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Optional

try:
    import paho.mqtt.client as mqtt
    _HAS_PAHO = True
except Exception:
    _HAS_PAHO = False

logger = logging.getLogger(__name__)

BROKER = "127.0.0.1"
PORT = 1883
KEEPALIVE = 15
TOPIC_PATTERN = "hermes/escalate/+"

HERMES_HOME = Path(os.environ.get("HERMES_HOME", Path.home() / ".hermes"))

_receiver_thread: Optional[threading.Thread] = None
_client: Optional[Any] = None
_shutdown_event = threading.Event()

# Callback that Phoenix registers to handle escalations.
# Signature: handle_escalation(payload: dict) -> None
_on_escalation: Optional[Callable[[Dict[str, Any]], None]] = None


def _default_handle_escalation(payload: Dict[str, Any]) -> None:
    """Default handler — inserts escalation into the notification bus.

    Phoenix's notification consumer picks it up, creates a Kanban ticket,
    and messages the user if triage says so.
    """
    logger.info(
        "ESCALATION from %s [%s/%s]: %s",
        payload.get("agent", "?"),
        payload.get("severity", "?"),
        payload.get("category", "?"),
        payload.get("summary", "?"),
    )
    try:
        import notification_bus as bus
        priority = _severity_to_priority(payload.get("severity", "medium"))
        bus.insert_message(
            source="a2a",
            topic=f"escalation/{payload.get('agent', 'unknown')}",
            payload=payload,
            priority=priority,
        )
    except Exception:
        logger.exception("Failed to insert escalation into notification bus")


def _severity_to_priority(severity: str) -> int:
    """Map escalation severity to bus priority (lower = more urgent)."""
    mapping = {
        "critical": 1,
        "high": 2,
        "medium": 3,
        "low": 5,
    }
    return mapping.get(severity.lower(), 3)


def _on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        logger.info("Escalation receiver connected to MQTT broker")
        client.subscribe(TOPIC_PATTERN, qos=1)
    else:
        logger.error("Escalation receiver connect failed, rc=%s", rc)


def _on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode("utf-8"))
        logger.info(
            "A2A escalation from %s: %s",
            payload.get("agent", "?"),
            payload.get("summary", "?"),
        )
        handler = _on_escalation or _default_handle_escalation
        try:
            handler(payload)
        except Exception as exc:
            logger.exception("A2A handler failed: %s", exc)
    except Exception as exc:
        logger.exception("Failed to handle A2A message on %s: %s", msg.topic, exc)


def _on_disconnect(client, userdata, disconnect_flags, rc, properties=None):
    logger.warning("Escalation receiver disconnected, rc=%s", rc)


def start_escalation_receiver(
    handler: Optional[Callable[[Dict[str, Any]], None]] = None,
) -> bool:
    """Start the A2A escalation receiver in a background thread.

    Args:
        handler: Callback invoked for each escalation payload.
                 If None, uses _default_handle_escalation.

    Returns:
        True if the receiver thread was started, False if paho-mqtt
        is missing or the broker is unreachable.
    """
    global _receiver_thread, _client, _on_escalation

    if not _HAS_PAHO:
        logger.warning("Escalation receiver: paho-mqtt not available")
        return False

    if _receiver_thread is not None and _receiver_thread.is_alive():
        logger.debug("Escalation receiver already running")
        return True

    _on_escalation = handler
    _shutdown_event.clear()

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.on_connect = _on_connect
    client.on_message = _on_message
    client.on_disconnect = _on_disconnect

    try:
        client.connect(BROKER, PORT, KEEPALIVE)
    except Exception as exc:
        logger.warning("Escalation receiver: broker unreachable (%s)", exc)
        return False

    def _run():
        client.loop_start()
        logger.info("Escalation receiver thread started")
        _shutdown_event.wait()
        client.loop_stop()
        client.disconnect()
        logger.info("Escalation receiver thread stopped")

    _client = client
    _receiver_thread = threading.Thread(target=_run, daemon=True, name="escalation-receiver")
    _receiver_thread.start()
    return True


def stop_escalation_receiver() -> None:
    """Signal the escalation receiver thread to stop."""
    _shutdown_event.set()
