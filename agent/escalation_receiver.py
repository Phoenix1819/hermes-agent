"""A2A escalation receiver for Phoenix.

Subscribes to hermes/escalate/+ on the local MQTT broker and creates
Kanban tickets + user notifications directly. This is the PRIMARY path.
The dumb-pipe daemon (escalation_pipe.py) is the fallback when Phoenix
is offline — it writes JSON files to disk that Phoenix processes on startup.

Usage (inside Phoenix gateway process):
    from agent.escalation_receiver import start_escalation_receiver
    start_escalation_receiver()

Or for one-shot file processing on startup:
    from agent.escalation_receiver import process_backlog
    process_backlog()
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

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
INCOMING_DIR = HERMES_HOME / "escalations" / "incoming"
PROCESSED_DIR = HERMES_HOME / "escalations" / "processed"

_receiver_thread: Optional[threading.Thread] = None
_client: Optional[Any] = None
_shutdown_event = threading.Event()

# Callback that Phoenix registers to handle escalations.
# Signature: handle_escalation(payload: dict) -> None
_on_escalation: Optional[Callable[[Dict[str, Any]], None]] = None


def _default_handle_escalation(payload: Dict[str, Any]) -> None:
    """Default handler — logs and creates a Kanban ticket via the todo tool."""
    logger.info(
        "ESCALATION from %s [%s/%s]: %s",
        payload.get("agent", "?"),
        payload.get("severity", "?"),
        payload.get("category", "?"),
        payload.get("summary", "?"),
    )
    # Phoenix will override this with its own handler that creates Kanban tickets
    # and messages the user. This default just logs so nothing is lost.


def _move_to_processed(path: Path) -> None:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    dest = PROCESSED_DIR / path.name
    try:
        path.rename(dest)
    except Exception:
        # If rename fails (cross-device), copy and delete
        import shutil
        shutil.copy2(path, dest)
        path.unlink()


def process_backlog() -> List[Dict[str, Any]]:
    """Process all JSON files in ~/.hermes/escalations/incoming/.

    Returns the list of payloads processed. Each file is moved to
    ~/.hermes/escalations/processed/ after handling.
    """
    INCOMING_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    payloads: List[Dict[str, Any]] = []
    for path in sorted(INCOMING_DIR.glob("*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            payloads.append(payload)
            handler = _on_escalation or _default_handle_escalation
            try:
                handler(payload)
            except Exception as exc:
                logger.exception("Backlog handler failed for %s: %s", path.name, exc)
            _move_to_processed(path)
        except Exception as exc:
            logger.exception("Failed to process backlog file %s: %s", path.name, exc)
    if payloads:
        logger.info("Processed %s backlog escalation(s)", len(payloads))
    return payloads


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
