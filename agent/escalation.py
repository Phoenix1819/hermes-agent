"""Fire-and-forget escalation publisher for the Hermes MQTT broker.

Imported by gateway/run.py, agent/auxiliary_client.py, and any other
code path that needs to escalate without blocking or crashing.

Usage:
    from agent.escalation import escalate
    escalate(summary="OpenRouter 402", severity="high", category="payment")

The function swallows all errors internally — callers never crash because
escalation failed.  If the broker is down or paho-mqtt is missing, the
escalation is silently dropped after a brief logging warning.
"""

from __future__ import annotations

import json
import logging
import os
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

BROKER = "127.0.0.1"
PORT = 1883
KEEPALIVE = 15
TOPIC_ESCALATE = "hermes/escalate/{agent}"

# Only warn once per process if paho-mqtt is missing or broker is down.
_paho_warned = False
_broker_warned = False


def _detect_agent() -> str:
    """Best-effort agent name detection."""
    # 1. Explicit env var (set by Hermes gateway startup)
    explicit = os.getenv("HERMES_PROFILE", "").strip()
    if explicit:
        return explicit
    # 2. Infer from profile directory in cwd or argv[0]
    cwd = os.getcwd()
    if ".hermes/profiles/" in cwd:
        parts = cwd.split(".hermes/profiles/")
        if len(parts) > 1:
            return parts[1].split("/")[0]
    # 3. Fallback
    return "unknown"


def _publish_sync(
    agent: str,
    payload: Dict[str, Any],
    qos: int = 1,
    timeout: float = 3.0,
) -> bool:
    """Blocking MQTT publish with hard timeout.  Returns True on success."""
    global _paho_warned, _broker_warned

    try:
        import paho.mqtt.client as mqtt
    except Exception as exc:
        if not _paho_warned:
            logger.warning("Escalation skipped: paho-mqtt not available (%s)", exc)
            _paho_warned = True
        return False

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.enable_logger(logger)

    published = threading.Event()
    result: list = [False]

    def _on_publish(_client, _userdata, _mid, _rc, _props=None):
        result[0] = True
        published.set()

    client.on_publish = _on_publish

    try:
        client.connect(BROKER, PORT, KEEPALIVE)
        client.loop_start()
        topic = TOPIC_ESCALATE.format(agent=agent)
        msg_info = client.publish(topic, json.dumps(payload, indent=None), qos=qos)
        # Wait for publish confirmation or timeout
        published.wait(timeout=timeout)
        client.loop_stop()
        client.disconnect()
        return result[0]
    except Exception as exc:
        if not _broker_warned:
            logger.warning("Escalation skipped: MQTT broker unreachable (%s)", exc)
            _broker_warned = True
        try:
            client.loop_stop()
            client.disconnect()
        except Exception:
            pass
        return False


def escalate(
    summary: str,
    *,
    severity: str = "medium",
    category: str = "unknown",
    details: str = "",
    user_request: str = "",
    attempted: Optional[List[str]] = None,
    log_path: str = "",
    skill_path: str = "",
    custom: Optional[Dict[str, Any]] = None,
    agent: Optional[str] = None,
    qos: int = 1,
) -> Optional[str]:
    """Publish an escalation ticket and return its ID (or None on failure).

    Args:
        summary: One-line description of the problem (required).
        severity: low | medium | high | critical.  Default medium.
        category: payment | auth | platform | model | tool | unknown.  Default unknown.
        details: Full error message, traceback, or additional context.
        user_request: The user's original message that triggered the failure.
        attempted: List of things already tried (e.g. ["fallback provider", "auth refresh"]).
        log_path: Path to relevant log file, if any.
        skill_path: Path to relevant skill file, if any.
        custom: Arbitrary extra fields merged into the payload.
        agent: Override agent name.  Auto-detected if omitted.
        qos: MQTT QoS level (0, 1, 2).  Default 1.

    Returns:
        Ticket ID (8-char hex) on success, None if publish failed or was skipped.
    """
    _agent = (agent or _detect_agent()).strip() or "unknown"
    ticket_id = str(uuid.uuid4())[:8]

    payload = {
        "id": ticket_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "agent": _agent,
        "severity": severity,
        "category": category,
        "summary": summary,
        "details": details,
        "user_request": user_request,
        "attempted": attempted or [],
        "log_path": log_path,
        "skill_path": skill_path,
    }
    if custom:
        payload.update(custom)

    # Fire-and-forget in a background thread so the caller never blocks.
    def _fire():
        try:
            ok = _publish_sync(_agent, payload, qos=qos)
            if not ok:
                logger.warning("Escalation publish failed for ticket %s — dropped (no fallback configured)", ticket_id)
        except Exception:
            logger.warning("Escalation publish crashed for ticket %s — dropped", ticket_id)

    threading.Thread(target=_fire, daemon=True, name=f"escalate-{ticket_id}").start()
    return ticket_id


# Convenience wrappers for common failure categories

def escalate_payment(
    summary: str,
    details: str = "",
    attempted: Optional[List[str]] = None,
    **kwargs: Any,
) -> Optional[str]:
    """Shorthand for payment/credit exhaustion escalations."""
    return escalate(summary, severity="high", category="payment", details=details, attempted=attempted, **kwargs)


def escalate_auth(
    summary: str,
    details: str = "",
    attempted: Optional[List[str]] = None,
    **kwargs: Any,
) -> Optional[str]:
    """Shorthand for auth/token failure escalations."""
    return escalate(summary, severity="high", category="auth", details=details, attempted=attempted, **kwargs)


def escalate_platform(
    summary: str,
    details: str = "",
    attempted: Optional[List[str]] = None,
    **kwargs: Any,
) -> Optional[str]:
    """Shorthand for platform/gateway failure escalations."""
    return escalate(summary, severity="critical", category="platform", details=details, attempted=attempted, **kwargs)


def escalate_model(
    summary: str,
    details: str = "",
    attempted: Optional[List[str]] = None,
    **kwargs: Any,
) -> Optional[str]:
    """Shorthand for model inference failure escalations."""
    return escalate(summary, severity="high", category="model", details=details, attempted=attempted, **kwargs)


def escalate_tool(
    summary: str,
    details: str = "",
    attempted: Optional[List[str]] = None,
    **kwargs: Any,
) -> Optional[str]:
    """Shorthand for tool/skill failure escalations."""
    return escalate(summary, severity="medium", category="tool", details=details, attempted=attempted, **kwargs)
