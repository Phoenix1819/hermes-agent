"""
Phoenix notification consumer — the sole consumer of the unified bus.

Polls the notification_bus.sqlite periodically, queries Holographic memory
for user preferences, and triages each message into one of:
  deliver — send to Signal immediately
  drop    — silent discard (learned noise)
  digest  — batch with other messages (normal non-urgent output)
  escalate — hand off to Phoenix active session for human review

Runs as a background thread inside the gateway process so live adapters
are available for E2EE rooms.
"""
from __future__ import annotations

import asyncio
import logging
import threading
import traceback
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
_POLL_INTERVAL = 30          # seconds between polls
_MAX_BATCH = 10              # process up to N messages per tick
_DELIVERY_PLATFORM_PREF = "signal"  # canonical output sink


def _now_iso() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# Triage decisions
# ---------------------------------------------------------------------------
class Action:
    DELIVER = "deliver"
    DROP = "drop"
    DIGEST = "digest"
    ESCALATE = "escalate"
    HOLD = "hold"


class TriageResult:
    action: str
    reason: str
    target_platform: str
    target_chat: Optional[str]
    priority_override: Optional[int]

    def __init__(
        self,
        action: str,
        reason: str = "",
        target_platform: str = "",
        target_chat: Optional[str] = None,
        priority_override: Optional[int] = None,
    ):
        self.action = action
        self.reason = reason
        self.target_platform = target_platform
        self.target_chat = target_chat
        self.priority_override = priority_override


# ---------------------------------------------------------------------------
# Preference learning via Holographic memory
# ---------------------------------------------------------------------------
class PreferenceEngine:
    """
    Thin wrapper around the Holographic MemoryStore.

    On each triage:\n
      1. Search for facts matching the topic (cron:job_name, escalation:*, etc.).\n
      2. Probe for the specific entity (e.g. 'backup job').\n
      3. Compute a nuisance score: high = user has dropped or muted this topic.\n
    The engine is stateless except for the DB connection.
    """

    def __init__(self, db_path: Optional[str] = None):
        self._store = None
        self._retriever = None
        self._db_path = db_path
        self._lock = threading.Lock()

    def _connect(self):
        if self._store is not None:
            return
        try:
            from plugins.memory.holographic.store import MemoryStore
            from plugins.memory.holographic.retrieval import FactRetriever
            self._store = MemoryStore(db_path=self._db_path)
            self._retriever = FactRetriever(self._store)
        except Exception:
            logger.debug("PreferenceEngine: could not connect to Holographic memory: %s", traceback.format_exc())

    def _entity_from_topic(self, topic: str) -> str:
        """Strip prefix so 'cron:sharesight-report' becomes 'sharesight report'."""
        if ":" in topic:
            _, tail = topic.split(":", 1)
            return tail.replace("-", " ").replace("_", " ")
        return topic.replace("-", " ").replace("_", " ")

    def triage(
        self,
        source: str,
        topic: str,
        priority: int,
        payload: dict,
    ) -> TriageResult:
        """Return a TriageResult based on learned preferences + hard rules."""
        # -- Hard rules first -------------------------------------------------
        # Priority 1-2 == failures/alerts → always deliver immediately
        if priority <= 2:
            return TriageResult(
                action=Action.DELIVER,
                reason=f"critical priority {priority}",
                target_platform=_DELIVERY_PLATFORM_PREF,
            )

        # A2A escalations always create Kanban ticket + notify
        if source == "a2a":
            return TriageResult(
                action=Action.ESCALATE,
                reason="A2A escalation → Kanban + notify",
                target_platform=_DELIVERY_PLATFORM_PREF,
            )

        # Priority >= 8 == low-priority noise → drop unless user explicitly
        # opted in to receive it.
        if priority >= 8:
            return TriageResult(
                action=Action.DROP,
                reason="low-priority noise muted by default",
            )

        # -- Holographic memory lookup ---------------------------------------
        entity = self._entity_from_topic(topic)
        nuisance_score = 0.0  # 0.0 = welcome, 1.0 = actively hated
        with self._lock:
            self._connect()
            if self._store is not None:
                try:
                    # Search for facts about this job / topic
                    facts = self._retriever.search(
                        query=f"{entity} notification mute silent drop ignore",
                        category="user_pref",
                        limit=5,
                    ) or self._retriever.search(
                        query=entity,
                        category="user_pref",
                        limit=5,
                    )
                    for f in facts:
                        # Negative trust = user marked unhelpful / annoyed
                        trust = f.get("trust_score", 0.5)
                        if trust < 0.3:
                            nuisance_score += (0.3 - trust)
                        # Content keywords that indicate annoyance
                        text = f"{f.get('content', '')} {f.get('tags', '')}".lower()
                        if any(kw in text for kw in ("annoy", "mute", "silent", "drop", "spam", "noisy")):
                            nuisance_score += 0.3
                        if any(kw in text for kw in ("keep", "want", "deliver", "important")):
                            nuisance_score -= 0.2
                except Exception:
                    pass
                try:
                    # Probe for explicit facts about this entity
                    probe_facts = self._retriever.probe(entity, limit=3)
                    for f in probe_facts:
                        trust = f.get("trust_score", 0.5)
                        if trust < 0.3:
                            nuisance_score += (0.3 - trust)
                except Exception:
                    pass

        # Clamp
        nuisance_score = max(0.0, min(1.0, nuisance_score))

        if nuisance_score >= 0.6:
            return TriageResult(
                action=Action.DROP,
                reason=f"learned nuisance score {nuisance_score:.2f}",
            )
        if nuisance_score >= 0.3:
            return TriageResult(
                action=Action.DIGEST,
                reason=f"moderate nuisance score {nuisance_score:.2f}",
                target_platform=_DELIVERY_PLATFORM_PREF,
            )

        # Default: deliver directly
        return TriageResult(
            action=Action.DELIVER,
            reason="default policy: deliver",
            target_platform=_DELIVERY_PLATFORM_PREF,
        )


# ---------------------------------------------------------------------------
# Delivery helper
# ---------------------------------------------------------------------------
async def _deliver_message_with_adapters(
    platform_name: str,
    chat_id: str,
    text: str,
    thread_id: str | None = None,
    media_files: list | None = None,
    adapters: dict | None = None,
) -> bool:
    """
    Deliver a message using live gateway adapters if available,
    falling back to standalone sender for out-of-process contexts.
    """
    media_files = media_files or []
    from gateway.config import load_gateway_config, Platform

    config = load_gateway_config()
    try:
        platform = Platform(platform_name.lower())
    except (ValueError, KeyError):
        logger.error("Unknown platform '%s'", platform_name)
        return False

    pconfig = config.platforms.get(platform)
    if not pconfig or not pconfig.enabled:
        # For out-of-process cron consumer, platform may not be "enabled"
        # in config but we still have credentials. Allow the attempt.
        logger.debug("Platform '%s' not enabled or missing config, trying standalone", platform_name)
        pconfig = getattr(config.platforms.get(platform), None) or config.platforms.get(platform)

    # Prefer live adapter if available in the gateway process
    if adapters:
        adapter = adapters.get(platform)
        if adapter is not None:
            try:
                result = await adapter.send(chat_id=chat_id, content=text)
                if result.success:
                    return True
                logger.warning("Adapter send failed: %s", result.error)
                return False
            except Exception:
                logger.exception("Adapter send raised")
                # Fall through to standalone

    # Standalone / out-of-process path
    try:
        from tools.send_message_tool import _send_to_platform
        result = await _send_to_platform(
            platform, pconfig, chat_id, text,
            thread_id=thread_id, media_files=media_files or [],
        )
        if isinstance(result, dict) and result.get("success"):
            return True
        if isinstance(result, dict) and "error" in result:
            logger.warning("Delivery to %s failed: %s", platform_name, result["error"])
            return False
        return True
    except Exception:
        logger.exception("Unexpected delivery error to %s", platform_name)
        return False


# Legacy alias for backwards compatibility
async def _deliver_message(
    platform_name: str,
    chat_id: str,
    text: str,
    thread_id: str | None = None,
    media_files: list | None = None,
) -> bool:
    return await _deliver_message_with_adapters(
        platform_name, chat_id, text, thread_id, media_files, adapters=None
    )


def _deliver_sync(
    platform_name: str,
    chat_id: str,
    text: str,
    thread_id: str | None = None,
    media_files: list | None = None,
    adapters: dict | None = None,
) -> bool:
    """Synchronous wrapper for _deliver_message_with_adapters."""
    try:
        return asyncio.run(_deliver_message_with_adapters(
            platform_name=platform_name,
            chat_id=chat_id,
            text=text,
            thread_id=thread_id,
            media_files=media_files,
            adapters=adapters,
        ))
    except RuntimeError:
        # Already in an event loop — schedule and wait
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(_deliver_message_with_adapters(
            platform_name=platform_name,
            chat_id=chat_id,
            text=text,
            thread_id=thread_id,
            media_files=media_files,
            adapters=adapters,
        ))


# Legacy alias for backwards compatibility
def _deliver_sync_legacy(**kwargs) -> bool:
    """Synchronous wrapper for _deliver_message."""
    try:
        return asyncio.run(_deliver_message(**kwargs))
    except RuntimeError:
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(_deliver_message(**kwargs))


# ---------------------------------------------------------------------------
# Consumer thread
# ---------------------------------------------------------------------------
class NotificationConsumer:
    """
    Poll the notification bus and triage/deliver each message.

    Parameters
    ----------
    stop_event : threading.Event
        Set this event to shut down cleanly.
    adapters : dict, optional
        Live adapter map from GatewayRunner (for adapter-level delivery).
    loop : asyncio.AbstractEventLoop, optional
        Gateway event loop (for scheduling async work from the thread).
    interval : int
        Poll interval in seconds (used as max idle time between polls).
    """

    def __init__(
        self,
        stop_event: threading.Event,
        adapters: dict | None = None,
        loop: asyncio.AbstractEventLoop | None = None,
        interval: int = _POLL_INTERVAL,
        config=None,
    ):
        self.stop_event = stop_event
        self.adapters = adapters
        self.loop = loop
        self.interval = interval
        self.pref = PreferenceEngine()
        self._executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="notif_consumer")
        # Event-driven wakeup: bus insert_message() flips this to wake us early
        self.wakeup_event = threading.Event()
        try:
            import notification_bus as bus
            bus.set_wakeup_event(self.wakeup_event)
        except Exception:
            pass
        # Pre-resolve home channels so the thread never hits import failures
        self._home_channels: dict[str, dict] = {}
        try:
            from gateway.config import Platform
            if config is not None:
                for p in Platform:
                    home = config.get_home_channel(p)
                    if home and home.chat_id:
                        self._home_channels[p.value] = {
                            "chat_id": home.chat_id,
                            "thread_id": home.thread_id,
                        }
        except Exception:
            logger.warning("Could not pre-resolve home channels: %s", traceback.format_exc())

    def _poll_once(self) -> list:
        """Return unprocessed InboxMessages from the bus."""
        try:
            import notification_bus as bus
            return bus.poll_unprocessed(limit=_MAX_BATCH)
        except Exception:
            logger.debug("Bus poll failed: %s", traceback.format_exc())
            return []

    def _process_one(self, msg) -> None:
        """Triage a single message and execute the decision."""
        try:
            import notification_bus as bus
        except Exception:
            return

        payload = msg.payload or {}
        source = msg.source or "unknown"
        topic = msg.topic or "unknown"
        priority = msg.priority or 5

        result = self.pref.triage(source, topic, priority, payload)

        logger.info(
            "[%s] %s/%s pri=%d → %s (%s)",
            msg.id, source, topic, priority, result.action, result.reason,
        )

        if result.action == Action.DELIVER:
            content = payload.get("content", "")
            # If payload has targets, try to resolve the best one
            targets = payload.get("targets", [])
            if targets:
                # Prefer signal, fall back to first target
                target = next(
                    (t for t in targets if t.get("platform") == result.target_platform),
                    targets[0],
                )
                platform = target.get("platform", result.target_platform)
                chat_id = target.get("chat_id", "")
                thread_id = target.get("thread_id")
            else:
                platform = result.target_platform
                chat_id = ""
                thread_id = None

            if not chat_id:
                # Resolve home channel from pre-cached map or gateway config
                home = self._home_channels.get(platform)
                if home:
                    chat_id = home["chat_id"]
                    thread_id = thread_id or home["thread_id"]
                else:
                    try:
                        from gateway.config import load_gateway_config, Platform
                        config = load_gateway_config()
                        p = Platform(platform)
                        home = config.get_home_channel(p)
                        if home:
                            chat_id = home.chat_id
                            thread_id = thread_id or home.thread_id
                    except Exception as exc:
                        logger.debug("Home channel resolve failed for %s: %s", platform, exc)

            if not chat_id:
                logger.warning("Cannot deliver msg %s: no chat_id resolved for %s", msg.id, platform)
                bus.mark_processed(msg.id, action="hold", delivered_to="")
                return

            # If gateway loop is available, schedule async delivery;
            # otherwise fall back to sync wrapper
            media = payload.get("media_files", [])
            ok = self._do_delivery(platform, chat_id, content, thread_id, media)

            if ok:
                bus.mark_processed(msg.id, action=Action.DELIVER, delivered_to=platform)
            else:
                bus.mark_processed(msg.id, action="hold", delivered_to="")

        elif result.action == Action.DROP:
            bus.mark_processed(msg.id, action=Action.DROP, delivered_to="")

        elif result.action == Action.DIGEST:
            # TODO: implement digest batching in v2
            # For now, deliver but mark as digest so the DB records the choice
            content = payload.get("content", "")
            targets = payload.get("targets", [])
            target = next(
                (t for t in targets if t.get("platform") == result.target_platform),
                targets[0] if targets else None,
            )
            if target:
                platform = target.get("platform", result.target_platform)
                chat_id = target.get("chat_id", "")
                thread_id = target.get("thread_id")
            else:
                platform = result.target_platform
                chat_id = ""
                thread_id = None
                # Resolve home channel from pre-cached map or gateway config
                home = self._home_channels.get(platform)
                if home:
                    chat_id = home["chat_id"]
                    thread_id = thread_id or home["thread_id"]
                else:
                    try:
                        from gateway.config import load_gateway_config, Platform
                        config = load_gateway_config()
                        p = Platform(platform)
                        home = config.get_home_channel(p)
                        if home:
                            chat_id = home.chat_id
                            thread_id = thread_id or home.thread_id
                    except Exception as exc:
                        logger.debug("Home channel resolve failed for %s: %s", platform, exc)
            if chat_id:
                self._do_delivery(platform, chat_id, content, thread_id, [])
                bus.mark_processed(msg.id, action=Action.DIGEST, delivered_to=platform)
            else:
                bus.mark_processed(msg.id, action="hold", delivered_to="")

        elif result.action == Action.ESCALATE:
            # Create Kanban ticket for the escalation, then deliver
            kanban_id = self._create_kanban_task(msg, payload)
            content = payload.get("content", "") or payload.get("summary", "")
            targets = payload.get("targets", [])
            target = next(
                (t for t in targets if t.get("platform") == result.target_platform),
                targets[0] if targets else None,
            )
            if target:
                platform = target.get("platform", result.target_platform)
                chat_id = target.get("chat_id", "")
                thread_id = target.get("thread_id")
            else:
                platform = result.target_platform
                chat_id = ""
                thread_id = None
                # Resolve home channel from pre-cached map or gateway config
                home = self._home_channels.get(platform)
                if home:
                    chat_id = home["chat_id"]
                    thread_id = thread_id or home["thread_id"]
                else:
                    try:
                        from gateway.config import load_gateway_config, Platform
                        config = load_gateway_config()
                        p = Platform(platform)
                        home = config.get_home_channel(p)
                        if home:
                            chat_id = home.chat_id
                            thread_id = thread_id or home.thread_id
                    except Exception as exc:
                        logger.debug("Home channel resolve failed for %s: %s", platform, exc)
            if chat_id:
                self._do_delivery(platform, chat_id, content, thread_id, [])
                bus.mark_processed(
                    msg.id,
                    action=Action.ESCALATE,
                    delivered_to=f"kanban:{kanban_id};{platform}" if kanban_id else platform,
                )
            else:
                bus.mark_processed(
                    msg.id,
                    action=Action.ESCALATE,
                    delivered_to=f"kanban:{kanban_id}" if kanban_id else "",
                )

        elif result.action == Action.HOLD:
            bus.mark_processed(msg.id, action=Action.HOLD, delivered_to="")

    def _create_kanban_task(self, msg, payload: dict) -> Optional[str]:
        """Create a Kanban task for an escalation. Returns task id or None."""
        try:
            from hermes_cli.kanban_db import connect, create_task

            agent_name = payload.get("agent", "unknown")
            severity = payload.get("severity", "medium")
            category = payload.get("category", "general")
            summary = payload.get("summary", "")
            detail = payload.get("detail", "")

            title = f"[{agent_name}] {summary}" if summary else f"Escalation from {agent_name}"
            body_lines = [
                f"Source: A2A escalation",
                f"Agent: {agent_name}",
                f"Severity: {severity}",
                f"Category: {category}",
                f"Bus msg id: {msg.id}",
                "",
            ]
            if detail:
                body_lines.append(f"Detail: {detail}")
            if payload.get("content"):
                body_lines.append(f"Content: {payload['content']}")

            body = "\n".join(body_lines)

            conn = connect()
            try:
                task_id = create_task(
                    conn,
                    title=title,
                    body=body,
                    assignee="phoenix",
                    created_by="phoenix",
                    priority=msg.priority,
                    triage=True,
                )
                logger.info("Created Kanban task %s for escalation %s", task_id, msg.id)
                return task_id
            finally:
                conn.close()
        except Exception:
            logger.exception("Failed to create Kanban task for escalation %s", msg.id)
            return None

    def _do_delivery(
        self,
        platform: str,
        chat_id: str,
        content: str,
        thread_id: str | None,
        media: list,
    ) -> bool:
        """Dispatch the actual send, respecting the gateway event loop if present."""
        if self.loop and self.loop.is_running():
            future = asyncio.run_coroutine_threadsafe(
                _deliver_message_with_adapters(
                    platform, chat_id, content, thread_id, media, adapters=self.adapters
                ),
                self.loop,
            )
            try:
                return future.result(timeout=30)
            except Exception:
                logger.exception("Delivery via gateway loop failed")
                return False
        else:
            return _deliver_sync(
                platform_name=platform,
                chat_id=chat_id,
                text=content,
                thread_id=thread_id,
                media_files=media,
                adapters=self.adapters,
            )

    def run(self):
        logger.info("Phoenix notification consumer started (interval=%ds)", self.interval)
        while not self.stop_event.is_set():
            try:
                pending = self._poll_once()
                for msg in pending:
                    try:
                        self._process_one(msg)
                    except Exception:
                        logger.exception("Error processing message id=%s", getattr(msg, "id", "?"))
                        try:
                            import notification_bus as bus
                            bus.mark_processed(getattr(msg, "id", 0), action="error", delivered_to="")
                        except Exception:
                            pass
            except Exception:
                logger.exception("Unexpected error in notification consumer poll")

            # Event-driven sleep: wake immediately when a message arrives,
            # up to 5 s to check stop_event.
            if not self.stop_event.is_set():
                cap = min(5.0, self.interval) if self.interval > 0 else 5.0
                if self.wakeup_event.wait(timeout=cap):
                    self.wakeup_event.clear()

        logger.info("Phoenix notification consumer stopped")
        self._executor.shutdown(wait=False)


# ---------------------------------------------------------------------------
# Factory for gateway integration
# ---------------------------------------------------------------------------
def start_consumer(
    stop_event: threading.Event,
    adapters: dict | None = None,
    loop: asyncio.AbstractEventLoop | None = None,
    interval: int = _POLL_INTERVAL,
    config=None,
) -> threading.Thread:
    """Start the consumer in a background thread and return the thread handle."""
    consumer = NotificationConsumer(
        stop_event, adapters=adapters, loop=loop, interval=interval, config=config
    )
    thread = threading.Thread(
        target=consumer.run,
        name="phoenix-notification-consumer",
        daemon=True,
    )
    thread.start()
    return thread
