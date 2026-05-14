"""
Unified notification inbox — SQLite message bus for Hermes.

All cron outputs, escalations, and system notifications land here FIRST.
Phoenix is the sole consumer. No message reaches Signal without passing
through the bus and being processed by Phoenix.

The bus is append-only with transactional read+mark-processed semantics.
"""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import threading
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

_DB_PATH: Optional[Path] = None
_CONNECTION: Optional[sqlite3.Connection] = None
_WAKEUP_EVENT: threading.Event | None = None


def _get_db_path() -> Path:
    global _DB_PATH
    if _DB_PATH is not None:
        return _DB_PATH
    _DB_PATH = Path(os.path.expanduser("~/.hermes/shared/notification_bus.db"))
    _DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    return _DB_PATH


def _init_db(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS message (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            received_at TEXT DEFAULT CURRENT_TIMESTAMP NOT NULL,
            source TEXT NOT NULL,
            topic TEXT NOT NULL,
            priority INTEGER DEFAULT 5,
            payload TEXT NOT NULL,
            dedup_hash TEXT DEFAULT '',
            processed INTEGER DEFAULT 0 NOT NULL,
            action TEXT DEFAULT '',
            delivered_to TEXT DEFAULT '',
            resolved_at TEXT
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_unprocessed ON message(processed, priority, id)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_source_topic ON message(source, topic)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_dedup ON message(dedup_hash) WHERE dedup_hash != ''
    """)


def _connection() -> sqlite3.Connection:
    global _CONNECTION
    if _CONNECTION is None:
        db_path = _get_db_path()
        _CONNECTION = sqlite3.connect(str(db_path), check_same_thread=False)
        _CONNECTION.row_factory = sqlite3.Row
        _init_db(_CONNECTION)
    return _CONNECTION


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _dedup_hash(payload: dict) -> str:
    """Deterministic hash for content-based deduplication."""
    canonical = json.dumps(payload, sort_keys=True, ensure_ascii=True)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]


@dataclass
class InboxMessage:
    id: int = 0
    received_at: str = ""
    source: str = ""
    topic: str = ""
    priority: int = 5
    payload: dict = field(default_factory=dict)
    dedup_hash: str = ""
    processed: int = 0
    action: str = ""
    delivered_to: str = ""
    resolved_at: str = ""

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "InboxMessage":
        payload = {}
        try:
            payload = json.loads(row["payload"] or "{}")
        except Exception:
            pass
        return cls(
            id=row["id"],
            received_at=row["received_at"] or "",
            source=row["source"] or "",
            topic=row["topic"] or "",
            priority=row["priority"],
            payload=payload,
            dedup_hash=row["dedup_hash"] or "",
            processed=row["processed"],
            action=row["action"] or "",
            delivered_to=row["delivered_to"] or "",
            resolved_at=row["resolved_at"] or "",
        )


def insert_message(
    source: str,
    topic: str,
    payload: dict,
    priority: int = 5,
) -> int:
    """
    Insert a message into the notification bus.

    Returns the new message id.
    """
    conn = _connection()
    payload_json = json.dumps(payload, ensure_ascii=False, default=str)
    dedup = _dedup_hash(payload)

    # Optional: skip if identical payload arrived in the last 5 minutes
    cursor = conn.execute(
        "SELECT id FROM message WHERE dedup_hash = ? AND received_at > datetime('now', '-5 minutes')",
        (dedup,),
    )
    if cursor.fetchone():
        # Deduplicate silently
        return 0

    cursor = conn.execute(
        """
        INSERT INTO message (received_at, source, topic, priority, payload, dedup_hash)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (_now_iso(), source, topic, priority, payload_json, dedup),
    )
    conn.commit()

    # Wakeup the notification consumer if it is sleeping
    _notify()

    return cursor.lastrowid or 0


def poll_unprocessed(
    limit: int = 1,
    source_filter: Optional[str] = None,
    topic_filter: Optional[list[str]] = None,
) -> list[InboxMessage]:
    """
    Poll for the next unprocessed messages, ordered by priority then id.

    If limit is 0, returns all pending.
    """
    conn = _connection()
    query = "SELECT * FROM message WHERE processed = 0"
    params: list = []

    if source_filter:
        query += " AND source = ?"
        params.append(source_filter)

    if topic_filter:
        placeholders = ",".join("?" for _ in topic_filter)
        query += f" AND topic IN ({placeholders})"
        params.extend(topic_filter)

    query += " ORDER BY priority ASC, id ASC"
    if limit > 0:
        query += f" LIMIT {limit}"

    cursor = conn.execute(query, params)
    return [InboxMessage.from_row(row) for row in cursor.fetchall()]


def poll_all_unprocessed() -> list[InboxMessage]:
    """Return every pending message (useful for diagnostics)."""
    return poll_unprocessed(limit=0)


def mark_processed(
    msg_id: int,
    action: str = "",
    delivered_to: str = "",
) -> None:
    """
    Mark a message as processed.

    action: 'deliver', 'drop', 'escalate', 'digest', or empty
    delivered_to: comma-separated platform names, e.g. 'signal,matrix'
    """
    conn = _connection()
    conn.execute(
        """
        UPDATE message
        SET processed = 1, action = ?, delivered_to = ?, resolved_at = ?
        WHERE id = ?
        """,
        (action, delivered_to, _now_iso(), msg_id),
    )
    conn.commit()


def get_stats() -> dict:
    """Return inbox statistics for diagnostics."""
    conn = _connection()
    total = conn.execute("SELECT COUNT(*) FROM message").fetchone()[0]
    pending = conn.execute("SELECT COUNT(*) FROM message WHERE processed = 0").fetchone()[0]
    by_topic = conn.execute(
        "SELECT topic, COUNT(*) FROM message WHERE processed = 0 GROUP BY topic"
    ).fetchall()
    return {
        "total": total,
        "pending": pending,
        "by_topic": {row[0]: row[1] for row in by_topic},
    }


def close() -> None:
    """Close the shared connection."""
    global _CONNECTION
    if _CONNECTION:
        _CONNECTION.commit()
        _CONNECTION.close()
        _CONNECTION = None


# ---------------------------------------------------------------------------
# Event-driven wakeup
# ---------------------------------------------------------------------------
def set_wakeup_event(event: threading.Event) -> None:
    """Register a threading.Event that insert_message() will trigger."""
    global _WAKEUP_EVENT
    _WAKEUP_EVENT = event


def _notify() -> None:
    """Wake the consumer if it is currently idle."""
    global _WAKEUP_EVENT
    evt = _WAKEUP_EVENT
    if evt is not None:
        try:
            evt.set()
        except Exception:
            pass
