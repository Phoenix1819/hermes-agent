"""local-sqlite-telemetry — Hermes plugin for local telemetry storage.

Writes LLM calls, tool calls, context pressure, and session summaries
to a SQLite DB at ~/.hermes/profiles/<profile>/telemetry.db.

Declare hooks in plugin.yaml:
  pre_llm_call, post_llm_call, pre_tool_call, post_tool_call,
  on_session_start, on_session_end, on_turn_start, on_turn_end
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import sqlite3
import threading
import time
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# ── per-profile DB path ────────────────────────────────────────────────────
_DB_PATH: Optional[str] = None


def _get_db_path() -> str:
    global _DB_PATH
    if _DB_PATH is None:
        hermes_home = os.environ.get("HERMES_HOME", os.path.expanduser("~/.hermes"))
        # Try profile-specific first, fall back to root
        profile = os.path.basename(hermes_home) if ".hermes" in hermes_home else None
        if profile and os.path.isdir(os.path.join(os.path.expanduser("~/.hermes"), "profiles", profile)):
            _DB_PATH = os.path.join(hermes_home, "telemetry.db")
        else:
            _DB_PATH = os.path.join(os.path.expanduser("~/.hermes"), "profiles", "phoenix", "telemetry.db")
    return _DB_PATH


def _ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript("""
CREATE TABLE IF NOT EXISTS llm_calls (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp INTEGER NOT NULL DEFAULT (strftime('%s','now')),
    session_id TEXT, task_id TEXT, model TEXT, provider TEXT,
    prompt_tokens INTEGER, completion_tokens INTEGER, total_tokens INTEGER,
    cache_read_tokens INTEGER, cache_write_tokens INTEGER, reasoning_tokens INTEGER,
    cost_usd REAL, duration_ms INTEGER, status TEXT DEFAULT 'success'
);
CREATE INDEX IF NOT EXISTS idx_llm_session ON llm_calls(session_id);
CREATE INDEX IF NOT EXISTS idx_llm_task ON llm_calls(task_id);
CREATE INDEX IF NOT EXISTS idx_llm_time ON llm_calls(timestamp);

CREATE TABLE IF NOT EXISTS tool_calls (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp INTEGER NOT NULL DEFAULT (strftime('%s','now')),
    session_id TEXT, task_id TEXT, tool_name TEXT NOT NULL,
    status TEXT DEFAULT 'success', duration_ms INTEGER,
    args_hash TEXT, error_message TEXT, token_cost INTEGER
);
CREATE INDEX IF NOT EXISTS idx_tool_session ON tool_calls(session_id);
CREATE INDEX IF NOT EXISTS idx_tool_task ON tool_calls(task_id);
CREATE INDEX IF NOT EXISTS idx_tool_name ON tool_calls(tool_name);
CREATE INDEX IF NOT EXISTS idx_tool_time ON tool_calls(timestamp);

CREATE TABLE IF NOT EXISTS context_pressure (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp INTEGER NOT NULL DEFAULT (strftime('%s','now')),
    session_id TEXT, turn_number INTEGER,
    context_used_chars INTEGER, context_limit_chars INTEGER,
    utilization_pct REAL, compression_triggered INTEGER DEFAULT 0, model TEXT
);
CREATE INDEX IF NOT EXISTS idx_ctx_session ON context_pressure(session_id);
CREATE INDEX IF NOT EXISTS idx_ctx_time ON context_pressure(timestamp);

CREATE TABLE IF NOT EXISTS session_summary (
    session_id TEXT PRIMARY KEY, platform TEXT,
    start_time INTEGER, end_time INTEGER,
    total_llm_calls INTEGER DEFAULT 0, total_tool_calls INTEGER DEFAULT 0,
    total_prompt_tokens INTEGER DEFAULT 0, total_completion_tokens INTEGER DEFAULT 0,
    total_cost_usd REAL DEFAULT 0.0, final_status TEXT, user_canonical_name TEXT
);
CREATE INDEX IF NOT EXISTS idx_ss_time ON session_summary(start_time);
""")
    conn.commit()


def _conn() -> sqlite3.Connection:
    db = _get_db_path()
    conn = sqlite3.connect(db, check_same_thread=False)
    _ensure_schema(conn)
    return conn


# ── in-memory state (per session) ──────────────────────────────────────────
# We track turn-level counters because hooks fire multiple times per turn.
class _State:
    __slots__ = ("lock", "turn_tool_calls", "turn_llm_calls", "turn_start_time",
                 "context_limit", "context_used", "turn_number", "session_platform",
                 "session_user", "model", "task_id")
    def __init__(self):
        self.lock = threading.Lock()
        self.turn_tool_calls = 0
        self.turn_llm_calls = 0
        self.turn_start_time = 0.0
        self.context_limit = 0
        self.context_used = 0
        self.turn_number = 0
        self.session_platform = ""
        self.session_user = ""
        self.model = ""
        self.task_id = ""


_STATE: Dict[str, _State] = {}


def _state(sid: str) -> _State:
    if sid not in _STATE:
        _STATE[sid] = _State()
    return _STATE[sid]


def _safe_insert(table: str, fields: List[str], values: List[Any]) -> None:
    try:
        with _conn() as conn:
            placeholders = ",".join("?" * len(fields))
            conn.execute(
                f"INSERT INTO {table} ({','.join(fields)}) VALUES ({placeholders})",
                values,
            )
            conn.commit()
    except Exception as exc:
        logger.debug("Telemetry insert failed: %s", exc)


def _update_session(sid: str, delta_cost: float = 0.0,
                     delta_prompt: int = 0, delta_completion: int = 0,
                     llm_calls: int = 0, tool_calls: int = 0) -> None:
    try:
        with _conn() as conn:
            conn.execute("""
                INSERT INTO session_summary (session_id, total_llm_calls, total_tool_calls,
                    total_prompt_tokens, total_completion_tokens, total_cost_usd)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(session_id) DO UPDATE SET
                total_llm_calls = total_llm_calls + excluded.total_llm_calls,
                total_tool_calls = total_tool_calls + excluded.total_tool_calls,
                total_prompt_tokens = total_prompt_tokens + excluded.total_prompt_tokens,
                total_completion_tokens = total_completion_tokens + excluded.total_completion_tokens,
                total_cost_usd = total_cost_usd + excluded.total_cost_usd
            """, (sid, llm_calls, tool_calls, delta_prompt, delta_completion, delta_cost))
            conn.commit()
    except Exception as exc:
        logger.debug("Telemetry session update failed: %s", exc)


def _args_hash(args: Any) -> str:
    try:
        return hashlib.md5(json.dumps(args, sort_keys=True, default=str).encode()).hexdigest()
    except Exception:
        return ""


# ── hook callbacks ──────────────────────────────────────────────────────────

def on_session_start(session_id: str = "", model: str = "", platform: str = "",
                     sender_id: str = "", **_kw) -> None:
    st = _state(session_id)
    with st.lock:
        st.session_platform = platform or ""
        st.session_user = sender_id or ""
        st.model = model or ""
        st.turn_number = 0
    try:
        with _conn() as conn:
            conn.execute(
                "INSERT INTO session_summary (session_id, platform, start_time, user_canonical_name) "
                "VALUES (?, ?, ?, ?) ON CONFLICT(session_id) DO NOTHING",
                (session_id, platform or "", int(time.time()), sender_id or ""),
            )
            conn.commit()
    except Exception as exc:
        logger.debug("Telemetry on_session_start failed: %s", exc)


def pre_llm_call(session_id: str = "", model: str = "", task_id: str = "", **_kw) -> None:
    st = _state(session_id)
    with st.lock:
        st.model = model or st.model
        st.turn_start_time = time.time()
        if task_id:
            st.task_id = task_id


def post_llm_call(session_id: str = "", model: str = "", response_meta: dict = None,
                 total_tokens: int = 0, prompt_tokens: int = 0,
                 completion_tokens: int = 0, cache_read: int = 0,
                 cache_write: int = 0, reasoning_tokens: int = 0,
                 task_id: str = "", **_kw) -> None:
    st = _state(session_id)
    with st.lock:
        st.turn_llm_calls += 1
        duration_ms = int((time.time() - st.turn_start_time) * 1000) if st.turn_start_time else 0
    _safe_insert("llm_calls", [
        "session_id", "task_id", "model", "prompt_tokens", "completion_tokens", "total_tokens",
        "cache_read_tokens", "cache_write_tokens", "reasoning_tokens",
        "duration_ms", "status",
    ], [
        session_id, task_id or getattr(st, "task_id", "") or "", model or st.model,
        prompt_tokens or 0, completion_tokens or 0, total_tokens or 0,
        cache_read or 0, cache_write or 0, reasoning_tokens or 0,
        duration_ms, "success",
    ])
    _update_session(session_id, delta_prompt=prompt_tokens or 0,
                    delta_completion=completion_tokens or 0, llm_calls=1)


def pre_tool_call(tool_name: str = "", session_id: str = "", task_id: str = "", **_kw) -> None:
    st = _state(session_id)
    with st.lock:
        st.turn_tool_calls += 1
        st.turn_start_time = time.time()
        if task_id:
            st.task_id = task_id


def post_tool_call(tool_name: str = "", session_id: str = "", result: Any = None,
                   error: str = "", duration_ms: int = 0, args: Any = None,
                   task_id: str = "", **_kw) -> None:
    st = _state(session_id)
    # Check for real errors: either an explicit error param from the caller,
    # or a JSON result with a non-null error field. Many tools (e.g. terminal)
    # return {"error": null} on success — the word "error" in the string
    # is not enough to flag failure.
    has_error = bool(error)
    if not has_error and isinstance(result, str):
        try:
            parsed = json.loads(result)
            if isinstance(parsed, dict) and parsed.get("error"):
                has_error = True
        except (json.JSONDecodeError, ValueError):
            pass
    _safe_insert("tool_calls", [
        "session_id", "task_id", "tool_name", "status", "duration_ms", "args_hash", "error_message",
    ], [
        session_id, task_id or getattr(st, "task_id", "") or "", tool_name,
        "failure" if has_error else "success",
        duration_ms, _args_hash(args), error or "",
    ])
    _update_session(session_id, tool_calls=1)


def on_turn_start(session_id: str = "", turn_number: int = 0,
                  context_length: int = 0, context_limit: int = 0,
                  compression_triggered: bool = False,
                  model: str = "", **_kw) -> None:
    st = _state(session_id)
    with st.lock:
        st.turn_number = turn_number
        st.context_limit = context_limit
        st.context_used = context_length
        st.model = model or st.model
    utilization = round((context_length / context_limit) * 100, 2) if context_limit else 0.0
    _safe_insert("context_pressure", [
        "session_id", "turn_number", "context_used_chars", "context_limit_chars",
        "utilization_pct", "compression_triggered", "model",
    ], [
        session_id, turn_number, context_length, context_limit,
        utilization, 1 if compression_triggered else 0, model or st.model,
    ])


def register(ctx) -> None:
    ctx.register_hook("on_session_start", on_session_start)
    ctx.register_hook("on_session_end", on_session_end)
    ctx.register_hook("pre_tool_call", pre_tool_call)
    ctx.register_hook("post_tool_call", post_tool_call)
    ctx.register_hook("pre_llm_call", pre_llm_call)
    ctx.register_hook("post_llm_call", post_llm_call)
    ctx.register_hook("on_turn_start", on_turn_start)


def on_session_end(session_id: str = "", interrupted: bool = False,
                   completed: bool = False, **_kw) -> None:
    try:
        with _conn() as conn:
            status = "interrupted" if interrupted else ("completed" if completed else "unknown")
            conn.execute(
                "UPDATE session_summary SET end_time = ?, final_status = ? WHERE session_id = ?",
                (int(time.time()), status, session_id),
            )
            conn.commit()
    except Exception as exc:
        logger.debug("Telemetry on_session_end failed: %s", exc)
    finally:
        _STATE.pop(session_id, None)
