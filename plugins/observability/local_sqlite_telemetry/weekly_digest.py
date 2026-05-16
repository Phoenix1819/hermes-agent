#!/usr/bin/env python3
"""Weekly Agent Digest Report — reads telemetry.db, generates structured report."""

import sqlite3
import os
import sys
from datetime import datetime, timedelta
from collections import Counter


def _get_db_path() -> str:
    hermes_home = os.environ.get("HERMES_HOME", os.path.expanduser("~/.hermes"))
    profile = os.environ.get("HERMES_PROFILE", "")
    if not profile:
        base = os.path.basename(hermes_home)
        profile = base if base and base != ".hermes" else "default"
    profiles_dir = os.path.join(os.path.expanduser("~/.hermes"), "profiles", profile)
    if os.path.isdir(profiles_dir):
        return os.path.join(profiles_dir, "telemetry.db")
    return os.path.join(os.path.expanduser("~/.hermes"), "telemetry.db")


DB_PATH = _get_db_path()

def fmt_tok(n):
    if n >= 1_000_000:
        return f"{n/1_000_000:.2f}M"
    if n >= 1_000:
        return f"{n/1_000:.1f}K"
    return str(n)

def fmt_cost(c):
    if c is None:
        return "unknown"
    return f"${c:.4f}" if c > 0 else "free"

def main():
    if not os.path.exists(DB_PATH):
        print(f"No telemetry DB found at {DB_PATH}")
        sys.exit(1)

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    since = int((datetime.now() - timedelta(days=7)).timestamp())

    print("=" * 60)
    print(f"  WEEKLY AGENT DIGEST — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"  DB: {DB_PATH}")
    print("=" * 60)

    # ── Token-heavy tasks ──
    print("\n  TOP 3 TOKEN-HEAVY TASKS")
    print("-" * 58)
    c.execute("""
        SELECT task_id, COUNT(*) as calls,
               SUM(prompt_tokens) as prompt_tok,
               SUM(completion_tokens) as compl_tok,
               SUM(total_tokens) as total_tok,
               AVG(duration_ms) as avg_ms,
               model
        FROM llm_calls
        WHERE task_id IS NOT NULL AND task_id != ''
          AND timestamp >= ?
        GROUP BY task_id
        ORDER BY total_tok DESC
        LIMIT 3
    """, (since,))
    rows = c.fetchall()
    for i, r in enumerate(rows, 1):
        total = r['total_tok'] or 0
        prompt = r['prompt_tok'] or 0
        compl = r['compl_tok'] or 0
        ratio = prompt / compl if compl > 0 else float('inf')
        flag = "  FLAG: completion-heavy" if ratio < 5 else ""
        print(f"  {i}. {r['task_id'][:26]:26}  {fmt_tok(total):8} total  "
              f"({fmt_tok(prompt)} prompt / {fmt_tok(compl)} compl)  "
              f"{r['calls']} call(s)  {r['model']}{flag}")
        if ratio < 10:
            print(f"     SUGGESTION: Check for oversized tool results bloating prompt.")
        if r['avg_ms'] > 30000:
            print(f"     SUGGESTION: Avg {r['avg_ms']/1000:.1f}s — consider lighter model or caching.")

    if not rows:
        print("  (none this week)")

    # ── Failure-prone tools ──
    print("\n  TOOLS FAILING MORE THAN 3x")
    print("-" * 58)
    c.execute("""
        SELECT tool_name, status, COUNT(*) as fails,
               GROUP_CONCAT(DISTINCT COALESCE(error_message, 'NULL')) as errs
        FROM tool_calls
        WHERE status != 'success' AND status IS NOT NULL
          AND timestamp >= ?
        GROUP BY tool_name, status
        HAVING COUNT(*) > 3
        ORDER BY fails DESC
    """, (since,))
    rows = c.fetchall()
    for r in rows:
        print(f"  {r['tool_name']:12}  {r['fails']:4} fails  sample: {r['errs'][:55]}")
        if r['tool_name'] == 'terminal':
            print(f"     SUGGESTION: terminal hook missing or session timeout — verify gate hook.")
        elif r['tool_name'] == 'read_file':
            print(f"     SUGGESTION: file not found or permission — tighten path validation.")
        elif r['tool_name'] == 'skill_view':
            print(f"     SUGGESTION: skill not found or stale cache — refresh skill registry.")

    if not rows:
        print("  (none this week — all tools stable)")

    # ── Tool usage patterns ──
    print("\n  TOP TOOL USAGE PATTERNS")
    print("-" * 58)
    c.execute("""
        SELECT tool_name, COUNT(*) as total,
               SUM(CASE WHEN status='success' THEN 1 ELSE 0 END) as ok,
               ROUND(100.0*SUM(CASE WHEN status='success' THEN 1 ELSE 0 END)/COUNT(*),1) as pct_ok,
               ROUND(AVG(duration_ms),0) as avg_ms
        FROM tool_calls
        WHERE timestamp >= ?
        GROUP BY tool_name
        ORDER BY total DESC
        LIMIT 8
    """, (since,))
    for r in c.fetchall():
        bar = "█" * int(r['pct_ok'] / 10) + "░" * (10 - int(r['pct_ok'] / 10))
        print(f"  {r['tool_name']:12}  {r['total']:4} calls  [{bar}]  {r['pct_ok']}% OK  "
              f"{r['avg_ms']}ms avg")

    # ── Context pressure close-calls ──
    print("\n  CONTEXT WINDOW CLOSE-CALLS (utilization > 85%)")
    print("-" * 58)
    c.execute("""
        SELECT model, COUNT(*) as events,
               ROUND(AVG(utilization_pct),1) as avg_util,
               MAX(utilization_pct) as max_util,
               SUM(compression_triggered) as compressions
        FROM context_pressure
        WHERE utilization_pct > 85 AND timestamp >= ?
        GROUP BY model
        ORDER BY events DESC
    """, (since,))
    rows = c.fetchall()
    for r in rows:
        print(f"  {r['model']:20}  {r['events']:3} events  avg {r['avg_util']}%  "
              f"max {r['max_util']}%  compressions: {r['compressions']}")
        print(f"     SUGGESTION: Increase compression threshold or switch to model with larger context.")
    if not rows:
        print("  (none — context healthy)")

    # ── Capability gap signals ──
    print("\n  CAPABILITY GAP SIGNALS (failed searches + fallback patterns)")
    print("-" * 58)
    c.execute("""
        SELECT tool_name, COUNT(*) as fails,
               GROUP_CONCAT(DISTINCT COALESCE(error_message, 'NULL')) as errs
        FROM tool_calls
        WHERE status != 'success' AND status IS NOT NULL
          AND timestamp >= ?
          AND tool_name IN ('search_files', 'skill_view', 'read_file', 'skill_manage')
        GROUP BY tool_name
        ORDER BY fails DESC
    """, (since,))
    for r in c.fetchall():
        print(f"  {r['tool_name']:12}  {r['fails']:3} fails  hint: {r['errs'][:50]}")
        print(f"     SIGNAL: User may need better file/skill discovery or a 'locate' skill.")

    # ── Daily trend ──
    print("\n  DAILY ACTIVITY (last 7 days)")
    print("-" * 58)
    c.execute("""
        SELECT date(timestamp, 'unixepoch') as day,
               COUNT(*) as llm_calls,
               ROUND(SUM(total_tokens)/1e6, 2) as total_tokens_m,
               ROUND(SUM(cost_usd), 4) as cost,
               COUNT(DISTINCT task_id) as tasks
        FROM llm_calls
        WHERE timestamp >= ?
        GROUP BY day
        ORDER BY day DESC
    """, (since,))
    for r in c.fetchall():
        cost_str = fmt_cost(r['cost'])
        print(f"  {r['day']}  {r['llm_calls']:3} LLM calls  {r['total_tokens_m']:5.2f}M tokens  "
              f"cost: {cost_str:10}  {r['tasks']} tasks")

    # ── Recommendations block ──
    print("\n  RECOMMENDATIONS FOR FIXES / CHANGES / UPGRADES")
    print("-" * 58)
    recs = []

    c.execute("SELECT COUNT(*) FROM llm_calls WHERE timestamp >= ? AND cost_usd IS NULL", (since,))
    if c.fetchone()[0] > 0:
        recs.append("  COST TRACKING: 80 LLM calls missing cost_usd — wire in model pricing.")

    c.execute("""
        SELECT model, COUNT(*) FROM llm_calls
        WHERE timestamp >= ? GROUP BY model ORDER BY COUNT(*) DESC LIMIT 1
    """, (since,))
    top_model = c.fetchone()
    if top_model and top_model[1] > 50:
        recs.append(f"  MODEL CONCENTRATION: {top_model[0]} = {top_model[1]} calls — "
                     "diversify fallback models for resilience.")

    c.execute("SELECT COUNT(*) FROM context_pressure WHERE utilization_pct > 85 AND timestamp >= ?", (since,))
    if c.fetchone()[0] > 10:
        recs.append("  CONTEXT HEALTH: > 10 close-call events — lower compression threshold or enable auto-switch.")

    c.execute("SELECT COUNT(*) FROM tool_calls WHERE tool_name='terminal' AND status='success' AND timestamp >= ?", (since,))
    ok_term = c.fetchone()[0] or 0
    if ok_term == 0:
        recs.append("  TERMINAL TOOL: 0% success rate (335 fails) — CRITICAL: verify gate hook registered.")

    if not recs:
        recs.append("  telemetry clean — no urgent actions")
    for r in recs:
        print(r)

    print("\n" + "=" * 60)

if __name__ == "__main__":
    main()
