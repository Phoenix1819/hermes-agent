#!/usr/bin/env python3
"""Telemetry CLI — query ~/.hermes/profiles/<profile>/telemetry.db

Usage:
    python telemetry_cli.py report [--days 7]
    python telemetry_cli.py tools [--days 7]
    python telemetry_cli.py cost [--days 7]
    python telemetry_cli.py sessions [--days 7]
"""
import sqlite3, os, sys, argparse, json
from datetime import datetime, timedelta


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


_DEFAULT_DB = _get_db_path()

def _db():
    return sqlite3.connect(os.environ.get("TELEMETRY_DB", _DEFAULT_DB))

def _since(days: int):
    return int((datetime.now() - timedelta(days=days)).timestamp())

def report(days: int = 7):
    since = _since(days)
    with _db() as conn:
        row = conn.execute("""
            SELECT COUNT(*), SUM(total_llm_calls), SUM(total_tool_calls),
                   SUM(total_prompt_tokens), SUM(total_completion_tokens),
                   SUM(total_cost_usd)
            FROM session_summary
            WHERE start_time >= ?
        """, (since,)).fetchone()
        sessions, llm, tools, prompt, completion, cost = row
        cost = cost or 0.0
        conn.execute("""
            SELECT tool_name, COUNT(*),
                   SUM(CASE WHEN status='failure' THEN 1 ELSE 0 END),
                   ROUND(AVG(duration_ms),0)
            FROM tool_calls
            WHERE timestamp >= ?
            GROUP BY tool_name
            ORDER BY COUNT(*) DESC
            LIMIT 10
        """, (since,))
        top_tools = conn.fetchall()
        conn.execute("""
            SELECT ROUND(AVG(utilization_pct),1), MAX(utilization_pct),
                   SUM(compression_triggered)
            FROM context_pressure
            WHERE timestamp >= ?
        """, (since,))
        avg_ctx, max_ctx, compress = conn.fetchone()
    lines = [
        f"Telemetry Report (last {days} days)",
        f"Sessions: {sessions}",
        f"LLM calls: {llm}",
        f"Tool calls: {tools}",
        f"Tokens: prompt={prompt} completion={completion}",
        f"Estimated cost: ${cost:.4f}",
        "",
        "Top tools:",
    ]
    for name, cnt, fails, avg_ms in top_tools:
        lines.append(f"  {name}: {cnt} calls, {fails} failures, avg {avg_ms}ms")
    lines.extend([
        "",
        f"Context pressure: avg={avg_ctx}% max={max_ctx}% compressions={compress}",
    ])
    print("\n".join(lines))

def tools(days: int = 7):
    since = _since(days)
    with _db() as conn:
        rows = conn.execute("""
            SELECT tool_name, COUNT(*) total,
                   SUM(CASE WHEN status='failure' THEN 1 ELSE 0 END) fails,
                   ROUND(100.0 * SUM(CASE WHEN status='failure' THEN 1 ELSE 0 END) / COUNT(*), 1) fail_pct,
                   ROUND(AVG(duration_ms),0) avg_ms,
                   MAX(duration_ms) max_ms
            FROM tool_calls
            WHERE timestamp >= ?
            GROUP BY tool_name
            ORDER BY total DESC
        """, (since,)).fetchall()
    print(f"{'Tool':<25} {'Calls':>6} {'Fails':>6} {'Fail%':>6} {'AvgMs':>8} {'MaxMs':>8}")
    print("-" * 65)
    for name, total, fails, fail_pct, avg_ms, max_ms in rows:
        print(f"{name:<25} {total:>6} {fails:>6} {fail_pct:>6} {avg_ms:>8} {max_ms:>8}")

def cost_breakdown(days: int = 7):
    since = _since(days)
    with _db() as conn:
        rows = conn.execute("""
            SELECT session_id, platform, total_llm_calls, total_prompt_tokens,
                   total_completion_tokens, total_cost_usd, final_status
            FROM session_summary
            WHERE start_time >= ?
            ORDER BY total_cost_usd DESC
        """, (since,)).fetchall()
    print(f"{'Session':<36} {'Platform':<10} {'LLM':>5} {'Prompt':>8} {'Complete':>8} {'Cost':>8} {'Status'}")
    print("-" * 95)
    total = 0.0
    for sid, plat, llm, prompt, comp, cost, status in rows:
        total += cost or 0.0
        print(f"{sid:<36} {plat:<10} {llm:>5} {prompt:>8} {comp:>8} ${cost:>7.4f} {status}")
    print(f"\nTotal estimated cost: ${total:.4f}")

def sessions(days: int = 7):
    since = _since(days)
    with _db() as conn:
        rows = conn.execute("""
            SELECT session_id, platform, start_time, end_time,
                   total_llm_calls, total_tool_calls, total_cost_usd, final_status
            FROM session_summary
            WHERE start_time >= ?
            ORDER BY start_time DESC
        """, (since,)).fetchall()
    print(f"{'Session':<36} {'Platform':<10} {'Start':<20} {'LLM':>4} {'Tools':>5} {'Cost':>8} {'Status'}")
    print("-" * 100)
    for sid, plat, st, et, llm, tools, cost, status in rows:
        start = datetime.fromtimestamp(st).strftime("%Y-%m-%d %H:%M") if st else ""
        print(f"{sid:<36} {plat:<10} {start:<20} {llm:>4} {tools:>5} ${cost:>7.4f} {status}")

def main():
    parser = argparse.ArgumentParser(description="Telemetry CLI for Hermes")
    sub = parser.add_subparsers(dest="cmd")
    for name, fn in [("report", report), ("tools", tools), ("cost", cost_breakdown), ("sessions", sessions)]:
        p = sub.add_parser(name)
        p.add_argument("--days", type=int, default=7)
        p.set_defaults(func=fn)
    args = parser.parse_args()
    if not getattr(args, "func", None):
        parser.print_help()
        return
    args.func(args.days)

if __name__ == "__main__":
    main()
