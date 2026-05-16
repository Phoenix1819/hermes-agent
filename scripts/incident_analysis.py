#!/usr/bin/env python3
"""Incident board analysis — query and report on escalations."""

from __future__ import annotations

import argparse
import os
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

DEFAULT_DB = os.path.expanduser("~/.hermes/kanban/boards/incidents/kanban.db")


def _ts(unix: Optional[int]) -> str:
    if unix is None:
        return "—"
    return datetime.fromtimestamp(unix, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def _query(
    db_path: str,
    status: Optional[str] = None,
    severity: Optional[str] = None,
    category: Optional[str] = None,
    since_hours: Optional[int] = None,
    limit: int = 50,
) -> List[Dict[str, Any]]:
    sql = "SELECT id, title, body, status, priority, created_at, tenant FROM tasks WHERE 1=1"
    params: list = []
    if status:
        sql += " AND status = ?"
        params.append(status)
    if since_hours:
        cutoff = int((datetime.now(timezone.utc) - timedelta(hours=since_hours)).timestamp())
        sql += " AND created_at >= ?"
        params.append(cutoff)
    sql += " ORDER BY created_at DESC LIMIT ?"
    params.append(limit)

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(sql, params).fetchall()
        return [dict(r) for r in rows]


def _stats(db_path: str, since_hours: Optional[int] = None) -> Dict[str, Any]:
    cutoff = None
    if since_hours:
        cutoff = int((datetime.now(timezone.utc) - timedelta(hours=since_hours)).timestamp())

    with sqlite3.connect(db_path) as conn:
        total = conn.execute("SELECT COUNT(*) FROM tasks" + (" WHERE created_at >= ?" if cutoff else ""),
                             ([cutoff] if cutoff else [])).fetchone()[0]
        by_status = conn.execute(
            "SELECT status, COUNT(*) FROM tasks" + (" WHERE created_at >= ?" if cutoff else "") + " GROUP BY status",
            ([cutoff] if cutoff else [])
        ).fetchall()
        by_tenant = conn.execute(
            "SELECT tenant, COUNT(*) FROM tasks" + (" WHERE created_at >= ?" if cutoff else "") + " GROUP BY tenant",
            ([cutoff] if cutoff else [])
        ).fetchall()
        blocked = conn.execute(
            "SELECT COUNT(*) FROM tasks WHERE status = 'blocked'" + (" AND created_at >= ?" if cutoff else ""),
            ([cutoff] if cutoff else [])
        ).fetchone()[0]
    return {
        "total": total,
        "by_status": dict(by_status),
        "by_tenant": dict(by_tenant),
        "blocked": blocked,
    }


def cmd_list(args: argparse.Namespace) -> None:
    rows = _query(
        args.db,
        status=args.status,
        since_hours=args.since,
        limit=args.limit,
    )
    if not rows:
        print("No incidents found.")
        return
    for r in rows:
        pri = "CRIT" if r["priority"] >= 10 else "HIGH" if r["priority"] >= 5 else "NORM"
        print(f"[{pri}] {r['id']} | {r['status']:8} | {r['tenant'] or '—':10} | {_ts(r['created_at'])}")
        print(f"      {r['title']}")
        if args.body and r["body"]:
            for line in r["body"].splitlines():
                print(f"      {line}")
        print()


def cmd_stats(args: argparse.Namespace) -> None:
    s = _stats(args.db, since_hours=args.since)
    print(f"Total incidents: {s['total']}")
    print(f"Blocked (needs attention): {s['blocked']}")
    print("\nBy status:")
    for st, cnt in s["by_status"].items():
        print(f"  {st:10} {cnt}")
    print("\nBy category (tenant):")
    for t, cnt in s["by_tenant"].items():
        print(f"  {t or '—':10} {cnt}")


def cmd_triage(args: argparse.Namespace) -> None:
    """Show blocked incidents with full detail for triage."""
    rows = _query(args.db, status="blocked", since_hours=args.since, limit=args.limit)
    if not rows:
        print("No blocked incidents — all clear.")
        return
    print(f"=== {len(rows)} BLOCKED INCIDENT(S) ===\n")
    for r in rows:
        pri = "CRIT" if r["priority"] >= 10 else "HIGH" if r["priority"] >= 5 else "NORM"
        print(f"ID:       {r['id']}")
        print(f"Priority: {pri}")
        print(f"Category: {r['tenant'] or '—'}")
        print(f"Created:  {_ts(r['created_at'])}")
        print(f"Title:    {r['title']}")
        if r["body"]:
            print(f"Body:\n{r['body']}")
        print("-" * 60)


def main() -> int:
    parser = argparse.ArgumentParser(description="Incident board analysis")
    parser.add_argument("--db", default=DEFAULT_DB, help="Path to kanban DB")
    parser.add_argument("--since", type=int, help="Only show incidents from last N hours")
    sub = parser.add_subparsers(dest="command")

    p_list = sub.add_parser("list", help="List incidents")
    p_list.add_argument("--status", help="Filter by status")
    p_list.add_argument("--limit", type=int, default=50)
    p_list.add_argument("--body", action="store_true", help="Show full body")

    p_stats = sub.add_parser("stats", help="Summary statistics")

    p_triage = sub.add_parser("triage", help="Show blocked incidents for triage")
    p_triage.add_argument("--limit", type=int, default=50)

    args = parser.parse_args()
    if not os.path.exists(args.db):
        print(f"Database not found: {args.db}", file=sys.stderr)
        return 1

    if args.command == "list":
        cmd_list(args)
    elif args.command == "stats":
        cmd_stats(args)
    elif args.command == "triage":
        cmd_triage(args)
    else:
        parser.print_help()
    return 0


if __name__ == "__main__":
    sys.exit(main())
