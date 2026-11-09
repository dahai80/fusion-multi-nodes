#!/usr/bin/env python3
"""Synthetic demo-data seeder for preview stacks (#108).

Seeds deterministic fake sessions into the cluster session store via the
master API — never real customer data (business/AGENTS.md rule 6 red line).
Session-store endpoints need no inference backend, so a preview stack is
fully demonstrable without fusion-mlx.

Usage: synthetic_seed.py --master-url http://127.0.0.1:21452 --token TOKEN [--count 3]
"""

from __future__ import annotations

import argparse
import json
import sys
import urllib.request

# Deterministic fixtures — identical output on every run, obviously fake.
SYNTHETIC_SESSIONS = [
    {
        "id": "preview-demo-sprint-planning",
        "title": "[SYNTHETIC DEMO] Sprint planning transcript",
        "app": "demo",
        "messages": [
            {"role": "user", "content": "Synthetic demo message: summarize sprint goals"},
            {"role": "assistant", "content": "Synthetic demo reply: goals are fixture data"},
        ],
    },
    {
        "id": "preview-demo-code-review",
        "title": "[SYNTHETIC DEMO] Code review session",
        "app": "demo",
        "messages": [
            {"role": "user", "content": "Synthetic demo message: review the fixture module"},
            {"role": "assistant", "content": "Synthetic demo reply: fixture looks consistent"},
        ],
    },
    {
        "id": "preview-demo-onboarding",
        "title": "[SYNTHETIC DEMO] Onboarding walkthrough",
        "app": "demo",
        "messages": [
            {"role": "user", "content": "Synthetic demo message: show the cluster dashboard"},
            {"role": "assistant", "content": "Synthetic demo reply: nodes and tasks are fixtures"},
        ],
    },
]


def seed(master_url: str, token: str, count: int) -> int:
    sessions = SYNTHETIC_SESSIONS[:count]
    created = 0
    for session in sessions:
        body = json.dumps(session).encode()
        req = urllib.request.Request(
            f"{master_url}/api/v1/session-store/{session['id']}",
            data=body,
            method="PUT",
            headers={"Content-Type": "application/json", "Authorization": f"Bearer {token}"},
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            if resp.status in (200, 201):
                created += 1
            else:
                print(f"[WARN] seed {session['id']} -> HTTP {resp.status}", file=sys.stderr)
    print(f"seeded {created}/{len(sessions)} synthetic sessions")
    return 0 if created == len(sessions) else 1


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--master-url", required=True)
    ap.add_argument("--token", required=True)
    ap.add_argument("--count", type=int, default=len(SYNTHETIC_SESSIONS))
    args = ap.parse_args()
    return seed(args.master_url, args.token, min(args.count, len(SYNTHETIC_SESSIONS)))


if __name__ == "__main__":
    sys.exit(main())
