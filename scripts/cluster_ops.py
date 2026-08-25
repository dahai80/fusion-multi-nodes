#!/usr/bin/env python3
"""容器集群运维小工具 — 审批/查询/重置节点。压测前用。

用法:
    python scripts/cluster_ops.py approve-all --master 127.0.0.1:11452 --token <T>
    python scripts/cluster_ops.py status --master 127.0.0.1:11452 --token <T>
    python scripts/cluster_ops.py unban-all --master 127.0.0.1:11452 --token <T>
"""

from __future__ import annotations

import argparse
import json
import logging
import time
import urllib.request

logger = logging.getLogger("cluster_ops")


def _req(url: str, token: str, data: dict | None = None) -> dict:
    hdr = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    req = urllib.request.Request(
        url, data=json.dumps(data).encode() if data else None, headers=hdr, method="POST" if data else "GET"
    )
    try:
        return json.load(urllib.request.urlopen(req, timeout=8))
    except urllib.error.HTTPError as e:
        body = e.read().decode()[:160]
        return {"error": f"HTTP {e.code}: {body}"}
    except Exception as e:
        return {"error": str(e)}


def cmd_status(base: str, token: str) -> None:
    nodes = _req(f"{base}/api/nodes", token)
    nl = nodes.get("nodes") if isinstance(nodes, dict) else nodes
    if not isinstance(nl, list):
        print("nodes 响应异常:", nodes)
        return
    online = 0
    for n in nl:
        st = n.get("status", "?")
        if st == "online":
            online += 1
        print(
            f"  {n.get('node_id','?'):14} {n.get('hostname','?'):16} ip={n.get('ip_address','?'):16} "
            f"status={st:8} active={n.get('active_tasks','?')}/{n.get('max_tasks','?')} "
            f"mem={n.get('available_memory_gb','?')}"
        )
    print(f"online {online}/{len(nl)}")


def cmd_approve_all(base: str, token: str) -> None:
    for _ in range(20):
        pend = _req(f"{base}/api/nodes/pending", token)
        lst = pend.get("pending") or pend.get("requests") or []
        if isinstance(pend, list):
            lst = pend
        if len(lst) >= 1:
            break
        time.sleep(2)
    pend = _req(f"{base}/api/nodes/pending", token)
    lst = pend.get("pending") or pend.get("requests") or []
    if isinstance(pend, list):
        lst = pend
    print(f"pending: {len(lst)}")
    for r in lst:
        nid = r.get("node_id", "")
        res = _req(f"{base}/api/nodes/approve", token, {"node_id": nid})
        print(f"  approve {nid}: {res}")
    time.sleep(3)
    cmd_status(base, token)


def cmd_unban_all(base: str, token: str) -> None:
    nodes = _req(f"{base}/api/nodes", token)
    nl = nodes.get("nodes") if isinstance(nodes, dict) else nodes
    if not isinstance(nl, list):
        print("nodes 响应异常:", nodes)
        return
    for n in nl:
        if n.get("status") == "fault":
            nid = n.get("node_id", "")
            res = _req(f"{base}/api/nodes/unban", token, {"node_id": nid})
            print(f"  unban {nid}: {res}")
    time.sleep(1)
    cmd_status(base, token)


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    p = argparse.ArgumentParser(description="fusion-multi-node 容器集群运维工具")
    p.add_argument("cmd", choices=["approve-all", "status", "unban-all"])
    p.add_argument("--master", default="127.0.0.1:11452")
    p.add_argument("--token", required=True)
    args = p.parse_args()
    base = f"http://{args.master}"
    if args.cmd == "approve-all":
        cmd_approve_all(base, args.token)
    elif args.cmd == "status":
        cmd_status(base, args.token)
    elif args.cmd == "unban-all":
        cmd_unban_all(base, args.token)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
