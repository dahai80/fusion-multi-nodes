#!/usr/bin/env python3
"""容器集群真压测客户端 — 经 master:11452 并发提交任务, 测吞吐/尾延迟/成功率。

用法:
    python scripts/stress_live.py --master 127.0.0.1:11452 --token <TOKEN> \
        --tasks 100 --concurrency 20 --model mlx-community-Llama-3.2-1B-Instruct-4bit

阶段3: 吞吐/延迟基线 (N=4 节点, 并发 M 任务)。
阶段4: 故障注入 (kill master→HA / kill agent→重派 / 网络分区→熔断)。

输出: JSON 汇总 + 逐任务 CSV → stress-result-<ts>.json / .csv
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import statistics
import sys
import time
from typing import Any

import httpx

logger = logging.getLogger("stress_live")

DEFAULT_PROMPT = "Say hello in one word."
DEFAULT_MAX_TOKENS = 16


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="fusion-multi-node 容器集群压测客户端")
    p.add_argument("--master", default="127.0.0.1:11452", help="master host:port")
    p.add_argument("--token", required=True, help="集群 Bearer token")
    p.add_argument("--tasks", type=int, default=100, help="提交任务总数")
    p.add_argument("--concurrency", type=int, default=20, help="并发提交数")
    p.add_argument("--model", default="mlx-community-Llama-3.2-1B-Instruct-4bit")
    p.add_argument("--prompt", default=DEFAULT_PROMPT)
    p.add_argument("--max-tokens", type=int, default=DEFAULT_MAX_TOKENS)
    p.add_argument("--mode", default="data", choices=["data", "pipeline"])
    p.add_argument("--timeout", type=float, default=120.0, help="单任务最长等待秒")
    p.add_argument("--rps", type=float, default=0.0, help="客户端提交速率上限 req/s (0=不限)。对齐上游限流桶避免 429")
    p.add_argument("--out-prefix", default="stress-result", help="输出文件前缀")
    p.add_argument("--tag", default="", help="结果标签 (写入 JSON)")
    return p.parse_args()


async def submit_and_poll(
    client: httpx.AsyncClient,
    base: str,
    headers: dict[str, str],
    payload: dict[str, Any],
    timeout: float,
    idx: int,
) -> dict[str, Any]:
    """提交单任务 → 轮询至终态, 返回时延/状态记录。"""
    t0 = time.monotonic()
    rec: dict[str, Any] = {"idx": idx}
    try:
        resp = await client.post(f"{base}/api/tasks/submit", json=payload, headers=headers, timeout=15.0)
        if resp.status_code != 200:
            rec["status"] = "submit_error"
            rec["error"] = f"HTTP {resp.status_code}: {resp.text[:120]}"
            rec["latency"] = time.monotonic() - t0
            return rec
        tid = resp.json().get("task_id", "")
        rec["task_id"] = tid
        if not tid:
            rec["status"] = "no_task_id"
            rec["latency"] = time.monotonic() - t0
            return rec
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            pr = await client.get(f"{base}/api/tasks/{tid}", headers=headers, timeout=10.0)
            if pr.status_code == 200:
                st = pr.json().get("status", "")
                if st in ("completed", "failed", "timeout"):
                    rec["status"] = st
                    rec["error"] = pr.json().get("error", "")[:120]
                    rec["latency"] = time.monotonic() - t0
                    return rec
            await asyncio.sleep(0.5)
        rec["status"] = "client_timeout"
        rec["latency"] = time.monotonic() - t0
        return rec
    except Exception as e:
        rec["status"] = "exception"
        rec["error"] = f"{type(e).__name__}: {e}"[:120]
        rec["latency"] = time.monotonic() - t0
        return rec


async def run_phase3(args: argparse.Namespace) -> dict[str, Any]:
    """阶段3: 并发提交 M 任务, 测吞吐/尾延迟/成功率。"""
    base = f"http://{args.master}"
    headers = {"Authorization": f"Bearer {args.token}", "Content-Type": "application/json"}
    payload_base = {
        "name": "stress",
        "model_name": args.model,
        "prompt": args.prompt,
        "mode": args.mode,
        "max_tokens": args.max_tokens,
    }
    logger.info(f"阶段3 启动: tasks={args.tasks} concurrency={args.concurrency} model={args.model} rps={args.rps}")
    sem = asyncio.Semaphore(args.concurrency)
    records: list[dict[str, Any]] = []
    t_start = time.monotonic()

    # 客户端速率门 — 对齐上游限流桶 (fusion-mlx 60rpm/key, 多 agent 共享 key=1 桶)。
    # token bucket: 每 1/rps 秒放一张提交券, 提交前 acquire。
    rps = max(0.0, args.rps)
    bucket_tokens = 0.0
    bucket_lock = asyncio.Lock()

    async def acquire_rate() -> None:
        nonlocal bucket_tokens
        if rps <= 0:
            return
        interval = 1.0 / rps
        while True:
            async with bucket_lock:
                if bucket_tokens >= 1.0:
                    bucket_tokens -= 1.0
                    return
                bucket_tokens += 1.0
            await asyncio.sleep(interval)

    async with httpx.AsyncClient() as client:
        async def one(idx: int) -> dict[str, Any]:
            async with sem:
                await acquire_rate()
                payload = dict(payload_base, name=f"stress-{idx}")
                return await submit_and_poll(client, base, headers, payload, args.timeout, idx)

        records = await asyncio.gather(*[one(i) for i in range(args.tasks)])

    wall = time.monotonic() - t_start
    summary = summarize(records, wall, args)
    write_outputs(summary, records, args.out_prefix)
    return summary


def summarize(records: list[dict[str, Any]], wall: float, args: argparse.Namespace) -> dict[str, Any]:
    """汇总压测指标。"""
    total = len(records)
    by_status: dict[str, int] = {}
    lats: list[float] = []
    for r in records:
        s = r.get("status", "unknown")
        by_status[s] = by_status.get(s, 0) + 1
        if s == "completed":
            lats.append(r.get("latency", 0.0))
    completed = by_status.get("completed", 0)
    summary = {
        "tag": args.tag,
        "tasks": total,
        "concurrency": args.concurrency,
        "model": args.model,
        "wall_seconds": round(wall, 3),
        "throughput_task_per_s": round(completed / wall, 3) if wall > 0 else 0,
        "by_status": by_status,
        "success_rate": round(completed / total, 4) if total else 0,
        "latency": {},
        "errors": [r for r in records if r.get("status") != "completed"][:20],
    }
    if lats:
        sl = sorted(lats)
        summary["latency"] = {
            "count": len(lats),
            "mean_s": round(statistics.mean(lats), 3),
            "median_s": round(statistics.median(lats), 3),
            "p95_s": round(sl[int(len(sl) * 0.95) - 1], 3) if len(sl) >= 20 else round(sl[-1], 3),
            "p99_s": round(sl[int(len(sl) * 0.99) - 1], 3) if len(sl) >= 100 else round(sl[-1], 3),
            "max_s": round(sl[-1], 3),
        }
    return summary


def write_outputs(summary: dict[str, Any], records: list[dict[str, Any]], prefix: str) -> None:
    """落盘 JSON + CSV。"""
    ts = int(time.time())
    jpath = f"{prefix}-{ts}.json"
    cpath = f"{prefix}-{ts}.csv"
    with open(jpath, "w") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    with open(cpath, "w") as f:
        f.write("idx,task_id,status,latency,error\n")
        for r in records:
            f.write(f"{r.get('idx','')},{r.get('task_id','')},{r.get('status','')},"
                    f"{r.get('latency',0):.3f},{(r.get('error','') or '').replace(',',';')}\n")
    logger.info(f"结果已写: {jpath} / {cpath}")


async def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args()
    summary = await run_phase3(args)
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0 if summary["success_rate"] >= 0.95 else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
