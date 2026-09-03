"""#73 SupervisorBridge — agent 本机调用 fusion-sv CLI (进程协调)。

fusion-sv (fusion-supervisor) 通过 UDS (/tmp/fusion-sv.sock) 管理本机服务生命周期。
agent 与 supervisor 同机部署, 经 CLI `fusion-sv <op> [svc]` shell-out 调用。
跨节点 supervisor 操作 = master HTTP → 对端 agent → 本机 shell-out。

离线安全: fusion-sv 未安装 (FileNotFoundError) → 返 available=False, 不崩溃
(agent 仍服务推理)。env FUSION_SV_BIN 覆盖二进制路径 (默认 fusion-sv)。
"""

from __future__ import annotations

import json
import logging
import os
import subprocess

logger = logging.getLogger(__name__)

# 允许的 supervisor 操作白名单 (防注入未知 op)。
_SUPERVISOR_OPS = {"status", "drain", "rollout", "shutdown", "backup"}


class SupervisorBridge:
    """本机 fusion-sv CLI 包装。"""

    def __init__(self, bin_path: str | None = None, default_timeout: float = 10.0):
        self._bin = bin_path or os.environ.get("FUSION_SV_BIN", "fusion-sv")
        self._default_timeout = default_timeout

    def call(self, op: str, svc: str = "", timeout: float | None = None) -> dict:
        """执行 `fusion-sv <op> [svc]`, 解析输出。

        返回:
          - 成功: {"ok": True, "op": op, "svc": svc, "output": <parsed>, "available": True}
          - fusion-sv 未安装: {"ok": False, "available": False, "error": "fusion-sv not installed"}
          - 未知 op: {"ok": False, "available": True, "error": "unknown op"}
          - 执行失败: {"ok": False, "available": True, "error": <msg>, "returncode": N}
        output 解析: status 输出尝试 JSON 解析, 失败回退纯文本。
        """
        if op not in _SUPERVISOR_OPS:
            logger.warning(f"#73 supervisor 拒未知 op: {op!r}")
            return {"ok": False, "available": True, "error": f"unknown op: {op}", "op": op}
        cmd = [self._bin, op]
        if svc:
            cmd.append(svc)
        to = timeout if timeout is not None else self._default_timeout
        logger.info(f"#73 supervisor 调用: {' '.join(cmd)} (timeout={to}s)")
        try:
            proc = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=to,
            )
        except FileNotFoundError:
            logger.debug(f"#73 fusion-sv 未安装 ({self._bin}), supervisor 不可用")
            return {
                "ok": False,
                "available": False,
                "error": "fusion-sv not installed",
                "op": op,
                "svc": svc,
            }
        except subprocess.TimeoutExpired:
            logger.warning(f"#73 supervisor 调用超时: {op} ({to}s)")
            return {
                "ok": False,
                "available": True,
                "error": f"timeout after {to}s",
                "op": op,
                "svc": svc,
            }
        if proc.returncode != 0:
            logger.warning(f"#73 supervisor 调用失败: {op} rc={proc.returncode} stderr={proc.stderr.strip()[:200]}")
            return {
                "ok": False,
                "available": True,
                "error": proc.stderr.strip() or f"exit {proc.returncode}",
                "returncode": proc.returncode,
                "op": op,
                "svc": svc,
            }
        raw = proc.stdout.strip()
        parsed: object = raw
        if op == "status" and raw:
            try:
                parsed = json.loads(raw)
            except (json.JSONDecodeError, ValueError):
                parsed = raw
        logger.info(f"#73 supervisor 调用成功: {op} rc=0")
        return {
            "ok": True,
            "available": True,
            "op": op,
            "svc": svc,
            "output": parsed,
        }

    def ping(self) -> bool:
        """轻量探测 fusion-sv 可用 (status 调用, 3s 超时)。供心跳上报 supervisor_available。

        available=True 即二进制存在且进程可执行 (不要求 status 业务 ok — supervisor 运行中
        但某服务异常仍算可用, 心跳仅报告可达性)。
        """
        r = self.call("status", timeout=3.0)
        return bool(r.get("available"))
