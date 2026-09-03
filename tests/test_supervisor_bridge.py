"""#73 SupervisorBridge — 本机 fusion-sv CLI 包装单元测试。

mock subprocess.run 覆盖:
(a) status 输出 JSON 解析;
(b) 非状态 op 纯文本;
(c) FileNotFoundError → available=False (离线安全);
(d) 未知 op 拒;
(e) timeout;
(f) rc!=0 失败;
(g) ping → available True/False。
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from fusion_multi_node.agent.supervisor_bridge import SupervisorBridge


def _proc(stdout="", stderr="", returncode=0):
    class _P:
        def __init__(self):
            self.stdout = stdout
            self.stderr = stderr
            self.returncode = returncode

    return _P()


class TestSupervisorBridgeCall:
    def test_status_parses_json(self):
        sv = SupervisorBridge()
        with patch("fusion_multi_node.agent.supervisor_bridge.subprocess.run") as m:
            m.return_value = _proc(stdout='{"running": 3, "uptime": 100}')
            r = sv.call("status")
        assert r["ok"] is True
        assert r["available"] is True
        assert r["output"] == {"running": 3, "uptime": 100}

    def test_status_fallback_text_on_bad_json(self):
        sv = SupervisorBridge()
        with patch("fusion_multi_node.agent.supervisor_bridge.subprocess.run") as m:
            m.return_value = _proc(stdout="not-json-status")
            r = sv.call("status")
        assert r["ok"] is True
        assert r["output"] == "not-json-status"

    def test_drain_returns_text(self):
        sv = SupervisorBridge()
        with patch("fusion_multi_node.agent.supervisor_bridge.subprocess.run") as m:
            m.return_value = _proc(stdout="drained")
            r = sv.call("drain", "mlx-svc")
        assert r["ok"] is True
        assert r["svc"] == "mlx-svc"
        assert r["output"] == "drained"
        args = m.call_args.args[0]
        assert args == ["fusion-sv", "drain", "mlx-svc"]

    def test_file_not_found_offline_safe(self):
        sv = SupervisorBridge()
        with patch("fusion_multi_node.agent.supervisor_bridge.subprocess.run", side_effect=FileNotFoundError):
            r = sv.call("status")
        assert r["ok"] is False
        assert r["available"] is False
        assert "not installed" in r["error"]

    def test_unknown_op_rejected(self):
        sv = SupervisorBridge()
        r = sv.call("evil-op")
        assert r["ok"] is False
        assert r["available"] is True
        assert "unknown" in r["error"]

    def test_timeout(self):
        import subprocess as sp

        sv = SupervisorBridge()
        with patch("fusion_multi_node.agent.supervisor_bridge.subprocess.run", side_effect=sp.TimeoutExpired("cmd", 5)):
            r = sv.call("backup", timeout=5)
        assert r["ok"] is False
        assert r["available"] is True
        assert "timeout" in r["error"]

    def test_nonzero_returncode(self):
        sv = SupervisorBridge()
        with patch("fusion_multi_node.agent.supervisor_bridge.subprocess.run") as m:
            m.return_value = _proc(stderr="boom", returncode=2)
            r = sv.call("shutdown")
        assert r["ok"] is False
        assert r["available"] is True
        assert r["returncode"] == 2
        assert r["error"] == "boom"


class TestSupervisorBridgePing:
    def test_ping_available_when_binary_present(self):
        sv = SupervisorBridge()
        with patch("fusion_multi_node.agent.supervisor_bridge.subprocess.run") as m:
            m.return_value = _proc(stdout="{}")
            assert sv.ping() is True

    def test_ping_unavailable_when_not_installed(self):
        sv = SupervisorBridge()
        with patch("fusion_multi_node.agent.supervisor_bridge.subprocess.run", side_effect=FileNotFoundError):
            assert sv.ping() is False


class TestSupervisorEnvOverride:
    @pytest.mark.asyncio
    async def test_env_bin_override(self, monkeypatch):
        monkeypatch.setenv("FUSION_SV_BIN", "/custom/fusion-sv")
        sv = SupervisorBridge()
        with patch("fusion_multi_node.agent.supervisor_bridge.subprocess.run") as m:
            m.return_value = _proc(stdout="ok")
            sv.call("status")
        assert m.call_args.args[0][0] == "/custom/fusion-sv"
