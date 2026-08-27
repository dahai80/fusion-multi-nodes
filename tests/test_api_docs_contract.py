"""F4 (#32): docs/API.md 漂移检测 — 每个 /api/v1 路由须在 API.md 出现。

防 docs rot: 新增 /api/v1 路由忘更文档时此测试 fail。FastAPI app routes 为源,
docs/API.md 为文档面, 二者须一致。路径变量 ({node_id}) 原样匹配。
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from fusion_multi_node.master import ClusterMaster
from fusion_multi_node.server.master_server import MasterServer

TEST_TOKEN = "test-cluster-token"


@pytest.fixture
def app():
    srv = MasterServer(master=ClusterMaster(heartbeat_timeout=60.0), shared_token=TEST_TOKEN)
    srv._approval_manager = None
    return srv.app


def _v1_routes(app):
    routes = set()
    for r in app.routes:
        p = getattr(r, "path", "")
        methods = getattr(r, "methods", set()) or set()
        if p.startswith("/api/v1"):
            for m in methods:
                routes.add((m, p))
    return routes


def _api_md():
    return Path(__file__).resolve().parent.parent / "docs" / "API.md"


def test_every_v1_route_documented(app):
    md = _api_md().read_text(encoding="utf-8")
    routes = _v1_routes(app)
    missing = []
    for method, path in sorted(routes):
        # 路径变量 {x} 在 markdown 原样出现 (反引号内)
        if not _route_in_doc(md, method, path):
            missing.append(f"{method} {path}")
    assert not missing, "docs/API.md 缺以下 /api/v1 路由文档:\n" + "\n".join(missing)


def _route_in_doc(md: str, method: str, path: str) -> bool:
    if path in md:
        return True
    # 含路径变量的: 匹配模板 (花括号转义)
    pat = path.replace("{", r"\{").replace("}", r"\}")
    return bool(re.search(pat, md))


def test_api_md_has_9_op_contract_table(app):
    md = _api_md().read_text(encoding="utf-8")
    # 9 操作契约表标题
    assert "9 Operations" in md or "9 操作" in md, "API.md 缺 9 操作契约表"
    # 9 操作路径全在
    contract = [
        ("GET", "/api/v1/nodes"),
        ("POST", "/api/v1/nodes/register"),
        ("DELETE", "/api/v1/nodes/{node_id}"),
        ("POST", "/api/v1/tasks/submit"),
        ("POST", "/api/v1/tasks/{task_id}/migrate"),
        ("POST", "/api/v1/tasks/{task_id}/degrade"),
        ("GET", "/api/v1/tasks/{task_id}/progress"),
        ("GET", "/api/v1/cluster/stats"),
        ("GET", "/api/v1/observability/suggestions"),
    ]
    missing = [f"{m} {p}" for m, p in contract if not _route_in_doc(md, m, p)]
    assert not missing, f"API.md 9-op 契约缺: {missing}"


def test_python_api_doc_exists():
    p = _api_md().parent / "PYTHON_API.md"
    assert p.exists(), "docs/PYTHON_API.md 须存在 (Python 类文档从 API.md 迁出)"
    assert "ClusterMaster" in p.read_text(encoding="utf-8")
