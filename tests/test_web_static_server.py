"""#95: web/serve_dist.py 静态服务语义回归。

静态文件 + /api 反代 token 注入 + 502 上游不可达。stdlib Stub server 模拟 master。
"""

import http.server
import importlib.util
import threading
import urllib.error
import urllib.request
from pathlib import Path

import pytest

_spec = importlib.util.spec_from_file_location(
    "serve_dist", Path(__file__).resolve().parent.parent / "web" / "serve_dist.py"
)
sd = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(sd)


class _StubMaster(http.server.BaseHTTPRequestHandler):
    def _ok(self, body: bytes, ctype: str = "application/json"):
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/api/nodes":
            auth = self.headers.get("Authorization", "")
            body = f'{{"auth":"{auth}"}}'.encode()
            self._ok(body)
        else:
            self._ok(b"{}", "application/json")

    def log_message(self, fmt, *args):
        pass


@pytest.fixture()
def stub_master(monkeypatch):
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _StubMaster)
    monkeypatch.setattr(sd, "MASTER_PORT", str(server.server_address[1]))
    monkeypatch.setattr(sd, "MASTER_HOST", "127.0.0.1")
    monkeypatch.setattr(sd, "cluster_token", "tok-123", raising=False)
    sd.Handler.cluster_token = "tok-123"
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield
    server.shutdown()
    server.server_close()


@pytest.fixture()
def web_server(monkeypatch, tmp_path):
    # CI 不跑 bun build — 用 fixture dist, 不依赖真实 web/dist 产物。
    dist = tmp_path / "dist"
    (dist / "assets").mkdir(parents=True)
    (dist / "index.html").write_text("<html><body>mn</body></html>")
    (dist / "assets" / "app.js").write_text("console.log(1)")
    monkeypatch.setattr(sd, "WEB_PORT", 0)
    monkeypatch.setattr(sd, "DIST_DIR", dist)
    sd.Handler.cluster_token = "tok-123"
    server = sd.ThreadingHTTPServer(("127.0.0.1", 0), sd.Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_address[1]}"
    server.shutdown()
    server.server_close()


def test_serves_index_and_assets(web_server):
    with urllib.request.urlopen(f"{web_server}/") as r:
        assert r.status == 200
        assert b"<html" in r.read().lower()
    with urllib.request.urlopen(f"{web_server}/assets/app.js") as r:
        assert r.status == 200


def test_unknown_path_falls_back_to_index(web_server):
    with urllib.request.urlopen(f"{web_server}/topology") as r:
        assert r.status == 200
        assert b"<html" in r.read().lower()


def test_api_proxy_injects_cluster_token(web_server, stub_master):
    with urllib.request.urlopen(f"{web_server}/api/nodes") as r:
        assert r.status == 200
        assert b"Bearer tok-123" in r.read()


def test_api_proxy_upstream_down_returns_502(web_server, monkeypatch):
    monkeypatch.setattr(sd, "MASTER_PORT", "1")
    monkeypatch.setattr(sd, "MASTER_HOST", "127.0.0.1")
    try:
        urllib.request.urlopen(f"{web_server}/api/nodes")
        raise AssertionError("expected HTTPError")
    except urllib.error.HTTPError as e:
        assert e.code == 502


def test_missing_dist_rejected(monkeypatch, tmp_path):
    monkeypatch.setattr(sd, "DIST_DIR", tmp_path)
    assert sd.main() == 1
