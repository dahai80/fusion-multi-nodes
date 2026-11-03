#!/usr/bin/env python3
"""#95 生产静态服务: 常驻托管 web/dist + 同源 /api 反代到 cluster master。

嵌入模式 (gateway /shell/fusion-multi-nodes/) 链路:
  browser → :11432 gateway /shell proxy → 本服务 :11472
  - 静态文件: dist 根 (base "./" + hash router, 无 SPA fallback 需求)
  - /api/*: 反代 master (FUSION_MULTINODE_PORT, 默认 11452), 注入
    Bearer cluster_token (~/.fusion/multi-node/.cluster_token) — modules
    api_key 未配时 gateway 剥 Authorization, master BearerAuthMiddleware
    拒裸请求; 本服务统一补 token, bundle 无秘密。

仅 stdlib, 无依赖。启动: start.sh start-web (nohup 常驻)。
"""

from __future__ import annotations

import logging
import mimetypes
import os
import sys
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("mn-web")

DIST_DIR = Path(__file__).resolve().parent / "dist"
WEB_PORT = int(os.environ.get("FUSION_MULTINODE_WEB_PORT", "11472"))
MASTER_PORT = os.environ.get("FUSION_MULTINODE_PORT", "11452")
MASTER_HOST = os.environ.get("FUSION_MULTINODE_HOST", "127.0.0.1")
TOKEN_FILE = Path(os.environ.get(
    "FUSION_MULTINODE_TOKEN_FILE",
    str(Path.home() / ".fusion" / "multi-node" / ".cluster_token"),
))
# master 健康端点豁免鉴权; 其余 /api/* 均需 Bearer。
MASTER_HEALTH_PATHS = {"/api/health"}


def load_cluster_token() -> str:
    env = os.environ.get("FUSION_CLUSTER_TOKEN")
    if env:
        return env
    try:
        return TOKEN_FILE.read_text().strip()
    except OSError as e:
        logger.warning(f"cluster token 不可读 ({e}), /api 反代将不带鉴权 (master 401)")
        return ""


class Handler(BaseHTTPRequestHandler):
    server_version = "mn-web/1.0"
    cluster_token = ""
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):
        logger.info(f"{self.address_string()} {fmt % args}")

    def _send_body(self, status: int, body: bytes, ctype: str) -> None:
        self.send_response(status)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def _proxy_api(self) -> None:
        length = int(self.headers.get("Content-Length") or 0)
        payload = self.rfile.read(length) if length else None
        url = f"http://{MASTER_HOST}:{MASTER_PORT}{self.path}"
        req = urllib.request.Request(url, data=payload, method=self.command)
        for header in ("Content-Type", "Accept"):
            if self.headers.get(header):
                req.add_header(header, self.headers[header])
        if self.path not in MASTER_HEALTH_PATHS and self.cluster_token:
            req.add_header("Authorization", f"Bearer {self.cluster_token}")
        try:
            with urllib.request.urlopen(req, timeout=120) as resp:
                body = resp.read()
                self._send_body(resp.status, body, resp.headers.get("Content-Type", "application/json"))
        except urllib.error.HTTPError as e:
            self._send_body(e.code, e.read(), e.headers.get("Content-Type", "application/json"))
        except Exception as e:
            logger.warning(f"/api 反代失败: {self.command} {self.path} err={e}")
            msg = f'{{"error":"upstream_unavailable","detail":"{e}"}}'
            self._send_body(502, msg.encode(), "application/json")

    def _serve_static(self) -> None:
        rel = self.path.lstrip("/").split("?")[0]
        if rel in ("", "index.html"):
            rel = "index.html"
        candidate = (DIST_DIR / rel).resolve()
        if not str(candidate).startswith(str(DIST_DIR)) or not candidate.is_file():
            # hash router: 非文件路径一律回落 index.html (仅 GET 导航)
            candidate = DIST_DIR / "index.html"
        ctype = mimetypes.guess_type(str(candidate))[0] or "application/octet-stream"
        try:
            body = candidate.read_bytes()
        except OSError as e:
            logger.warning(f"静态文件读取失败: {candidate} err={e}")
            self._send_body(500, b'{"error":"internal"}', "application/json")
            return
        self._send_body(200, body, ctype)

    def do_GET(self):
        self._handle()

    def do_HEAD(self):
        self._handle()

    def do_POST(self):
        self._proxy_api()

    def do_PUT(self):
        self._proxy_api()

    def do_DELETE(self):
        self._proxy_api()

    def do_PATCH(self):
        self._proxy_api()

    def _handle(self):
        if self.path.startswith("/api/"):
            self._proxy_api()
        else:
            self._serve_static()


def main() -> int:
    if not (DIST_DIR / "index.html").is_file():
        logger.error(f"dist 缺 index.html: {DIST_DIR} — 先 bun run build")
        return 1
    Handler.cluster_token = load_cluster_token()
    server = ThreadingHTTPServer(("127.0.0.1", WEB_PORT), Handler)
    logger.info(
        f"mn-web 静态服务启动 :{WEB_PORT} dist={DIST_DIR} "
        f"master={MASTER_HOST}:{MASTER_PORT} token={'已加载' if Handler.cluster_token else '缺失'}"
    )
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
