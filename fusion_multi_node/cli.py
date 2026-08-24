"""Fusion-Multi-Node CLI 入口 — 集群管理命令行工具。"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
import uuid

import click

from . import __app_name__, __version__
from .agent import NodeAgent
from .config import ClusterConfig
from .distributed_mlx import CavemanManager, KVSharingManager
from .master import ClusterMaster, ClusterTask, NodeStatus, ParallelMode, TaskStatus
from .observability import ClusterObservability
from .utils import NetworkTopologyDetector, setup_logger

logger = logging.getLogger(__name__)

_config = ClusterConfig()
_master: ClusterMaster | None = None
_agent: NodeAgent | None = None
_observability: ClusterObservability | None = None


@click.group()
@click.option("--verbose", "-v", is_flag=True, help="详细输出")
@click.version_option(version=__version__, prog_name=__app_name__)
def cli(verbose: bool):
    """Fusion-Multi-Node — 分布式 Apple Silicon MLX 集群调度核心。"""
    level = logging.DEBUG if verbose else logging.INFO
    setup_logger(level=level, verbose=verbose)


# ── 节点管理 ──


@cli.group()
def node():
    """节点管理：发现、注册、状态查看。"""


@node.command("list")
@click.option("--online", is_flag=True, help="仅显示在线节点")
def list_nodes(online: bool):
    """列出集群所有节点。"""
    asyncio.run(_async_list_nodes(online))


async def _async_list_nodes(online_only: bool):
    master = _get_master()
    nodes = await master.get_online_nodes() if online_only else await master.snapshot_nodes()

    if not nodes:
        click.echo("暂无节点")
        return

    click.echo()
    click.echo(f"{'节点ID':<20} {'主机名':<20} {'IP':<16} {'状态':<10} {'内存(GB)':<12} {'负载':<8}")
    click.echo("-" * 90)

    for n in nodes:
        status_icon = {
            NodeStatus.ONLINE: "🟢",
            NodeStatus.OFFLINE: "🔴",
            NodeStatus.BUSY: "🟡",
            NodeStatus.ERROR: "⛔",
        }.get(n.status, "⚪")
        mem_str = f"{n.available_memory_gb:.1f}/{n.total_memory_gb:.1f}"
        load_str = f"{n.active_tasks}/{n.max_tasks}"
        click.echo(
            f"{n.node_id:<20} {n.hostname:<20} {n.ip_address:<16} "
            f"{status_icon} {n.status.value:<8} {mem_str:<12} {load_str:<8}"
        )

    click.echo()
    click.echo(f"总计: {len(nodes)} 节点")


@node.command("info")
@click.argument("node_id")
def node_info(node_id: str):
    """查看节点详细信息。"""
    master = _get_master()
    node = master.nodes.get(node_id)
    if not node:
        click.echo(f"节点不存在: {node_id}")
        return

    click.echo()
    click.echo(f"节点信息: {node_id}")
    click.echo(f"  主机名:      {node.hostname}")
    click.echo(f"  IP:          {node.ip_address}:{node.port}")
    click.echo(f"  架构:        {node.arch}")
    click.echo(f"  MLX 版本:    {node.mlx_version or '未知'}")
    click.echo(f"  GPU 核心:    {node.gpu_cores}")
    click.echo(f"  CPU 核心:    {node.cpu_cores}")
    click.echo(f"  总内存:      {node.total_memory_gb:.1f} GB")
    click.echo(f"  可用内存:    {node.available_memory_gb:.1f} GB")
    click.echo(f"  状态:        {node.status.value}")
    click.echo(f"  活跃任务:    {node.active_tasks}/{node.max_tasks}")
    click.echo(f"  网络延迟:    {node.network_rtt_ms:.1f} ms")
    click.echo(f"  标签:        {', '.join(node.tags) if node.tags else '无'}")


@node.command("start")
@click.option("--role", type=click.Choice(["master", "agent"]), required=True, help="节点角色")
@click.option("--host", default="127.0.0.1", help="监听地址")
@click.option("--port", default=0, help="监听端口 (0=默认)")
@click.option("--master-host", default="localhost", help="Master 地址 (agent)")
@click.option("--master-port", default=11452, help="Master 端口 (agent)")
@click.option(
    "--transport",
    type=click.Choice(["http", "fmp"]),
    default="http",
    help="通信协议: http 或 fmp",
)
@click.option("--no-mdns", is_flag=True, help="禁用 mDNS 发现")
@click.option("--auto-discover", is_flag=True, help="Agent 自动发现 Master")
def node_start(
    role: str,
    host: str,
    port: int,
    master_host: str,
    master_port: int,
    transport: str,
    no_mdns: bool,
    auto_discover: bool,
):
    """启动节点服务。"""
    asyncio.run(
        _async_node_start(
            role,
            host,
            port,
            master_host,
            master_port,
            transport,
            no_mdns,
            auto_discover,
        )
    )


async def _async_node_start(
    role: str,
    host: str,
    port: int,
    master_host: str,
    master_port: int,
    transport: str,
    no_mdns: bool,
    auto_discover: bool,
):
    global _master, _agent

    click.echo(f"🚀 启动 {role} 节点 (transport={transport})")

    if role == "master":
        actual_port = port or 11452
        _master = ClusterMaster(host=host, port=actual_port)
        with_mdns = not no_mdns
        await _master.start(with_server=True, with_mdns=with_mdns)

        if transport == "fmp":
            from .protocol import FMPServer

            fmp_server = FMPServer()
            await fmp_server.start(host=host, port=11446)
            _master._fmp_server = fmp_server
            click.echo(f"  FMP 服务已启动: {host}:11446")

        click.echo(
            f"✅ Master 已启动: {host}:{actual_port} (mDNS={'ON' if with_mdns else 'OFF'}, transport={transport})"
        )
    else:
        from .agent import AgentConfig

        actual_port = port or 11445
        config = AgentConfig(
            master_host=master_host,
            master_port=master_port,
            agent_port=actual_port,
        )
        _agent = NodeAgent(config)
        await _agent.start(with_server=True, auto_discover=auto_discover)

        if transport == "fmp":
            from .protocol import FMPConnectionManager

            fmp_conn = FMPConnectionManager()
            await fmp_conn.connect(master_host, 11446)
            _agent._fmp_conn = fmp_conn
            click.echo(f"  FMP 已连接 Master: {master_host}:11446")

        click.echo(f"✅ Agent 已启动: {_agent.config.node_id} (auto_discover={auto_discover}, transport={transport})")

    click.echo("按 Ctrl+C 停止...")
    stop_event = asyncio.Event()

    def _signal_handler():
        logger.info("收到停止信号, 开始优雅关停 drain...")
        stop_event.set()

    loop = asyncio.get_running_loop()
    import signal as _signal

    for sig in (_signal.SIGTERM, _signal.SIGINT):
        try:
            loop.add_signal_handler(sig, _signal_handler)
        except (NotImplementedError, RuntimeError):
            # 某些平台 (Windows) 不支持 add_signal_handler, 退回 KeyboardInterrupt
            pass

    try:
        await stop_event.wait()
    except KeyboardInterrupt:
        pass
    finally:
        # 优雅关停: drain 在途任务 (Master.stop / Agent.stop 内部 await 关停)
        if _agent:
            try:
                await _agent.stop()
            except Exception as e:
                logger.warning(f"Agent 关停异常: {e}")
        if _master:
            try:
                await _master.stop()
            except Exception as e:
                logger.warning(f"Master 关停异常: {e}")
        click.echo("⏹️  已停止")


@node.command("discover")
@click.option("--timeout", default=5.0, help="发现超时(秒)")
def node_discover(timeout: float):
    """通过 mDNS 发现局域网内节点。"""
    asyncio.run(_async_node_discover(timeout))


async def _async_node_discover(timeout: float):
    from .discovery import MDNSDiscovery

    click.echo(f"🔍 mDNS 发现中... (超时: {timeout}s)")
    mdns = MDNSDiscovery()
    nodes = await mdns.browse_async(timeout=timeout)

    if not nodes:
        click.echo("未发现任何节点")
        return

    click.echo()
    click.echo(f"{'名称':<24} {'IP':<16} {'端口':<8} {'角色':<10} {'属性'}")
    click.echo("-" * 80)

    for n in nodes:
        role = n.properties.get("role", "unknown")
        props_str = " ".join(f"{k}={v}" for k, v in n.properties.items() if k != "role")
        click.echo(f"{n.name:<24} {n.host:<16} {n.port:<8} {role:<10} {props_str}")

    click.echo()
    click.echo(f"发现 {len(nodes)} 个节点")


# ── 集群管理 ──


@cli.group()
def cluster():
    """集群管理：启动、停止、状态。"""


@cluster.command("start")
@click.option("--mode", type=click.Choice(["master", "agent", "both"]), default="master")
@click.option("--transport", type=click.Choice(["http", "fmp"]), default="http")
def cluster_start(mode: str, transport: str):
    """启动集群服务。"""
    asyncio.run(_async_cluster_start(mode, transport))


async def _async_cluster_start(mode: str, transport: str = "http"):
    global _master, _agent, _observability

    if mode in ("master", "both"):
        _master = ClusterMaster(
            host=_config.get("cluster.master_host", "127.0.0.1"),
            port=_config.get("cluster.master_port", 11452),
        )
        await _master.start()

        if transport == "fmp":
            from .protocol import FMPServer

            fmp_server = FMPServer()
            await fmp_server.start(host=_master.host, port=11446)
            _master._fmp_server = fmp_server

        click.echo(f"✅ Cluster Master 已启动 (端口 {_master.port}, transport={transport})")

    if mode in ("agent", "both"):
        agent_config = _config.to_node_agent_config()
        _agent = NodeAgent(agent_config)
        await _agent.start()
        click.echo(f"✅ Node Agent 已启动: {_agent.config.node_id}")

    if mode in ("master", "both"):
        _observability = ClusterObservability(retention_hours=_config.get("observability.retention_hours", 168.0))
        await _observability.start()
        click.echo("✅ 可观测模块已启动")


@cluster.command("stop")
def cluster_stop():
    """停止集群服务。"""
    asyncio.run(_async_cluster_stop())


async def _async_cluster_stop():
    global _master, _agent, _observability

    if _observability:
        await _observability.stop()
    if _agent:
        await _agent.stop()
    if _master:
        await _master.stop()

    click.echo("⏹️  集群服务已停止")


@cluster.command("status")
def cluster_status():
    """查看集群状态。"""
    asyncio.run(_async_cluster_status())


async def _async_cluster_status():
    master = _get_master()
    stats = await master.get_stats()

    click.echo()
    click.echo("📊 集群状态")
    click.echo(f"  总节点:     {stats['total_nodes']}")
    click.echo(f"  在线节点:   {stats['online_nodes']}")
    click.echo(f"  总任务:     {stats['total_tasks']}")
    click.echo(f"  活跃任务:   {stats['active_tasks']}")
    click.echo(f"  已完成:     {stats['completed_tasks']}")
    click.echo(f"  失败:       {stats['failed_tasks']}")
    click.echo(f"  KV 缓存:    {stats['kv_cache_entries']} 条目")
    click.echo(f"  总内存:     {stats['total_memory_gb']:.1f} GB")
    click.echo(f"  可用内存:   {stats['available_memory_gb']:.1f} GB")


# ── 节点审批 ──


async def _master_http(method: str, path: str, json_body: dict | None = None) -> dict:
    """通过 HTTP 调用远程 Master（带 Bearer token）。"""
    import httpx

    from fusion_multi_node.utils.auth import load_or_create_token

    host = os.environ.get("FUSION_MULTINODE_HOST") or _config.get("cluster.master_host", "127.0.0.1")
    port = int(os.environ.get("FUSION_MULTINODE_PORT") or _config.get("cluster.master_port", 11452))
    token = load_or_create_token()
    headers = {"Authorization": f"Bearer {token}"}
    url = f"http://{host}:{port}{path}"
    async with httpx.AsyncClient(timeout=10.0, headers=headers) as client:
        if method == "GET":
            resp = await client.get(url)
        else:
            resp = await client.post(url, json=json_body or {})
        if resp.status_code >= 400:
            raise click.ClickException(f"Master 返回 {resp.status_code}: {resp.text}")
        return resp.json()


@cluster.command("approve")
@click.argument("node_id")
@click.option("--by", "approved_by", default="admin", help="审批人")
def cluster_approve(node_id: str, approved_by: str):
    """审批通过待加入节点。"""
    result = asyncio.run(_master_http("POST", "/api/nodes/approve", {"node_id": node_id, "approved_by": approved_by}))
    click.echo(f"✅ 节点 {result.get('node_id')} 审批通过 (by {result.get('approved_by')})")


@cluster.command("reject")
@click.argument("node_id")
@click.option("--reason", default="", help="拒绝原因")
def cluster_reject(node_id: str, reason: str):
    """拒绝待加入节点。"""
    result = asyncio.run(_master_http("POST", "/api/nodes/reject", {"node_id": node_id, "reason": reason}))
    click.echo(f"❌ 节点 {result.get('node_id')} 已拒绝")


@cluster.command("pending")
def cluster_pending():
    """列出待审批节点。"""
    result = asyncio.run(_master_http("GET", "/api/nodes/pending"))
    pending = result.get("pending", [])
    if not pending:
        click.echo("暂无待审批节点")
        return
    click.echo(f"待审批节点 ({len(pending)}):")
    for p in pending:
        click.echo(f"  {p['node_id']:<20} {p.get('hostname', ''):<16} {p.get('ip_address', '')}")


# ── 任务管理 ──


@cli.group()
def task():
    """任务管理：提交、查看、取消。"""


@task.command("submit")
@click.option("--name", "-n", required=True, help="任务名称")
@click.option("--model", "-m", default="", help="模型名称")
@click.option("--mode", type=click.Choice(["pipeline", "data"]), default="pipeline")
@click.option("--prompt", "-p", default="", help="推理 prompt")
@click.option("--timeout", "-t", default=300, help="超时秒数")
def task_submit(name: str, model: str, mode: str, prompt: str, timeout: int):
    """提交任务到集群。"""
    asyncio.run(_async_task_submit(name, model, mode, prompt, timeout))


async def _async_task_submit(name: str, model: str, mode: str, prompt: str, timeout: int):
    master = _get_master()

    task = ClusterTask(
        task_id=f"task_{uuid.uuid4().hex[:12]}",
        name=name,
        mode=ParallelMode.PIPELINE if mode == "pipeline" else ParallelMode.DATA,
        model_name=model,
        timeout_seconds=float(timeout),
    )

    if await master.assign_task(task):
        click.echo(f"✅ 任务已提交: {task.task_id}")
        click.echo(f"   名称:     {name}")
        click.echo(f"   模式:     {mode}")
        click.echo(f"   模型:     {model or '默认'}")
        click.echo(f"   节点:     {', '.join(task.assigned_nodes)}")
    else:
        click.echo("❌ 任务提交失败: 可用节点不足")


@task.command("list")
def task_list():
    """列出所有任务。"""
    master = _get_master()
    tasks = list(master.tasks.values())

    if not tasks:
        click.echo("暂无任务")
        return

    click.echo()
    click.echo(f"{'任务ID':<16} {'名称':<20} {'模式':<10} {'状态':<12} {'耗时':<10}")
    click.echo("-" * 70)

    for t in tasks:
        duration = ""
        if t.started_at > 0:
            end = t.completed_at or time.time()
            duration = f"{end - t.started_at:.1f}s"
        status_icon = {
            TaskStatus.PENDING: "⏳",
            TaskStatus.RUNNING: "🔄",
            TaskStatus.COMPLETED: "✅",
            TaskStatus.FAILED: "❌",
            TaskStatus.MIGRATED: "➡️",
            TaskStatus.TIMEOUT: "⏰",
        }.get(t.status, "⚪")
        click.echo(
            f"{t.task_id[:14]:<16} {t.name:<20} {t.mode.value:<10} {status_icon} {t.status.value:<10} {duration:<10}"
        )

    click.echo()
    click.echo(f"总计: {len(tasks)} 任务")


@task.command("cancel")
@click.argument("task_id")
def task_cancel(task_id: str):
    """取消任务。"""
    asyncio.run(_async_task_cancel(task_id))


async def _async_task_cancel(task_id: str):
    master = _get_master()
    task = await master.get_task(task_id)
    if not task:
        click.echo(f"任务不存在: {task_id}")
        return
    ok = await master.cancel_task(task_id, reason="cancelled by user", cancel_sub_tasks=True)
    if ok:
        click.echo(f"已取消任务: {task_id}")
    else:
        click.echo(f"取消任务失败 (状态不允许): {task_id}")


# ── 配置管理 ──


@cli.group()
def config():
    """配置管理。"""


@config.command("list")
def config_list():
    """列出所有配置。"""
    click.echo()
    click.echo("⚙️  Fusion-Multi-Node 配置")
    click.echo(f"  配置文件: {_config.config_path}")
    click.echo()
    click.echo(json.dumps(_config._data, indent=2, ensure_ascii=False))


@config.command("get")
@click.argument("key")
def config_get(key: str):
    """获取配置项。"""
    value = _config.get(key)
    if value is not None:
        click.echo(f"{key} = {json.dumps(value, ensure_ascii=False)}")
    else:
        click.echo(f"未知配置项: {key}")


@config.command("set")
@click.argument("key")
@click.argument("value")
def config_set(key: str, value: str):
    """设置配置项。"""
    try:
        parsed = json.loads(value)
    except (json.JSONDecodeError, TypeError):
        parsed = value
    try:
        _config.set(key, parsed)
    except Exception as e:
        click.echo(f"配置校验失败: {e}", err=True)
        raise SystemExit(1)
    click.echo(f"已设置 {key} = {json.dumps(parsed, ensure_ascii=False)}")


def _get_master() -> ClusterMaster:
    global _master
    if _master is None:
        _master = ClusterMaster(
            host=_config.get("cluster.master_host", "127.0.0.1"),
            port=_config.get("cluster.master_port", 11452),
        )
    return _master


# ── 网络拓扑命令 ──


@cli.group()
def network():
    """网络拓扑检测与链路优化。"""


@network.command("detect")
def network_detect():
    """检测本机网络拓扑和链路类型。"""
    asyncio.run(_async_network_detect())


async def _async_network_detect():
    click.echo()
    click.echo("🌐 网络拓扑检测")
    click.echo()

    detector = NetworkTopologyDetector()
    interfaces = await detector.detect()

    if not interfaces:
        click.echo("  未检测到网络接口")
        return

    click.echo(f"{'接口':<12} {'类型':<20} {'带宽':<12} {'延迟':<10} {'RDMA':<8}")
    click.echo("-" * 65)

    for name, link in sorted(interfaces.items(), key=lambda x: x[1].priority):
        rdma = "✅" if link.is_rdma else "❌"
        click.echo(
            f"{name:<12} {link.type.value:<20} {link.bandwidth_mbps:.0f}Mbps{'':<4} "
            f"{link.latency_ms:<10.2f}ms {rdma:<8}"
        )

    click.echo()
    best = detector.get_best_link()
    if best:
        click.echo(f"✅ 最优链路: {best.interface} ({best.type.value})")
        click.echo(f"   推荐压缩策略: {detector.get_recommended_compression()}")
    if detector.is_thunderbolt_available():
        click.echo("⚡ Thunderbolt 高速链路可用，推荐 RDMA 传输")


# ── Caveman 压缩命令 ──


@cli.group()
def caveman():
    """Caveman Token 压缩管理。"""


@caveman.command("test")
@click.argument("data", default="The quick brown fox jumps over the lazy dog. " * 10)
def caveman_test(data: str):
    """测试 Caveman 压缩效果。"""
    asyncio.run(_async_caveman_test(data))


async def _async_caveman_test(data: str):
    click.echo()
    click.echo("🔧 Caveman 压缩测试")
    click.echo()

    raw_bytes = data.encode("utf-8")
    manager = CavemanManager()

    for method in ["zlib", "diff", "dict"]:
        _compressed, _used_method, stats = await manager.compress_tensor(raw_bytes, link_type="ethernet_1g")
        click.echo(
            f"  {method:<10} 原始: {stats.original_bytes:>8} bytes → "
            f"压缩: {stats.compressed_bytes:>8} bytes "
            f"({stats.ratio * 100:.1f}%) 耗时: {stats.time_ms:.1f}ms"
        )

    click.echo()
    overall = manager.get_stats()
    click.echo(f"  总体压缩率: {overall['overall_ratio'] * 100:.1f}%")
    click.echo(f"  节省带宽:   {overall['savings_percent']:.1f}%")


# ── KV 缓存命令 ──


@cli.group()
def kv():
    """KV 缓存共享管理。"""


@kv.command("stats")
def kv_stats():
    """查看 KV 缓存统计。"""
    asyncio.run(_async_kv_stats())


async def _async_kv_stats():
    master = _get_master()
    stats = await master.get_stats()
    click.echo()
    click.echo("📦 KV 缓存统计")
    click.echo(f"  缓存条目: {stats.get('kv_cache_entries', 0)}")


@kv.command("warm")
@click.option("--prompt", "-p", multiple=True, help="预热 prompt")
@click.option("--nodes", "-n", multiple=True, help="目标节点")
def kv_warm(prompt: list, nodes: list):
    """预热 KV 缓存。"""
    asyncio.run(_async_kv_warm(list(prompt), list(nodes)))


async def _async_kv_warm(prompts: list, nodes: list):
    if not prompts:
        click.echo("请指定 --prompt")
        return
    if not nodes:
        nodes = list(await _get_master().get_online_nodes())
        nodes = [n.node_id for n in nodes]

    manager = KVSharingManager()
    results = await manager.warm_cache("default", prompts, nodes)
    click.echo(f"预热完成: {results['success']} 成功, {results['failed']} 失败")


def main():
    cli()


if __name__ == "__main__":
    main()
