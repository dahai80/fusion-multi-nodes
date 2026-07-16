"""Fusion-Multi-Node CLI 入口 — 集群管理命令行工具。"""

from __future__ import annotations

import asyncio
import json
import logging
import sys
import time
from typing import Any, Dict, Optional

import click

from . import __version__, __app_name__
from .config import ClusterConfig
from .master import ClusterMaster, NodeInfo, NodeStatus, ParallelMode, ClusterTask, TaskStatus
from .agent import NodeAgent
from .distributed_mlx import DistributedMLXBridge, DistMode
from .mcp_gateway import MCPClusterGateway
from .observability import ClusterObservability
from .utils import setup_logger, get_data_dir

logger = logging.getLogger(__name__)

# 全局实例
_config = ClusterConfig()
_master: Optional[ClusterMaster] = None
_agent: Optional[NodeAgent] = None
_observability: Optional[ClusterObservability] = None


@click.group()
@click.option("--verbose", "-v", is_flag=True, help="详细输出")
@click.version_option(version=__version__, prog_name=__app_name__)
def cli(verbose: bool):
    """Fusion-Multi-Node — 分布式 Apple Silicon MLX 集群调度核心。

    将多台 Mac 组成分布式推理集群，支持流水线并行和数据并行。
    """
    level = logging.DEBUG if verbose else logging.INFO
    setup_logger(level=level, verbose=verbose)


# ── 节点管理 ──

@cli.group()
def node():
    """节点管理：发现、注册、状态查看。"""
    pass


@node.command("list")
@click.option("--online", is_flag=True, help="仅显示在线节点")
def list_nodes(online: bool):
    """列出集群所有节点。"""
    asyncio.run(_async_list_nodes(online))


async def _async_list_nodes(online_only: bool):
    master = _get_master()
    nodes = master.get_online_nodes() if online_only else list(master.nodes.values())

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
        click.echo(f"{n.node_id:<20} {n.hostname:<20} {n.ip_address:<16} "
                  f"{status_icon} {n.status.value:<8} {mem_str:<12} {load_str:<8}")

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


# ── 集群管理 ──

@cli.group()
def cluster():
    """集群管理：启动、停止、状态。"""
    pass


@cluster.command("start")
@click.option("--mode", type=click.Choice(["master", "agent", "both"]), default="master")
def cluster_start(mode: str):
    """启动集群服务。"""
    asyncio.run(_async_cluster_start(mode))


async def _async_cluster_start(mode: str):
    global _master, _agent, _observability

    if mode in ("master", "both"):
        _master = ClusterMaster(
            host=_config.get("cluster.master_host", "0.0.0.0"),
            port=_config.get("cluster.master_port", 9753),
        )
        await _master.start()
        click.echo(f"✅ Cluster Master 已启动 (端口 {_master.port})")

    if mode in ("agent", "both"):
        agent_config = _config.to_node_agent_config()
        _agent = NodeAgent(agent_config)
        await _agent.start()
        click.echo(f"✅ Node Agent 已启动: {_agent.config.node_id}")

    if mode in ("master", "both"):
        _observability = ClusterObservability(
            retention_hours=_config.get("observability.retention_hours", 24.0)
        )
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
    master = _get_master()
    stats = master.get_stats()

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


# ── 任务管理 ──

@cli.group()
def task():
    """任务管理：提交、查看、取消。"""
    pass


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
        task_id=f"task_{int(time.time())}",
        name=name,
        mode=ParallelMode.PIPELINE if mode == "pipeline" else ParallelMode.DATA,
        model_name=model,
        timeout_seconds=float(timeout),
    )

    if master.assign_task(task):
        click.echo(f"✅ 任务已提交: {task.task_id}")
        click.echo(f"   名称:     {name}")
        click.echo(f"   模式:     {mode}")
        click.echo(f"   模型:     {model or '默认'}")
        click.echo(f"   节点:     {', '.join(task.assigned_nodes)}")
    else:
        click.echo(f"❌ 任务提交失败: 可用节点不足")


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
        click.echo(f"{t.task_id[:14]:<16} {t.name:<20} {t.mode.value:<10} "
                  f"{status_icon} {t.status.value:<10} {duration:<10}")

    click.echo()
    click.echo(f"总计: {len(tasks)} 任务")


@task.command("cancel")
@click.argument("task_id")
def task_cancel(task_id: str):
    """取消任务。"""
    master = _get_master()
    task = master.tasks.get(task_id)
    if not task:
        click.echo(f"任务不存在: {task_id}")
        return
    master.complete_task(task_id, "cancelled by user")
    click.echo(f"已取消任务: {task_id}")


# ── 配置管理 ──

@cli.group()
def config():
    """配置管理。"""
    pass


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
    # 尝试解析值类型
    try:
        parsed = json.loads(value)
    except (json.JSONDecodeError, TypeError):
        parsed = value
    _config.set(key, parsed)
    click.echo(f"已设置 {key} = {json.dumps(parsed, ensure_ascii=False)}")


# ── 工具函数 ──

def _get_master() -> ClusterMaster:
    """获取或创建 Cluster Master 实例。"""
    global _master
    if _master is None:
        _master = ClusterMaster(
            host=_config.get("cluster.master_host", "0.0.0.0"),
            port=_config.get("cluster.master_port", 9753),
        )
    return _master


def main():
    """Fusion-Multi-Node 主入口。"""
    cli()


if __name__ == "__main__":
    main()