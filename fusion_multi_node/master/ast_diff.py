"""M6-04 AST Diff-only 传输 — 仅传输 AST 变更部分，减少网络开销。

- compute_ast_diff: 计算 old_ast 与 new_ast 之间的差异
- apply_ast_diff: 从 base_ast + diff 重建完整 AST
- 差异类型: added_nodes, removed_nodes, modified_nodes
"""

from __future__ import annotations

import copy
import logging
from typing import Any, Dict, List, Set

logger = logging.getLogger(__name__)


def _collect_nodes(tree: Dict[str, Any], prefix: str = "") -> Dict[str, Dict[str, Any]]:
    nodes: Dict[str, Dict[str, Any]] = {}
    node_id = tree.get("id", "")
    path = f"{prefix}/{node_id}" if prefix else node_id

    if node_id:
        nodes[path] = {
            "id": node_id,
            "type": tree.get("type", ""),
            "value": tree.get("value"),
            "children_count": len(tree.get("children", [])),
        }

    for child in tree.get("children", []):
        child_nodes = _collect_nodes(child, path)
        nodes.update(child_nodes)

    return nodes


def compute_ast_diff(old_ast: Dict[str, Any], new_ast: Dict[str, Any]) -> Dict[str, Any]:
    old_nodes = _collect_nodes(old_ast)
    new_nodes = _collect_nodes(new_ast)

    old_paths: Set[str] = set(old_nodes.keys())
    new_paths: Set[str] = set(new_nodes.keys())

    added_paths = new_paths - old_paths
    removed_paths = old_paths - new_paths
    common_paths = old_paths & new_paths

    added_nodes: List[Dict[str, Any]] = []
    for p in sorted(added_paths):
        added_nodes.append({"path": p, **new_nodes[p]})

    removed_nodes: List[str] = sorted(removed_paths)

    modified_nodes: List[Dict[str, Any]] = []
    for p in sorted(common_paths):
        old_n = old_nodes[p]
        new_n = new_nodes[p]
        if old_n != new_n:
            diff_entry: Dict[str, Any] = {"path": p}
            for key in new_n:
                if old_n.get(key) != new_n.get(key):
                    diff_entry[key] = new_n[key]
            modified_nodes.append(diff_entry)

    diff = {
        "added_nodes": added_nodes,
        "removed_nodes": removed_nodes,
        "modified_nodes": modified_nodes,
        "stats": {
            "added": len(added_nodes),
            "removed": len(removed_nodes),
            "modified": len(modified_nodes),
        },
    }

    logger.info(
        f"AST diff: +{len(added_nodes)} -{len(removed_nodes)} "
        f"~{len(modified_nodes)} changed"
    )
    return diff


def apply_ast_diff(base_ast: Dict[str, Any], diff: Dict[str, Any]) -> Dict[str, Any]:
    result = copy.deepcopy(base_ast)

    removed_paths = set(diff.get("removed_nodes", []))
    _remove_nodes(result, removed_paths)

    for mod in diff.get("modified_nodes", []):
        path = mod["path"]
        _update_node(result, path, mod)

    for added in diff.get("added_nodes", []):
        path = added["path"]
        _insert_node(result, path, added)

    stats = diff.get("stats", {})
    logger.info(
        f"AST diff applied: +{stats.get('added', 0)} "
        f"-{stats.get('removed', 0)} ~{stats.get('modified', 0)}"
    )
    return result


def _find_node(tree: Dict[str, Any], path: str) -> Dict[str, Any] | None:
    parts = path.strip("/").split("/")
    if not parts or parts[0] == "":
        return tree

    if parts[0] == tree.get("id"):
        if len(parts) == 1:
            return tree
        parts = parts[1:]

    current = tree
    for part in parts[:-1]:
        found = False
        for child in current.get("children", []):
            if child.get("id") == part:
                current = child
                found = True
                break
        if not found:
            return None

    last = parts[-1]
    for child in current.get("children", []):
        if child.get("id") == last:
            return child
    if current.get("id") == last:
        return current
    return None


def _remove_nodes(tree: Dict[str, Any], paths: Set[str]) -> None:
    for path in paths:
        parts = path.strip("/").split("/")
        if len(parts) < 1:
            continue
        if parts[0] == tree.get("id") and len(parts) > 1:
            parts = parts[1:]
        current = tree
        for part in parts[:-1]:
            found = False
            for child in current.get("children", []):
                if child.get("id") == part:
                    current = child
                    found = True
                    break
            if not found:
                break

        last_id = parts[-1]
        current["children"] = [
            c for c in current.get("children", [])
            if c.get("id") != last_id
        ]


def _update_node(tree: Dict[str, Any], path: str, updates: Dict[str, Any]) -> None:
    node = _find_node(tree, path)
    if node is None:
        logger.warning(f"AST diff: 未找到节点 {path}, 跳过更新")
        return
    for key, value in updates.items():
        if key == "path":
            continue
        node[key] = value


def _insert_node(tree: Dict[str, Any], path: str, node_data: Dict[str, Any]) -> None:
    parts = path.strip("/").split("/")
    if len(parts) <= 1:
        logger.warning(f"AST diff: 无法插入根级节点 {path}")
        return

    parent_path = "/".join(parts[:-1])
    parent = _find_node(tree, parent_path)
    if parent is None:
        logger.warning(f"AST diff: 父节点 {parent_path} 不存在, 跳过插入 {path}")
        return

    new_node: Dict[str, Any] = {
        "id": node_data.get("id", parts[-1]),
        "type": node_data.get("type", ""),
        "children": [],
    }
    if "value" in node_data:
        new_node["value"] = node_data["value"]

    parent.setdefault("children", []).append(new_node)
