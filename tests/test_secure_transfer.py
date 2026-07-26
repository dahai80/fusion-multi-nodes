from fusion_multi_node.security.secure_transfer import SecureTransferPipeline
from fusion_multi_node.security.data_scrubber import DataScrubber, ScrubRule


def test_ast_diff_scrubbed_end_to_end():
    pipeline = SecureTransferPipeline()
    old_ast = {
        "id": "root",
        "type": "module",
        "children": [
            {"id": "func1", "type": "function", "value": "clean", "children": []},
        ],
    }
    new_ast = {
        "id": "root",
        "type": "module",
        "children": [
            {"id": "func1", "type": "function", "value": "updated", "children": []},
            {"id": "func2", "type": "function", "value": "new", "children": []},
        ],
    }
    transfer = pipeline.prepare_transfer(old_ast, new_ast)
    assert transfer["type"] == "ast_diff_scrubbed"
    assert "diff" in transfer
    assert "scrubbed_rules" in transfer
    assert "stats" in transfer
    assert isinstance(transfer["stats"]["reduction_ratio"], float)
    assert transfer["stats"]["original_size"] > 0


def test_ast_diff_apply_restores_ast():
    pipeline = SecureTransferPipeline()
    old_ast = {
        "id": "root",
        "type": "module",
        "children": [
            {"id": "a", "type": "var", "value": "1", "children": []},
        ],
    }
    new_ast = {
        "id": "root",
        "type": "module",
        "children": [
            {"id": "a", "type": "var", "value": "2", "children": []},
            {"id": "b", "type": "var", "value": "3", "children": []},
        ],
    }
    transfer = pipeline.prepare_transfer(old_ast, new_ast)
    restored = pipeline.apply_transfer(old_ast, transfer)
    assert restored["id"] == "root"
    assert len(restored["children"]) == 2


def test_ast_diff_with_pii_data():
    pipeline = SecureTransferPipeline()
    old_ast = {
        "id": "root",
        "type": "module",
        "children": [],
    }
    new_ast = {
        "id": "root",
        "type": "module",
        "children": [
            {"id": "user", "type": "string", "value": "13800138000", "children": []},
            {"id": "email", "type": "string", "value": "test@example.com", "children": []},
        ],
    }
    transfer = pipeline.prepare_transfer(old_ast, new_ast)
    diff = transfer["diff"]
    added_nodes = diff.get("added_nodes", [])
    added_values = [n.get("value", "") for n in added_nodes]
    for v in added_values:
        if v:
            assert "13800138000" not in v
            assert "test@example.com" not in v
    assert len(transfer["scrubbed_rules"]) > 0


def test_no_change_diff_empty():
    pipeline = SecureTransferPipeline()
    same_ast = {
        "id": "root",
        "type": "module",
        "children": [
            {"id": "a", "type": "var", "value": "1", "children": []},
        ],
    }
    transfer = pipeline.prepare_transfer(same_ast, same_ast)
    diff = transfer["diff"]
    assert diff["stats"]["added"] == 0
    assert diff["stats"]["removed"] == 0
    assert diff["stats"]["modified"] == 0
    assert transfer["scrubbed_rules"] == []


def test_text_transfer_scrubs_pii():
    pipeline = SecureTransferPipeline()
    text = "用户手机13800138000，邮箱test@example.com"
    scrubbed, hits = pipeline.prepare_text_transfer(text)
    assert "13800138000" not in scrubbed
    assert "test@example.com" not in scrubbed
    assert len(hits) >= 2


def test_dict_transfer_deep_scrub():
    pipeline = SecureTransferPipeline()
    data = {
        "user": {
            "phone": "13800138000",
            "email": "test@example.com",
            "name": "张三",
        },
        "items": ["身份证110101199001011234", "正常文本"],
    }
    scrubbed, hits = pipeline.prepare_dict_transfer(data)
    assert "13800138000" not in str(scrubbed)
    assert "test@example.com" not in str(scrubbed)
    assert "110101199001011234" not in str(scrubbed)
    assert "张三" in str(scrubbed)
    assert len(hits) >= 3


def test_transfer_stats_reduction_ratio():
    pipeline = SecureTransferPipeline()
    old_ast = {"id": "root", "type": "module", "children": []}
    new_ast = {
        "id": "root",
        "type": "module",
        "children": [
            {"id": f"n{i}", "type": "var", "value": f"val_{i}", "children": []}
            for i in range(20)
        ],
    }
    transfer = pipeline.prepare_transfer(old_ast, new_ast)
    stats = transfer["stats"]
    assert stats["original_size"] > 0
    assert stats["diff_size"] > 0
    assert stats["scrubbed_size"] > 0
    assert isinstance(stats["reduction_ratio"], float)


def test_apply_transfer_unknown_type_returns_base():
    pipeline = SecureTransferPipeline()
    base = {"id": "root", "type": "module", "children": []}
    transfer = {"type": "unknown_type", "diff": {}}
    result = pipeline.apply_transfer(base, transfer)
    assert result == base


def test_custom_scrubber_in_pipeline():
    custom = DataScrubber(
        custom_rules=[
            ScrubRule(
                name="project_id",
                pattern=r"PROJ-\d{6}",
                replacement="***PROJ***",
            ),
        ],
    )
    pipeline = SecureTransferPipeline(scrubber=custom)
    old_ast = {"id": "root", "type": "module", "children": []}
    new_ast = {
        "id": "root",
        "type": "module",
        "children": [
            {"id": "ref", "type": "string", "value": "PROJ-123456", "children": []},
        ],
    }
    transfer = pipeline.prepare_transfer(old_ast, new_ast)
    assert "PROJ-123456" not in str(transfer["diff"])
