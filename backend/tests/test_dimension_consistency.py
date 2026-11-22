"""维度读取一致性与并发替换测试。

覆盖：
- 关联概览保留原始空格、空项和重复项；
- 层级、下钻和透视的可用维度裁掉空项和两端空格，保留顺序和重复项；
- 层级、下钻、透视各自读到旧记录后，即使仓库被独立替换，仍出具完整旧结果；
- 透视配置与关联模型成组发布时按版本校验，避免旧轴配到新模型。
"""
from __future__ import annotations

import threading

import pytest

from app.services.dimension_model import DimensionModelService
from app.services.drill_down import DrillDownService
from app.services.pivot_table import PivotTableService
from app.store import StoreVersionConflict, store


@pytest.fixture()
def isolated_store():
    """保存并恢复仓库，避免并发场景污染其他测试。"""
    saved = {
        name: [dict(row) for row in store._tables.get(name, [])]
        for name in store.module_names()
    }
    saved_revision = store.revision
    hooks = (store.after_entry_read, store.on_entry_read, store.on_record_read)
    yield store
    store._tables = {name: [dict(row) for row in rows] for name, rows in saved.items()}
    store._revision = saved_revision
    store.after_entry_read, store.on_entry_read, store.on_record_read = hooks


def _pause_after_entry_read(monkeypatch, reached: threading.Event, resume: threading.Event):
    def hook(module: str, entry: dict) -> None:
        reached.set()
        assert resume.wait(5), "测试未恢复被暂停的请求"

    store.after_entry_read = hook


class TestDimensionNormalization:
    def test_overview_keeps_raw_while_consumers_normalize(self, isolated_store):
        store.replace_rows("dimension_model", [{
            "id": 1, "status": "已上线", "pending": False, "abnormal": False,
            "模型名称": "一致性模型", "事实表": "fact_sales",
            "维度列表": " 省 , ,市,省 ,邮编 ", "粒度定义": "日",
            "发布状态": "已上线",
        }])
        store.replace_rows("drill_down", [{
            "id": 1, "status": "已生效", "pending": False, "abnormal": False,
            "路径名称": "一致性路径", "起始指标": "一致性模型",
            "下钻维度序列": " 市 , , 邮编 ", "跳转规则": "旧规则", "默认展示": "市",
        }])
        store.replace_rows("pivot_table", [{
            "id": 1, "status": "已保存", "pending": False, "abnormal": False,
            "配置名称": "一致性透视", "数据源指标": "一致性模型",
            "行维度": " 省 ", "列维度": " 市 ", "聚合方式": "SUM",
        }])

        overview = DimensionModelService().get_model_with_analytics(1)
        assert overview["dimensions"] == [" 省 ", " ", "市", "省 ", "邮编 "]

        hierarchy = DimensionModelService().get_dimension_hierarchy(1)
        assert [level["dimension"] for level in hierarchy["levels"]] == ["省", "市", "省", "邮编"]

        drill = DrillDownService().execute_drill_down(1, current_level=0)
        assert drill["model_dimensions"] == ["省", "市", "省", "邮编"]
        assert drill["dimension"] == "市"

        pivot = PivotTableService().compute_pivot(1)
        assert pivot["available_dimensions"] == ["省", "市", "省", "邮编"]
        # 实际取数轴仍来自配置本身，不做规范化。
        assert pivot["row_dimension"] == " 省 "
        assert pivot["column_dimension"] == " 市 "

    def test_drill_export_uses_explicit_path_order(self, isolated_store):
        store.replace_rows("drill_down", [{
            "id": 1, "status": "已生效", "pending": False, "abnormal": False,
            "路径名称": "导出路径", "起始指标": "一致性模型",
            "下钻维度序列": " 市 , 省 ,市 ", "跳转规则": "", "默认展示": "",
        }])
        result = DrillDownService().export_to_pivot(1)
        assert result["row_dimension"] == "市"
        assert result["column_dimension"] == "省"

    def test_exported_suggestion_registers_pivot_and_aggregates(self, isolated_store):
        store.replace_rows("dimension_model", [{
            "id": 1, "status": "已上线", "pending": False, "abnormal": False,
            "模型名称": "一致性模型", "事实表": "fact_sales",
            "维度列表": " 省 , , 市 ", "粒度定义": "日", "发布状态": "已上线",
        }])
        store.replace_rows("drill_down", [{
            "id": 1, "status": "已生效", "pending": False, "abnormal": False,
            "路径名称": "导出路径", "起始指标": "一致性模型",
            "下钻维度序列": " 市 , 省 ", "跳转规则": "", "默认展示": "",
        }])

        outcome = DrillDownService().export_and_register_pivot(1)
        suggestion = outcome["suggestion"]
        assert (suggestion["row_dimension"], suggestion["column_dimension"]) == ("市", "省")
        assert outcome["registered"]["行维度"] == "市"

        pivot = outcome["pivot"]
        assert pivot["row_dimension"] == "市"
        assert pivot["column_dimension"] == "省"
        assert pivot["available_dimensions"] == ["省", "市"]
        assert pivot["row_count"] == 3 and pivot["column_count"] == 3
        assert pivot["row_totals"] == {"市_1": 300, "市_2": 600, "市_3": 900}


class TestSnapshotIsolation:
    def test_hierarchy_keeps_old_model_while_store_replaced(self, isolated_store, monkeypatch):
        store.replace_rows("dimension_model", [{
            "id": 1, "status": "已上线", "pending": False, "abnormal": False,
            "模型名称": "一致性模型", "事实表": "fact_old",
            "维度列表": "地区,日", "粒度定义": "日", "发布状态": "已上线",
        }])
        reached, resume = threading.Event(), threading.Event()
        _pause_after_entry_read(monkeypatch, reached, resume)
        outcome: dict = {}

        def request() -> None:
            outcome["hierarchy"] = DimensionModelService().get_dimension_hierarchy(1)

        worker = threading.Thread(target=request)
        worker.start()
        try:
            assert reached.wait(5)
            store.replace_rows("dimension_model", [{
                "id": 1, "status": "已上线", "pending": False, "abnormal": False,
                "模型名称": "一致性模型", "事实表": "fact_new",
                "维度列表": "地区,周", "粒度定义": "周", "发布状态": "已上线",
            }])
            resume.set()
        finally:
            store.after_entry_read = None
            resume.set()
            worker.join(5)

        old = outcome["hierarchy"]
        assert old["root"] == "fact_old"
        assert [level["dimension"] for level in old["levels"]] == ["地区", "日"]
        assert all(level["granularity"] == "日" for level in old["levels"])

        new = DimensionModelService().get_dimension_hierarchy(1)
        assert new["root"] == "fact_new"
        assert [level["dimension"] for level in new["levels"]] == ["地区", "周"]
        assert all(level["granularity"] == "周" for level in new["levels"])

    def test_drill_keeps_old_path_while_store_replaced(self, isolated_store, monkeypatch):
        store.replace_rows("dimension_model", [{
            "id": 1, "status": "已上线", "pending": False, "abnormal": False,
            "模型名称": "一致性模型", "事实表": "fact_sales",
            "维度列表": "省,市,邮编", "粒度定义": "日", "发布状态": "已上线",
        }])
        store.replace_rows("drill_down", [{
            "id": 1, "status": "已生效", "pending": False, "abnormal": False,
            "路径名称": "一致性路径", "起始指标": "一致性模型",
            "下钻维度序列": "省,市", "跳转规则": "旧规则", "默认展示": "市",
        }])
        reached, resume = threading.Event(), threading.Event()
        _pause_after_entry_read(monkeypatch, reached, resume)
        outcome: dict = {}

        worker = threading.Thread(
            target=lambda: outcome.update(
                drill=DrillDownService().execute_drill_down(1, current_level=1)))
        worker.start()
        try:
            assert reached.wait(5)
            store.replace_rows("drill_down", [{
                "id": 1, "status": "已生效", "pending": False, "abnormal": False,
                "路径名称": "一致性路径", "起始指标": "一致性模型",
                "下钻维度序列": "省,邮编", "跳转规则": "新规则", "默认展示": "邮编",
            }])
            resume.set()
        finally:
            store.after_entry_read = None
            resume.set()
            worker.join(5)

        old = outcome["drill"]
        assert old["dimension"] == "市"
        assert old["next_dimension"] is None
        assert old["jump_rule"] == "旧规则"
        assert old["total_levels"] == 2
        assert old["model_dimensions"] == ["省", "市", "邮编"]

        new = DrillDownService().execute_drill_down(1, current_level=1)
        assert new["dimension"] == "邮编"
        assert new["next_dimension"] is None
        assert new["jump_rule"] == "新规则"

    def test_pivot_keeps_old_axes_and_dimensions_on_bundle_publish(
            self, isolated_store, monkeypatch):
        store.publish_bundle({
            "dimension_model": [{
                "id": 1, "status": "已上线", "pending": False, "abnormal": False,
                "模型名称": "一致性模型", "事实表": "fact_sales",
                "维度列表": "省,市", "粒度定义": "日", "发布状态": "已上线",
            }],
            "pivot_table": [{
                "id": 1, "status": "已保存", "pending": False, "abnormal": False,
                "配置名称": "一致性透视", "数据源指标": "一致性模型",
                "行维度": "省", "列维度": "市", "聚合方式": "SUM",
            }],
        })
        reached, resume = threading.Event(), threading.Event()
        _pause_after_entry_read(monkeypatch, reached, resume)
        outcome: dict = {}

        worker = threading.Thread(
            target=lambda: outcome.update(
                pivot=PivotTableService().compute_pivot(1)))
        worker.start()
        try:
            assert reached.wait(5)
            store.publish_bundle({
                "dimension_model": [{
                    "id": 1, "status": "已上线", "pending": False, "abnormal": False,
                    "模型名称": "一致性模型", "事实表": "fact_sales",
                    "维度列表": "区,邮编", "粒度定义": "周", "发布状态": "已上线",
                }],
                "pivot_table": [{
                    "id": 1, "status": "已保存", "pending": False, "abnormal": False,
                    "配置名称": "一致性透视", "数据源指标": "一致性模型",
                    "行维度": "区", "列维度": "邮编", "聚合方式": "SUM",
                }],
            })
            resume.set()
        finally:
            store.after_entry_read = None
            resume.set()
            worker.join(5)

        old = outcome["pivot"]
        assert old["row_dimension"] == "省"
        assert old["column_dimension"] == "市"
        assert old["available_dimensions"] == ["省", "市"]
        assert {cell["row"] for cell in old["grid"]} == {"省_1", "省_2", "省_3"}
        assert {cell["column"] for cell in old["grid"]} == {"市_1", "市_2", "市_3"}
        assert set(old["row_totals"]) == {"省_1", "省_2", "省_3"}

        new = PivotTableService().compute_pivot(1)
        assert new["row_dimension"] == "区"
        assert new["column_dimension"] == "邮编"
        assert new["available_dimensions"] == ["区", "邮编"]
        assert {cell["row"] for cell in new["grid"]} == {"区_1", "区_2", "区_3"}


class TestBundlePublication:
    def test_bundle_publishes_new_model_and_pivots_together(self, isolated_store):
        store.replace_rows("dimension_model", [{
            "id": 1, "status": "已上线", "pending": False, "abnormal": False,
            "模型名称": "一致性模型", "事实表": "fact_sales",
            "维度列表": "省,市", "粒度定义": "日", "发布状态": "已上线",
        }])
        store.replace_rows("pivot_table", [{
            "id": 1, "status": "已保存", "pending": False, "abnormal": False,
            "配置名称": "旧透视", "数据源指标": "一致性模型",
            "行维度": "省", "列维度": "市", "聚合方式": "SUM",
        }])

        result = DimensionModelService().publish_with_pivots(
            1,
            [{"配置名称": "新透视", "数据源指标": "一致性模型",
              "行维度": "区", "列维度": "邮编", "聚合方式": "SUM"}],
            model_values={"维度列表": "区,邮编", "粒度定义": "周"},
        )
        assert "error" not in result
        # 模型和透视配置同时到达新版本。
        assert DimensionModelService().get_dimension_hierarchy(1)["levels"][0]["dimension"] == "区"
        pivot = PivotTableService().compute_pivot(result["pivot_configs"][0]["id"])
        assert pivot["available_dimensions"] == ["区", "邮编"]
        assert pivot["row_dimension"] == "区"

    def test_stale_revision_is_rejected(self, isolated_store):
        store.publish_bundle({"dimension_model": [
            {"id": 1, "模型名称": "旧模型", "维度列表": "省,市", "status": "已上线"},
        ]})
        stale = store.revision
        store.publish_bundle({"dimension_model": [
            {"id": 1, "模型名称": "新模型", "维度列表": "区,邮编", "status": "已上线"},
        ]})
        with pytest.raises(StoreVersionConflict):
            store.publish_bundle({
                "pivot_table": [{"id": 1, "行维度": "省", "列维度": "市"}],
            }, expected_revision=stale)
        model = store.find("dimension_model", 1)
        assert model["维度列表"] == "区,邮编"
        # 被拒绝的整组写入不落地，旧透视配置保持原样。
        assert store.find("pivot_table", 1)["行维度"] == "透视表配置样例1"

    def test_service_bundle_reports_conflict_without_partial_write(self, isolated_store):
        store.replace_rows("dimension_model", [{
            "id": 1, "status": "已上线", "pending": False, "abnormal": False,
            "模型名称": "一致性模型", "事实表": "fact_sales",
            "维度列表": "区,邮编", "粒度定义": "周", "发布状态": "已上线",
        }])
        stale = store.revision
        store.replace_rows("dimension_model", [{
            "id": 1, "status": "已上线", "pending": False, "abnormal": False,
            "模型名称": "一致性模型", "事实表": "fact_sales",
            "维度列表": "区,邮编", "粒度定义": "周", "发布状态": "已上线",
        }])
        result = DimensionModelService().publish_with_pivots(
            1,
            [{"配置名称": "新透视", "数据源指标": "一致性模型",
              "行维度": "区", "列维度": "邮编", "聚合方式": "SUM"}],
            expected_revision=stale,
        )
        assert "error" in result
        assert not any(row.get("配置名称") == "新透视" for row in store.rows("pivot_table"))
