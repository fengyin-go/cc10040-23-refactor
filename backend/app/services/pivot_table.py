"""透视表配置领域服务：管理多维透视表的行列维度、聚合方式和共享配置。

跨模块数据流: drill_down -> pivot_table -> report_template
透视表消费下钻路径的维度结构，产出可嵌入报告模板的汇总视图。
"""
from __future__ import annotations

from typing import Any

from app.store import store
from app.services.base import BaseModuleService
from app.services.dimensions import read_available_dimensions


class PivotTableService(BaseModuleService):
    MODULE = "pivot_table"
    ENTITY = "透视配置"
    OPERATION_TERM = "聚合"
    LIST_FIELDS = ["配置名称", "数据源指标", "行维度", "列维度", "聚合方式"]
    REQUIRED_FIELDS = ["配置名称", "数据源指标", "行维度"]
    STATUS_ORDER = ["草稿", "已保存", "已共享"]
    ACTION_RULES = {"保存配置": "已保存", "共享配置": "已共享"}
    NEGATIVE_ACTIONS = []

    def compute_pivot(self, entry_id: int) -> dict[str, Any] | None:
        """执行透视表计算，返回二维汇总数据。

        跨模块数据流: dimension_model -> drill_down -> pivot_table
        """
        with store.read_basis():
            return self._compute_pivot(entry_id)

    def _compute_pivot(self, entry_id: int) -> dict[str, Any] | None:
        config = store.find(self.MODULE, entry_id)
        if config is None:
            return {"error": f"透视配置 {entry_id} 不存在"}
        store.notify_entry_read(self.MODULE, config)

        metric = config.get("数据源指标", "")
        # 实际取数轴始终来自配置本身，保持配置登记时的行列顺序。
        row_dim = config.get("行维度", "")
        col_dim = config.get("列维度", "")
        agg_method = config.get("聚合方式", "COUNT")

        # 可用维度来自共用的模型读取，与层级、下钻口径一致。
        dimensions_available = read_available_dimensions(store, metric)

        # 模拟透视表计算
        row_values = [f"{row_dim}_{i+1}" for i in range(3)]
        col_values = [f"{col_dim}_{j+1}" for j in range(3)]

        grid = []
        for rv in row_values:
            for cv in col_values:
                grid.append({
                    "row": rv,
                    "column": cv,
                    "value": 50 * (row_values.index(rv) + 1) * (col_values.index(cv) + 1),
                })

        # 计算行汇总
        row_totals = {}
        for rv in row_values:
            row_totals[rv] = sum(g["value"] for g in grid if g["row"] == rv)

        return {
            "config_name": config.get("配置名称"),
            "metric": metric,
            "row_dimension": row_dim,
            "column_dimension": col_dim,
            "aggregation": agg_method,
            "grid": grid,
            "row_totals": row_totals,
            "row_count": len(row_values),
            "column_count": len(col_values),
            "available_dimensions": dimensions_available,
        }

    def share_to_report(self, entry_id: int, report_template_id: int) -> dict[str, Any]:
        """将透视表共享到报告模板。

        跨模块数据流: pivot_table -> report_template
        """
        # 读取与聚合在同一套基准内完成；写回真实仓库放在基准之外，
        # 避免把共享结果写进只读副本。
        with store.read_basis():
            result = self._share_to_report(entry_id, report_template_id)
            snapshot_report = store.find("report_template", report_template_id)
            pivot_result = snapshot_report.get("透视表结果") if snapshot_report else None

        if "error" not in result and pivot_result is not None:
            report = store.find("report_template", report_template_id)
            if report is not None:
                report["透视表结果"] = pivot_result
        return result

    def _share_to_report(self, entry_id: int, report_template_id: int) -> dict[str, Any]:
        config = store.find(self.MODULE, entry_id)
        if config is None:
            return {"error": f"透视配置 {entry_id} 不存在"}

        report = store.find("report_template", report_template_id)
        if report is None:
            return {"error": f"报告模板 {report_template_id} 不存在"}

        # 计算透视结果
        pivot_result = self._compute_pivot(entry_id)
        if "error" in pivot_result:
            return pivot_result

        # 将透视结果注入报告模板
        report["透视表结果"] = pivot_result

        return {
            "pivot_name": config.get("配置名称"),
            "report_name": report.get("模板名称"),
            "status": "已关联",
            "pivot_summary": f"行={pivot_result.get('row_count')} x 列={pivot_result.get('column_count')}",
        }