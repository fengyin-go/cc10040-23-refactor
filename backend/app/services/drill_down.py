"""下钻分析领域服务：设计从汇总到明细的多层次下钻分析路径和联动规则。

跨模块数据流: dimension_model -> drill_down -> pivot_table -> report_template
下钻路径基于维度模型构建，结果可导出到透视表和报告模板。
"""
from __future__ import annotations

from typing import Any

from app.store import store
from app.services.base import BaseModuleService
from app.services.dimensions import (
    read_available_dimensions,
    read_path_dimensions,
)


class DrillDownService(BaseModuleService):
    MODULE = "drill_down"
    ENTITY = "下钻路径"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["路径名称", "起始指标", "下钻维度序列", "跳转规则", "默认展示"]
    REQUIRED_FIELDS = ["路径名称", "起始指标", "下钻维度序列"]
    STATUS_ORDER = ["待生效", "已生效", "已停用"]
    ACTION_RULES = {"启用路径": "已生效", "停用路径": "已停用"}
    NEGATIVE_ACTIONS = ["停用路径"]

    def execute_drill_down(self, entry_id: int, current_level: int = 0) -> dict[str, Any] | None:
        """执行下钻分析，从当前层级钻取到下一级明细。

        跨模块数据流: drill_down -> dimension_model（获取维度结构）
                     drill_down -> pivot_table（透视汇总结果）
        """
        with store.read_basis():
            return self._execute_drill_down(entry_id, current_level)

    def _execute_drill_down(self, entry_id: int, current_level: int = 0) -> dict[str, Any] | None:
        path = store.find(self.MODULE, entry_id)
        if path is None:
            return {"error": f"下钻路径 {entry_id} 不存在"}
        store.notify_entry_read(self.MODULE, path)
        if path["status"] != "已生效":
            return {"error": f"下钻路径 {entry_id} 未生效"}

        metric_name = path.get("起始指标", "")
        # 下钻维度明细沿路径自身的显式序列生成，保持轴顺序。
        dimension_seq = read_path_dimensions(path)

        if current_level >= len(dimension_seq):
            return {"error": "已达到最细粒度，无法继续下钻"}

        current_dimension = dimension_seq[current_level]

        # 可用维度来自共用的模型读取，与层级、透视口径一致。
        model_dimensions = read_available_dimensions(store, metric_name)

        # 模拟下钻结果
        drill_result = {
            "path_name": path.get("路径名称"),
            "current_level": current_level + 1,
            "dimension": current_dimension,
            "total_levels": len(dimension_seq),
            "next_dimension": dimension_seq[current_level + 1] if current_level + 1 < len(dimension_seq) else None,
            "model_dimensions": model_dimensions,
            "jump_rule": path.get("跳转规则", ""),
            "rows": [
                {"dimension_value": f"{current_dimension}_值{i+1}",
                 "metric_value": 100 * (i + 1) * (current_level + 1),
                 "can_drill_further": current_level + 1 < len(dimension_seq)}
                for i in range(5)
            ],
        }
        return drill_result

    def get_drill_tree(self, entry_id: int) -> dict[str, Any] | None:
        """构建下钻路径的完整树状结构。"""
        with store.read_basis():
            return self._get_drill_tree(entry_id)

    def _get_drill_tree(self, entry_id: int) -> dict[str, Any] | None:
        path = store.find(self.MODULE, entry_id)
        if path is None:
            return None

        dimension_seq = read_path_dimensions(path)

        nodes = []
        for i, dim in enumerate(dimension_seq):
            nodes.append({
                "level": i + 1,
                "dimension": dim,
                "has_children": i + 1 < len(dimension_seq),
                "children_dimension": dimension_seq[i + 1] if i + 1 < len(dimension_seq) else None,
            })

        return {
            "path_id": entry_id,
            "path_name": path.get("路径名称"),
            "metric": path.get("起始指标"),
            "node_count": len(nodes),
            "tree": nodes,
        }

    def export_to_pivot(self, entry_id: int) -> dict[str, Any] | None:
        """将下钻路径的维度结构导出为透视表配置推荐。

        跨模块数据流: drill_down -> pivot_table
        """
        with store.read_basis():
            return self._export_to_pivot(entry_id)

    def _export_to_pivot(self, entry_id: int) -> dict[str, Any] | None:
        path = store.find(self.MODULE, entry_id)
        if path is None:
            return None

        dimension_seq = read_path_dimensions(path)

        if len(dimension_seq) < 2:
            return {"error": "下钻路径维度少于两个，无法生成透视配置"}

        pivot_suggestion = {
            "source_path": path.get("路径名称"),
            "metric": path.get("起始指标"),
            "row_dimension": dimension_seq[0] if dimension_seq else "",
            "column_dimension": dimension_seq[1] if len(dimension_seq) > 1 else "",
            "aggregation_method": "SUM",
            "suggested_name": f"{path.get('路径名称')}_透视",
        }
        return pivot_suggestion

    def export_and_register_pivot(self, entry_id: int) -> dict[str, Any]:
        """按导出建议登记透视配置并立即执行聚合。

        跨模块数据流: drill_down -> pivot_table
        行列建议来自路径的显式维度序列；登记后的实际取数轴沿用该建议，
        可用维度则由透视配置一侧共用读取模型维度列表。
        """
        from app.services.pivot_table import PivotTableService

        # 建议读取在同一套读取基准内完成。
        with store.read_basis():
            suggestion = self._export_to_pivot(entry_id)
        if suggestion is None:
            return {"error": f"下钻路径 {entry_id} 不存在"}
        if "error" in suggestion:
            return suggestion

        # 登记写入真实仓库，必须放在读取基准之外，否则只写进只读副本。
        pivot_service = PivotTableService()
        entry, missing = pivot_service.create_entry({
            "配置名称": suggestion["suggested_name"],
            "数据源指标": suggestion["metric"],
            "行维度": suggestion["row_dimension"],
            "列维度": suggestion["column_dimension"],
            "聚合方式": suggestion["aggregation_method"],
        })
        if missing or entry is None:
            return {"error": f"缺少必填字段：{'、'.join(missing)}"}

        pivot = pivot_service.compute_pivot(int(entry["id"]))
        return {
            "suggestion": suggestion,
            "registered": entry,
            "pivot": pivot,
        }
