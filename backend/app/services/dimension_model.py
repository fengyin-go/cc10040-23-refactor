"""维度建模领域服务：管理星型/雪花型维度模型的注册、粒度和事实表映射。

跨模块数据流: dimension_model -> drill_down -> pivot_table -> report_template
维度模型定义了下钻路径的基础和透视表聚合的维度来源。
"""
from __future__ import annotations

from typing import Any

from app.store import store
from app.services.base import BaseModuleService
from app.services.dimensions import (
    raw_dimensions,
    read_model_dimensions,
)
from app.services.pivot_table import PivotTableService


class DimensionModelService(BaseModuleService):
    MODULE = "dimension_model"
    ENTITY = "维度模型"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["模型名称", "事实表", "维度列表", "粒度定义", "发布状态"]
    REQUIRED_FIELDS = ["模型名称", "事实表", "维度列表"]
    STATUS_ORDER = ["设计态", "已上线", "已下线"]
    ACTION_RULES = {"上线模型": "已上线", "下线模型": "已下线"}
    NEGATIVE_ACTIONS = ["下线模型"]

    def get_model_with_analytics(self, entry_id: int) -> dict[str, Any] | None:
        """获取维度模型及其关联的分析配置。

        跨模块数据流: dimension_model -> drill_down -> pivot_table
        """
        with store.read_basis():
            return self._get_model_with_analytics(entry_id)

    def _get_model_with_analytics(self, entry_id: int) -> dict[str, Any] | None:
        model_data = store.get_dimension_model_data(entry_id)
        if model_data is None:
            return None

        model = model_data["model"]
        return {
            "model_id": entry_id,
            "model_name": model.get("模型名称", ""),
            "fact_table": model.get("事实表", ""),
            # 概览沿用原始切分：保留空格、空项和重复项。
            "dimensions": raw_dimensions(model.get("维度列表", "")),
            "granularity": model.get("粒度定义", ""),
            "status": model.get("status", ""),
            "drill_paths": [
                {"id": p["id"], "name": p.get("路径名称", ""),
                 "dimension_sequence": p.get("下钻维度序列", ""),
                 "status": p.get("status", "")}
                for p in model_data["drill_paths"]
            ],
            "pivot_configs": [
                {"id": c["id"], "name": c.get("配置名称", ""),
                 "row_dim": c.get("行维度", ""), "col_dim": c.get("列维度", ""),
                 "status": c.get("status", "")}
                for c in model_data["pivot_configs"]
            ],
            "total_drill_paths": len(model_data["drill_paths"]),
            "total_pivot_configs": len(model_data["pivot_configs"]),
        }

    def get_dimension_hierarchy(self, entry_id: int) -> dict[str, Any] | None:
        """从维度模型构建完整的维度层次结构。

        跨模块数据流: dimension_model -> drill_down（构建下钻树）
        """
        with store.read_basis():
            return self._get_dimension_hierarchy(entry_id)

    def _get_dimension_hierarchy(self, entry_id: int) -> dict[str, Any] | None:
        model = store.find(self.MODULE, entry_id)
        if model is None:
            return None
        store.notify_entry_read(self.MODULE, model)

        dimensions = read_model_dimensions(model)

        # 查找关联的下钻路径构建层次
        drill_paths = store.find_by_field("drill_down", "起始指标", model.get("模型名称", ""))

        hierarchy = {
            "model_name": model.get("模型名称"),
            "root": model.get("事实表", ""),
            "levels": [],
        }
        for i, dim in enumerate(dimensions):
            level = {"level": i + 1, "dimension": dim,
                     "drill_paths": len([p for p in drill_paths if dim in str(p.get("下钻维度序列", ""))]),
                     "granularity": model.get("粒度定义", "")}
            hierarchy["levels"].append(level)

        return hierarchy

    def publish_with_pivots(self, entry_id: int,
                            pivot_configs: list[dict[str, Any]],
                            *, model_values: dict[str, Any] | None = None,
                            expected_revision: int | None = None) -> dict[str, Any]:
        """把维度模型与配套透视配置作为一组发布。

        跨模块数据流: dimension_model -> pivot_table
        两组记录在同一版本下一起可见；版本冲突时整组拒绝，避免旧轴配到新模型。
        """
        from app.store import StoreVersionConflict

        models = store.snapshot_rows(self.MODULE)
        model = next(
            (row for row in models if int(row.get("id", 0)) == entry_id), None)
        if model is None:
            return {"error": f"维度模型 {entry_id} 不存在"}

        if model_values:
            model = {**model, **model_values, "id": model.get("id", entry_id)}
            models = [
                model if int(row.get("id", 0)) == entry_id else row
                for row in models
            ]

        pivots = store.snapshot_rows("pivot_table")
        next_id = max((int(row.get("id", 0)) for row in pivots), default=0) + 1
        created = []
        for values in pivot_configs:
            missing = [
                field for field in ("配置名称", "数据源指标", "行维度")
                if not str(values.get(field) or "").strip()
            ]
            if missing:
                return {"error": f"缺少必填字段：{'、'.join(missing)}"}
            entry = {
                "id": next_id,
                "status": "草稿",
                "pending": True,
                "abnormal": False,
            }
            entry.update({field: values.get(field) for field in PivotTableService.LIST_FIELDS})
            created.append(entry)
            next_id += 1

        try:
            revision = store.publish_bundle(
                {"dimension_model": models, "pivot_table": [*pivots, *created]},
                expected_revision=expected_revision,
            )
        except StoreVersionConflict as exc:
            return {"error": str(exc)}

        return {"entry": store.find(self.MODULE, entry_id),
                "pivot_configs": created, "revision": revision}