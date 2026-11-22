"""内存数据仓库：给每个业务模块准备一份可筛选、可流转的示例数据。

真实项目里这里会换成数据库访问层；当前实现只依赖标准库，保证克隆下来就能起。
同时提供跨模块查询和数据流处理能力。
"""
from __future__ import annotations

import threading
import time
from contextlib import contextmanager
from typing import Any, Iterator

from app.seed import SEED_ROWS


class StoreVersionConflict(RuntimeError):
    """成组发布时读取版本已变化，整组写入被拒绝。"""


class ReadBasis:
    """一次请求固定下来的只读数据视图。

    视图里的每张表都是读取基准建立时的副本。请求之后无论内存仓库如何被替换，
    本视图都只给出同一套旧值，保证一次请求不会新旧混读。
    """

    def __init__(self, tables: dict[str, list[dict[str, Any]]], revision: int) -> None:
        self._tables = tables
        self.revision = revision

    def rows(self, module: str) -> list[dict[str, Any]]:
        return self._tables.setdefault(module, [])

    def find(self, module: str, entry_id: int) -> dict[str, Any] | None:
        for row in self.rows(module):
            if int(row.get("id", 0)) == entry_id:
                return row
        return None

    def find_by_field(self, module: str, field: str, value: Any) -> list[dict[str, Any]]:
        return [row for row in self.rows(module) if row.get(field) == value]

    def find_by_status(self, module: str, status: str) -> list[dict[str, Any]]:
        return [row for row in self.rows(module) if row.get("status") == status]


class Store:
    def __init__(self) -> None:
        self._tables: dict[str, list[dict[str, Any]]] = {
            name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()
        }
        self._id_counters: dict[str, int] = {}
        # Cache layer storage
        self._cache: dict[str, tuple[float, Any]] = {}
        # Cross-module journal
        self._query_log: list[dict[str, Any]] = []
        # Cohort snapshots
        self._cohort_snapshots: dict[str, list[dict[str, Any]]] = {}
        # Anomaly records
        self._anomaly_records: list[dict[str, Any]] = []
        # Recalc history
        self._recalc_history: list[dict[str, Any]] = []
        # Lock for thread safety
        self._lock = threading.Lock()
        # 每次整表替换/成组发布递增，用于请求级读取基准和版本冲突检测
        self._revision = 0
        # 当前线程已固定的读取基准栈，支持嵌套复用
        self._local = threading.local()
        # 读取基准建立后的观察钩子（可用于请求暂停、审计和一致性测试）
        self.on_basis_created: Any = None
        # 读取到请求主记录后的观察钩子：此时读取基准已固定，可安全地暂停
        # 让其他线程替换仓库，再继续验证后续跨模块读取仍用旧值。
        self.after_entry_read: Any = None
        # 兼容别名
        self.on_entry_read: Any = None
        self.on_record_read: Any = None

    def notify_entry_read(self, module: str, entry: dict[str, Any]) -> None:
        for hook in (self.after_entry_read, self.on_entry_read, self.on_record_read):
            if hook is not None:
                hook(module, entry)

    @property
    def lock(self):
        return self._lock

    @property
    def revision(self) -> int:
        return self._revision

    # ── Request-scoped read basis ──

    def _basis_stack(self) -> list[ReadBasis]:
        stack = getattr(self._local, "stack", None)
        if stack is None:
            stack = []
            self._local.stack = stack
        return stack

    @property
    def current_basis(self) -> ReadBasis | None:
        stack = self._basis_stack()
        return stack[-1] if stack else None

    def _copy_tables(self) -> dict[str, list[dict[str, Any]]]:
        return {
            name: [dict(row) for row in rows]
            for name, rows in self._tables.items()
        }

    @contextmanager
    def read_basis(self) -> Iterator[ReadBasis]:
        """在当前线程固定一套读取基准；退出后恢复。

        同一请求内嵌套调用会复用最外层基准，确保多次跨模块读取使用同一套值。
        建立基准时一次性复制全部表，之后该线程的所有 rows/find 读取都落在副本上。
        """
        stack = self._basis_stack()
        if stack:
            yield stack[-1]
            return
        with self._lock:
            basis = ReadBasis(self._copy_tables(), self._revision)
        stack.append(basis)
        try:
            hook = self.on_basis_created
            if hook is not None:
                hook(basis)
            yield basis
        finally:
            stack.pop()

    # 便于调用方按常见命名使用同一种能力
    read_view = read_basis
    read_session = read_basis

    def module_names(self) -> list[str]:
        basis = self.current_basis
        if basis is not None:
            return sorted(basis._tables)
        return sorted(self._tables)

    def rows(self, module: str) -> list[dict[str, Any]]:
        basis = self.current_basis
        if basis is not None:
            return basis.rows(module)
        return self._tables.setdefault(module, [])

    def find(self, module: str, entry_id: int) -> dict[str, Any] | None:
        for row in self.rows(module):
            if int(row.get("id", 0)) == entry_id:
                return row
        return None

    def next_id(self, module: str) -> int:
        rows = self.rows(module)
        if not rows:
            return 1
        return max(int(row.get("id", 0)) for row in rows) + 1

    # ── Atomic replacement / grouped publication ──

    def replace_rows(self, module: str, rows: list[dict[str, Any]]) -> int:
        """整表替换一个模块，返回新版本号。"""
        with self._lock:
            self._tables[module] = [dict(row) for row in rows]
            self._revision += 1
            return self._revision

    # 常见别名，便于调用方按语义选择
    set_rows = replace_rows

    def append_row(self, module: str, values: dict[str, Any]) -> dict[str, Any]:
        """向真实仓库追加一条记录（不受当前线程读取基准影响）。"""
        with self._lock:
            rows = self._tables.setdefault(module, [])
            next_id = max((int(row.get("id", 0)) for row in rows), default=0) + 1
            entry = {"id": next_id, **values}
            rows.append(entry)
            self._revision += 1
            return dict(entry)

    def snapshot_rows(self, module: str) -> list[dict[str, Any]]:
        """读取某模块在真实仓库中的当前值（绕过读取基准），用于成组发布。"""
        with self._lock:
            return [dict(row) for row in self._tables.get(module, [])]

    def replace_row(self, module: str, entry_id: int, values: dict[str, Any]) -> int:
        """原位替换一条记录（其余字段保留），返回新版本号。"""
        with self._lock:
            rows = self._tables.setdefault(module, [])
            for index, row in enumerate(rows):
                if int(row.get("id", 0)) == entry_id:
                    merged = {**row, **values, "id": row.get("id", entry_id)}
                    rows[index] = merged
                    break
            else:
                rows.append({**values, "id": entry_id})
            self._revision += 1
            return self._revision

    def publish_bundle(self, updates: dict[str, list[dict[str, Any]]],
                       *, expected_revision: int | None = None) -> int:
        """成组发布多个模块：要么整组可见，要么全部不可见。

        传入 expected_revision 时做乐观并发校验，版本不一致则拒绝写入。
        """
        with self._lock:
            if expected_revision is not None and expected_revision != self._revision:
                raise StoreVersionConflict(
                    f"版本冲突：期望 {expected_revision}，当前 {self._revision}")
            for module, rows in updates.items():
                self._tables[module] = [dict(row) for row in rows]
            self._revision += 1
            return self._revision

    def publish_dimension_bundle(
        self,
        updates: dict[str, list[dict[str, Any]]] | None = None,
        *,
        model_rows: list[dict[str, Any]] | None = None,
        drill_rows: list[dict[str, Any]] | None = None,
        pivot_rows: list[dict[str, Any]] | None = None,
        expected_revision: int | None = None,
    ) -> int:
        """成组发布维度分析相关模块的便捷入口。

        既接受 ``{"dimension_model": [...]}`` 形式，也接受具名关键字形式；
        同一次调用里的模块在新版本下一起可见。
        """
        merged: dict[str, list[dict[str, Any]]] = dict(updates or {})
        if model_rows is not None:
            merged["dimension_model"] = model_rows
        if drill_rows is not None:
            merged["drill_down"] = drill_rows
        if pivot_rows is not None:
            merged["pivot_table"] = pivot_rows
        return self.publish_bundle(merged, expected_revision=expected_revision)

    def replace_dimension_model(self, rows: list[dict[str, Any]]) -> int:
        return self.replace_rows("dimension_model", rows)

    def replace_drill_path(self, rows: list[dict[str, Any]]) -> int:
        return self.replace_rows("drill_down", rows)

    def replace_pivot_config(self, rows: list[dict[str, Any]]) -> int:
        return self.replace_rows("pivot_table", rows)

    def overview(self) -> dict[str, object]:
        modules: list[dict[str, object]] = []
        for name in self.module_names():
            rows = self.rows(name)
            modules.append({
                "name": name,
                "created": len(rows),
                "pending": sum(1 for row in rows if row.get("pending")),
                "abnormal": sum(1 for row in rows if row.get("abnormal")),
            })
        cards = [
            {"label": "业务模块", "value": len(modules)},
            {"label": "今日新增", "value": sum(int(item["created"]) for item in modules)},
            {"label": "待处理", "value": sum(int(item["pending"]) for item in modules)},
            {"label": "异常量", "value": sum(int(item["abnormal"]) for item in modules)},
        ]
        return {"cards": cards, "modules": modules}

    # ── Cross-module query helpers ──

    def find_by_field(self, module: str, field: str, value: Any) -> list[dict[str, Any]]:
        """查找模块中 field 等于 value 的所有记录。"""
        return [row for row in self.rows(module) if row.get(field) == value]

    def find_by_status(self, module: str, status: str) -> list[dict[str, Any]]:
        return [row for row in self.rows(module) if row.get("status") == status]

    # ── Query journal ──

    def log_query(self, metric_id: int, operator: str, sql_summary: str,
                  row_count: int, duration_ms: float, status: str = "正常") -> dict[str, Any]:
        entry = {
            "id": len(self._query_log) + 1,
            "查询时间": time.strftime("%Y-%m-%d %H:%M:%S"),
            "操作人": operator,
            "涉及指标": f"metric_{metric_id}",
            "查询语句摘要": sql_summary,
            "返回行数": str(row_count),
            "status": status,
            "duration_ms": duration_ms,
            "pending": status != "正常",
            "abnormal": status in ("慢查询", "异常终止"),
        }
        self._query_log.append(entry)
        return entry

    def query_logs(self) -> list[dict[str, Any]]:
        return self._query_log

    # ── Cache operations ──

    def cache_get(self, key: str) -> Any | None:
        """获取缓存，过期返回 None。"""
        entry = self._cache.get(key)
        if entry is None:
            return None
        expires_at, value = entry
        if time.time() > expires_at:
            del self._cache[key]
            return None
        return value

    def cache_set(self, key: str, value: Any, ttl: int) -> None:
        """设置缓存，ttl 单位为秒。"""
        self._cache[key] = (time.time() + ttl, value)

    def cache_invalidate(self, key: str) -> bool:
        return self._cache.pop(key, None) is not None

    def cache_invalidate_by_prefix(self, prefix: str) -> int:
        count = 0
        for key in list(self._cache):
            if key.startswith(prefix):
                del self._cache[key]
                count += 1
        return count

    # ── Cohort operations ──

    def save_cohort_snapshot(self, cohort_id: int, members: list[dict[str, Any]]) -> None:
        self._cohort_snapshots[str(cohort_id)] = members

    def get_cohort_snapshot(self, cohort_id: int) -> list[dict[str, Any]] | None:
        return self._cohort_snapshots.get(str(cohort_id))

    # ── Anomaly detection operations ──

    def record_anomaly(self, metric_id: int, severity: str, description: str,
                       value: float, threshold: float) -> dict[str, Any]:
        record = {
            "id": len(self._anomaly_records) + 1,
            "指标ID": metric_id,
            "严重程度": severity,
            "描述": description,
            "当前值": value,
            "阈值": threshold,
            "时间": time.strftime("%Y-%m-%d %H:%M:%S"),
        }
        self._anomaly_records.append(record)
        return record

    def anomaly_records(self) -> list[dict[str, Any]]:
        return self._anomaly_records

    # ── Recalc operations ──

    def record_recalc(self, metric_id: int, old_version: str, new_version: str,
                      time_range: str, diff_summary: str) -> dict[str, Any]:
        record = {
            "id": len(self._recalc_history) + 1,
            "关联指标": f"metric_{metric_id}",
            "旧口径": old_version,
            "新口径": new_version,
            "历史时间范围": time_range,
            "产出差异": diff_summary,
            "重算时间": time.strftime("%Y-%m-%d %H:%M:%S"),
        }
        self._recalc_history.append(record)
        return record

    def recalc_history(self) -> list[dict[str, Any]]:
        return self._recalc_history

    # ── Data artifacts for cross-module consumption ──

    def get_metric_data(self, metric_id: int) -> dict[str, Any] | None:
        """Get metric data with its calibers, queries, and services."""
        metric = self.find("metric_definition", metric_id)
        if metric is None:
            return None
        return {
            "metric": metric,
            "calibers": self.find_by_field("caliber_version", "所属指标",
                                           metric.get("指标名称", "")),
            "queries": self.find_by_field("query_builder", "关联指标",
                                          metric.get("指标名称", "")),
            "services": [row for row in self.rows("data_service")
                         if metric.get("指标名称", "") in str(row.get("关联指标或模型", ""))],
            "subscriptions": self.find_by_field("data_subscription", "关联指标",
                                                 metric.get("指标名称", "")),
        }

    def get_dimension_model_data(self, model_id: int) -> dict[str, Any] | None:
        """Get dimension model with its drill-down paths and pivot configs."""
        model = self.find("dimension_model", model_id)
        if model is None:
            return None
        store.notify_entry_read("dimension_model", model)
        return {
            "model": model,
            "drill_paths": self.find_by_field("drill_down", "起始指标",
                                              model.get("模型名称", "")),
            "pivot_configs": self.find_by_field("pivot_table", "数据源指标",
                                                 model.get("模型名称", "")),
        }

    def get_cohort_analysis(self, cohort_id: int) -> dict[str, Any] | None:
        """Get cohort with experiments and segment comparisons."""
        cohort = self.find("user_cohort", cohort_id)
        if cohort is None:
            return None
        cohort_name = cohort.get("分群名称", "")
        return {
            "cohort": cohort,
            "experiments": [row for row in self.rows("experiment_group")
                            if cohort_name in str(row.get("筛选条件", ""))],
            "comparisons": [row for row in self.rows("segment_compare")
                            if cohort_name in (str(row.get("分群A", "")) + str(row.get("分群B", "")))],
            "anomaly_rules": [row for row in self.rows("anomaly_detector")
                              if cohort_name in str(row.get("目标指标", ""))],
        }

    def get_field_lineage(self, field_name: str) -> dict[str, Any]:
        """Trace a field's complete lineage chain."""
        entries = self.find_by_field("field_lineage", "目标字段", field_name)
        if not entries:
            return {"field": field_name, "lineage": [], "depth": 0}

        lineage = [{
            "source_fields": entry.get("来源字段列表", ""),
            "transform": entry.get("转换逻辑", ""),
            "intermediate_table": entry.get("中间表", ""),
            "depth": entry.get("血缘深度", ""),
            "status": entry.get("status", ""),
        } for entry in entries]

        return {
            "field": field_name,
            "lineage": lineage,
            "depth": len(lineage),
        }


store = Store()