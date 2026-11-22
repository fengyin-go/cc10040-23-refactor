"""维度读取的共用实现：模型维度列表、下钻维度序列和轴维度统一从这里解析。

维度建模、下钻分析和透视表配置原本各自复制了一份 `split(",")` 逻辑，
现在都通过本模块读取。两种读取口径刻意保持不同：

- ``raw_dimensions``：模型关联概览使用，保留原始空格、空项和重复项；
- ``normalized_dimensions``：层级、下钻和透视的可用维度使用，裁掉空项和
  两端空格，但保留原有顺序和重复项。

另外提供按名称查找模型的共用入口，避免各处各写一遍匹配规则。
"""
from __future__ import annotations

from typing import Any, Protocol


class DimensionReader(Protocol):
    """可以读取数据的对象（内存仓库或其请求级读取基准）。"""

    def rows(self, module: str) -> list[dict[str, Any]]: ...

    def find(self, module: str, entry_id: int) -> dict[str, Any] | None: ...

    def find_by_field(self, module: str, field: str, value: Any) -> list[dict[str, Any]]: ...


def raw_dimensions(raw: Any) -> list[str]:
    """按原始文本切分维度，保留空格、空项和重复项。

    与维度建模概览的历史输出完全一致：空文本得到空列表，否则按逗号切开，
    不做 strip，也不丢弃空串。
    """
    if not raw:
        return []
    return str(raw).split(",")


def normalized_dimensions(raw: Any) -> list[str]:
    """切分并规范化维度：裁掉空项和两端空格，保留顺序和重复项。"""
    return [item.strip() for item in str(raw or "").split(",") if item.strip()]


def read_model_dimensions(
    model: dict[str, Any] | None, *, normalize: bool = True
) -> list[str]:
    """从模型记录读取维度列表。"""
    if model is None:
        return []
    field = model.get("维度列表", "")
    return normalized_dimensions(field) if normalize else raw_dimensions(field)


def read_path_dimensions(path: dict[str, Any] | None) -> list[str]:
    """从下钻路径读取显式维度序列，保持轴顺序。"""
    if path is None:
        return []
    return normalized_dimensions(path.get("下钻维度序列", ""))


def find_dimension_model(
    reader: DimensionReader, model_name: Any
) -> dict[str, Any] | None:
    """按模型名称查找维度模型，供下钻和透视共用。"""
    models = reader.find_by_field("dimension_model", "模型名称", model_name)
    return models[0] if models else None


def read_available_dimensions(
    reader: DimensionReader, model_name: Any
) -> list[str]:
    """读取某个指标/模型名称对应的可用维度（规范化口径）。"""
    return read_model_dimensions(find_dimension_model(reader, model_name))
