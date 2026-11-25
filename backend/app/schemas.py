"""通用接口契约；模块字段由领域服务持有，不再为每个模块复制一份模型类。"""
from __future__ import annotations

from typing import Any, Generic, TypeVar

from pydantic import BaseModel, Field

T = TypeVar("T")


class PageResult(BaseModel, Generic[T]):
    items: list[T]
    total: int
    page: int = 1
    size: int = 20


class ActionResult(BaseModel):
    ok: bool
    message: str
    entry: dict[str, Any] | None = None


class EntryPayload(BaseModel):
    values: dict[str, Any] = Field(default_factory=dict)
    remark: str | None = None


class ActionPayload(BaseModel):
    action: str
    remark: str | None = None


class BatchQueryItemPayload(BaseModel):
    """批量查询中的单份数据服务查询请求。"""

    request_id: str | None = None
    service_id: int | None = None
    path: str | None = None
    fields: list[str] | None = None
    scope: dict[str, Any] = Field(default_factory=dict)


class BatchQueryPayload(BaseModel):
    """批量查询提交：多份请求合并为一个查询批次回执。"""

    batch_key: str | None = None
    submitted_by: str = "ops"
    requests: list[BatchQueryItemPayload] = Field(default_factory=list)


class UpstreamStubPayload(BaseModel):
    """模拟上游接口行为：ok 返回固定行、timeout/error 模拟失败。"""

    path: str
    mode: str = "ok"
    rows: list[dict[str, Any]] | None = None
    message: str | None = None
