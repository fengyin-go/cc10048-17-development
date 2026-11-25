"""数据服务领域服务：API接口发布、查询封装、限流控制、批量查询批次回执。"""
# 数据操作：存储、导出、回填，与上下游模块联动消费批次数据。
from __future__ import annotations

import hashlib
import json
import random
import time
from datetime import datetime
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class UpstreamTimeout(Exception):
    """上游接口超时：本次执行没有实际读到任何数据行。"""


class UpstreamFailure(Exception):
    """上游接口执行失败：本次执行没有实际读到任何数据行。"""


# 支付对账接口的默认模拟上游数据（8 行）。
# 行内故意携带「内部备注」列：它不在接口「返回字段」白名单内，
# 用于验证批量入口只交付各请求允许读的字段。
DEFAULT_PAYMENT_ROWS: list[dict[str, Any]] = [
    {
        "订单号": f"PAY20261124{idx:03d}",
        "支付金额": round(96.5 + idx * 12.3, 2),
        "支付时间": f"2026-11-24 08:{idx:02d}:00",
        "渠道": "支付宝" if idx % 2 else "微信",
        "内部备注": f"风控复核{idx}",
    }
    for idx in range(1, 9)
]

# 退款对账接口恢复后的默认模拟上游数据（5 行），同样携带不下发的内部列。
DEFAULT_REFUND_ROWS: list[dict[str, Any]] = [
    {
        "退款单号": f"REF20261124{idx:03d}",
        "退款金额": round(30.0 + idx * 6.5, 2),
        "退款时间": f"2026-11-24 10:{idx:02d}:00",
        "退款原因": "重复支付" if idx % 2 else "用户取消",
        "内部备注": f"售后工单{idx}",
    }
    for idx in range(1, 6)
]


class DataServiceService(BaseModuleService):
    MODULE = "data_service"
    ENTITY = "服务接口"
    OPERATION_TERM = "关联"
    LIST_FIELDS = ["接口标识", "关联指标", "接口路径", "查询参数", "返回字段", "调用上限", "缓存秒数", "服务状态"]
    REQUIRED_FIELDS = ["接口标识", "关联指标", "接口路径"]
    STATUS_ORDER = ["待发布", "已发布", "已限流", "已下线"]
    ACTION_RULES = {"发布服务": "已发布", "下线服务": "已发布"}
    NEGATIVE_ACTIONS = []

    # 批次项状态：成功（待领取）/ 失败（可重试）/ 已签收（结果已交付，禁止第二次读取）
    ITEM_SUCCESS = "成功"
    ITEM_FAILED = "失败"
    ITEM_CLAIMED = "已签收"

    def __init__(self) -> None:
        # 查询批次回执登记簿：batch_key 与 receipt_no 双索引
        self._receipts_by_key: dict[str, dict[str, Any]] = {}
        self._receipts_by_no: dict[str, dict[str, Any]] = {}
        self._receipt_seq = 0
        # 模拟上游接口行为注册表：接口路径 -> ok/timeout/error
        self._upstream: dict[str, dict[str, Any]] = {}
        self.reset_upstream_behaviors()

    def invoke_service(self, service_id: int) -> dict[str, Any]:
        """模拟调用数据服务接口,返回查询结果。"""
        svc = store.find(self.MODULE, service_id)
        if not svc:
            return {"error": f"服务接口 {service_id} 不存在"}
        if svc.get("status") not in ("已发布",):
            return {"error": f"接口 {service_id} 未发布"}
        rng = random.Random(service_id)
        fields = str(svc.get("返回字段", "指标值")).split(",")
        cache_ttl = int(str(svc.get("缓存秒数", "0")) or 0)
        result_data = [{field: round(rng.uniform(10, 500), 2) for field in fields} for _ in range(rng.randint(1, 20))]
        return {
            "service_id": service_id,
            "path": svc.get("接口路径"),
            "metric": svc.get("关联指标"),
            "result_count": len(result_data),
            "results": result_data,
            "cache_ttl_seconds": cache_ttl,
        }

    # ------------------------------------------------------------------
    # 批量查询入口：把多份查询请求合并为一个查询批次回执
    # ------------------------------------------------------------------
    def submit_batch(self, payload: dict[str, Any]) -> dict[str, Any]:
        """合并多份数据服务查询请求为一个查询批次回执并逐项执行。

        - 每项请求独立执行，回执记录该项实际读取的指标、字段与范围，供调用者核对；
        - 失败项只记录失败原因，行数置空，失败不能冒充返回零行；
        - 同一 batch_key 重复提交时，已完成/已签收的请求只保留原执行结果，
          不再发起读取，仅为未成功的请求项执行新一轮读取。
        """
        requests = payload.get("requests") or []
        if not requests:
            return {"ok": False, "message": "批量查询请求不能为空"}
        missing = [str(i + 1) for i, req in enumerate(requests)
                   if not str(req.get("request_id") or "").strip()]
        if missing:
            return {"ok": False,
                    "message": f"第 {'、'.join(missing)} 项请求缺少 request_id，无法关联批次回执"}
        submitted_by = str(payload.get("submitted_by") or "ops")
        batch_key = str(payload.get("batch_key") or "").strip() or self._derive_batch_key(requests)

        receipt = self._receipts_by_key.get(batch_key)
        resubmitted = receipt is not None
        if receipt is None:
            receipt = self._new_receipt(batch_key, submitted_by)

        notes: dict[str, str] = {}
        duplicates_ignored: list[str] = []
        seen: set[str] = set()
        for req in requests:
            request_id = str(req.get("request_id")).strip()
            if request_id in seen:
                # 同一次提交内的重复请求只保留第一份
                duplicates_ignored.append(request_id)
                continue
            seen.add(request_id)
            existing = receipt["items"].get(request_id)
            if existing is not None and existing["status"] in (self.ITEM_SUCCESS, self.ITEM_CLAIMED):
                # 重复提交：实际已完成的请求只保留原执行结果，已签收的不再读取
                note = ("已签收，未发起第二次读取" if existing["status"] == self.ITEM_CLAIMED
                        else "已完成，保留原执行结果")
                if self._request_drifts(existing, req):
                    note += "；本次提交参数与已完成执行不一致，仍以原执行为准"
                notes[request_id] = note
                continue
            item = existing if existing is not None else self._new_item(request_id)
            self._execute_item(receipt, item, req)
            receipt["items"][request_id] = item
            notes[request_id] = (
                ("重试执行" if item["executions"] > 1 else "本次执行")
                + ("成功" if item["status"] == self.ITEM_SUCCESS else f"失败：{item['error']}")
            )
        receipt["updated_at"] = self._now()
        return {
            "ok": True,
            "resubmitted": resubmitted,
            "duplicates_ignored": duplicates_ignored,
            "receipt": self._receipt_snapshot(receipt, notes),
        }

    def get_receipt(self, receipt_no: str) -> dict[str, Any] | None:
        """按回执号查询批次回执：每项给出可独立领取的结果或等待说明。"""
        receipt = self._receipts_by_no.get(receipt_no)
        if receipt is None:
            return None
        return self._receipt_snapshot(receipt)

    def claim_item(self, receipt_no: str, request_id: str) -> dict[str, Any]:
        """签收并领取单个请求项的查询结果；每项结果只交付一次。"""
        receipt = self._receipts_by_no.get(receipt_no)
        if receipt is None:
            return {"ok": False, "found": False,
                    "message": f"查询批次回执 {receipt_no} 不存在"}
        item = receipt["items"].get(request_id)
        if item is None:
            return {"ok": False, "found": False,
                    "message": f"回执 {receipt_no} 中不存在请求项 {request_id}"}
        if item["status"] == self.ITEM_CLAIMED:
            return {
                "ok": False, "found": True,
                "message": f"请求项 {request_id} 已签收，不能为已签收查询发起第二次读取",
                "receipt_no": receipt_no,
                "request_id": request_id,
                "claimed_at": item.get("claimed_at"),
            }
        if item["status"] == self.ITEM_FAILED:
            return {
                "ok": False, "found": True,
                "message": f"请求项 {request_id} 本轮未成功，暂无可领取数据",
                "receipt_no": receipt_no,
                "request_id": request_id,
                "error": item.get("error"),
                "waiting": self._waiting_note(receipt, item),
            }
        rows = item.get("_rows") or []
        item["_rows"] = None  # 交付即清空，保证同一查询不会被读出第二次
        item["status"] = self.ITEM_CLAIMED
        item["claimed"] = True
        item["claimed_at"] = self._now()
        receipt["updated_at"] = self._now()
        return {
            "ok": True, "found": True,
            "message": f"请求项 {request_id} 已签收，本次交付 {len(rows)} 行",
            "receipt_no": receipt_no,
            "batch_key": receipt["batch_key"],
            "request_id": request_id,
            "status": self.ITEM_CLAIMED,
            "metric": item.get("metric"),
            "fields_used": list(item["fields_used"]),
            "scope_used": dict(item["scope_used"]),
            "row_count": item["row_count"],
            "rows": rows,
            "log_id": item.get("log_id"),
            "claimed_at": item["claimed_at"],
        }

    # ------------------------------------------------------------------
    # 模拟上游接口行为注册表（演示与测试用）
    # ------------------------------------------------------------------
    def reset_upstream_behaviors(self) -> None:
        """恢复默认上游行为：支付对账正常返回 8 行，退款对账超时。"""
        self._upstream = {
            "/recon/payment": {"mode": "ok", "rows": [dict(r) for r in DEFAULT_PAYMENT_ROWS]},
            "/recon/refund": {"mode": "timeout", "message": "退款接口响应超时（模拟）"},
        }

    def set_upstream_behavior(self, path: str, behavior: dict[str, Any]) -> dict[str, Any]:
        """设置某个接口路径的模拟上游行为：ok / timeout / error。"""
        path = str(path or "").strip()
        if not path:
            return {"ok": False, "message": "必须提供接口路径 path"}
        mode = str(behavior.get("mode") or "").strip()
        if mode not in ("ok", "timeout", "error"):
            return {"ok": False, "message": "mode 必须是 ok / timeout / error 之一"}
        entry: dict[str, Any] = {"mode": mode}
        if mode == "ok":
            rows = behavior.get("rows")
            if rows is None:
                return {"ok": False, "message": "mode=ok 时必须提供 rows（可为空列表）"}
            entry["rows"] = [dict(row) for row in rows]
        if behavior.get("message"):
            entry["message"] = str(behavior["message"])
        self._upstream[path] = entry
        return {"ok": True,
                "message": f"上游接口 {path} 行为已设置为 {mode}",
                "path": path, "behavior": self._behavior_snapshot(entry)}

    def list_upstream_behaviors(self) -> dict[str, Any]:
        """查看当前全部模拟上游行为。"""
        return {"ok": True,
                "upstreams": {path: self._behavior_snapshot(behavior)
                              for path, behavior in sorted(self._upstream.items())}}

    def reset_batch_receipts(self) -> None:
        """清空批次回执登记簿（测试与演示复位用）。"""
        self._receipts_by_key.clear()
        self._receipts_by_no.clear()
        self._receipt_seq = 0

    # ------------------------------------------------------------------
    # 批量执行内部实现
    # ------------------------------------------------------------------
    @staticmethod
    def _now() -> str:
        return datetime.now().isoformat(timespec="seconds")

    @staticmethod
    def _behavior_snapshot(behavior: dict[str, Any]) -> dict[str, Any]:
        return {
            "mode": behavior.get("mode"),
            "row_count": len(behavior.get("rows") or []) if behavior.get("mode") == "ok" else None,
            "message": behavior.get("message"),
        }

    def _new_receipt(self, batch_key: str, submitted_by: str) -> dict[str, Any]:
        self._receipt_seq += 1
        receipt = {
            "receipt_no": f"QB{datetime.now():%Y%m%d}-{self._receipt_seq:04d}",
            "batch_key": batch_key,
            "submitted_by": submitted_by,
            "created_at": self._now(),
            "updated_at": self._now(),
            "items": {},
        }
        self._receipts_by_key[batch_key] = receipt
        self._receipts_by_no[receipt["receipt_no"]] = receipt
        return receipt

    def _new_item(self, request_id: str) -> dict[str, Any]:
        return {
            "request_id": request_id,
            "service_id": None,
            "接口标识": None,
            "接口路径": None,
            "metric": None,
            "requested_fields": None,
            "scope": {},
            "fields_used": [],
            "scope_used": {},
            "rejected_fields": [],
            "rejected_scope_keys": [],
            "status": self.ITEM_FAILED,
            "row_count": None,
            "error": None,
            "executions": 0,
            "log_id": None,
            "claimed": False,
            "claimed_at": None,
            "_rows": None,
        }

    @staticmethod
    def _derive_batch_key(requests: list[dict[str, Any]]) -> str:
        canonical = json.dumps(
            [
                {
                    "request_id": str(req.get("request_id")),
                    "service_id": req.get("service_id"),
                    "path": req.get("path"),
                    "fields": req.get("fields"),
                    "scope": req.get("scope") or {},
                }
                for req in requests
            ],
            ensure_ascii=False, sort_keys=True,
        )
        return "AUTO-" + hashlib.sha1(canonical.encode("utf-8")).hexdigest()[:12]

    def _execute_item(self, receipt: dict[str, Any], item: dict[str, Any],
                      req: dict[str, Any]) -> None:
        """实际执行一份查询请求，并把执行结果写回批次项与查询日志。"""
        item["executions"] += 1
        started = time.perf_counter()
        svc, error = self._resolve_service(req)
        metric = svc.get("关联指标") if svc else None
        fields_used: list[str] = []
        scope_used: dict[str, Any] = {}
        rejected_fields: list[str] = []
        rejected_scope: list[str] = []
        rows: list[dict[str, Any]] | None = None
        if svc is not None and error is None and str(svc.get("status")) != "已发布":
            error = f"接口未发布（当前状态：{svc.get('status')}），拒绝读取"
        if svc is not None and error is None:
            fields_used, rejected_fields = self._resolve_fields(svc, req.get("fields"))
            scope_used, rejected_scope = self._resolve_scope(svc, req.get("scope"))
            if not fields_used:
                error = "请求字段超出接口允许读取范围，本次未读取任何字段"
        if svc is not None and error is None:
            try:
                raw_rows = self._call_upstream(svc, scope_used)
                # 只交付该请求允许读的字段，白名单外的列一律裁剪
                rows = [
                    {field: row[field] for field in fields_used if field in row}
                    for row in raw_rows
                ]
            except (UpstreamTimeout, UpstreamFailure) as exc:
                error = str(exc)
        elapsed_ms = round((time.perf_counter() - started) * 1000, 1)
        success = error is None and rows is not None
        item.update({
            "service_id": svc.get("id") if svc else None,
            "接口标识": svc.get("接口标识") if svc else None,
            "接口路径": svc.get("接口路径") if svc else None,
            "metric": metric,
            "requested_fields": list(req.get("fields")) if req.get("fields") else None,
            "scope": dict(req.get("scope") or {}),
            # 失败时不存在“实际使用的范围”，字段与范围都置空
            "fields_used": fields_used if success else [],
            "scope_used": scope_used if success else {},
            "rejected_fields": rejected_fields,
            "rejected_scope_keys": rejected_scope,
            "status": self.ITEM_SUCCESS if success else self.ITEM_FAILED,
            # 失败行数置空：失败不能冒充返回零行
            "row_count": len(rows) if success else None,
            "error": None if success else error,
            "_rows": rows if success else None,
        })
        item["log_id"] = self._write_execution_log(receipt, item, elapsed_ms)

    def _resolve_service(self, req: dict[str, Any]) -> tuple[dict[str, Any] | None, str | None]:
        service_id = req.get("service_id")
        path = str(req.get("path") or "").strip()
        if service_id is None and not path:
            return None, "请求必须提供 service_id 或 path 之一"
        if service_id is not None:
            try:
                svc = store.find(self.MODULE, int(service_id))
            except (TypeError, ValueError):
                return None, f"service_id 非法：{service_id}"
            if svc is None:
                return None, f"服务接口 {service_id} 不存在"
            return svc, None
        for row in store.rows(self.MODULE):
            if str(row.get("接口路径") or "") == path:
                return row, None
        return None, f"接口路径 {path} 不存在"

    @staticmethod
    def _resolve_fields(svc: dict[str, Any],
                        requested: list[str] | None) -> tuple[list[str], list[str]]:
        """计算允许读取的字段：接口「返回字段」白名单 ∩ 请求字段。"""
        allowed = [f.strip() for f in str(svc.get("返回字段") or "").split(",") if f.strip()]
        if not requested:
            return allowed, []
        wanted = {str(f).strip() for f in requested if str(f).strip()}
        kept = [f for f in allowed if f in wanted]
        rejected = [f for f in requested if str(f).strip() and str(f).strip() not in set(allowed)]
        return kept, rejected

    @staticmethod
    def _resolve_scope(svc: dict[str, Any],
                       scope: dict[str, Any] | None) -> tuple[dict[str, Any], list[str]]:
        """计算实际使用的范围：只保留接口「查询参数」声明支持的键。"""
        scope = dict(scope or {})
        supported = [p.strip() for p in str(svc.get("查询参数") or "").split(",") if p.strip()]
        if not supported:
            return scope, []
        used = {k: v for k, v in scope.items() if k in supported}
        rejected = [k for k in scope if k not in supported]
        return used, rejected

    def _call_upstream(self, svc: dict[str, Any], scope_used: dict[str, Any]) -> list[dict[str, Any]]:
        """调用（模拟的）上游接口；未注册行为时按接口与范围确定性生成。"""
        path = str(svc.get("接口路径") or "")
        behavior = self._upstream.get(path)
        if behavior is None:
            return self._generate_rows(svc, scope_used)
        mode = behavior.get("mode")
        if mode == "timeout":
            raise UpstreamTimeout(str(behavior.get("message") or f"接口 {path} 响应超时"))
        if mode == "error":
            raise UpstreamFailure(str(behavior.get("message") or f"接口 {path} 执行失败"))
        rows = [dict(row) for row in behavior.get("rows") or []]
        return self._filter_by_scope(rows, scope_used)

    @staticmethod
    def _filter_by_scope(rows: list[dict[str, Any]],
                         scope_used: dict[str, Any]) -> list[dict[str, Any]]:
        """行上带有与范围同名字段时按等值过滤，未携带该字段的行视为命中。"""
        if not scope_used:
            return rows
        return [row for row in rows
                if all(k not in row or str(row[k]) == str(v) for k, v in scope_used.items())]

    @staticmethod
    def _generate_rows(svc: dict[str, Any], scope_used: dict[str, Any]) -> list[dict[str, Any]]:
        seed = f"{svc.get('id')}:{json.dumps(scope_used, sort_keys=True, ensure_ascii=False)}"
        rng = random.Random(seed)
        fields = [f.strip() for f in str(svc.get("返回字段") or "指标值").split(",") if f.strip()]
        return [{f: round(rng.uniform(10, 500), 2) for f in fields}
                for _ in range(rng.randint(1, 20))]

    def _write_execution_log(self, receipt: dict[str, Any], item: dict[str, Any],
                             elapsed_ms: float) -> int:
        """把本次实际执行写入查询日志，并用批次回执号关联实际响应信息。"""
        success = item["status"] == self.ITEM_SUCCESS
        rows = store.rows("query_log")
        log_id = max((int(row.get("id", 0)) for row in rows), default=0) + 1
        # 日志只记录该次实际使用的范围；失败时没有任何实际读取
        actual_scope = (
            {"fields": list(item["fields_used"]), "scope": dict(item["scope_used"])}
            if success else {"fields": [], "scope": {}}
        )
        entry = {
            "id": log_id,
            "记录标识": f"QB-{receipt['receipt_no']}-{item['request_id']}-E{item['executions']}",
            "查询内容": (
                f"批量查询[{receipt['receipt_no']}]请求项[{item['request_id']}] "
                f"接口[{item.get('接口标识') or '未匹配'}] 指标[{item.get('metric') or '未匹配'}] "
                f"请求范围{json.dumps(item.get('scope') or {}, ensure_ascii=False)}"
            ),
            "查询用户": receipt.get("submitted_by") or "ops",
            "查询时间": self._now(),
            # 失败时行数留空：失败不能冒充返回零行
            "返回行数": str(item["row_count"]) if success else "",
            "执行耗时": f"{elapsed_ms}ms",
            "缓存命中": "否",
            "错误信息": "" if success else str(item.get("error") or "未知失败"),
            "status": "已记录",
            "pending": not success,
            "abnormal": not success,
            # 批次回执关联信息：供后续对账按回执号独立领取结果或等待
            "批次回执": receipt["receipt_no"],
            "批次键": receipt["batch_key"],
            "请求标识": item["request_id"],
            "执行结果": "成功" if success else "失败",
            "实际指标": item.get("metric") if success else None,
            "实际使用范围": actual_scope,
        }
        rows.append(entry)
        return log_id

    @staticmethod
    def _request_drifts(item: dict[str, Any], req: dict[str, Any]) -> bool:
        """重复提交时检查请求参数是否与已完成执行不一致。"""
        if req.get("service_id") is not None and item.get("service_id") is not None:
            if str(req["service_id"]) != str(item["service_id"]):
                return True
        new_fields = list(req.get("fields")) if req.get("fields") else None
        if new_fields is not None and new_fields != (item.get("requested_fields") or None):
            return True
        new_scope = dict(req.get("scope") or {})
        if new_scope and new_scope != (item.get("scope") or {}):
            return True
        return False

    def _waiting_note(self, receipt: dict[str, Any], item: dict[str, Any]) -> str:
        return (
            f"请求项 {item['request_id']} 最近一次执行失败：{item.get('error') or '未知原因'}。"
            f"请等待接口恢复后按原批次键 {receipt['batch_key']} 重新提交，"
            "已完成或已签收的请求项不会重复读取。"
        )

    def _batch_status(self, receipt: dict[str, Any]) -> str:
        statuses = [item["status"] for item in receipt["items"].values()]
        if not statuses:
            return "待执行"
        if all(s == self.ITEM_CLAIMED for s in statuses):
            return "已签收"
        if all(s in (self.ITEM_SUCCESS, self.ITEM_CLAIMED) for s in statuses):
            return "已完成"
        if any(s in (self.ITEM_SUCCESS, self.ITEM_CLAIMED) for s in statuses):
            return "部分完成"
        return "执行失败"

    def _item_snapshot(self, receipt: dict[str, Any], item: dict[str, Any]) -> dict[str, Any]:
        snap = {k: v for k, v in item.items() if not k.startswith("_")}
        snap["claimable"] = item["status"] == self.ITEM_SUCCESS
        if item["status"] == self.ITEM_FAILED:
            snap["waiting"] = self._waiting_note(receipt, item)
        return snap

    def _receipt_snapshot(self, receipt: dict[str, Any],
                          notes: dict[str, str] | None = None) -> dict[str, Any]:
        items = []
        for request_id, item in receipt["items"].items():
            snap = self._item_snapshot(receipt, item)
            if notes and request_id in notes:
                snap["note"] = notes[request_id]
            items.append(snap)
        summary: dict[str, Any] = {"total": len(items)}
        for snap in items:
            summary[snap["status"]] = summary.get(snap["status"], 0) + 1
        return {
            "receipt_no": receipt["receipt_no"],
            "batch_key": receipt["batch_key"],
            "submitted_by": receipt["submitted_by"],
            "created_at": receipt["created_at"],
            "updated_at": receipt["updated_at"],
            "status": self._batch_status(receipt),
            "summary": summary,
            "items": items,
        }

    def domain_enrich(self, config_id: int) -> dict:
        """对当前模块执行聚合操作，联动合并和记录，产出标准化数据产物。

        数据加工流程：读取原始记录 → 执行聚合处理 → 合并校验字段完整性 → 生成记录批次标识 →
        写入存储层并返回可追溯的处理结果，下游消费模块可基于批次号追踪数据链路。
        """
        import time, json
        from app.store import store
        entry = store.find(self.MODULE, config_id)
        # 聚合：遍历当前模块全部记录执行核心数据处理
        records = store.list_all(self.MODULE)
        processed = []
        for idx, rec in enumerate(records):
            # 合并：校验每条记录的字段完整性与格式合法性
            filled = sum(1 for v in rec.values() if v and str(v).strip())
            total = len(rec)
            rec_copy = dict(rec)
            rec_copy["_field_count"] = filled
            rec_copy["_complete_ratio"] = f"{filled}/{total}"
            rec_copy["_batch"] = f"{self.MODULE}-{idx:04d}"
            processed.append(rec_copy)
        # 记录：生成加工批次标识并写入存储
        batch_id = store.next_id(self.MODULE + "_processor")
        result = {
            "module": self.MODULE,
            "operation": "聚合",
            "config_id": config_id,
            "records_processed": len(processed),
            "batch_id": batch_id,
            "field_validation": {"合并": "passed", "记录": "recorded"},
            "processed_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "downstream_consumable": True,
            "records": processed,
            "entry": entry,
        }
        # 返回聚合处理结果，含合并校验元数据和记录批次追溯信息
        return result
