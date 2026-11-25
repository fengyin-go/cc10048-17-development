"""数据服务批量查询入口验收：批次回执、部分失败、幂等重提与签收。

场景主线：
1. 同一次批量提交同时请求支付、退款两份独立对账材料，支付返回 8 行、退款超时；
2. 运营马上用支付结果开展只核支付的工作（独立领取支付项）；
3. 同一批量请求重复提交，只保留实际已完成的请求，已签收查询不能发第二次读取；
4. 恢复退款后再次提交同一批，检查两项查询和签收材料。
"""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.services import SERVICES
from app.services.data_service import DataServiceService

client = TestClient(app)

PAY_SERVICE_ID = 4      # 支付对账接口 SVC-PAY-RECON
REFUND_SERVICE_ID = 5   # 退款对账接口 SVC-REFUND-RECON
BATCH_KEY = "RECON-2026-11-24"

PAY_ROWS = [
    {"订单号": f"PAY20261124{i:03d}", "支付金额": 100.0 + i,
     "支付时间": f"2026-11-24 08:{i:02d}:00", "渠道": "支付宝", "内部备注": f"风控-{i}"}
    for i in range(1, 9)   # 8 行
]
REFUND_ROWS = [
    {"退款单号": f"REF20261124{i:03d}", "退款金额": 50.0 + i,
     "退款时间": f"2026-11-24 10:{i:02d}:00", "退款原因": "重复支付", "内部备注": f"售后-{i}"}
    for i in range(1, 6)   # 5 行
]


def data_service() -> DataServiceService:
    svc = SERVICES.get("data_service")
    assert isinstance(svc, DataServiceService)
    return svc


@pytest.fixture(autouse=True)
def fresh_state():
    svc = data_service()
    svc.reset_batch_receipts()
    svc.reset_upstream_behaviors()
    yield
    svc.reset_batch_receipts()
    svc.reset_upstream_behaviors()


def set_upstream(path: str, behavior: dict) -> None:
    resp = client.post("/api/domain/data_service/upstream-stubs",
                       json={"path": path, **behavior})
    assert resp.status_code == 200
    assert resp.json()["ok"] is True


def submit_recon_batch(**overrides) -> dict:
    body = {
        "batch_key": BATCH_KEY,
        "submitted_by": "ops_recon",
        "requests": [
            {"request_id": "pay-1", "service_id": PAY_SERVICE_ID,
             "fields": ["订单号", "支付金额", "内部备注"],
             "scope": {"date": "2026-11-24"}},
            {"request_id": "ref-1", "service_id": REFUND_SERVICE_ID,
             "fields": ["退款单号", "退款金额"],
             "scope": {"date": "2026-11-24"}},
        ],
    }
    body.update(overrides)
    resp = client.post("/api/domain/data_service/batch-query", json=body)
    assert resp.status_code == 200
    return resp.json()


def items_by_id(receipt: dict) -> dict:
    return {item["request_id"]: item for item in receipt["items"]}


def get_log(log_id: int) -> dict:
    resp = client.get(f"/api/query_log/{log_id}")
    assert resp.status_code == 200
    return resp.json()


def claim(receipt_no: str, request_id: str) -> dict:
    resp = client.post(
        f"/api/domain/data_service/batch-query/{receipt_no}/claim/{request_id}")
    assert resp.status_code == 200
    return resp.json()


class TestPartialFailureAndRecovery:
    """主场景：支付成功退款超时 → 领取支付 → 幂等重提 → 恢复退款 → 两项齐全。"""

    def test_full_reconciliation_flow(self):
        # 先令支付返回 8 行、退款超时
        set_upstream("/recon/payment", {"mode": "ok", "rows": PAY_ROWS})
        set_upstream("/recon/refund", {"mode": "timeout", "message": "退款接口响应超时"})

        # ---- 同一次批量提交：支付 + 退款两份独立对账材料 ----
        data = submit_recon_batch()
        assert data["ok"] is True
        assert data["resubmitted"] is False
        receipt = data["receipt"]
        receipt_no = receipt["receipt_no"]
        assert receipt["batch_key"] == BATCH_KEY
        assert receipt["status"] == "部分完成"
        assert receipt["summary"]["成功"] == 1
        assert receipt["summary"]["失败"] == 1
        items = items_by_id(receipt)

        # 调用者能核对支付项实际读取的指标与范围
        pay = items["pay-1"]
        assert pay["status"] == "成功"
        assert pay["row_count"] == 8
        assert pay["metric"] == "支付金额"
        assert pay["接口标识"] == "SVC-PAY-RECON"
        assert pay["fields_used"] == ["订单号", "支付金额"]
        assert pay["rejected_fields"] == ["内部备注"]     # 白名单外字段被拒绝
        assert pay["scope_used"] == {"date": "2026-11-24"}
        assert pay["claimable"] is True
        assert pay["error"] is None

        # 退款项失败：行数为空而不是 0，失败不能冒充返回零行
        ref = items["ref-1"]
        assert ref["status"] == "失败"
        assert ref["row_count"] is None
        assert "超时" in ref["error"]
        assert ref["claimable"] is False
        assert ref["fields_used"] == []
        assert ref["scope_used"] == {}
        assert "重新提交" in ref["waiting"]               # 等待说明

        # ---- 查询日志用批次回执关联实际响应信息 ----
        pay_log = get_log(pay["log_id"])
        assert pay_log["批次回执"] == receipt_no
        assert pay_log["批次键"] == BATCH_KEY
        assert pay_log["请求标识"] == "pay-1"
        assert pay_log["执行结果"] == "成功"
        assert pay_log["返回行数"] == "8"
        assert pay_log["错误信息"] == ""
        assert pay_log["实际指标"] == "支付金额"
        # 日志只记录该次实际使用的范围
        assert pay_log["实际使用范围"] == {
            "fields": ["订单号", "支付金额"], "scope": {"date": "2026-11-24"}}

        ref_log = get_log(ref["log_id"])
        assert ref_log["批次回执"] == receipt_no
        assert ref_log["执行结果"] == "失败"
        assert ref_log["返回行数"] == ""                  # 失败不记 0 行
        assert "超时" in ref_log["错误信息"]
        assert ref_log["实际使用范围"] == {"fields": [], "scope": {}}

        # ---- 运营马上用支付结果开展只核支付的工作 ----
        got = claim(receipt_no, "pay-1")
        assert got["ok"] is True
        assert got["status"] == "已签收"
        assert got["row_count"] == 8
        assert len(got["rows"]) == 8
        # 部分数据返回期间只交付该请求允许读的字段
        assert all(set(row) == {"订单号", "支付金额"} for row in got["rows"])
        assert got["log_id"] == pay["log_id"]

        # 退款项不可领取，返回等待说明
        blocked = claim(receipt_no, "ref-1")
        assert blocked["ok"] is False
        assert "超时" in blocked["error"]
        assert "重新提交" in blocked["waiting"]

        # ---- 确认后不能为已签收查询发第二个读取 ----
        again = claim(receipt_no, "pay-1")
        assert again["ok"] is False
        assert "已签收" in again["message"]
        assert "第二次读取" in again["message"]

        # ---- 同一批量请求重复提交：只保留实际已完成的请求 ----
        data2 = submit_recon_batch()
        assert data2["ok"] is True
        assert data2["resubmitted"] is True
        receipt2 = data2["receipt"]
        assert receipt2["receipt_no"] == receipt_no       # 同一回执
        items2 = items_by_id(receipt2)
        assert items2["pay-1"]["status"] == "已签收"
        assert items2["pay-1"]["executions"] == 1          # 没有第二次读取
        assert "已签收" in items2["pay-1"]["note"]
        assert items2["ref-1"]["status"] == "失败"          # 退款仍超时
        assert items2["ref-1"]["executions"] == 2          # 失败项被重试
        assert "重试" in items2["ref-1"]["note"]

        # ---- 恢复退款后再次提交同一批 ----
        set_upstream("/recon/refund", {"mode": "ok", "rows": REFUND_ROWS})
        data3 = submit_recon_batch()
        receipt3 = data3["receipt"]
        assert receipt3["receipt_no"] == receipt_no
        assert receipt3["status"] == "已完成"
        items3 = items_by_id(receipt3)
        assert items3["pay-1"]["status"] == "已签收"
        assert items3["pay-1"]["executions"] == 1          # 已签收项始终不重读
        ref3 = items3["ref-1"]
        assert ref3["status"] == "成功"
        assert ref3["row_count"] == 5
        assert ref3["executions"] == 3
        assert ref3["metric"] == "退款金额"
        assert ref3["fields_used"] == ["退款单号", "退款金额"]
        assert ref3["scope_used"] == {"date": "2026-11-24"}
        assert ref3["claimable"] is True

        # ---- 检查两项查询和签收材料 ----
        got_ref = claim(receipt_no, "ref-1")
        assert got_ref["ok"] is True
        assert got_ref["row_count"] == 5
        assert all(set(row) == {"退款单号", "退款金额"} for row in got_ref["rows"])

        final = client.get(f"/api/domain/data_service/batch-query/{receipt_no}").json()
        assert final["ok"] is True
        final_receipt = final["receipt"]
        assert final_receipt["status"] == "已签收"
        final_items = items_by_id(final_receipt)
        assert final_items["pay-1"]["claimed"] is True
        assert final_items["ref-1"]["claimed"] is True
        assert final_items["pay-1"]["claimed_at"]
        assert final_items["ref-1"]["claimed_at"]

        # 两项查询的全部执行日志都能按批次回执核对：
        # 支付只有 1 条（没有第二次读取），退款 3 条（两次超时 + 恢复后成功）
        logs = client.get("/api/query_log", params={"size": 200}).json()["items"]
        batch_logs = [row for row in logs if row.get("批次回执") == receipt_no]
        pay_logs = [row for row in batch_logs if row.get("请求标识") == "pay-1"]
        ref_logs = [row for row in batch_logs if row.get("请求标识") == "ref-1"]
        assert len(pay_logs) == 1
        assert len(ref_logs) == 3
        assert [row["执行结果"] for row in ref_logs] == ["失败", "失败", "成功"]
        success_ref_log = ref_logs[-1]
        assert success_ref_log["返回行数"] == "5"
        assert success_ref_log["实际使用范围"] == {
            "fields": ["退款单号", "退款金额"], "scope": {"date": "2026-11-24"}}


class TestBatchValidation:
    """批量入口的校验与边界行为。"""

    def test_empty_requests_rejected(self):
        resp = client.post("/api/domain/data_service/batch-query",
                           json={"batch_key": "EMPTY", "requests": []})
        assert resp.status_code == 200
        assert resp.json()["ok"] is False

    def test_missing_request_id_rejected(self):
        resp = client.post("/api/domain/data_service/batch-query",
                           json={"requests": [{"service_id": PAY_SERVICE_ID}]})
        assert resp.status_code == 200
        body = resp.json()
        assert body["ok"] is False
        assert "request_id" in body["message"]

    def test_unknown_service_fails_without_fake_rows(self):
        data = submit_recon_batch(batch_key="BAD-SVC", requests=[
            {"request_id": "x-1", "service_id": 99999, "scope": {}},
        ])
        item = items_by_id(data["receipt"])["x-1"]
        assert item["status"] == "失败"
        assert item["row_count"] is None
        assert "不存在" in item["error"]
        log = get_log(item["log_id"])
        assert log["返回行数"] == ""
        assert log["错误信息"]
        assert log["执行结果"] == "失败"

    def test_unpublished_service_rejected(self):
        # 种子接口 1 状态为「待发布」
        data = submit_recon_batch(batch_key="UNPUBLISHED", requests=[
            {"request_id": "u-1", "service_id": 1, "scope": {}},
        ])
        item = items_by_id(data["receipt"])["u-1"]
        assert item["status"] == "失败"
        assert "未发布" in item["error"]
        assert item["row_count"] is None

    def test_fields_out_of_whitelist_rejected(self):
        data = submit_recon_batch(batch_key="BAD-FIELDS", requests=[
            {"request_id": "f-1", "service_id": PAY_SERVICE_ID,
             "fields": ["内部备注"], "scope": {"date": "2026-11-24"}},
        ])
        item = items_by_id(data["receipt"])["f-1"]
        assert item["status"] == "失败"
        assert "超出接口允许读取范围" in item["error"]
        assert item["row_count"] is None

    def test_duplicate_request_ids_in_one_submission(self):
        data = submit_recon_batch(batch_key="DUP", requests=[
            {"request_id": "dup-1", "service_id": PAY_SERVICE_ID,
             "scope": {"date": "2026-11-24"}},
            {"request_id": "dup-1", "service_id": REFUND_SERVICE_ID,
             "scope": {"date": "2026-11-24"}},
        ])
        assert data["duplicates_ignored"] == ["dup-1"]
        receipt = data["receipt"]
        assert receipt["summary"]["total"] == 1
        item = receipt["items"][0]
        assert item["service_id"] == PAY_SERVICE_ID       # 只保留第一份
        assert item["executions"] == 1

    def test_receipt_not_found(self):
        resp = client.get("/api/domain/data_service/batch-query/QB-NONE-0000")
        assert resp.status_code == 404
        resp = client.post("/api/domain/data_service/batch-query/QB-NONE-0000/claim/x-1")
        assert resp.status_code == 404

    def test_claim_unknown_item_not_found(self):
        data = submit_recon_batch(batch_key="KNOWN", requests=[
            {"request_id": "k-1", "service_id": PAY_SERVICE_ID, "scope": {}},
        ])
        receipt_no = data["receipt"]["receipt_no"]
        resp = client.post(
            f"/api/domain/data_service/batch-query/{receipt_no}/claim/no-such-item")
        assert resp.status_code == 404

    def test_zero_rows_success_is_not_confused_with_failure(self):
        # 上游真实返回 0 行是成功结果，与失败明确区分
        set_upstream("/recon/payment", {"mode": "ok", "rows": []})
        data = submit_recon_batch(batch_key="ZERO", requests=[
            {"request_id": "z-1", "service_id": PAY_SERVICE_ID,
             "scope": {"date": "2026-11-25"}},
        ])
        item = items_by_id(data["receipt"])["z-1"]
        assert item["status"] == "成功"
        assert item["row_count"] == 0
        log = get_log(item["log_id"])
        assert log["返回行数"] == "0"                      # 真实零行如实记录
        assert log["错误信息"] == ""
        got = claim(data["receipt"]["receipt_no"], "z-1")
        assert got["ok"] is True
        assert got["rows"] == []

    def test_unregistered_service_uses_deterministic_generator(self):
        # 未注册模拟行为的接口按接口与范围确定性生成
        def submit_gen(key: str) -> dict:
            return submit_recon_batch(batch_key=key, requests=[
                {"request_id": "g-1", "service_id": 2, "scope": {}},
            ])
        first = items_by_id(submit_gen("GEN-1")["receipt"])["g-1"]
        second = items_by_id(submit_gen("GEN-2")["receipt"])["g-1"]
        assert first["status"] == "成功"
        assert first["row_count"] == second["row_count"]

    def test_auto_batch_key_idempotent(self):
        # 未提供 batch_key 时按请求内容派生，相同请求复用同一回执
        body = {"requests": [
            {"request_id": "a-1", "service_id": PAY_SERVICE_ID, "scope": {}},
        ]}
        r1 = client.post("/api/domain/data_service/batch-query", json=body).json()
        r2 = client.post("/api/domain/data_service/batch-query", json=body).json()
        assert r1["receipt"]["receipt_no"] == r2["receipt"]["receipt_no"]
        assert r2["resubmitted"] is True
        item = items_by_id(r2["receipt"])["a-1"]
        assert item["executions"] == 1                     # 已完成项未重复执行


class TestUpstreamStubConsole:
    """模拟上游行为设置端点。"""

    def test_set_and_list_behaviors(self):
        set_upstream("/recon/refund", {"mode": "ok", "rows": REFUND_ROWS})
        resp = client.get("/api/domain/data_service/upstream-stubs")
        assert resp.status_code == 200
        upstreams = resp.json()["upstreams"]
        assert upstreams["/recon/refund"]["mode"] == "ok"
        assert upstreams["/recon/refund"]["row_count"] == 5
        assert upstreams["/recon/payment"]["mode"] == "ok"      # 默认行为
        assert upstreams["/recon/payment"]["row_count"] == 8

    def test_invalid_mode_rejected(self):
        resp = client.post("/api/domain/data_service/upstream-stubs",
                           json={"path": "/recon/payment", "mode": "bogus"})
        assert resp.status_code == 200
        assert resp.json()["ok"] is False

    def test_ok_mode_requires_rows(self):
        resp = client.post("/api/domain/data_service/upstream-stubs",
                           json={"path": "/recon/payment", "mode": "ok"})
        assert resp.status_code == 200
        assert resp.json()["ok"] is False

    def test_default_behaviors_match_story(self):
        # 默认状态：支付正常、退款超时，直接提交即可复现部分失败
        data = submit_recon_batch(batch_key="DEFAULT-STORY", requests=[
            {"request_id": "pay-1", "service_id": PAY_SERVICE_ID,
             "scope": {"date": "2026-11-24"}},
            {"request_id": "ref-1", "service_id": REFUND_SERVICE_ID,
             "scope": {"date": "2026-11-24"}},
        ])
        items = items_by_id(data["receipt"])
        assert items["pay-1"]["status"] == "成功"
        assert items["pay-1"]["row_count"] == 8
        assert items["ref-1"]["status"] == "失败"
        assert "超时" in items["ref-1"]["error"]
