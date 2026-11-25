"""领域业务操作接口：每个模块的计算/处理/聚合等真实业务操作。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException

from app.schemas import BatchQueryPayload, UpstreamStubPayload
from app.services import SERVICES

router = APIRouter(prefix="/api/domain", tags=["领域业务"])


def _data_service_svc():
    from app.services.data_service import DataServiceService
    svc = SERVICES.get("data_service")
    if not svc or not isinstance(svc, DataServiceService):
        raise HTTPException(404, "数据服务不可用")
    return svc


@router.get("/metric_def/{metric_id}/compute")
def metric_compute(metric_id: int) -> dict[str, Any]:
    """指标聚合计算。"""
    from app.services.metric_def import MetricDefService
    svc = SERVICES.get("metric_def")
    if not svc or not isinstance(svc, MetricDefService):
        raise HTTPException(404, "指标口径服务不可用")
    return svc.domain_compute(metric_id)


@router.get("/dim_query/{query_id}/execute")
def dim_query_execute(query_id: int) -> dict[str, Any]:
    """多维交叉查询执行。"""
    from app.services.dim_query import DimQueryService
    svc = SERVICES.get("dim_query")
    if not svc or not isinstance(svc, DimQueryService):
        raise HTTPException(404, "多维查询服务不可用")
    return svc.execute_query(query_id)


@router.get("/experiment/{experiment_id}/compute-effect")
def experiment_compute(experiment_id: int) -> dict[str, Any]:
    """实验效果计算。"""
    from app.services.experiment import ExperimentService
    svc = SERVICES.get("experiment")
    if not svc or not isinstance(svc, ExperimentService):
        raise HTTPException(404, "实验管理服务不可用")
    return svc.compute_effect(experiment_id)


@router.get("/significance/{task_id}/run-test")
def significance_run_test(task_id: int) -> dict[str, Any]:
    """显著性检验执行。"""
    from app.services.significance import SignificanceService
    svc = SERVICES.get("significance")
    if not svc or not isinstance(svc, SignificanceService):
        raise HTTPException(404, "显著性检验服务不可用")
    return svc.run_test(task_id)


@router.get("/cohort/{cohort_id}/estimate-scale")
def cohort_estimate(cohort_id: int) -> dict[str, Any]:
    """人群规模预估。"""
    from app.services.cohort import CohortService
    svc = SERVICES.get("cohort")
    if not svc or not isinstance(svc, CohortService):
        raise HTTPException(404, "人群圈选服务不可用")
    return svc.estimate_scale(cohort_id)


@router.post("/data_service/batch-query")
def data_service_batch_query(payload: BatchQueryPayload) -> dict[str, Any]:
    """数据服务批量查询入口：把多份查询请求合并为一个查询批次回执。

    同一 batch_key 重复提交时，已完成/已签收的请求项保留原执行结果，
    仅为未成功的请求项发起新一轮读取。
    """
    return _data_service_svc().submit_batch(payload.model_dump())


@router.get("/data_service/batch-query/{receipt_no}")
def data_service_batch_receipt(receipt_no: str) -> dict[str, Any]:
    """按回执号查询批次回执：每项给出实际读取范围、可领取结果或等待说明。"""
    svc = _data_service_svc()
    receipt = svc.get_receipt(receipt_no)
    if receipt is None:
        raise HTTPException(404, f"查询批次回执 {receipt_no} 不存在")
    return {"ok": True, "receipt": receipt}


@router.post("/data_service/batch-query/{receipt_no}/claim/{request_id}")
def data_service_batch_claim(receipt_no: str, request_id: str) -> dict[str, Any]:
    """签收并领取单个请求项的结果；已签收的查询不能发起第二次读取。"""
    svc = _data_service_svc()
    result = svc.claim_item(receipt_no, request_id)
    if not result.get("found", True):
        raise HTTPException(404, result["message"])
    result.pop("found", None)
    return result


@router.get("/data_service/upstream-stubs")
def data_service_upstream_stubs() -> dict[str, Any]:
    """查看模拟上游接口行为（演示部分失败与恢复场景）。"""
    return _data_service_svc().list_upstream_behaviors()


@router.post("/data_service/upstream-stubs")
def data_service_set_upstream_stub(payload: UpstreamStubPayload) -> dict[str, Any]:
    """设置某个接口路径的模拟上游行为：ok / timeout / error。"""
    return _data_service_svc().set_upstream_behavior(
        payload.path, payload.model_dump(exclude={"path"}))


@router.get("/data_service/{service_id}/invoke")
def data_service_invoke(service_id: int) -> dict[str, Any]:
    """数据服务接口调用。"""
    svc = _data_service_svc()
    return svc.invoke_service(service_id)


@router.get("/anomaly_detect/{rule_id}/detect")
def anomaly_detect_run(rule_id: int) -> dict[str, Any]:
    """异常检测执行。"""
    from app.services.anomaly_detect import AnomalyDetectService
    svc = SERVICES.get("anomaly_detect")
    if not svc or not isinstance(svc, AnomalyDetectService):
        raise HTTPException(404, "异常检测服务不可用")
    return svc.detect_anomalies(rule_id)


@router.get("/analysis_chart/{chart_id}/render")
def chart_render(chart_id: int) -> dict[str, Any]:
    """图表数据渲染。"""
    from app.services.analysis_chart import AnalysisChartService
    svc = SERVICES.get("analysis_chart")
    if not svc or not isinstance(svc, AnalysisChartService):
        raise HTTPException(404, "分析图表服务不可用")
    return svc.render_chart_data(chart_id)


@router.get("/aggregate_job/{job_id}/execute")
def aggregate_job_execute(job_id: int) -> dict[str, Any]:
    """预聚合任务执行。"""
    from app.services.aggregate_job import AggregateJobService
    svc = SERVICES.get("aggregate_job")
    if not svc or not isinstance(svc, AggregateJobService):
        raise HTTPException(404, "预聚合任务服务不可用")
    return svc.execute_aggregate(job_id)


@router.get("/drill_down/{path_id}/execute")
def drill_down_execute(path_id: int) -> dict[str, Any]:
    """下钻分析执行。"""
    from app.services.drill_down import DrillDownService
    svc = SERVICES.get("drill_down")
    if not svc or not isinstance(svc, DrillDownService):
        raise HTTPException(404, "下钻分析服务不可用")
    return svc.execute_drill_down(path_id)


@router.get("/data_export/{task_id}/execute")
def data_export_execute(task_id: int) -> dict[str, Any]:
    """数据导出执行。"""
    from app.services.data_export import DataExportService
    svc = SERVICES.get("data_export")
    if not svc or not isinstance(svc, DataExportService):
        raise HTTPException(404, "数据导出服务不可用")
    return svc.execute_export(task_id)


@router.get("/data_freshness/{check_id}/check")
def data_freshness_check(check_id: int) -> dict[str, Any]:
    """数据时效检查执行。"""
    from app.services.data_freshness import DataFreshnessService
    svc = SERVICES.get("data_freshness")
    if not svc or not isinstance(svc, DataFreshnessService):
        raise HTTPException(404, "数据时效服务不可用")
    return svc.run_freshness_check(check_id)


@router.get("/query_cache/{strategy_id}/check-status")
def query_cache_check(strategy_id: int) -> dict[str, Any]:
    """查询缓存状态检查。"""
    from app.services.query_cache import QueryCacheService
    svc = SERVICES.get("query_cache")
    if not svc or not isinstance(svc, QueryCacheService):
        raise HTTPException(404, "查询缓存服务不可用")
    return svc.check_cache_status(strategy_id)


@router.get("/segment_compare/{plan_id}/execute")
def segment_compare_execute(plan_id: int) -> dict[str, Any]:
    """分群对比执行。"""
    from app.services.segment_compare import SegmentCompareService
    svc = SERVICES.get("segment_compare")
    if not svc or not isinstance(svc, SegmentCompareService):
        raise HTTPException(404, "分群对比服务不可用")
    return svc.execute_compare(plan_id)


@router.get("/metric_calibrate/{record_id}/compute")
def metric_calibrate_compute(record_id: int) -> dict[str, Any]:
    """指标校准偏差计算。"""
    from app.services.metric_calibrate import MetricCalibrateService
    svc = SERVICES.get("metric_calibrate")
    if not svc or not isinstance(svc, MetricCalibrateService):
        raise HTTPException(404, "指标校准服务不可用")
    return svc.compute_calibration(record_id)


@router.post("/query_log/log")
def query_log_record() -> dict[str, Any]:
    """查询操作记录。"""
    from app.services.query_log import QueryLogService
    svc = SERVICES.get("query_log")
    if not svc or not isinstance(svc, QueryLogService):
        raise HTTPException(404, "查询日志服务不可用")
    return svc.log_query()


@router.get("/data_sample/{config_id}/execute")
def data_sample_execute(config_id: int) -> dict[str, Any]:
    """数据采样执行。"""
    from app.services.data_sample import DataSampleService
    svc = SERVICES.get("data_sample")
    if not svc or not isinstance(svc, DataSampleService):
        raise HTTPException(404, "数据采样服务不可用")
    return svc.execute_sampling(config_id)


@router.get("/report_schedule/{task_id}/generate")
def report_schedule_generate(task_id: int) -> dict[str, Any]:
    """报告生成。"""
    from app.services.report_schedule import ReportScheduleService
    svc = SERVICES.get("report_schedule")
    if not svc or not isinstance(svc, ReportScheduleService):
        raise HTTPException(404, "报告调度服务不可用")
    return svc.generate_report(task_id)