"""跨模块真实数据流编排：在原子动作基础上建立业务链路消费关系。

每条链路封装为 FlowStep，由 FlowEngine 按序执行并收集副作用。
不做独立CRUD，只在业务动作完成后触发下游消费逻辑。
"""
from __future__ import annotations

import datetime
import json
import math
import random
import re
from dataclasses import dataclass, field
from typing import Any, Callable

from app.store import store

# ---------------------------------------------------------------------------
# 链路副作用记录
# ---------------------------------------------------------------------------


@dataclass
class FlowEffect:
    """单次链路触发产生的副作用记录。"""
    name: str
    module: str
    entry_id: int
    description: str
    timestamp: str = field(default_factory=lambda: datetime.datetime.now().isoformat())


# ---------------------------------------------------------------------------
# 链路定义：每条链路 = path + trigger 函数
# ---------------------------------------------------------------------------

FLOW_LINKS: list[dict[str, Any]] = []


def _register(path: str, *, desc: str):
    """装饰器工厂：在 FLOW_LINKS 中注册链路定义。"""
    def decorator(trigger: Callable[..., list[FlowEffect]]):
        FLOW_LINKS.append({"path": path, "trigger": trigger, "desc": desc})
        return trigger
    return decorator


# ============================ 链路1: 特征注册→衍生工程→时间窗口 ============================


@_register(
    "feature_reg/审核注册 → derive_rule/启效规则 → time_window/启动计算",
    desc="特征注册后自动触发对应衍生规则生效、时间窗口模板计算",
)
def flow_reg_to_derive_to_window(feature_reg_id: int) -> list[FlowEffect]:
    effects: list[FlowEffect] = []
    feature_reg_entry = store.find("feature_reg", feature_reg_id)
    if feature_reg_entry is None or feature_reg_entry.get("status") != "已注册":
        return effects

    feat_no = str(feature_reg_entry.get("特征编号", ""))
    feat_name = str(feature_reg_entry.get("特征名称", ""))

    # 查找关联的衍生规则并启效
    derive_rows = store.rows("derive_rule")
    for row in derive_rows:
        if str(row.get("输入特征", "")) and feat_name in str(row.get("输入特征", "")):
            if row.get("status") == "草稿":
                row["status"] = "生效中"
                row["pending"] = True
                row["abnormal"] = False
                effects.append(FlowEffect(
                    "启效规则", "derive_rule", int(row.get("id", 0)),
                    f"特征 {feat_no} 注册后联动启效衍生规则 {row.get('规则编号','')}",
                ))
                # 同时联动时间窗口
                _window_compute_for_derived(row, feat_no, effects)

    return effects


def _window_compute_for_derived(
    derive_row: dict[str, Any], source_feat_no: str, effects: list[FlowEffect]
):
    """为已启效的衍生规则关联的时间窗口触发计算。"""
    win_rows = store.rows("time_window")
    output_feat = str(derive_row.get("输出特征", ""))
    for wrow in win_rows:
        if wrow.get("status") == "草稿":
            wrow["status"] = "计算中"
            wrow["pending"] = True
            wrow["awakened_by"] = f"feature_reg/{source_feat_no}"
            effects.append(FlowEffect(
                "启动计算", "time_window", int(wrow.get("id", 0)),
                f"衍生规则 {derive_row.get('规则编号','')} 启效联动窗口 {wrow.get('模板编号','')} 启动计算",
            ))


# ============================ 链路2: 时间窗口→特征回溯→特征存储/监控 ============================


@_register(
    "time_window/窗口关闭 → backfill_job/启动回溯 → feature_store/启用存储 → feature_mon/启用监控",
    desc="窗口关闭后发起特征回溯，回溯完成写入存储并启用监控。",
)
def flow_window_to_backfill_to_store_mon(time_window_id: int) -> list[FlowEffect]:
    effects: list[FlowEffect] = []
    win = store.find("time_window", time_window_id)
    if win is None or win.get("status") != "窗口关闭":
        return effects

    template_no = str(win.get("模板编号", ""))
    feat_list = str(win.get("特征列表", ""))

    # 触发回溯任务
    backfill_rows = store.rows("backfill_job")
    for brow in backfill_rows:
        if brow.get("status") == "待执行":
            brow["status"] = "回溯中"
            brow["pending"] = True
            brow["triggered_by"] = f"time_window/{template_no}"
            brow["当前进度"] = "0%"
            effects.append(FlowEffect(
                "启动回溯", "backfill_job", int(brow.get("id", 0)),
                f"窗口 {template_no} 关闭后触发回溯任务 {brow.get('任务编号','')}",
            ))
            # 模拟回溯完成 → 触发存储写入
            _backfill_done_store_mon(brow, effects)
            break

    return effects


def _backfill_done_store_mon(backfill_row: dict[str, Any], effects: list[FlowEffect]):
    """回溯完成：触发特征存储启用和监控规则检测。"""
    backfill_row["status"] = "已完成"
    backfill_row["pending"] = False
    backfill_row["当前进度"] = "100%"

    job_no = str(backfill_row.get("任务编号", ""))
    target = str(backfill_row.get("目标特征", ""))
    row_count = random.randint(50000, 500000)
    backfill_row["数据行数"] = f"{row_count}行"

    effects.append(FlowEffect(
        "回溯完成", "backfill_job", int(backfill_row.get("id", 0)),
        f"回溯任务 {job_no} 完成，产出 {row_count} 行，触发存储写入和监控检测",
    ))

    # 触发特征存储写入
    store_rows = store.rows("feature_store")
    for srow in store_rows:
        if srow.get("status") == "草稿":
            srow["status"] = "已启用"
            srow["pending"] = True
            srow["storage_size_gb"] = round(row_count * 0.0001, 2)
            srow["triggered_by"] = f"backfill_job/{job_no}"
            effects.append(FlowEffect(
                "启用存储", "feature_store", int(srow.get("id", 0)),
                f"回溯 {job_no} 产出 {row_count}行 触发存储配置 {srow.get('配置编号','')} 启用",
            ))
            break

    # 触发特征监控检测
    mon_rows = store.rows("feature_mon")
    for mrow in mon_rows:
        if mrow.get("status") == "草稿" and target in str(mrow.get("目标特征", "")):
            mrow["status"] = "监控中"
            mrow["pending"] = True
            mrow["最近检测"] = datetime.date.today().isoformat()
            mrow["触发源"] = f"backfill_job/{job_no}"
            # 随机模拟漂移检测
            drift = random.uniform(0, 0.15)
            if drift > 0.1:
                mrow["status"] = "分布漂移"
                mrow["漂移方向"] = "正向偏移"
            effects.append(FlowEffect(
                "启用监控/检测漂移", "feature_mon", int(mrow.get("id", 0)),
                f"回溯 {job_no} 完成触发监控 {mrow.get('规则编号','')} 检测，漂移={drift:.3f}",
            ))
            break


# ============================ 链路3: 特征存储→在线服务/离线导出 ============================


@_register(
    "feature_store/启用存储 → online_serv/部署服务 → offline_exp/启动导出",
    desc="特征存储启用后同步部署在线服务和触发离线导出管道。",
)
def flow_store_to_online_offline(feature_store_id: int) -> list[FlowEffect]:
    effects: list[FlowEffect] = []
    fs = store.find("feature_store", feature_store_id)
    if fs is None or fs.get("status") != "已启用":
        return effects

    config_no = str(fs.get("配置编号", ""))

    # 触发在线服务部署
    online_rows = store.rows("online_serv")
    for orow in online_rows:
        if orow.get("status") == "待部署":
            orow["status"] = "运行中"
            orow["pending"] = True
            orow["热加载"] = "已开启"
            orow["运行状态"] = "SERVING"
            orow["triggered_by"] = f"feature_store/{config_no}"
            effects.append(FlowEffect(
                "部署服务", "online_serv", int(orow.get("id", 0)),
                f"存储 {config_no} 启用后自动部署在线服务 {orow.get('端点编号','')}",
            ))
            break

    # 触发离线导出
    offline_rows = store.rows("offline_exp")
    for erow in offline_rows:
        if erow.get("status") == "待执行":
            erow["status"] = "导出中"
            erow["pending"] = True
            erow["最后导出"] = datetime.datetime.now().isoformat()
            erow["triggered_by"] = f"feature_store/{config_no}"
            effects.append(FlowEffect(
                "启动导出", "offline_exp", int(erow.get("id", 0)),
                f"存储 {config_no} 启用后联动离线导出管道 {erow.get('管道编号','')}",
            ))
            break

    return effects


# ============================ 链路4: 衍生规则→特征血缘 ============================


@_register(
    "derive_rule/启效规则 → feature_line/确认链路",
    desc="衍生规则生效后自动推导并确认特征血缘链路。",
)
def flow_derive_to_lineage(derive_rule_id: int) -> list[FlowEffect]:
    effects: list[FlowEffect] = []
    dr = store.find("derive_rule", derive_rule_id)
    if dr is None or dr.get("status") != "生效中":
        return effects

    rule_no = str(dr.get("规则编号", ""))
    input_feat = str(dr.get("输入特征", ""))
    output_feat = str(dr.get("输出特征", ""))
    func_name = str(dr.get("衍生函数", ""))

    # 自动推导血缘并确认
    line_rows = store.rows("feature_line")
    for lrow in line_rows:
        if lrow.get("status") == "自动推导":
            lrow["status"] = "已确认"
            lrow["pending"] = False
            lrow["上游特征"] = input_feat
            lrow["下游特征"] = output_feat
            lrow["转换函数"] = func_name
            lrow["依赖深度"] = "1"
            lrow["最后更新"] = datetime.datetime.now().isoformat()
            lrow["triggered_by"] = f"derive_rule/{rule_no}"
            effects.append(FlowEffect(
                "确认链路", "feature_line", int(lrow.get("id", 0)),
                f"衍生规则 {rule_no} 启效后自动推导血缘 {input_feat}→{output_feat}",
            ))
            break

    return effects


# ============================ 链路5: 特征血缘→版本管理→特征监控 ============================


@_register(
    "feature_line/确认链路 → version_mgr/灰度发布 → feature_mon/启用监控",
    desc="血缘确认后触发版本灰度发布，并关联启用特征监控规则。",
)
def flow_lineage_to_version_to_mon(feature_line_id: int) -> list[FlowEffect]:
    effects: list[FlowEffect] = []
    fl = store.find("feature_line", feature_line_id)
    if fl is None or fl.get("status") != "已确认":
        return effects

    line_no = str(fl.get("链路编号", ""))
    downstream = str(fl.get("下游特征", ""))

    # 版本灰度发布
    ver_rows = store.rows("version_mgr")
    for vrow in ver_rows:
        if vrow.get("status") == "草稿" and downstream in str(vrow.get("关联特征", "")):
            vrow["status"] = "灰度中"
            vrow["pending"] = True
            vrow["灰度比例"] = "30%"
            vrow["版本状态"] = "灰度测试中"
            vrow["triggered_by"] = f"feature_line/{line_no}"
            effects.append(FlowEffect(
                "灰度发布", "version_mgr", int(vrow.get("id", 0)),
                f"血缘 {line_no} 确认后触发版本 {vrow.get('版本编号','')} 灰度发布",
            ))
            break

    # 启用监控
    mon_rows = store.rows("feature_mon")
    for mrow in mon_rows:
        if mrow.get("status") == "草稿" and downstream in str(mrow.get("目标特征", "")):
            mrow["status"] = "监控中"
            mrow["pending"] = True
            mrow["最近检测"] = datetime.date.today().isoformat()
            mrow["triggered_by"] = f"feature_line/{line_no}"
            effects.append(FlowEffect(
                "启用监控", "feature_mon", int(mrow.get("id", 0)),
                f"血缘 {line_no} 确认后启用监控 {mrow.get('规则编号','')}",
            ))
            break

    return effects


# ============================ 链路6: 版本管理→在线服务 ============================


@_register(
    "version_mgr/全量切换 → online_serv/部署服务(热加载)",
    desc="版本全量切换后触发在线服务热加载，更新服务端点特征列表。",
)
def flow_version_to_online(version_mgr_id: int) -> list[FlowEffect]:
    effects: list[FlowEffect] = []
    vm = store.find("version_mgr", version_mgr_id)
    if vm is None or vm.get("status") != "全量生效":
        return effects

    ver_no = str(vm.get("版本编号", ""))
    feat = str(vm.get("关联特征", ""))
    ver_label = str(vm.get("版本标签", ""))

    # 触发在线服务热加载
    online_rows = store.rows("online_serv")
    updated = False
    for orow in online_rows:
        if orow.get("status") == "运行中":
            orig_features = str(orow.get("特征列表", ""))
            orow["特征列表"] = f"{orig_features},v{ver_label}"
            orow["热加载"] = f"已重载({ver_no})"
            orow["last_version_switch"] = datetime.datetime.now().isoformat()
            orow["triggered_by"] = f"version_mgr/{ver_no}"
            effects.append(FlowEffect(
                "热加载服务", "online_serv", int(orow.get("id", 0)),
                f"版本 {ver_no}({ver_label}) 全量切换后热加载在线服务 {orow.get('端点编号','')}",
            ))
            updated = True
            break

    if not updated:
        # 如果无运行中端点则部署新端点
        for orow in online_rows:
            if orow.get("status") == "待部署":
                orow["status"] = "运行中"
                orow["pending"] = True
                orow["热加载"] = "已开启"
                orow["运行状态"] = "SERVING"
                orow["特征列表"] = feat
                orow["triggered_by"] = f"version_mgr/{ver_no}"
                effects.append(FlowEffect(
                    "部署服务", "online_serv", int(orow.get("id", 0)),
                    f"版本 {ver_no} 全量切换后部署在线服务 {orow.get('端点编号','')}",
                ))
                break

    return effects


# ============================ 链路7: 样本分层→标注派发→标注审核→一致性计算 ============================


@_register(
    "sample_strata/已出样 → label_task/派发任务 → label_review/确认通过 → consent_calc/启动计算",
    desc="样本确认出样后派发标注任务，审核通过后自动触发一致性计算。",
)
def flow_sample_to_label_to_review_to_consent(sample_strata_id: int) -> list[FlowEffect]:
    effects: list[FlowEffect] = []
    ss = store.find("sample_strata", sample_strata_id)
    if ss is None or ss.get("status") != "已出样":
        return effects

    strategy_no = str(ss.get("策略编号", ""))
    sample_count = int(ss.get("样本数量", 10))

    # 标注派发
    label_task = _dispatch_label_task(strategy_no, sample_count, effects)

    if label_task is None:
        return effects

    # 模拟标注完成 → 审核通过
    _label_review_pass(label_task, effects)
    # 审核通过 → 一致性计算
    _consent_calc_trigger(label_task, effects)

    return effects


def _dispatch_label_task(
    strategy_no: str, sample_count: int, effects: list[FlowEffect]
) -> dict[str, Any] | None:
    """派发标注任务。"""
    label_rows = store.rows("label_task")
    for lrow in label_rows:
        if lrow.get("status") == "待派发":
            lrow["status"] = "标注中"
            lrow["pending"] = True
            lrow["样本范围"] = f"样本{strategy_no}"
            lrow["完成进度"] = "100%"
            lrow["派发时间"] = datetime.datetime.now().isoformat()
            lrow["triggered_by"] = f"sample_strata/{strategy_no}"
            effects.append(FlowEffect(
                "派发任务", "label_task", int(lrow.get("id", 0)),
                f"样本 {strategy_no}({sample_count}条) 出样后派发标注任务 {lrow.get('任务编号','')}",
            ))
            return lrow
    return None


def _label_review_pass(label_task: dict[str, Any], effects: list[FlowEffect]):
    """标注审核通过。"""
    task_no = str(label_task.get("任务编号", ""))
    review_rows = store.rows("label_review")
    for rrow in review_rows:
        if rrow.get("status") == "待审核":
            rrow["status"] = "已通过"
            rrow["pending"] = False
            rrow["标注任务"] = task_no
            rrow["样本数量"] = 30
            rrow["通过数量"] = 29
            rrow["驳回数量"] = 1
            rrow["一致率"] = "96.7%"
            rrow["审核时间"] = datetime.datetime.now().isoformat()
            rrow["triggered_by"] = f"label_task/{task_no}"
            effects.append(FlowEffect(
                "确认通过", "label_review", int(rrow.get("id", 0)),
                f"标注 {task_no} 审核通过，一致率 {rrow['一致率']}，触发一致性计算",
            ))
            break


def _consent_calc_trigger(label_task: dict[str, Any], effects: list[FlowEffect]):
    """审核通过后触发一致性计算。"""
    task_no = str(label_task.get("任务编号", ""))
    consent_rows = store.rows("consent_calc")
    for crow in consent_rows:
        if crow.get("status") == "待计算":
            crow["status"] = "已出分"
            crow["pending"] = False
            crow["标注任务"] = task_no
            kappa = round(random.uniform(0.75, 0.95), 3)
            crow["Kappa系数"] = str(kappa)
            crow["一致率"] = f"{round(kappa * 100, 1)}%"
            crow["评估结论"] = "高一致性" if kappa > 0.8 else "中等一致性"
            crow["计算时间"] = datetime.datetime.now().isoformat()
            crow["triggered_by"] = f"label_review/{task_no}"
            effects.append(FlowEffect(
                "启动计算", "consent_calc", int(crow.get("id", 0)),
                f"审核 {task_no} 通过后计算一致性，Kappa={kappa}",
            ))
            break


# ============================ 链路8: 特征目录→治理审计→特征报告 ============================


@_register(
    "feat_catalog/发布目录 → govern_audit/发起审计 → report_task/启动生成",
    desc="特征目录发布后触发治理审计，审计完成后生成特征报告。",
)
def flow_catalog_to_audit_to_report(feat_catalog_id: int) -> list[FlowEffect]:
    effects: list[FlowEffect] = []
    fc = store.find("feat_catalog", feat_catalog_id)
    if fc is None or fc.get("status") != "已发布":
        return effects

    index_no = str(fc.get("索引编号", ""))
    catalog_name = str(fc.get("目录名称", ""))

    # 触发治理审计
    audit_rows = store.rows("govern_audit")
    audit_entry = None
    for arow in audit_rows:
        if arow.get("status") == "待审计":
            arow["status"] = "已通过"
            arow["pending"] = False
            arow["审计对象"] = catalog_name
            arow["检查结果"] = "通过"
            arow["审计时间"] = datetime.datetime.now().isoformat()
            arow["整改状态"] = "无需整改"
            arow["triggered_by"] = f"feat_catalog/{index_no}"
            effects.append(FlowEffect(
                "发起审计", "govern_audit", int(arow.get("id", 0)),
                f"目录 {index_no}({catalog_name}) 发布后审计通过，触发特征报告生成",
            ))
            audit_entry = arow
            break

    if audit_entry is None:
        # 创建隐式审计记录
        audit_entry = {"id": 999, "检查结果": "通过"}

    # 触发特征报告生成
    _report_generate(catalog_name, index_no, effects)

    return effects


def _report_generate(catalog_name: str, index_no: str, effects: list[FlowEffect]):
    """生成特征报告。"""
    report_rows = store.rows("report_task")
    for rrow in report_rows:
        if rrow.get("status") == "草稿":
            rrow["status"] = "已生成"
            rrow["pending"] = False
            rrow["特征范围"] = catalog_name
            rrow["最近生成"] = datetime.datetime.now().isoformat()
            rrow["triggered_by"] = f"feat_catalog/{index_no}"
            effects.append(FlowEffect(
                "启动生成", "report_task", int(rrow.get("id", 0)),
                f"审计 {catalog_name} 通过后生成特征报告 {rrow.get('报告编号','')}",
            ))
            break


# ============================ 链路9: 质量规则→特征监控→特征报告 ============================


@_register(
    "quality_rule/确认阻断 → feature_mon/确认漂移 → report_task/启动生成",
    desc="质量规则阻断后联动特征监控漂移告警并生成异常报告。",
)
def flow_quality_to_mon_to_report(quality_rule_id: int) -> list[FlowEffect]:
    effects: list[FlowEffect] = []
    qr = store.find("quality_rule", quality_rule_id)
    if qr is None or qr.get("status") != "已阻断":
        return effects

    rule_no = str(qr.get("规则编号", ""))
    target_feat = str(qr.get("目标特征", ""))

    # 联动特征监控确认漂移
    mon_rows = store.rows("feature_mon")
    for mrow in mon_rows:
        if target_feat in str(mrow.get("目标特征", "")) and mrow.get("status") == "监控中":
            mrow["status"] = "分布漂移"
            mrow["pending"] = True
            mrow["漂移方向"] = "正向偏移(质量阻断触发)"
            mrow["triggered_by"] = f"quality_rule/{rule_no}"
            effects.append(FlowEffect(
                "确认漂移", "feature_mon", int(mrow.get("id", 0)),
                f"质量规则 {rule_no} 阻断后联动监控 {mrow.get('规则编号','')} 标记分布漂移",
            ))
            break

    # 生成异常报告
    report_rows = store.rows("report_task")
    for rrow in report_rows:
        if rrow.get("status") == "草稿":
            rrow["status"] = "已生成"
            rrow["pending"] = False
            rrow["报告类型"] = "质量异常报告"
            rrow["特征范围"] = target_feat
            rrow["最近生成"] = datetime.datetime.now().isoformat()
            rrow["triggered_by"] = f"quality_rule/{rule_no}"
            effects.append(FlowEffect(
                "启动生成", "report_task", int(rrow.get("id", 0)),
                f"质量规则 {rule_no} 阻断后生成异常报告 {rrow.get('报告编号','')}",
            ))
            break

    return effects


# ============================ 链路10: 调度编排→批量接入/回溯 ============================

# 调度计划只按「任务类型 + 关联对象」里登记的任务编号定位下游记录，绝不按列表顺序
# 抓取第一条「待执行」任务，避免改写无关的接入/回溯记录。
_BATCH_LABELS = {"批量接入", "接入", "接入任务", "batch_load", "batchload", "batch", "load"}
_BACKFILL_LABELS = {
    "特征回溯", "回溯", "回溯任务", "回填", "backfill", "backfill_job", "backfilljob", "bf",
}

# 关联对象里常见的分隔符：逗号/顿号/分号/斜杠/竖线/箭头/加号/空白
_ASSOC_SPLIT = re.compile(r"[\s,，、;；|/→+]+")
_LABEL_SPLIT = re.compile(r"^\s*([^:=：/]+?)\s*[:=：/]\s*(.+?)\s*$")
_MODULE_PREFIX = re.compile(
    r"(?i)\b(batch_load|batchload|backfill_job|backfilljob)\s*/\s*"
)
_CN_MODULE_PREFIX = re.compile(
    r"(批量接入|接入任务|接入|特征回溯|回溯任务|回溯)\s*/\s*"
)


def _plan_type_flags(task_type: str) -> tuple[bool, bool, bool]:
    """解析计划任务类型：返回 (是否含接入步骤, 是否含回溯步骤, 是否声明了类型)。"""
    text = str(task_type or "").casefold()
    want_batch = bool(re.search(r"批量接入|接入|batch|load|ingest", text))
    want_backfill = bool(re.search(r"回溯|回填|backfill|back_fill|\bbf\b", text))
    return want_batch, want_backfill, (want_batch or want_backfill)


def _assoc_entries(raw: str) -> list[tuple[str, str]]:
    """拆解「关联对象」为 (类型标签, 任务引用) 列表，兼容 JSON/逗号/分号/箭头等写法。"""
    text = str(raw or "").strip()
    if not text:
        return []

    entries: list[tuple[str, str]] = []
    if text.startswith("["):
        try:
            parsed = json.loads(text)
        except (TypeError, ValueError):
            parsed = None
        if isinstance(parsed, list):
            for item in parsed:
                if isinstance(item, dict):
                    label = str(item.get("类型") or item.get("任务类型") or item.get("模块") or "")
                    value = str(
                        item.get("任务编号") or item.get("关联对象") or item.get("id") or ""
                    )
                    if value:
                        entries.append((label, value))
                elif item not in (None, ""):
                    entries.append(("", str(item)))
            return entries

    # 把 module/任务编号、中文类型/任务编号 归一成 module:任务编号，再统一拆分。
    text = _MODULE_PREFIX.sub(r"\1:", text)
    text = _CN_MODULE_PREFIX.sub(r"\1:", text)

    for token in _ASSOC_SPLIT.split(text):
        token = token.strip().strip("\"'")
        if not token:
            continue
        match = _LABEL_SPLIT.match(token)
        if match:
            entries.append((match.group(1).strip(), match.group(2).strip().strip("\"'")))
        else:
            entries.append(("", token))
    return entries


def _label_kind(label: str) -> str:
    """把关联对象里的类型标签归一成模块 key，未知标签返回空串。"""
    text = str(label or "").strip().casefold()
    if not text:
        return ""
    if text in {item.casefold() for item in _BATCH_LABELS}:
        return "batch_load"
    if text in {item.casefold() for item in _BACKFILL_LABELS}:
        return "backfill_job"
    return ""


def _match_task(module: str, ref: str) -> dict[str, Any] | None:
    """按任务编号或 id 精确匹配一条任务记录。"""
    key = str(ref or "").strip().strip("\"'")
    if not key:
        return None
    by_id = False
    if key.casefold().startswith("id:"):
        key = key[3:].strip()
        by_id = True
    elif key.startswith("#"):
        key = key[1:].strip()
        by_id = True
    rows = store.rows(module)
    if not by_id:
        for row in rows:
            if str(row.get("任务编号", "")).strip().casefold() == key.casefold():
                return row
    if key.isdigit():
        for row in rows:
            if int(row.get("id", 0)) == int(key):
                return row
    return None


def _resolve_task_ref(
    ref: str, label: str, allowed: set[str]
) -> tuple[str, dict[str, Any] | None, str]:
    """解析单个任务引用，返回 (模块, 记录, 错误信息)。

    `allowed` 由计划的任务类型推导：类型只声明接入时，回溯任务即使存在也按类型
    不匹配处理，绝不改动。
    """
    clean = str(ref or "").strip().strip("\"'")
    if not clean:
        return "", None, "关联对象存在空引用"
    label_kind = _label_kind(label)

    search = [label_kind] if label_kind else ["batch_load", "backfill_job"]
    matches = [(module, _match_task(module, clean)) for module in search]
    matches = [(module, row) for module, row in matches if row is not None]

    if not matches:
        return "", None, f"关联对象「{clean}」不存在"
    if len(matches) > 1:
        usable = [(module, row) for module, row in matches if module in allowed]
        if len(usable) != 1:
            return "", None, f"关联对象「{clean}」同时匹配接入任务和回溯任务，无法确定类型"
        matches = usable

    module, row = matches[0]
    assert row is not None
    if module not in allowed:
        plan_kind = "、".join(_label_text(item) for item in sorted(allowed)) or "该计划"
        return "", None, f"关联对象「{clean}」是{_label_text(module)}，与计划任务类型（{plan_kind}）不匹配"
    return module, row, ""


def _label_text(module: str) -> str:
    return "批量接入任务" if module == "batch_load" else "特征回溯任务"


def resolve_plan_steps(plan: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
    """按计划的任务类型和关联对象解析出唯一的接入/回溯步骤。

    返回 ({"batch_load": row|None, "backfill_job": row|None}, "")，解析失败时返回
    (None, 原因)。解析失败的计划不产生任何下游写入。
    """
    entries = _assoc_entries(str(plan.get("关联对象") or ""))
    if not entries:
        return None, "关联对象为空"

    want_batch, want_backfill, _ = _plan_type_flags(str(plan.get("任务类型") or ""))
    plan_type = str(plan.get("任务类型") or "")
    if want_batch and not want_backfill:
        allowed = {"batch_load"}
    elif want_backfill and not want_batch:
        allowed = {"backfill_job"}
    else:
        allowed = {"batch_load", "backfill_job"}

    steps: dict[str, list[dict[str, Any]]] = {"batch_load": [], "backfill_job": []}
    seen: set[tuple[str, int]] = set()

    for label, ref in entries:
        module, row, error = _resolve_task_ref(ref, label, allowed)
        if error:
            return None, error
        assert row is not None
        marker = (module, int(row.get("id", 0)))
        if marker in seen:
            continue
        seen.add(marker)
        steps[module].append(row)

    if want_batch and want_backfill:
        if not steps["batch_load"] or not steps["backfill_job"]:
            missing = "接入任务" if not steps["batch_load"] else "回溯任务"
            return None, f"任务类型「{plan_type}」包含接入和回溯两个步骤，但关联对象缺少{missing}"
    elif want_batch and not steps["batch_load"]:
        return None, f"任务类型为「{plan_type}」，但关联对象没有接入任务"
    elif want_backfill and not steps["backfill_job"]:
        return None, f"任务类型为「{plan_type}」，但关联对象没有回溯任务"

    if len(steps["batch_load"]) > 1:
        return None, "关联对象登记了多个接入任务，无法确定编排对象"
    if len(steps["backfill_job"]) > 1:
        return None, "关联对象登记了多个回溯任务，无法确定编排对象"
    if not steps["batch_load"] and not steps["backfill_job"]:
        return None, "关联对象没有可执行的接入或回溯任务"

    return {
        "batch_load": steps["batch_load"][0] if steps["batch_load"] else None,
        "backfill_job": steps["backfill_job"][0] if steps["backfill_job"] else None,
    }, ""


def _qualification_date(backfill_row: dict[str, Any]) -> str:
    date = str(backfill_row.get("起始日期") or "").strip()
    return date or f"backfill_job/{backfill_row.get('id', '')}"


def _qualification_holder(
    backfill_row: dict[str, Any], date: str
) -> dict[str, Any] | None:
    """找出同一日期上仍持有回溯资格的其它回溯任务。"""
    for row in store.rows("backfill_job"):
        if int(row.get("id", 0)) == int(backfill_row.get("id", -1)):
            continue
        if not row.get("资格持有"):
            continue
        if str(row.get("status", "")) == "已完成":
            continue
        if _qualification_date(row) == date:
            return row
    return None


def release_backfill_qualification(backfill_row: dict[str, Any], reason: str = "") -> None:
    """释放回溯占用的日期资格，让同日期后续任务可以开工。"""
    if not backfill_row.get("资格持有"):
        return
    backfill_row["资格持有"] = False
    backfill_row["资格状态"] = "已释放"
    backfill_row["资格释放时间"] = datetime.datetime.now().isoformat()
    if reason:
        backfill_row["资格释放原因"] = reason


def peek_backfill_qualification(
    backfill_row: dict[str, Any], plan_no: str
) -> tuple[bool, str]:
    """预检回溯日期资格，不写任何记录。

    同一日期只允许一个回溯落地：中断但尚未完成的回溯仍占着日期，别的计划不能
    抢跑；只有真正完成后才释放资格。
    """
    date = _qualification_date(backfill_row)
    if backfill_row.get("资格持有"):
        holder_plan = str(backfill_row.get("资格计划") or "")
        if holder_plan and holder_plan != plan_no:
            return False, (
                f"日期 {date} 的回溯资格由计划 {holder_plan} 持有，"
                f"回溯任务 {backfill_row.get('任务编号', '')} 不能重复开工"
            )
        return True, ""
    holder = _qualification_holder(backfill_row, date)
    if holder is not None:
        return False, (
            f"日期 {date} 的回溯资格由计划 {holder.get('资格计划', '') or '未知'}"
            f"的回溯任务 {holder.get('任务编号', '')} 持有，暂不能开工"
        )
    return True, ""


def _reserve_backfill_qualification(
    plan: dict[str, Any], backfill_row: dict[str, Any]
) -> tuple[bool, str]:
    """预检并占用回溯日期资格；同一计划的续办可沿用之前占用的资格。

    预检失败时不写任何记录，避免别的计划改写正在续办的任务。
    """
    plan_no = str(plan.get("计划编号", ""))
    ok, message = peek_backfill_qualification(backfill_row, plan_no)
    if not ok:
        return False, message
    date = _qualification_date(backfill_row)
    backfill_row["资格持有"] = True
    backfill_row["资格日期"] = date
    backfill_row["资格计划"] = plan_no
    backfill_row["资格状态"] = "使用中"
    backfill_row["预检状态"] = "通过"
    backfill_row["预检计划"] = plan_no
    backfill_row["预检时间"] = datetime.datetime.now().isoformat()
    return True, ""


_COUNT_PATTERN = re.compile(r"^\s*(\d[\d,]*)\s*(?:行|条|rows?)?\s*$", re.IGNORECASE)


def _as_int(value: Any, default: int) -> int:
    """解析数据行数：只接受纯数字/带千分位/带「行」的写法，样例文本回退默认值。"""
    if isinstance(value, bool):
        return default
    if isinstance(value, (int, float)):
        return int(value)
    match = _COUNT_PATTERN.match(str(value or ""))
    if not match:
        return default
    return int(match.group(1).replace(",", ""))


def _batch_delivered(batch_row: dict[str, Any]) -> bool:
    """接入任务是否已经交付过批次（重试时不得重写行数和接入时间）。"""
    if batch_row.get("已交付"):
        return True
    return bool(
        str(batch_row.get("status", "")) == "已完成"
        and str(batch_row.get("接入时间") or "").strip()
    )


def _deliver_batch(
    plan: dict[str, Any],
    batch_row: dict[str, Any],
    backfill_row: dict[str, Any] | None,
    effects: list[FlowEffect],
) -> tuple[dict[str, Any], bool]:
    """交付/复用接入批次，生成编排接入数据并同步给计划和回溯。

    已经交付过的接入任务只复用原批次和原有接入结果，绝不重写接入行数、
    拒绝行数和接入时间；只有首次交付才写入实际接入结果。
    """
    plan_no = str(plan.get("计划编号", ""))
    batch_no = str(batch_row.get("任务编号", ""))
    delivered = _batch_delivered(batch_row)

    if delivered:
        # 已交付：完全沿用原有接入结果，只补齐编排元数据。
        rows_text = str(batch_row.get("接入行数", ""))
        rejects_text = str(batch_row.get("拒绝行数", ""))
        stamp = str(batch_row.get("接入时间") or "")
        batch_code = str(batch_row.get("数据批次") or "").strip() or (
            f"LOT-{int(batch_row.get('id', 0)):04d}"
        )
        rows = _as_int(rows_text, 0)
        rejects = _as_int(rejects_text, 0)
    else:
        batch_code = str(batch_row.get("数据批次") or "").strip() or (
            f"LOT-{int(batch_row.get('id', 0)):04d}"
        )
        rows = _as_int(batch_row.get("接入行数"), 1250000)
        rejects = _as_int(batch_row.get("拒绝行数"), 350)
        rows_text = str(rows)
        rejects_text = str(rejects)
        # 首次交付记录实际接入时间；续办一律沿用这个时间，不再重写。
        stamp = datetime.datetime.now().isoformat()
        batch_row["status"] = "已完成"
        batch_row["pending"] = False
        batch_row["abnormal"] = False
        batch_row["接入行数"] = rows_text
        batch_row["拒绝行数"] = rejects_text
        batch_row["接入时间"] = stamp
        effects.append(FlowEffect(
            "启动接入", "batch_load", int(batch_row.get("id", 0)),
            f"调度 {plan_no} 按关联对象执行接入 {batch_no}，交付批次 {batch_code}（{rows_text}行）",
        ))

    accepted = max(rows - rejects, 0)
    payload = {
        "批次编号": batch_code,
        "来源接入任务": batch_no,
        "来源计划": plan_no,
        "接入行数": rows,
        "拒绝行数": rejects,
        "有效行数": accepted,
        "接入时间": stamp,
        "起始日期": str(backfill_row.get("起始日期", "")) if backfill_row else "",
    }

    batch_row["数据批次"] = batch_code
    batch_row["已交付"] = True
    batch_row["编排接入数据"] = payload
    batch_row["triggered_by"] = f"schedule_job/{plan_no}"

    plan["接入任务"] = batch_no
    plan["数据批次"] = batch_code
    plan["接入产出"] = payload
    plan["编排接入数据"] = payload
    plan["接入行数"] = rows_text
    plan["拒绝行数"] = rejects_text
    plan["接入时间"] = stamp
    plan["当前步骤"] = "回溯" if backfill_row is not None else "已完成"

    if backfill_row is not None:
        backfill_row["编排接入数据"] = payload
        backfill_row["数据批次"] = batch_code
        backfill_row["来源接入任务"] = batch_no
        backfill_row["来源计划"] = plan_no
        backfill_row["接入行数"] = rows_text
        backfill_row["有效行数"] = str(accepted)
        backfill_row["接入时间"] = stamp
        backfill_row["接入产出确认"] = "已交付"

    return payload, not delivered


def _write_failure_budget(backfill_row: dict[str, Any]) -> int:
    """计算本次回溯写入要模拟的失败次数。

    显式注入优先：`写入故障次数` 指定剩余次数，`写入故障` 表示再来一次失败，
    `跳过写入故障` 表示直接成功。都没有时按首次写入中断处理一次，用于验证续办。
    """
    if backfill_row.get("跳过写入故障"):
        return 0
    if "写入故障次数" in backfill_row:
        return max(_as_int(backfill_row.get("写入故障次数"), 0), 0)
    if backfill_row.get("写入故障"):
        return 1
    return 1 if int(backfill_row.get("写入尝试") or 0) == 0 else 0


def _consume_write_failure(backfill_row: dict[str, Any], budget: int) -> None:
    """失败次数扣减，保证显式注入的故障只命中约定的次数。"""
    remaining = budget - 1
    if "写入故障次数" in backfill_row:
        backfill_row["写入故障次数"] = remaining
    if backfill_row.get("写入故障") and remaining <= 0:
        backfill_row["写入故障"] = False


def _write_backfill(
    plan: dict[str, Any],
    backfill_row: dict[str, Any],
    payload: dict[str, Any],
    effects: list[FlowEffect],
) -> str:
    """把接入批次写入回溯，返回 completed / interrupted / already。

    首次写入中断只把回溯留在「回溯中」，不把状态翻成已完成；续办确认接入产出
    已交付后重试，成功后只记一次完成。
    """
    plan_no = str(plan.get("计划编号", ""))
    job_no = str(backfill_row.get("任务编号", ""))
    batch_code = str(payload.get("批次编号", ""))
    rows = _as_int(payload.get("接入行数"), 0)
    attempts = int(backfill_row.get("写入尝试") or 0)

    # 已完成的任务不再重写行数、时间或完成次数，重复续办保持幂等。
    if str(backfill_row.get("status", "")) == "已完成":
        backfill_row["完成次数"] = max(int(backfill_row.get("完成次数") or 0), 1)
        backfill_row.setdefault("已写入批次", batch_code)
        release_backfill_qualification(backfill_row, "回溯已完成")
        return "already"

    budget = _write_failure_budget(backfill_row)
    backfill_row["写入尝试"] = attempts + 1
    backfill_row["待写入批次"] = batch_code
    backfill_row["接入产出确认"] = "已交付"

    if budget > 0:
        _consume_write_failure(backfill_row, budget)
        # 中断的回溯仍占着日期资格：接入已交付、回溯尚未落地，别的计划不能抢跑。
        backfill_row["status"] = "回溯中"
        backfill_row["pending"] = True
        backfill_row["abnormal"] = False
        backfill_row["当前进度"] = "0%"
        backfill_row["写入状态"] = "写入失败"
        backfill_row["中断批次"] = batch_code
        backfill_row["中断时间"] = datetime.datetime.now().isoformat()
        backfill_row["失败原因"] = "回溯首次写入中断，接入产出已保留，待续办重试"
        backfill_row["triggered_by"] = f"schedule_job/{plan_no}"
        plan["编排结果"] = f"接入批次 {batch_code} 已交付，回溯 {job_no} 首次写入中断，待续办"
        plan["当前步骤"] = "回溯待续办"
        plan["编排状态"] = "回溯等待"
        plan["pending"] = True
        effects.append(FlowEffect(
            "回溯写入中断", "backfill_job", int(backfill_row.get("id", 0)),
            f"调度 {plan_no} 的回溯 {job_no} 首次写入批次 {batch_code} 失败，"
            f"接入产出已保留，日期 {_qualification_date(backfill_row)} 资格仍由本计划持有",
        ))
        return "interrupted"

    backfill_row["status"] = "已完成"
    backfill_row["pending"] = False
    backfill_row["abnormal"] = False
    backfill_row["当前进度"] = "100%"
    backfill_row["写入状态"] = "已完成"
    backfill_row["已写入批次"] = batch_code
    backfill_row["完成批次"] = batch_code
    backfill_row["完成次数"] = max(int(backfill_row.get("完成次数") or 0), 1)
    if rows > 0:
        backfill_row["数据行数"] = f"{rows}行"
    backfill_row["完成时间"] = datetime.datetime.now().isoformat()
    backfill_row["完成计划"] = plan_no
    backfill_row["中断批次"] = ""
    backfill_row["失败原因"] = ""
    backfill_row["triggered_by"] = f"schedule_job/{plan_no}"
    release_backfill_qualification(backfill_row, "回溯完成")
    plan["回溯状态"] = "已完成"
    plan["回溯产出"] = f"{rows}行" if rows > 0 else str(backfill_row.get("数据行数", ""))
    plan["编排结果"] = f"回溯 {job_no} 采用批次 {batch_code} 完成，完成一次"
    plan["当前步骤"] = "已完成"
    plan["编排状态"] = "已完成"
    plan["status"] = "已完成"
    plan["pending"] = False
    effects.append(FlowEffect(
        "回溯完成", "backfill_job", int(backfill_row.get("id", 0)),
        f"调度 {plan_no} 的回溯 {job_no} 采用批次 {batch_code}（{rows}行）完成，仅完成一次",
    ))
    return "completed"


@_register(
    "schedule_job/激活计划 → batch_load/启动接入 → backfill_job/启动回溯",
    desc="调度计划按登记的接入/回溯对象依次执行，接入交付后回溯采用同一数据批次。",
)
@_register(
    "schedule_job/立即执行 → batch_load/启动接入 → backfill_job/启动回溯",
    desc="调度计划按登记的接入/回溯对象依次执行，接入交付后回溯采用同一数据批次。",
)
def flow_schedule_to_batch_to_backfill(schedule_job_id: int) -> list[FlowEffect]:
    effects: list[FlowEffect] = []

    sj = store.find("schedule_job", schedule_job_id)
    if sj is None or sj.get("status") not in ("执行中", "已完成"):
        return effects

    plan_no = str(sj.get("计划编号", ""))
    steps, error = resolve_plan_steps(sj)
    if steps is None:
        # 关联对象不存在或类型不匹配：不改写任何接入/回溯记录，计划原地等待。
        sj["编排结果"] = f"未执行：{error}"
        sj["当前步骤"] = "关联校验未通过"
        sj["编排状态"] = "未开工"
        sj["pending"] = True
        return effects

    batch_row = steps["batch_load"]
    backfill_row = steps["backfill_job"]
    sj["接入任务"] = str(batch_row.get("任务编号", "")) if batch_row else ""
    sj["回溯任务"] = str(backfill_row.get("任务编号", "")) if backfill_row else ""

    # 先做回溯日期预检；预检不通过时连接入步骤也不执行，整个计划原地等待。
    # 预检失败不写回溯记录，避免覆盖正在持有该日期资格的计划信息。
    if backfill_row is not None:
        ok, message = _reserve_backfill_qualification(sj, backfill_row)
        if not ok:
            sj["编排结果"] = f"回溯预检未通过：{message}"
            sj["当前步骤"] = "回溯预检未通过"
            sj["编排状态"] = "等待回溯资格"
            sj["pending"] = True
            effects.append(FlowEffect(
                "回溯预检未通过", "backfill_job", int(backfill_row.get("id", 0)), message,
            ))
            return effects
        effects.append(FlowEffect(
            "回溯预检通过", "backfill_job", int(backfill_row.get("id", 0)),
            f"调度 {plan_no} 的回溯 {backfill_row.get('任务编号', '')} 预检通过，日期资格已占用",
        ))
        # 通过预检后才登记归属，别的计划不会把这条回溯当成自己的。
        backfill_row["回溯归属计划"] = plan_no

    # 「立即执行」会把计划状态直接置为已完成，但回溯还在等待时不能这样标。
    # 编排状态以接入/回溯的实际进度为准，动作层的结果只是请求意图。
    if backfill_row is not None and str(backfill_row.get("status", "")) != "已完成":
        sj["status"] = "执行中"

    payload: dict[str, Any] | None = None
    if batch_row is not None:
        # 首次交付写入实际接入结果；续办时 _deliver_batch 只复用原批次，绝不重写。
        payload, _ = _deliver_batch(sj, batch_row, backfill_row, effects)
    elif backfill_row is not None:
        existing = backfill_row.get("编排接入数据")
        if isinstance(existing, dict) and existing.get("批次编号"):
            payload = existing
            backfill_row["接入产出确认"] = "已交付"
            sj["数据批次"] = str(existing.get("批次编号", ""))
        else:
            # 纯回溯计划没有接入步骤：生成只含回溯事实的编排数据，交给回溯使用。
            payload = {
                "批次编号": "",
                "来源接入任务": "",
                "来源计划": plan_no,
                "接入行数": 0,
                "拒绝行数": 0,
                "有效行数": 0,
                "接入时间": "",
                "起始日期": str(backfill_row.get("起始日期", "")),
            }
            backfill_row["编排接入数据"] = payload
            backfill_row["接入产出确认"] = "无接入步骤"
            backfill_row["数据批次"] = str(backfill_row.get("数据批次") or "")

    if backfill_row is None:
        # 单步接入计划：只交付接入，不牵动任何回溯记录。
        sj["编排状态"] = "已完成"
        sj["编排结果"] = (
            f"接入 {batch_row.get('任务编号', '')} 已交付批次 {sj.get('数据批次', '')}"
        )
        sj["当前步骤"] = "已完成"
        sj["status"] = "已完成"
        sj["pending"] = False
        return effects

    # 接入已交付但回溯尚未落地时，计划不能被「立即执行」等动作提前标成已完成。
    if str(backfill_row.get("status", "")) != "已完成":
        sj["status"] = "执行中"

    # 续办前确认该计划的接入产出是否已经交付，已交付则沿用同一批次。
    if batch_row is not None and _batch_delivered(batch_row):
        backfill_row["接入产出确认"] = "已交付"
    _write_backfill(sj, backfill_row, payload, effects)
    return effects


# ---------------------------------------------------------------------------
# 链路路由表：根据 (action_module, action_name, status) 找到下游链路
# ---------------------------------------------------------------------------

# key = f"{module}/{action_name}" → [/→/→ 效果收集]
FLOW_TRIGGERS: dict[str, list[Callable[[int], list[FlowEffect]]]] = {}

for link in FLOW_LINKS:
    path = str(link["path"])
    trigger = link["trigger"]
    # first segment is the trigger origin: module/action
    parts = [p.strip() for p in path.split("→")]
    if parts:
        origin = parts[0]  # e.g. "feature_reg/审核注册"
        FLOW_TRIGGERS.setdefault(origin, []).append(trigger)


# ---------------------------------------------------------------------------
# FlowEngine: 注册到路由后的统一入口
# ---------------------------------------------------------------------------


class FlowEngine:
    """跨模块数据流编排引擎。"""

    def on_action(self, module: str, action: str, entry_id: int) -> dict[str, Any]:
        """在业务动作路由执行后调用，触发下游链路消费。

        Returns:
            {"chains": [...链名称], "effects": [...副作用记录], "hits": N}
        """
        triggers = FLOW_TRIGGERS.get(f"{module}/{action}", [])
        all_effects: list[FlowEffect] = []
        chain_hits: list[str] = []

        seen_triggers: set[int] = set()
        for trigger_fn in triggers:
            if id(trigger_fn) in seen_triggers:
                continue
            seen_triggers.add(id(trigger_fn))
            found = False
            for link in FLOW_LINKS:
                if link["trigger"] is trigger_fn and not found:
                    chain_hits.append(str(link["desc"]))
                    found = True
            effects = trigger_fn(entry_id)
            all_effects.extend(effects)

        return {
            "chains": chain_hits,
            "effects": [
                {"name": e.name, "module": e.module, "entry_id": e.entry_id,
                 "description": e.description, "timestamp": e.timestamp}
                for e in all_effects
            ],
            "hits": len(all_effects),
        }

    def replay_link(self, path_key: str, entry_id: int) -> list[FlowEffect]:
        """手动重放指定链路。path_key 如 "derive_rule/启效规则"."""
        triggers = FLOW_TRIGGERS.get(path_key, [])
        effects: list[FlowEffect] = []
        for fn in triggers:
            effects.extend(fn(entry_id))
        return effects


flow_engine = FlowEngine()