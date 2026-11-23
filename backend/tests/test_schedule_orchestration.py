"""调度编排链路的回归测试：接入/回溯按计划关联对象选取，状态依次流转。

直接驱动 FlowEngine 与内存仓库，不依赖 HTTP 客户端。
"""
from __future__ import annotations

import unittest

from app.seed import SEED_ROWS
from app.services.data_flow import flow_engine, resolve_plan_steps
from app.store import store


def reset_store() -> None:
    store._tables = {name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()}


def add(module: str, **values) -> dict:
    rows = store.rows(module)
    entry = {"id": max((int(r.get("id", 0)) for r in rows), default=0) + 1}
    entry.update(values)
    entry.setdefault("status", "待激活" if module == "schedule_job" else "待执行")
    entry.setdefault("pending", True)
    entry.setdefault("abnormal", False)
    rows.append(entry)
    return entry


def activate(plan: dict) -> None:
    plan["status"] = "执行中"
    flow_engine.on_action("schedule_job", "激活计划", int(plan["id"]))


class ScheduleOrchestrationTest(unittest.TestCase):
    def setUp(self) -> None:
        reset_store()
        self.i = add("batch_load", 任务编号="BATC-I01", 数据源="s", 目标特征="f")
        self.j = add("batch_load", 任务编号="BATC-J01", 数据源="s", 目标特征="f")
        self.b = add("backfill_job", 任务编号="BACK-B01", 目标特征="f", 起始日期="2026-10-01")
        self.c = add("backfill_job", 任务编号="BACK-C01", 目标特征="f", 起始日期="2026-10-02")
        self.p = add(
            "schedule_job", 计划编号="SCHE-P01", 任务类型="批量接入+特征回溯",
            关联对象="batch_load/BATC-I01,backfill_job/BACK-B01",
        )

    def test_missing_target_does_not_touch_other_tasks(self) -> None:
        plan = add(
            "schedule_job", 计划编号="SCHE-BAD", 任务类型="批量接入+特征回溯",
            关联对象="batch_load/NOPE,backfill_job/NOPE2",
        )
        before = (dict(self.j), dict(self.c))
        activate(plan)
        self.assertEqual(before, (self.j, self.c))
        self.assertEqual(plan["当前步骤"], "关联校验未通过")

    def test_type_mismatch_is_rejected_without_writes(self) -> None:
        plan = add(
            "schedule_job", 计划编号="SCHE-T", 任务类型="批量接入",
            关联对象="backfill_job/BACK-B01",
        )
        before = dict(self.b)
        activate(plan)
        self.assertEqual(before, self.b)
        self.assertEqual(resolve_plan_steps(plan)[0], None)

    def test_single_step_batch_plan(self) -> None:
        solo = add("batch_load", 任务编号="BATC-SOLO", 数据源="s", 目标特征="f")
        plan = add(
            "schedule_job", 计划编号="SCHE-SOLO", 任务类型="批量接入",
            关联对象="batch_load/BATC-SOLO",
        )
        before_b, before_c = dict(self.b), dict(self.c)
        activate(plan)
        self.assertEqual(solo["status"], "已完成")
        self.assertEqual((before_b, before_c), (self.b, self.c))

    def test_two_step_plan_interrupts_then_resumes_once(self) -> None:
        activate(self.p)
        self.assertEqual(self.i["status"], "已完成")
        batch_code = self.i["数据批次"]
        rows_value = self.i["接入行数"]
        time_value = self.i["接入时间"]
        self.assertNotEqual(self.b["status"], "已完成")
        self.assertEqual(self.b["写入状态"], "写入失败")
        self.assertEqual(self.b["数据批次"], batch_code)
        self.assertEqual(self.p["数据批次"], batch_code)
        self.assertEqual(self.p["编排状态"], "回溯等待")

        # 同日期被中断的 P 占着，另一个计划不能开工。
        other_date = add(
            "backfill_job", 任务编号="BACK-B02", 目标特征="f", 起始日期="2026-10-01",
        )
        q = add(
            "schedule_job", 计划编号="SCHE-Q", 任务类型="特征回溯",
            关联对象="backfill_job/BACK-B02",
        )
        activate(q)
        self.assertEqual(q["编排状态"], "等待回溯资格")
        self.assertEqual(other_date["status"], "待执行")

        # 续办：B 采用同一批次、只完成一次，I 的行数与接入时间不被改写。
        activate(self.p)
        self.assertEqual(self.b["status"], "已完成")
        self.assertEqual(self.b["完成次数"], 1)
        self.assertEqual(self.b["已写入批次"], batch_code)
        self.assertEqual(self.i["接入行数"], rows_value)
        self.assertEqual(self.i["接入时间"], time_value)
        self.assertEqual(self.p["编排状态"], "已完成")

        # 幂等：重复续办不增加完成次数。
        activate(self.p)
        self.assertEqual(self.b["完成次数"], 1)

    def test_unrelated_tasks_untouched_by_normal_run(self) -> None:
        before_j, before_c = dict(self.j), dict(self.c)
        activate(self.p)
        self.assertEqual((before_j, before_c), (self.j, self.c))


if __name__ == "__main__":
    unittest.main()
