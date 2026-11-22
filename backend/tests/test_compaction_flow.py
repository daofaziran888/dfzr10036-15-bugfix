"""端到端核验：取消/完成状态流转、合并数据文件确认清单、过期预演与流聚合计数。

运行：.venv/bin/python -I tests/test_compaction_flow.py
"""
from __future__ import annotations

import os
import sys

# -I 隔离模式下不读 PYTHONPATH，显式把 backend 根目录加入模块搜索路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.services import service_for
from app.services.orchestrator import flow_lakehouse_maintenance
from app.store import store

failures: list[str] = []


def check(name: str, condition: bool, detail: str = "") -> None:
    mark = "PASS" if condition else "FAIL"
    print(f"[{mark}] {name}" + (f" -> {detail}" if detail else ""))
    if not condition:
        failures.append(name)


def reset_store() -> None:
    # 重新装载种子数据，保证各用例互不干扰
    from app import seed
    store._tables = {name: [dict(row) for row in rows] for name, rows in seed.SEED_ROWS.items()}


compaction = service_for("compaction")
expire = service_for("expire_snap")
metric = service_for("metric_aggr")

# ---------------------------------------------------------------- 场景1
# 在小文件合并工作台取消一个“尚未写出文件”的任务（CMP-0002 / id=2），
# 再执行湖仓维护的接续入口。
reset_store()
task = compaction.get_entry(2)
check("1.1 初始任务处于合并中", task["status"] == "合并中", task["status"])
check("1.2 取消前没有任何替换文件落地", compaction._landed_paths(task) == [])
check("1.3 取消前确认清单为空", compaction._confirmed_manifest(task) == [])

entry, msg = compaction.run_action(2, "取消合并")
check("1.4 取消后状态是已取消而不是已完成", entry["status"] == "已取消", entry["status"])
check("1.5 取消被标记为异常待处理", entry["abnormal"] is True and entry["pending"] is True)
check("1.6 取消动作返回保留说明", "保留" in msg and "不会被清理" in msg, msg)

# 终态守卫：已取消不能再取消，也不能确认
_, reject_cancel = compaction.run_action(2, "取消合并")
check("1.7 已取消任务拒绝再次取消", "终态" in reject_cancel, reject_cancel)
_, reject_confirm = compaction.run_action(2, "确认合并文件")
check("1.8 已取消任务拒绝确认合并文件", reject_confirm is not None and "不允许" in reject_confirm, reject_confirm)

# 湖仓维护接续入口
flow = flow_lakehouse_maintenance(2)
check("1.9 接续入口对已取消任务做跳过处理", flow["skipped"] is True, flow.get("reason", ""))
check("1.10 跳过时预演路径为空", flow["preview_paths"] == [])
check("1.11 跳过不产生任何下游调用", flow["call_count"] == 0 and flow["trace"] == [])

# 过期策略未被污染：payment_log 表的预演清理路径仍为空
exp_payment = next(r for r in store.rows("expire_snap") if r["目标表"] == "ods.payment_log_iceberg")
check("1.12 过期策略未被取消任务污染", exp_payment.get("预演清理路径", []) == []
      and "确认清单" in exp_payment["预演结果"], exp_payment["预演结果"])

# 流聚合没有为该表新增“合并批次”窗口
batch_metrics = [r for r in store.rows("metric_aggr")
                 if r["数据流"] == "ods.payment_log_iceberg" and r["聚合键"] == "合并批次"]
check("1.13 取消任务不进入流聚合统计", batch_metrics == [])

# 取消后可重试，且重试不清理原始输入
inputs_before = list(entry["原始输入文件"])
retry_entry, retry_msg = compaction.run_action(2, "重新合并")
check("1.14 已取消任务可以重新合并", retry_entry["status"] == "合并中", retry_msg)
check("1.15 重试保留全部原始输入路径", retry_entry["原始输入文件"] == inputs_before
      and len(retry_entry["原始输入文件"]) == 95)

# ---------------------------------------------------------------- 场景2
# 并发撞单：CMP-0003 已有一组替换文件确认在途（1确认 + 1落地未确认），
# 值班员发来取消请求。取消不能越级成整批已提交。
reset_store()
race = compaction.get_entry(3)
check("2.1 撞单任务初始为合并中且部分确认", race["status"] == "合并中"
      and race["文件确认状态"].startswith("部分确认"), race["文件确认状态"])
landed = compaction._landed_paths(race)
check("2.2 取消前已有两个替换文件落地", len(landed) == 2, str(landed))
confirmed_before = compaction._confirmed_manifest(race)
check("2.3 取消前仅一个替换文件进入确认清单", len(confirmed_before) == 1)

cancelled, cancel_msg = compaction.run_action(3, "取消合并")
check("2.4 撞单取消后状态仍是已取消而非已完成", cancelled["status"] == "已取消", cancelled["status"])
check("2.5 已落地文件路径在取消后全部保留",
      compaction._landed_paths(cancelled) == landed)
check("2.6 确认清单只保留真实确认过的那一组",
      len(compaction._confirmed_manifest(cancelled)) == 1
      and compaction._confirmed_manifest(cancelled)[0]["替换文件"] == confirmed_before[0]["替换文件"])
check("2.7 未确认的落地文件未取得清理资格",
      cancelled["替换文件计划"][1]["已确认"] is False)

flow2 = flow_lakehouse_maintenance(3)
check("2.8 撞单取消后接续入口同样跳过", flow2["skipped"] is True, flow2.get("reason", ""))
check("2.9 跳过时不列出任何预演清理路径", flow2["preview_paths"] == [])
# 该表的过期策略不能被部分确认数据污染
exp_dwd = next(r for r in store.rows("expire_snap") if r["目标表"] == "dwd.trade_detail_iceberg")
check("2.10 无替换依据的文件不得从过期预演取得清理资格",
      exp_dwd.get("预演清理路径", []) == [] and exp_dwd["status"] == "生效中")

# ---------------------------------------------------------------- 场景3
# 正常链路：新建任务 -> 启动 -> 逐批写出 -> 校验确认 -> 接续湖仓维护。
reset_store()
created, missing = compaction.create_entry({
    "任务编号": "CMP-NEW", "目标表": "ads.order_events_iceberg", "文件数量": 10,
    "合并策略": "BinPack-128MB", "触发阈值": "文件数>=8", "执行内存": "2GB",
})
check("3.1 新建任务为待执行并自动生成文件计划",
      created["status"] == "待执行" and len(created["原始输入文件"]) == 10
      and len(created["替换文件计划"]) == 2)
new_id = created["id"]

# 未启动时写出/确认都应被拒绝
_, reject_write = compaction.run_action(new_id, "写出替换文件")
check("3.2 待执行任务不能直接写出替换文件", "不允许" in reject_write, reject_write)

compaction.run_action(new_id, "启动合并")
# 只写出、不确认：接续入口必须跳过
first_path = compaction.run_action(new_id, "写出替换文件")[1]
running = compaction.get_entry(new_id)
check("3.3 替换文件先落地但未确认", running["替换文件计划"][0]["已落地"] is True
      and running["替换文件计划"][0]["已确认"] is False, first_path)
flow3a = flow_lakehouse_maintenance(new_id)
check("3.4 仅落地未确认时接续入口跳过", flow3a["skipped"] is True
      and flow3a["preview_paths"] == [], flow3a.get("reason", ""))

# 继续写出第二个替换文件，然后一次性确认两批
compaction.run_action(new_id, "写出替换文件")
_, write_full = compaction.run_action(new_id, "写出替换文件")
check("3.5 计划全部落地后再写被拒绝", write_full is not None and "全部落地" in write_full, write_full)

confirmed_entry, confirm_msg = compaction.run_action(new_id, "确认合并文件")
check("3.6 确认清单收录两个替换文件",
      len(compaction._confirmed_manifest(confirmed_entry)) == 2, confirm_msg)
check("3.7 全部原始输入被确认替换后任务才完成",
      confirmed_entry["status"] == "已完成"
      and confirmed_entry["文件确认状态"] == "全部确认", confirmed_entry["status"])
check("3.8 完成任务不再挂起", confirmed_entry["pending"] is False)

# 再次确认幂等，且完成态拒绝重复确认
_, confirm_again = compaction.run_action(new_id, "确认合并文件")
check("3.9 完成态拒绝重复确认", "不允许" in confirm_again, confirm_again)

flow3b = flow_lakehouse_maintenance(new_id)
check("3.10 已确认任务驱动下游接续", flow3b["skipped"] is False and flow3b["call_count"] == 2)
check("3.11 预演路径与确认清单的替换依据完全一致",
      sorted(flow3b["preview_paths"]) == sorted(confirmed_entry["原始输入文件"])
      and len(flow3b["preview_paths"]) == 10)
exp_ads = next(r for r in store.rows("expire_snap") if r["目标表"] == "ads.order_events_iceberg")
check("3.12 过期预演状态与清理路径按确认清单限定",
      exp_ads["status"] == "预演完成"
      and exp_ads["预演清理路径"] == flow3b["preview_paths"]
      and "确认替换文件2个" in exp_ads["预演结果"], exp_ads["预演结果"])
batch = next(r for r in store.rows("metric_aggr")
             if r["数据流"] == "ads.order_events_iceberg" and r["聚合键"] == "合并批次")
check("3.13 流聚合按实际确认结果更新计数",
      batch["确认文件数"] == 2 and batch["确认替换输入数"] == 10
      and batch["合并任务编号"] == "CMP-NEW", str(batch.get("确认文件数")))

# 接续入口幂等：再跑一次不应重复计数/重复追加
flow3c = flow_lakehouse_maintenance(new_id)
batch_rows = [r for r in store.rows("metric_aggr")
              if r["数据流"] == "ads.order_events_iceberg" and r["聚合键"] == "合并批次"]
check("3.14 接续入口重复调用保持幂等", len(batch_rows) == 1 and flow3c["call_count"] == 2)

# ---------------------------------------------------------------- 场景4
# 校验拦截：替换依据缺失/指向未知原始输入的文件不得进入确认清单。
reset_store()
bad, _ = compaction.create_entry({
    "任务编号": "CMP-BAD", "目标表": "dwd.trade_detail_iceberg", "文件数量": 10,
})
bid = bad["id"]
compaction.run_action(bid, "启动合并")
# 篡改第一批替换文件：移除替换依据并落地
bad = compaction.get_entry(bid)
bad["替换文件计划"][0]["替换依据"] = []
bad["替换文件计划"][0]["已落地"] = True
bad["替换文件计划"][1]["已落地"] = True
_, bad_msg = compaction.run_action(bid, "确认合并文件")
bad = compaction.get_entry(bid)
check("4.1 无替换依据的文件被拒绝确认且有提示", "缺少替换依据" in bad_msg, bad_msg)
check("4.2 确认清单只收录依据完整的文件",
      len(compaction._confirmed_manifest(bad)) == 1
      and compaction._confirmed_manifest(bad)[0]["替换文件"] == bad["替换文件计划"][1]["替换文件"])
check("4.3 部分确认时任务保持合并中，不越级完成", bad["status"] == "合并中")
flow4 = flow_lakehouse_maintenance(bid)
check("4.4 部分确认任务的接续仍被跳过", flow4["skipped"] is True)

# ---------------------------------------------------------------- 场景5
# 湖仓提交链路（cdc_to_lake）产生的合并任务必须自带确认清单，可直接续做。
reset_store()
from app.services.iceberg_commit import IcebergCommitService
ice = IcebergCommitService()
result = ice.trigger_compaction_and_expire(1)  # 种子里 id=1 已提交，表 ads.order_events_iceberg
new_comp = result["compaction"]
check("5.1 提交链路生成带确认清单的合并任务",
      len(new_comp.get("合并数据文件确认清单", [])) > 0
      and new_comp["status"] == "已完成", new_comp["status"])
flow5 = flow_lakehouse_maintenance(new_comp["id"])
check("5.2 提交链路的合并任务可直接接续湖仓维护",
      flow5["skipped"] is False
      and len(flow5["preview_paths"]) == len(compaction._confirmed_input_paths(new_comp))
      and flow5["preview_paths"] == compaction._confirmed_input_paths(new_comp))

print()
if failures:
    print(f"共 {len(failures)} 项失败：{failures}")
    sys.exit(1)
print("全部核验通过")
