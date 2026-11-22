"""冰山上层提交：提交完成后触发compaction小文件合并和expire_snap过期清理。"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.services.compaction import CompactionService
from app.store import store


class IcebergCommitService(BaseModuleService):
    MODULE = "iceberg_commit"
    ENTITY = "提交快照"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["快照编号", "目标表", "数据文件", "清单文件", "快照ID", "提交状态", "影响行数", "提交时间"]
    REQUIRED_FIELDS = ["快照编号", "目标表", "数据文件"]
    STATUS_ORDER = ["待提交", "提交中", "已提交", "提交冲突"]
    ACTION_RULES = {"启动提交": "提交中", "重试提交": "已提交", "标记冲突": "提交冲突"}
    NEGATIVE_ACTIONS = []

    def trigger_compaction_and_expire(self, entry_id: int) -> dict[str, Any] | None:
        """Iceberg提交完成后触发compaction小文件合并和expire_snap过期快照清理。

        数据流链路: iceberg_commit -> compaction + expire_snap
        """
        entry = self.get_entry(entry_id)
        if entry is None or entry.get("status") != "已提交":
            return None

        table = str(entry.get("目标表", "unknown"))
        results: dict[str, Any] = {"compaction": None, "expire_snap": None}

        # 检查文件数，触发 compaction（经确认清单生成的合并任务，避免裸“已完成”）
        comp_service = CompactionService()
        comp_rows = store.rows("compaction")
        existing_comp = [r for r in comp_rows if str(r.get("目标表", "")) == table]
        if not existing_comp:
            comp = comp_service.build_completed_compaction(table)
            results["compaction"] = comp
        else:
            # 同表已有合并任务时直接接续，不篡改文件数量，保持文件计划与确认清单一致
            existing = existing_comp[0]
            comp_service.ensure_file_plan(existing)
            results["compaction"] = existing

        # 触发 expire_snap
        exp_rows = store.rows("expire_snap")
        existing_exp = [r for r in exp_rows if str(r.get("目标表", "")) == table]
        if existing_exp:
            existing_exp[0]["status"] = "生效中"
            existing_exp[0]["pending"] = True
            existing_exp[0]["abnormal"] = False
            results["expire_snap"] = existing_exp[0]
        else:
            new_exp_id = max((int(r.get("id", 0)) for r in exp_rows), default=0) + 1
            expire = {
                "id": new_exp_id,
                "策略编号": f"EXPI-{new_exp_id:04d}",
                "目标表": table,
                "保留数量": 10,
                "保留天数": "7天",
                "清理模式": "按快照时间过期",
                "预演结果": f"待清理{new_exp_id * 2}个历史快照",
                "下次执行": "2026-11-19 02:00:00",
                "上次清理": "2026-11-18 02:00:00",
                "status": "生效中",
                "pending": True,
                "abnormal": False,
            }
            exp_rows.append(expire)
            results["expire_snap"] = expire

        return results

    def handle_commit_conflict(self, entry_id: int) -> dict[str, Any] | None:
        """提交冲突时触发数据比对校验和告警。

        数据流链路: iceberg_commit(提交冲突) -> data_diff + alert_rule
        """
        entry = self.get_entry(entry_id)
        if entry is None or entry.get("status") != "提交冲突":
            return None

        table = str(entry.get("目标表", "unknown"))
        results = {"data_diff": None, "alert": None}

        # 触发 data_diff 比对
        diff_rows = store.rows("data_diff")
        new_diff_id = max((int(r.get("id", 0)) for r in diff_rows), default=0) + 1
        diff = {
            "id": new_diff_id,
            "任务编号": f"DATA-{new_diff_id:04d}",
            "源端": "CDC实时流",
            "目标端": table,
            "比对字段": "全字段",
            "差异类型": "数据不一致",
            "差异行数": entry.get("影响行数", 0),
            "修复建议": "回滚pg_snapshot并重放",
            "比对时间": "2026-11-18 10:38:00",
            "status": "存在差异",
            "pending": True,
            "abnormal": True,
        }
        diff_rows.append(diff)
        results["data_diff"] = diff

        # 创建告警
        alert_rows = store.rows("alert_rule")
        new_alert_id = max((int(r.get("id", 0)) for r in alert_rows), default=0) + 1
        alert = {
            "id": new_alert_id,
            "策略编号": f"ALER-{new_alert_id:04d}",
            "监控指标": "Iceberg提交冲突",
            "告警条件": f"表{table}提交冲突",
            "告警级别": "P1-紧急",
            "通知渠道": "电话+飞书+钉钉",
            "聚合窗口": "不聚合",
            "静默时段": "无静默",
            "触发次数": 1,
            "status": "已触发",
            "pending": True,
            "abnormal": True,
        }
        alert_rows.append(alert)
        results["alert"] = alert

        return results