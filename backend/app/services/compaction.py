"""小文件合并服务。

状态流转（修复后）：
    待执行 --启动合并--> 合并中
    合并中 --写出替换文件--> 合并中（替换文件先落到存储，尚未确认）
    合并中 --确认合并文件--> 已确认 / 已完成（以合并数据文件确认清单为准）
    合并中 --取消合并--> 已取消（绝不能越级成“已完成”）
    合并中/已取消/合并失败 --标记失败--> 合并失败
    已取消/合并失败 --重新合并--> 合并中

关键约束：
- 取消只改变任务状态，真实落地的替换文件与原始输入路径都保留，回滚/重试不清理。
- 没有替换依据（未确认的合并数据文件）的文件，不具备从过期预演取得清理资格。
- 快照过期预演与流聚合计数只消费“合并数据文件确认清单”里已确认的替换结果。
"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.store import store

# 合并任务终态：进入终态后不再接受取消/确认等动作
TERMINAL_STATUSES = {"已完成", "已取消", "合并失败"}
# 只有确认清单里存在已确认替换文件的任务，才允许驱动下游湖仓维护
CONFIRMED_STATUSES = {"已确认", "已完成"}


def _table_path(table: str) -> str:
    return f"s3://warehouse/{table.replace('.', '/')}/data"


def build_input_files(table: str, file_count: int) -> list[str]:
    """按目标表和文件数量生成确定性的原始输入文件路径。"""
    return [f"{_table_path(table)}/input/part-{index:05d}.parquet" for index in range(1, file_count + 1)]


def build_replacement_plan(table: str, file_count: int) -> list[dict[str, Any]]:
    """合并计划：每个替换文件负责一组原始输入（替换依据），初始均未落地、未确认。"""
    if file_count <= 0:
        return []
    group_size = 8
    plan: list[dict[str, Any]] = []
    start = 1
    index = 1
    while start <= file_count:
        end = min(start + group_size - 1, file_count)
        plan.append({
            "替换文件": f"{_table_path(table)}/compact/merged-part-{index:05d}.parquet",
            "替换依据": [
                f"{_table_path(table)}/input/part-{seq:05d}.parquet"
                for seq in range(start, end + 1)
            ],
            "已落地": False,
            "已确认": False,
        })
        start = end + 1
        index += 1
    return plan


class CompactionService(BaseModuleService):
    MODULE = "compaction"
    ENTITY = "合并任务"
    OPERATION_TERM = "合并"
    LIST_FIELDS = [
        "任务编号", "目标表", "文件数量", "合并策略", "触发阈值", "执行内存",
        "合并耗时", "释放空间", "文件确认状态",
    ]
    REQUIRED_FIELDS = ["任务编号", "目标表", "文件数量"]
    # 已取消放在最后：前端指标卡取末态统计取消任务数
    STATUS_ORDER = ["待执行", "合并中", "已完成", "合并失败", "已取消"]
    # 仅声明动作归属；真正的目标状态由 run_action 按当前状态与确认清单裁决，
    # 取消不再无条件映射成“已完成”。
    ACTION_RULES = {
        "启动合并": "合并中",
        "重新合并": "合并中",
        "写出替换文件": "合并中",
        "确认合并文件": "已完成",
        "取消合并": "已取消",
        "标记失败": "合并失败",
    }
    NEGATIVE_ACTIONS = ["取消合并", "标记失败"]

    # ---- 数据装载：为历史/新建任务补齐文件计划与确认清单 ----

    def ensure_file_plan(self, entry: dict[str, Any]) -> dict[str, Any]:
        """确保任务带有原始输入、替换计划和合并数据文件确认清单。

        已有的确认结果不会被覆盖（取消后保留真实已落地数据与路径）。
        """
        table = str(entry.get("目标表", "unknown"))
        file_count = int(entry.get("文件数量", 0) or 0)
        if not entry.get("原始输入文件"):
            entry["原始输入文件"] = build_input_files(table, file_count)
        if not entry.get("替换文件计划"):
            entry["替换文件计划"] = build_replacement_plan(table, file_count)
        entry.setdefault("合并数据文件确认清单", [])
        return entry

    def list_entries(self, *args: Any, **kwargs: Any) -> tuple[list[dict[str, Any]], int]:
        items, total = super().list_entries(*args, **kwargs)
        for entry in items:
            self.ensure_file_plan(entry)
            self._refresh_confirm_state(entry)
        return items, total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        entry = super().get_entry(entry_id)
        if entry is not None:
            self.ensure_file_plan(entry)
            self._refresh_confirm_state(entry)
        return entry

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        entry, missing = super().create_entry(values)
        if entry is None:
            return entry, missing
        self.ensure_file_plan(entry)
        self._refresh_confirm_state(entry)
        return entry, missing

    # ---- 状态机：取消与完成严格分离 ----

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"
        if action not in self.ACTION_RULES:
            return None, f"动作「{action}」不属于{self.MODULE}可执行范围"
        self.ensure_file_plan(entry)
        status = str(entry.get("status"))

        if action == "启动合并":
            if status == "合并中":
                return None, "合并任务已在执行中，不能重复启动"
            if status in CONFIRMED_STATUSES:
                return None, "合并文件已确认完成，无需再次启动"
            entry["status"] = "合并中"
            entry["pending"] = True
            entry["abnormal"] = False
            return entry, f"{self.ENTITY}已{action}"

        if action == "重新合并":
            if status not in {"已取消", "合并失败", "待执行"}:
                return None, f"当前状态「{status}」不允许重新合并"
            entry["status"] = "合并中"
            entry["pending"] = True
            entry["abnormal"] = False
            # 重试只基于未确认部分继续，已确认/已落地的原始输入与文件路径不清空
            return entry, f"{self.ENTITY}已{action}，未确认的原始输入保持不变"

        if action == "标记失败":
            entry["status"] = "合并失败"
            entry["pending"] = True
            entry["abnormal"] = True
            return entry, f"{self.ENTITY}已{action}"

        if action == "取消合并":
            if status in TERMINAL_STATUSES:
                return None, f"当前状态「{status}」为终态，不能取消"
            # 核心修复：取消就是取消，绝不写成“已完成”。
            entry["status"] = "已取消"
            entry["pending"] = True
            entry["abnormal"] = True
            self._refresh_confirm_state(entry)
            landed = self._landed_paths(entry)
            confirmed = self._confirmed_manifest(entry)
            return entry, (
                f"{self.ENTITY}已{action}：已落地替换文件{len(landed)}个、"
                f"已确认{len(confirmed)}个均保留，未确认的原始输入不会被清理"
            )

        if action == "写出替换文件":
            if status != "合并中":
                return None, f"当前状态「{status}」不允许写出替换文件"
            written = self._land_next_replacement(entry)
            if written is None:
                return None, "替换文件计划已全部落地，等待确认合并文件"
            entry["pending"] = True
            return entry, f"替换文件已落地：{written}（尚未确认，不具备清理资格）"

        if action == "确认合并文件":
            if status != "合并中":
                return None, f"当前状态「{status}」不允许确认合并文件"
            accepted, rejected = self._confirm_landed_files(entry)
            if not accepted:
                return None, "没有已落地且替换依据完整的合并数据文件可确认"
            self._refresh_confirm_state(entry)
            message = f"合并数据文件确认清单已确认{accepted}个替换文件"
            if rejected:
                message += f"；{rejected}个文件缺少替换依据，不予确认"
            if entry["status"] == "已完成":
                message += "，全部原始输入均已有确认替换，任务完成"
            else:
                message += "，仍有原始输入缺少确认替换，任务保持合并中"
            return entry, message

        return None, f"动作「{action}」不属于{self.MODULE}可执行范围"

    # ---- 合并数据文件确认清单 ----

    def _land_next_replacement(self, entry: dict[str, Any]) -> str | None:
        """模拟存储侧写出下一个替换文件：先落地，确认是独立的后续动作。"""
        for plan_item in entry["替换文件计划"]:
            if not plan_item.get("已落地"):
                plan_item["已落地"] = True
                return str(plan_item["替换文件"])
        return None

    def _confirm_landed_files(self, entry: dict[str, Any]) -> tuple[int, int]:
        """校验已落地的合并数据文件并生成/追加确认清单。

        校验规则：
        - 必须已经真实落地（未写出的文件不能确认）；
        - 必须携带替换依据，且依据指向本任务登记过的原始输入文件；
        - 重复确认幂等跳过。
        """
        known_inputs = set(entry.get("原始输入文件", []))
        manifest = entry.setdefault("合并数据文件确认清单", [])
        confirmed_paths = {item["替换文件"] for item in manifest}
        accepted = 0
        rejected = 0
        for plan_item in entry["替换文件计划"]:
            path = str(plan_item.get("替换文件", ""))
            basis = [str(p) for p in plan_item.get("替换依据", [])]
            if not plan_item.get("已落地"):
                continue
            if path in confirmed_paths:
                continue
            if not basis or any(p not in known_inputs for p in basis):
                # 没有替换依据的文件不得进入确认清单，也就拿不到清理资格
                rejected += 1
                continue
            plan_item["已确认"] = True
            manifest.append({
                "替换文件": path,
                "替换依据": list(basis),
                "确认时间": "2026-11-18 10:50:00",
            })
            confirmed_paths.add(path)
            accepted += 1
        return accepted, rejected

    def _confirmed_manifest(self, entry: dict[str, Any]) -> list[dict[str, Any]]:
        return [item for item in entry.get("合并数据文件确认清单", []) if item.get("替换文件")]

    def _landed_paths(self, entry: dict[str, Any]) -> list[str]:
        return [
            str(item["替换文件"])
            for item in entry.get("替换文件计划", [])
            if item.get("已落地")
        ]

    def _confirmed_input_paths(self, entry: dict[str, Any]) -> list[str]:
        """已被确认替换结果覆盖、因此具备过期清理资格的原始输入路径。"""
        covered: list[str] = []
        for item in self._confirmed_manifest(entry):
            for path in item.get("替换依据", []):
                if path not in covered:
                    covered.append(str(path))
        return covered

    def _refresh_confirm_state(self, entry: dict[str, Any]) -> None:
        """根据确认清单刷新文件确认状态字段；终态（如已取消）不被回写。"""
        manifest = self._confirmed_manifest(entry)
        total_inputs = len(entry.get("原始输入文件", []))
        covered = len(self._confirmed_input_paths(entry))
        if not manifest:
            entry["文件确认状态"] = "未确认"
        elif covered >= total_inputs and total_inputs > 0:
            entry["文件确认状态"] = "全部确认"
        else:
            entry["文件确认状态"] = f"部分确认({len(manifest)})"
        # 全部原始输入都有确认替换时才允许进入“已完成”；已取消等终态保持不变
        if entry.get("status") == "合并中" and total_inputs > 0 and covered >= total_inputs:
            entry["status"] = "已完成"
            entry["pending"] = False
            entry["abnormal"] = False

    # ---- 湖仓维护接续入口 ----

    def trigger_expire_and_metric(self, entry_id: int) -> dict[str, Any] | None:
        """合并确认后的接续入口：驱动快照过期预演与流聚合计数更新。

        只消费合并数据文件确认清单：
        - 已取消/未确认任务直接跳过，不产生预演清理路径，也不进入统计；
        - 过期预演范围限定为确认清单里有替换依据的原始输入文件；
        - 流聚合按实际确认的替换结果同步计数。
        """
        entry = self.get_entry(entry_id)
        if entry is None:
            return None
        status = str(entry.get("status"))
        manifest = self._confirmed_manifest(entry)
        if status not in CONFIRMED_STATUSES or not manifest:
            # 不能把取消当成整批已提交：没有确认清单就没有下游接续
            return {
                "compaction": entry,
                "expire_snap": None,
                "metric_aggr": None,
                "skipped": True,
                "reason": self._skip_reason(entry, status, manifest),
                "confirmed_manifest": [],
                "preview_paths": [],
            }

        table = str(entry.get("目标表", "unknown"))
        preview_paths = self._confirmed_input_paths(entry)
        results: dict[str, Any] = {
            "compaction": entry,
            "expire_snap": None,
            "metric_aggr": None,
            "skipped": False,
            "reason": "",
            "confirmed_manifest": manifest,
            "preview_paths": preview_paths,
        }

        # 快照过期：预演范围严格限定在确认清单覆盖的路径内
        for exp in store.rows("expire_snap"):
            if str(exp.get("目标表", "")) == table:
                exp["status"] = "预演完成"
                exp["pending"] = True
                exp["abnormal"] = False
                exp["预演结果"] = (
                    f"确认替换文件{len(manifest)}个，具备清理资格的原始数据文件"
                    f"{len(preview_paths)}个"
                )
                exp["预演清理路径"] = list(preview_paths)
                results["expire_snap"] = exp
                break

        # 流聚合：按实际确认的替换结果同步更新计数（同表同批次幂等）
        metric_rows = store.rows("metric_aggr")
        metric = next(
            (row for row in metric_rows
             if str(row.get("数据流", "")) == table and row.get("聚合键") == "合并批次"),
            None,
        )
        if metric is None:
            new_metric_id = max((int(r.get("id", 0)) for r in metric_rows), default=0) + 1
            metric = {
                "id": new_metric_id,
                "窗口编号": f"METR-{new_metric_id:04d}",
                "数据流": table,
                "聚合键": "合并批次",
                "聚合函数": "SUM(file_count), AVG(file_size)",
                "窗口宽度": "1h",
                "输出频率": "每次合并确认后",
                "延迟更新": "实时",
                "最后写入": "2026-11-18 10:50:00",
                "status": "聚合中",
                "pending": True,
                "abnormal": False,
            }
            metric_rows.append(metric)
        metric["确认文件数"] = len(manifest)
        metric["确认替换输入数"] = len(preview_paths)
        metric["合并任务编号"] = str(entry.get("任务编号", ""))
        results["metric_aggr"] = metric

        return results

    def _skip_reason(
        self, entry: dict[str, Any], status: str, manifest: list[dict[str, Any]]
    ) -> str:
        if status == "已取消":
            landed = len(self._landed_paths(entry))
            return (
                f"合并任务已取消：保留{landed}个已落地替换文件，"
                "未确认文件不具备过期清理资格，跳过预演与聚合"
            )
        if not manifest:
            return "合并数据文件确认清单为空，没有替换依据，跳过预演与聚合"
        return f"当前状态「{status}」未完成文件确认，跳过预演与聚合"

    def build_running_compaction(self, table: str, file_count: int) -> dict[str, Any]:
        """供上游（落盘阻塞等）创建一个待确认的合并中任务，自动携带文件计划。"""
        comp_rows = store.rows(self.MODULE)
        new_id = max((int(r.get("id", 0)) for r in comp_rows), default=0) + 1
        entry: dict[str, Any] = {
            "id": new_id,
            "任务编号": f"COMP-{new_id:04d}",
            "目标表": table,
            "文件数量": file_count,
            "合并策略": "BinPack-按大小合并",
            "触发阈值": "文件数 >= 50",
            "执行内存": "2GB",
            "合并耗时": "未执行",
            "释放空间": "未执行",
            "status": "合并中",
            "pending": True,
            "abnormal": False,
        }
        comp_rows.append(entry)
        self.ensure_file_plan(entry)
        self._refresh_confirm_state(entry)
        return entry

    def build_completed_compaction(
        self, table: str, file_count: int = 42
    ) -> dict[str, Any]:
        """供湖仓提交链路创建一份文件已全部确认的合并任务（带确认清单）。"""
        comp_rows = store.rows(self.MODULE)
        new_id = max((int(r.get("id", 0)) for r in comp_rows), default=0) + 1
        entry: dict[str, Any] = {
            "id": new_id,
            "任务编号": f"COMP-{new_id:04d}",
            "目标表": table,
            "文件数量": file_count,
            "合并策略": "BinPack-128MB",
            "触发阈值": "文件数 >= 30",
            "执行内存": "1GB",
            "合并耗时": "12.5s",
            "释放空间": "450MB",
            "status": "合并中",
            "pending": True,
            "abnormal": False,
        }
        comp_rows.append(entry)
        self.ensure_file_plan(entry)
        # 提交链路产生的合并结果视为替换文件全部落地并确认
        for plan_item in entry["替换文件计划"]:
            plan_item["已落地"] = True
        self._confirm_landed_files(entry)
        self._refresh_confirm_state(entry)
        return entry
