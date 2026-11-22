"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

from app.services.base import BaseModuleService


class ExpireSnapService(BaseModuleService):
    MODULE = "expire_snap"
    ENTITY = "过期策略"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["策略编号", "目标表", "保留数量", "保留天数", "清理模式", "预演结果", "预演清理路径", "下次执行", "上次清理"]
    REQUIRED_FIELDS = ["策略编号", "目标表", "保留数量"]
    STATUS_ORDER = ["草稿", "生效中", "预演完成", "已停用"]
    ACTION_RULES = {"启效策略": "生效中", "预演清理": "预演完成", "停用策略": "已停用"}
    NEGATIVE_ACTIONS = ["停用策略"]
