"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

from app.services.base import BaseModuleService


class MetricAggrService(BaseModuleService):
    MODULE = "metric_aggr"
    ENTITY = "聚合窗口"
    OPERATION_TERM = "聚合"
    LIST_FIELDS = ["窗口编号", "数据流", "聚合键", "聚合函数", "窗口宽度", "输出频率", "延迟更新", "最后写入", "确认文件数", "确认替换输入数"]
    REQUIRED_FIELDS = ["窗口编号", "数据流", "聚合键"]
    STATUS_ORDER = ["草稿", "聚合中", "窗口关闭", "已停止"]
    ACTION_RULES = {"启动聚合": "聚合中", "关闭窗口": "窗口关闭", "停止聚合": "已停止"}
    NEGATIVE_ACTIONS = []
