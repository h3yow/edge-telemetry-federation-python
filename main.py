"""本地启动入口：装配边缘遥测联邦服务。

运行：uv run python main.py  （默认 127.0.0.1:8000）
"""

from __future__ import annotations

import uvicorn

from edge_telemetry.api import create_app
from edge_telemetry.models import SeriesPolicy, StorePolicy
from edge_telemetry.service import TelemetryService

service = TelemetryService()
# 示例：两个集群。c1 演示可配置的乱序窗口与采样率策略。
service.register_cluster(
    "cluster-a",
    StorePolicy(default=SeriesPolicy(
        reorder_delay_ms=30_000, future_skew_ms=5_000,
        min_sample_interval_ms=0)),
)
service.register_cluster("cluster-b")

app = create_app(service)


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
