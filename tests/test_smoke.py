"""骨架冒烟测试：仅验证包可导入。"""


def test_imports() -> None:
    import edge_telemetry  # noqa: F401
    from edge_telemetry import federation, models, service, store  # noqa: F401
