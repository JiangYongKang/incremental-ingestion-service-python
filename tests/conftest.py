"""pytest 公共夹具：独立数据目录、TestClient、日志打印。"""

from __future__ import annotations

import logging
import tempfile
from collections.abc import Iterator

import pytest
from fastapi.testclient import TestClient

from app.api import create_app


@pytest.fixture
def client() -> Iterator[TestClient]:
    data_dir = tempfile.mkdtemp(prefix="ingest-test-")
    app = create_app(data_dir)
    # 测试日志直接输出到控制台（pytest -s 可见），包含事件标识/位点/判定依据
    logging.disable(logging.NOTSET)
    with TestClient(app) as c:
        c.headers.update({"x-test-data-dir": data_dir})
        yield c
