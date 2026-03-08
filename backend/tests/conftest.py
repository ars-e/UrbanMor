import os
from collections.abc import Iterator

import pytest
from anyio import run
from fastapi.testclient import TestClient

os.environ.setdefault("APP_ENV", "test")

from app.db.session import dispose_async_engine
from app.main import app


@pytest.fixture()
def client() -> Iterator[TestClient]:
    with TestClient(app) as test_client:
        yield test_client
    run(dispose_async_engine)
