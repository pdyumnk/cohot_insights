"""Integration-test fixtures: real MongoDB + Redis (see README "Tests"), app driven in-process
via httpx.AsyncClient + ASGITransport. Workers are NOT started automatically; tests advance the
pipeline explicitly with `drain()` / `worker.run_once()` so outcomes are deterministic."""

import os

import httpx
import pytest

from app.core.config import Settings
from app.main import create_app

TEST_SETTINGS = Settings(
    mongo_url=os.getenv("TEST_MONGO_URL", "mongodb://localhost:27018"),
    mongo_db=os.getenv("TEST_MONGO_DB", "cohort_insights_test"),
    redis_url=os.getenv("TEST_REDIS_URL", "redis://localhost:6380/15"),
    log_level="WARNING",
    embedded_workers=False,
    stage_time_scale=0.001,  # stage durations become 5-20 ms
    failure_rate=0.0,
    retry_base_delay_s=0.0,
    worker_poll_interval_s=0.01,
)


@pytest.fixture(scope="session")
async def app():
    application = create_app(TEST_SETTINGS)
    async with application.router.lifespan_context(application):
        yield application


@pytest.fixture(scope="session")
async def api(app):
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


@pytest.fixture
async def container(app):
    c = app.state.container
    await c.resources.documents.delete_many({})
    await c.resources.redis.flushdb()
    return c


def hdr(user: str) -> dict[str, str]:
    return {"X-User-Id": user}


async def submit(api, user: str, content: str, ref: str | None = None, title: str = "doc"):
    body = {"user_id": user, "title": title, "content": content}
    if ref is not None:
        body["client_doc_ref"] = ref
    return await api.post("/documents", json=body, headers=hdr(user))


async def get_doc(api, user: str, document_id: str) -> dict:
    response = await api.get(f"/documents/{document_id}", headers=hdr(user))
    assert response.status_code == 200, response.text
    return response.json()


async def drain(container, worker=None, max_steps: int = 50) -> int:
    """Run pipeline stages until nothing is claimable. Returns the number of stages run."""
    worker = worker or container.worker("test")
    for step in range(max_steps):
        if not await worker.run_once():
            return step
    raise AssertionError("pipeline did not drain")


def assert_no_mixed_version(body: dict) -> None:
    """The core staleness invariant, as seen by a reader."""
    result, stale = body["result"], body["stale_result"]
    assert not (result and stale)
    if result is not None:
        assert body["result_is_current"] is True
        assert result["content_version"] == body["content_version"]
        assert result["content_hash"] == body["content_hash"]
        assert body["status"] == "completed"
    else:
        assert body["result_is_current"] is False
    if stale is not None:
        assert stale["content_version"] < body["content_version"]
