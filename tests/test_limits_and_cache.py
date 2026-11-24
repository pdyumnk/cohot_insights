"""Per-user active-job limit and the content cache, including Redis-outage fallbacks."""

import time

from bson import ObjectId
from conftest import TEST_SETTINGS, drain, get_doc, hdr, submit
from redis.asyncio import Redis
from redis.asyncio.retry import Retry
from redis.backoff import NoBackoff

from app.domain import Stage, hash_content, utcnow
from app.services.content_cache import ContentCache
from app.services.rate_limiter import RECONCILE_GRACE_S, ActiveJobLimiter


def dead_redis() -> Redis:
    return Redis(host="127.0.0.1", port=1, socket_connect_timeout=0.2, socket_timeout=0.2, retry=Retry(NoBackoff(), 0))


# ------------------------------------------------------------------ rate limit


async def test_fourth_active_document_gets_429(api, container):
    codes = [(await submit(api, "pat", f"document {i}")).status_code for i in range(4)]
    assert codes == [201, 201, 201, 429]
    assert "Retry-After" in (await submit(api, "pat", "document 5")).headers
    # Another user is unaffected.
    assert (await submit(api, "quinn", "document 0")).status_code == 201


async def test_limit_spans_all_active_stages_and_frees_on_completion(api, container):
    for i in range(3):
        await submit(api, "rex", f"rex doc {i}")
    worker = container.worker("w")
    await worker.run_once()  # enriching first (none), so this runs processing for one doc
    docs = await container.resources.documents.find({"user_id": "rex"}).to_list(None)
    assert sorted(d["status"] for d in docs) == ["enriching", "queued", "queued"]
    assert (await submit(api, "rex", "rex doc 3")).status_code == 429  # enriching still counts

    await worker.run_once()  # enriching -> completed frees one slot
    assert (await submit(api, "rex", "rex doc 3")).status_code == 201
    assert (await submit(api, "rex", "rex doc 4")).status_code == 429


async def test_patching_an_active_document_does_not_take_a_second_slot(api, container):
    ids = [(await submit(api, "sam", f"sam doc {i}")).json()["document_id"] for i in range(3)]
    response = await api.patch(f"/documents/{ids[0]}", json={"content": "sam doc 0 edited"}, headers=hdr("sam"))
    assert response.status_code == 200


async def test_patching_a_completed_document_needs_a_free_slot(api, container):
    done_id = (await submit(api, "tia", "tia finished")).json()["document_id"]
    await drain(container)
    for i in range(3):
        await submit(api, "tia", f"tia active {i}")
    response = await api.patch(f"/documents/{done_id}", json={"content": "tia reopened"}, headers=hdr("tia"))
    assert response.status_code == 429
    assert (await get_doc(api, "tia", done_id))["content"] == "tia finished"  # untouched


async def test_lost_redis_state_is_rebuilt_from_mongo(api, container):
    for i in range(3):
        await submit(api, "uma", f"uma doc {i}")
    await container.resources.redis.flushdb()  # e.g. Redis restarted without persistence
    assert (await submit(api, "uma", "uma doc 3")).status_code == 429


async def test_stale_slot_from_lost_release_is_pruned(api, container):
    ids = [(await submit(api, "vic", f"vic doc {i}")).json()["document_id"] for i in range(3)]
    # Simulate: doc 0 finished while Redis was down, so its ZREM never happened.
    await container.resources.documents.update_one({"_id": ObjectId(ids[0])}, {"$set": {"status": "completed"}})
    old = time.time() - RECONCILE_GRACE_S - 1
    await container.resources.redis.zadd(ActiveJobLimiter.key("vic"), {i: old for i in ids})
    assert (await submit(api, "vic", "vic doc 3")).status_code == 201
    assert (await submit(api, "vic", "vic doc 4")).status_code == 429


async def test_limiter_falls_back_to_mongo_when_redis_is_down(api, container):
    for i in range(3):
        await submit(api, "wes", f"wes doc {i}")
    limiter = ActiveJobLimiter(dead_redis(), container.repo, limit=3, ttl_s=60)
    assert await limiter.acquire("wes", ObjectId()) is False
    assert await limiter.acquire("new-user", ObjectId()) is True
    await limiter.release("wes", ObjectId())  # logs, does not raise


# ------------------------------------------------------------------ content cache


async def test_identical_content_returns_cached_result_immediately(api, container):
    first = (await submit(api, "xia", "cache this exact text about galaxies")).json()
    await drain(container)
    original = await get_doc(api, "xia", first["document_id"])

    second = await submit(api, "yan", "cache this exact text about galaxies")
    assert second.status_code == 201 and second.json()["status"] == "completed"
    cached = await get_doc(api, "yan", second.json()["document_id"])
    assert cached["result"]["source"] == "cache" and cached["result_is_current"]
    assert cached["result"]["summary"] == original["result"]["summary"]
    assert cached["result"]["tags"] == original["result"]["tags"]
    assert await drain(container) == 0  # nothing was queued
    # a cache hit does not consume a rate-limit slot
    assert await container.resources.redis.zcard(ActiveJobLimiter.key("yan")) == 0


async def test_cache_is_keyed_by_content_not_document(api, container):
    document_id = (await submit(api, "zed", "first content about deserts")).json()["document_id"]
    await drain(container)
    await api.patch(f"/documents/{document_id}", json={"content": "second content about oceans"}, headers=hdr("zed"))
    await drain(container)

    redis = container.resources.redis
    old = await redis.get(ContentCache.key(hash_content("first content about deserts")))
    new = await redis.get(ContentCache.key(hash_content("second content about oceans")))
    assert "deserts" in old and "oceans" not in old
    assert "oceans" in new and "deserts" not in new
    assert not any(document_id in key for key in await redis.keys("*"))

    # PATCHing back to the first content is served from its own cache entry, at the NEW version.
    back = await api.patch(f"/documents/{document_id}", json={"content": "first content about deserts"}, headers=hdr("zed"))
    body = back.json()
    assert body["status"] == "completed" and body["content_version"] == 3
    assert body["result"]["content_version"] == 3 and "deserts" in body["result"]["summary"]


async def test_cache_hit_patch_of_in_flight_document_voids_the_old_run(api, container):
    await submit(api, "amy", "known content about volcanoes")
    await drain(container)
    document_id = (await submit(api, "amy", "in flight content")).json()["document_id"]
    now = utcnow()
    claimed = await container.repo.claim(Stage.PROCESSING, "tok", now, now + container.worker("w")._lease())
    await api.patch(f"/documents/{document_id}", json={"content": "known content about volcanoes"}, headers=hdr("amy"))
    await container.worker("w").run_stage(Stage.PROCESSING, claimed, "tok")
    body = await get_doc(api, "amy", document_id)
    assert body["status"] == "completed" and body["content_version"] == 2
    assert "volcanoes" in body["result"]["summary"]
    assert await container.resources.redis.zcard(ActiveJobLimiter.key("amy")) == 0  # slot freed


async def test_cache_falls_back_to_mongo_when_redis_is_down_or_evicted(api, container):
    await submit(api, "bea", "durable result text")
    await drain(container)
    content_hash = hash_content("durable result text")

    outage = ContentCache(dead_redis(), container.repo, ttl_s=60)
    assert (await outage.get(content_hash))["summary"] == "durable result text"

    await container.resources.redis.flushdb()  # TTL expiry / eviction
    hit = await submit(api, "cy", "durable result text")
    assert hit.json()["status"] == "completed"
    assert await container.resources.redis.exists(ContentCache.key(content_hash))  # refilled


async def test_cache_entries_have_a_ttl(api, container):
    await submit(api, "dov", "ttl check text")
    await drain(container)
    ttl = await container.resources.redis.ttl(ContentCache.key(hash_content("ttl check text")))
    assert 0 < ttl <= TEST_SETTINGS.cache_ttl_s


async def test_reconcile_is_throttled_per_user(api, container, monkeypatch):
    for i in range(3):
        await submit(api, "hog", f"hog doc {i}")
    calls = 0
    original = container.repo.active_ids

    async def counting(*args, **kwargs):
        nonlocal calls
        calls += 1
        return await original(*args, **kwargs)

    old = time.time() - RECONCILE_GRACE_S - 1
    members = await container.resources.redis.zrange(ActiveJobLimiter.key("hog"), 0, -1)
    await container.resources.redis.zadd(ActiveJobLimiter.key("hog"), {m: old for m in members})
    monkeypatch.setattr(container.repo, "active_ids", counting)
    codes = [(await submit(api, "hog", f"hammer {i}")).status_code for i in range(20)]
    assert set(codes) == {429}
    assert calls == 1  # one MongoDB check, not twenty
