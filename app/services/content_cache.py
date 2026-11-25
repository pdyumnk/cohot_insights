"""Content-addressed result cache.

Key: `cache:v1:{content_hash}` -> {"summary", "tags"}, with a TTL. The key is a function of
the content only — never of a document_id — so a PATCH (new content, new hash) can never
pick up the old content's entry, and two documents with identical content share one.
`v1` names the version of text_digest: changing its algorithm bumps it, which
invalidates every entry at once without a flush.

Redis is a hot layer, MongoDB is the source of truth: on a Redis miss or outage we look for
any document whose published `result` has this hash (index `result_content_hash`). That
lookup is safe because a published result carries the hash of the content it came from.
"""

import json
import logging

from redis.asyncio import Redis
from redis.exceptions import RedisError

from app.repositories.documents import DocumentRepository

logger = logging.getLogger("cohort.cache")

CACHE_NAMESPACE = "cache:v1"


class ContentCache:
    def __init__(self, redis: Redis, repo: DocumentRepository, ttl_s: int):
        self.redis = redis
        self.repo = repo
        self.ttl_s = ttl_s

    @staticmethod
    def key(content_hash: str) -> str:
        return f"{CACHE_NAMESPACE}:{content_hash}"

    async def get(self, content_hash: str) -> dict | None:
        redis_ok = True
        try:
            raw = await self.redis.get(self.key(content_hash))
            if raw:
                return json.loads(raw)
        except RedisError:
            redis_ok = False
            logger.warning("redis unavailable; cache lookup falls back to MongoDB")
        found = await self.repo.find_result_by_hash(content_hash)
        if found is None:
            return None
        value = {"summary": found["summary"], "tags": found["tags"]}
        if redis_ok:
            await self.set(content_hash, value["summary"], value["tags"])  # read-through refill
        return value

    async def set(self, content_hash: str, summary: str, tags: list[str]) -> None:
        try:
            await self.redis.set(
                self.key(content_hash), json.dumps({"summary": summary, "tags": tags}), ex=self.ttl_s
            )
        except RedisError:
            logger.warning("redis unavailable; result not cached", extra={"content_hash": content_hash})
