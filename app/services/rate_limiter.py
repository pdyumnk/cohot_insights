"""Per-user limit on documents in the active pipeline states (queued|processing|enriching).

Redis key `active:{user_id}` is a SORTED SET of the user's active document ids, scored by
the time the slot was taken. Compared with an INCR/DECR counter:

* acquire is idempotent per document (re-acquiring a member is a no-op), so a PATCH of a
  document that is already in flight never double-counts it;
* release is idempotent (ZREM), so a duplicate or out-of-order "finished" never drives the
  count below the truth;
* it can be checked against MongoDB: members whose documents are no longer active are
  pruned (reconcile), which heals any release that was lost while Redis was unreachable.

Check-and-add runs as one Lua script, so concurrent submits cannot both see "2 of 3".
If Redis is unavailable the limiter degrades to counting active documents in MongoDB.
"""

import logging
import time

from bson import ObjectId
from bson.errors import InvalidId
from redis.asyncio import Redis
from redis.exceptions import RedisError

from app.repositories.documents import DocumentRepository

logger = logging.getLogger("cohort.rate_limiter")

_ACQUIRE = """
local key, member = KEYS[1], ARGV[1]
local limit, ttl, now = tonumber(ARGV[2]), tonumber(ARGV[3]), tonumber(ARGV[4])
if redis.call('ZSCORE', key, member) then
  redis.call('EXPIRE', key, ttl)
  return 1
end
if redis.call('ZCARD', key) >= limit then
  return 0
end
redis.call('ZADD', key, now, member)
redis.call('EXPIRE', key, ttl)
return 1
"""

# A member younger than this may belong to a request that has taken its slot but not yet
# written MongoDB (POST inserts after acquiring), so reconcile leaves it alone.
RECONCILE_GRACE_S = 30
RECONCILE_INTERVAL_S = 5


class ActiveJobLimiter:
    def __init__(self, redis: Redis, repo: DocumentRepository, limit: int, ttl_s: int):
        self.redis = redis
        self.repo = repo
        self.limit = limit
        self.ttl_s = ttl_s
        self._acquire_script = redis.register_script(_ACQUIRE)

    @staticmethod
    def key(user_id: str) -> str:
        return f"active:{user_id}"

    async def acquire(self, user_id: str, document_id: ObjectId) -> bool:
        try:
            await self._seed_if_missing(user_id)
            if await self._try_acquire(user_id, document_id):
                return True
            # Full: drop members whose documents already finished (e.g. a release lost
            # during a Redis outage), then try once more.
            if await self._reconcile(user_id):
                return await self._try_acquire(user_id, document_id)
            return False
        except RedisError:
            logger.warning("redis unavailable; rate limit falls back to MongoDB count", extra={"user_id": user_id})
            active = await self.repo.count_active(user_id, exclude_id=document_id)
            return active < self.limit

    async def release(self, user_id: str, document_id: ObjectId | str) -> None:
        try:
            await self.redis.zrem(self.key(user_id), str(document_id))
        except RedisError:
            # Not fatal: the stale member is pruned by _reconcile the next time the user is at the limit.
            logger.warning(
                "redis unavailable; slot release deferred to reconcile",
                extra={"user_id": user_id, "document_id": str(document_id)},
            )

    async def _try_acquire(self, user_id: str, document_id: ObjectId) -> bool:
        allowed = await self._acquire_script(
            keys=[self.key(user_id)], args=[str(document_id), self.limit, self.ttl_s, time.time()]
        )
        return allowed == 1

    async def _seed_if_missing(self, user_id: str) -> None:
        """If the key is gone (TTL, Redis restart/flush), rebuild it from MongoDB so a lost
        key can't hand a user a fresh allowance on top of documents already in flight."""
        key = self.key(user_id)
        if await self.redis.exists(key):
            return
        active = await self.repo.active_ids(user_id)
        if active:
            await self.redis.zadd(key, {doc_id: time.time() for doc_id in active}, nx=True)
            await self.redis.expire(key, self.ttl_s)

    async def _reconcile(self, user_id: str) -> bool:
        # At most one MongoDB check per user per interval: a client hammering while at its
        # limit must not turn every 429 into a database query.
        if not await self.redis.set(f"reconcile:{user_id}", 1, nx=True, ex=RECONCILE_INTERVAL_S):
            return False
        key = self.key(user_id)
        cutoff = time.time() - RECONCILE_GRACE_S
        candidates = await self.redis.zrangebyscore(key, "-inf", cutoff)
        if not candidates:
            return False
        ids: list[ObjectId] = []
        stale: list[str] = []
        for member in candidates:
            try:
                ids.append(ObjectId(member))
            except InvalidId:
                stale.append(member)
        still_active = await self.repo.active_ids(user_id, among=ids)
        stale += [member for member in candidates if member not in still_active and member not in stale]
        if stale:
            await self.redis.zrem(key, *stale)
            logger.info("pruned stale rate-limit slots", extra={"user_id": user_id, "pruned": len(stale)})
        return bool(stale)
