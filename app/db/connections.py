"""MongoDB / Redis clients and index management."""

from dataclasses import dataclass

from pymongo import ASCENDING, DESCENDING, AsyncMongoClient, IndexModel
from pymongo.asynchronous.collection import AsyncCollection
from pymongo.asynchronous.database import AsyncDatabase
from redis.asyncio import Redis
from redis.asyncio.retry import Retry
from redis.backoff import ExponentialBackoff

from app.core.config import Settings

DOCUMENTS = "documents"

# Every index the service relies on, and the query that needs it (see README "Indexes").
DOCUMENT_INDEXES = [
    # GET /users/{id}/documents (newest first), with and without a status filter;
    # also the Mongo fallback count for the rate limiter.
    IndexModel([("user_id", ASCENDING), ("created_at", DESCENDING), ("_id", DESCENDING)], name="user_created"),
    IndexModel(
        [("user_id", ASCENDING), ("status", ASCENDING), ("created_at", DESCENDING), ("_id", DESCENDING)],
        name="user_status_created",
    ),
    # Crosswalk: GET /documents/by-ref/{ref}. Unique only when the field is a string, so the
    # many documents without a ref never collide.
    IndexModel(
        [("client_doc_ref", ASCENDING)],
        name="client_doc_ref_unique",
        unique=True,
        partialFilterExpression={"client_doc_ref": {"$type": "string"}},
    ),
    # Content cache fallback when Redis is down: find any published result for this hash.
    IndexModel(
        [("result.content_hash", ASCENDING)],
        name="result_content_hash",
        partialFilterExpression={"result.content_hash": {"$type": "string"}},
    ),
    # Workers claiming jobs oldest-first: every $or branch of the claim query is a status
    # equality, so each branch walks this index in updated_at order (SORT_MERGE, no blocking sort).
    IndexModel([("status", ASCENDING), ("updated_at", ASCENDING)], name="status_updated"),
]


@dataclass
class Resources:
    mongo: AsyncMongoClient
    db: AsyncDatabase
    redis: Redis

    @property
    def documents(self) -> AsyncCollection:
        return self.db[DOCUMENTS]

    async def close(self) -> None:
        await self.redis.aclose()
        await self.mongo.close()


def connect(settings: Settings) -> Resources:
    mongo = AsyncMongoClient(
        settings.mongo_url,
        tz_aware=True,
        serverSelectionTimeoutMS=settings.mongo_timeout_ms,
    )
    redis = Redis.from_url(
        settings.redis_url,
        decode_responses=True,
        socket_timeout=settings.redis_timeout_s,
        socket_connect_timeout=settings.redis_timeout_s,
        # Fail fast: every Redis use has a MongoDB fallback, so a long retry loop would only
        # turn a Redis outage into API latency. (redis-py's default is 10 retries.)
        retry=Retry(ExponentialBackoff(cap=0.05, base=0.01), retries=1),
    )
    return Resources(mongo=mongo, db=mongo[settings.mongo_db], redis=redis)


async def ensure_indexes(resources: Resources) -> None:
    await resources.documents.create_indexes(DOCUMENT_INDEXES)
