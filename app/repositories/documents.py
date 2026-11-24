"""All MongoDB access for the `documents` collection.

Every write the pipeline makes is *guarded*: its filter includes the `content_version` and
the `lease_token` the worker claimed. A PATCH replaces the version and clears the token in
the same atomic write, so a result computed for older content can never land.
"""

from datetime import datetime
from typing import Any

from bson import ObjectId
from pymongo import ReturnDocument
from pymongo.asynchronous.collection import AsyncCollection

from app.domain import ACTIVE_STATUSES, Stage, StageState, Status, fresh_stages


def ref_filter(client_doc_ref: str) -> dict[str, Any]:
    # `$type: "string"` makes the predicate provably inside the partial index's filter
    # expression; a bare equality would NOT be eligible for the partial index.
    return {"client_doc_ref": {"$eq": client_doc_ref, "$type": "string"}}


def _guard(doc: dict[str, Any], token: str) -> dict[str, Any]:
    return {"_id": doc["_id"], "content_version": doc["content_version"], "lease_token": token}


class DocumentRepository:
    def __init__(self, collection: AsyncCollection):
        self.collection = collection

    # ---------------------------------------------------------------- reads (API)

    async def insert(self, document: dict[str, Any]) -> None:
        await self.collection.insert_one(document)

    async def get_owned(self, document_id: ObjectId, user_id: str) -> dict[str, Any] | None:
        # Ownership is part of the query itself: a foreign id is indistinguishable from a missing one.
        return await self.collection.find_one({"_id": document_id, "user_id": user_id})

    async def get_owned_by_ref(self, client_doc_ref: str, user_id: str) -> dict[str, Any] | None:
        return await self.collection.find_one({**ref_filter(client_doc_ref), "user_id": user_id})

    async def find_by_ref(self, client_doc_ref: str) -> dict[str, Any] | None:
        return await self.collection.find_one(ref_filter(client_doc_ref))

    async def list_for_user(
        self, user_id: str, status: str | None, skip: int, limit: int
    ) -> list[dict[str, Any]]:
        query: dict[str, Any] = {"user_id": user_id}
        if status is not None:
            query["status"] = status
        cursor = (
            self.collection.find(query, {"content": 0})
            .sort([("created_at", -1), ("_id", -1)])
            .skip(skip)
            .limit(limit)
        )
        return await cursor.to_list(length=limit)

    async def find_result_by_hash(self, content_hash: str) -> dict[str, Any] | None:
        doc = await self.collection.find_one(
            {"result.content_hash": content_hash}, {"result.summary": 1, "result.tags": 1}
        )
        return doc["result"] if doc else None

    # ---------------------------------------------------------------- rate-limit support

    async def count_active(self, user_id: str, exclude_id: ObjectId | None = None) -> int:
        query: dict[str, Any] = {"user_id": user_id, "status": {"$in": list(ACTIVE_STATUSES)}}
        if exclude_id is not None:
            query["_id"] = {"$ne": exclude_id}
        return await self.collection.count_documents(query)

    async def active_ids(self, user_id: str, among: list[ObjectId] | None = None) -> set[str]:
        query: dict[str, Any] = {"user_id": user_id, "status": {"$in": list(ACTIVE_STATUSES)}}
        if among is not None:
            query["_id"] = {"$in": among}
        cursor = self.collection.find(query, {"_id": 1})
        return {str(doc["_id"]) for doc in await cursor.to_list(length=None)}

    # ---------------------------------------------------------------- writes (API)

    async def replace_content(
        self, document_id: ObjectId, user_id: str, expected_version: int, fields: dict[str, Any]
    ) -> dict[str, Any] | None:
        """Atomic PATCH. Matches only if nobody changed the content since `expected_version`."""
        return await self.collection.find_one_and_update(
            {"_id": document_id, "user_id": user_id, "content_version": expected_version},
            {"$set": fields},
            return_document=ReturnDocument.AFTER,
        )

    async def requeue_failed(
        self, doc: dict[str, Any], stage: Stage, now: datetime
    ) -> dict[str, Any] | None:
        """Resume a failed document at the stage that failed, keeping earlier stage output."""
        if stage is Stage.PROCESSING:
            update = {"status": Status.QUEUED.value, "stages": fresh_stages()}
        else:
            update = {
                "status": Status.ENRICHING.value,
                "stages.enriching.state": StageState.PENDING.value,
                "stages.enriching.attempts": 0,
                "stages.enriching.error": None,
                "stages.enriching.started_at": None,
                "stages.enriching.finished_at": None,
            }
        return await self.collection.find_one_and_update(
            {
                "_id": doc["_id"],
                "content_version": doc["content_version"],
                "status": Status.FAILED.value,
                f"stages.{stage}.state": StageState.FAILED.value,
            },
            {"$set": {**update, "lease_token": None, "lease_until": None, "updated_at": now}},
            return_document=ReturnDocument.AFTER,
        )

    # ---------------------------------------------------------------- pipeline

    async def claim(self, stage: Stage, token: str, now: datetime, lease_until: datetime):
        """Atomically claim one job for `stage`.

        A job is claimable when it is waiting for the stage, or when a previous worker's
        lease on it expired (crash / restart). `find_one_and_update` is atomic on a single
        document, so two workers can never both win the same claim; the new `lease_token`
        fences off the previous holder's late writes.
        """
        if stage is Stage.PROCESSING:
            query = {
                "$or": [
                    {"status": Status.QUEUED.value},
                    {"status": Status.PROCESSING.value, "lease_until": {"$lt": now}},
                ]
            }
        else:
            query = {
                "status": Status.ENRICHING.value,
                "$or": [{"lease_until": None}, {"lease_until": {"$lt": now}}],
            }
        return await self.collection.find_one_and_update(
            query,
            {
                "$set": {
                    "status": stage.value,
                    "lease_token": token,
                    "lease_until": lease_until,
                    f"stages.{stage}.state": StageState.RUNNING.value,
                    f"stages.{stage}.started_at": now,
                    "updated_at": now,
                }
            },
            sort=[("updated_at", 1)],
            return_document=ReturnDocument.AFTER,
        )

    async def start_attempt(
        self, doc: dict[str, Any], stage: Stage, token: str, attempt: int, now: datetime, lease_until: datetime
    ) -> bool:
        result = await self.collection.update_one(
            _guard(doc, token),
            {
                "$set": {
                    "lease_until": lease_until,
                    f"stages.{stage}.attempts": attempt,
                    "updated_at": now,
                }
            },
        )
        return result.matched_count == 1

    async def record_attempt_error(
        self, doc: dict[str, Any], stage: Stage, token: str, error: str, now: datetime
    ) -> bool:
        result = await self.collection.update_one(
            _guard(doc, token), {"$set": {f"stages.{stage}.error": error, "updated_at": now}}
        )
        return result.matched_count == 1

    async def fail_stage(
        self, doc: dict[str, Any], stage: Stage, token: str, error: str, now: datetime
    ) -> bool:
        result = await self.collection.update_one(
            _guard(doc, token),
            {
                "$set": {
                    "status": Status.FAILED.value,
                    f"stages.{stage}.state": StageState.FAILED.value,
                    f"stages.{stage}.error": error,
                    f"stages.{stage}.finished_at": now,
                    "lease_token": None,
                    "lease_until": None,
                    "updated_at": now,
                }
            },
        )
        return result.matched_count == 1

    async def complete_processing(
        self, doc: dict[str, Any], token: str, summary: str, now: datetime
    ) -> bool:
        result = await self.collection.update_one(
            _guard(doc, token),
            {
                "$set": {
                    "status": Status.ENRICHING.value,
                    "stages.processing.state": StageState.COMPLETED.value,
                    "stages.processing.content_version": doc["content_version"],
                    "stages.processing.summary": summary,
                    "stages.processing.error": None,
                    "stages.processing.finished_at": now,
                    "lease_token": None,
                    "lease_until": None,
                    "updated_at": now,
                }
            },
        )
        return result.matched_count == 1

    async def complete_enriching(
        self, doc: dict[str, Any], token: str, published: dict[str, Any], now: datetime
    ) -> bool:
        """Publish summary + tags together as one `result` sub-document, in one write."""
        result = await self.collection.update_one(
            _guard(doc, token),
            {
                "$set": {
                    "status": Status.COMPLETED.value,
                    "stages.enriching.state": StageState.COMPLETED.value,
                    "stages.enriching.content_version": doc["content_version"],
                    "stages.enriching.error": None,
                    "stages.enriching.finished_at": now,
                    "result": published,
                    "lease_token": None,
                    "lease_until": None,
                    "updated_at": now,
                }
            },
        )
        return result.matched_count == 1
