"""Use-cases behind the HTTP endpoints. Raises domain errors; the API layer maps them to HTTP."""

import logging
from dataclasses import dataclass
from typing import Any

from bson import ObjectId
from bson.errors import InvalidId
from pymongo.errors import DuplicateKeyError

from app.domain import ACTIVE_STATUSES, Stage, Status, content_fields, failed_stage, hash_content, utcnow
from app.repositories.documents import DocumentRepository
from app.schemas.documents import DocumentCreate, DocumentUpdate
from app.services.content_cache import ContentCache
from app.services.rate_limiter import ActiveJobLimiter

logger = logging.getLogger("cohort.documents")


class DocumentNotFound(Exception):
    pass


class RateLimited(Exception):
    pass


class Forbidden(Exception):
    pass


class Conflict(Exception):
    def __init__(self, message: str, **details: Any):
        super().__init__(message)
        self.message = message
        self.details = details


@dataclass
class SubmitOutcome:
    document: dict[str, Any]
    created: bool  # False => idempotent replay of an earlier submission (same ref + same content)


def parse_object_id(document_id: str) -> ObjectId:
    try:
        return ObjectId(document_id)
    except (InvalidId, TypeError):
        # A malformed id gets the same 404 as a missing or foreign one.
        raise DocumentNotFound() from None


class DocumentService:
    def __init__(self, repo: DocumentRepository, limiter: ActiveJobLimiter, cache: ContentCache):
        self.repo = repo
        self.limiter = limiter
        self.cache = cache

    # ------------------------------------------------------------------ POST /documents

    async def submit(self, body: DocumentCreate, caller: str) -> SubmitOutcome:
        if body.user_id != caller:
            raise Forbidden("user_id must match the authenticated caller")

        content_hash = hash_content(body.content)
        if body.client_doc_ref is not None:
            existing = await self.repo.find_by_ref(body.client_doc_ref)
            if existing is not None:
                return self._repeat_ref(existing, caller, content_hash)

        document_id = ObjectId()
        cached = await self.cache.get(content_hash)
        if cached is None and not await self.limiter.acquire(caller, document_id):
            # A concurrent submission of the same ref may have taken the last slot and
            # inserted meanwhile; if so this request is a replay, not a new document.
            if body.client_doc_ref is not None:
                existing = await self.repo.find_by_ref(body.client_doc_ref)
                if existing is not None:
                    return self._repeat_ref(existing, caller, content_hash)
            raise RateLimited()

        now = utcnow()
        document = {
            "_id": document_id,
            "user_id": caller,
            "title": body.title,
            **({"client_doc_ref": body.client_doc_ref} if body.client_doc_ref is not None else {}),
            "result": None,  # replaced by content_fields on a cache hit
            **content_fields(body.content, content_hash, 1, cached, now),
            "created_at": now,
        }
        try:
            await self.repo.insert(document)
        except DuplicateKeyError:
            # Two submissions with the same new ref raced; the unique index picked a winner.
            if cached is None:
                await self.limiter.release(caller, document_id)
            existing = await self.repo.find_by_ref(body.client_doc_ref) if body.client_doc_ref else None
            if existing is None:
                raise
            return self._repeat_ref(existing, caller, content_hash)

        logger.info(
            "document submitted",
            extra={"document_id": str(document_id), "user_id": caller, "cache_hit": cached is not None},
        )
        return SubmitOutcome(document=document, created=True)

    def _repeat_ref(self, existing: dict[str, Any], caller: str, content_hash: str) -> SubmitOutcome:
        """Crosswalk policy for a client_doc_ref that is already registered.

        * same owner, same content as the document's CURRENT content -> idempotent replay (200)
        * same owner, different content -> 409; content changes must go through PATCH
        * different owner -> 409 that names no document
        """
        if existing["user_id"] != caller:
            raise Conflict("client_doc_ref is already registered")
        if existing["content_hash"] == content_hash:
            return SubmitOutcome(document=existing, created=False)
        raise Conflict(
            "client_doc_ref is already registered with different content; "
            "use PATCH /documents/{document_id} with expected_version to change it",
            document_id=str(existing["_id"]),
            content_version=existing["content_version"],
        )

    # ------------------------------------------------------------------ reads

    async def get(self, document_id: str, caller: str) -> dict[str, Any]:
        doc = await self.repo.get_owned(parse_object_id(document_id), caller)
        if doc is None:
            raise DocumentNotFound()
        return doc

    async def get_by_ref(self, client_doc_ref: str, caller: str) -> dict[str, Any]:
        doc = await self.repo.get_owned_by_ref(client_doc_ref, caller)
        if doc is None:
            raise DocumentNotFound()
        return doc

    async def list_for_user(
        self, user_id: str, caller: str, page: int, page_size: int, status: Status | None
    ) -> tuple[list[dict[str, Any]], bool]:
        if user_id != caller:
            raise DocumentNotFound()
        docs = await self.repo.list_for_user(
            user_id, status.value if status else None, skip=(page - 1) * page_size, limit=page_size + 1
        )
        return docs[:page_size], len(docs) > page_size

    # ------------------------------------------------------------------ PATCH /documents/{id}

    async def update_content(self, document_id: str, body: DocumentUpdate, caller: str) -> dict[str, Any]:
        oid = parse_object_id(document_id)
        current = await self.repo.get_owned(oid, caller)
        if current is None:
            raise DocumentNotFound()

        base_version = body.expected_version if body.expected_version is not None else current["content_version"]
        if base_version != current["content_version"]:
            raise Conflict("content_version mismatch", content_version=current["content_version"])

        content_hash = hash_content(body.content)
        if content_hash == current["content_hash"]:
            # Identical content: nothing to reprocess (makes partner retries of a PATCH idempotent).
            return current

        cached = await self.cache.get(content_hash)
        if cached is None and not await self.limiter.acquire(caller, oid):
            raise RateLimited()

        now = utcnow()
        updated = await self.repo.replace_content(
            oid, caller, base_version, content_fields(body.content, content_hash, base_version + 1, cached, now)
        )
        if updated is None:
            # Someone else changed the content first: the version moved under us.
            latest = await self.repo.get_owned(oid, caller)
            if latest is None:
                raise DocumentNotFound()
            if cached is None and latest["status"] not in ACTIVE_STATUSES:
                await self.limiter.release(caller, oid)
            raise Conflict("content_version mismatch", content_version=latest["content_version"])

        if updated["status"] not in ACTIVE_STATUSES:
            # Cache hit: completed immediately. If the old version was mid-pipeline its slot is freed here.
            await self.limiter.release(caller, oid)
        logger.info(
            "document content replaced",
            extra={
                "document_id": document_id,
                "content_version": updated["content_version"],
                "cache_hit": cached is not None,
            },
        )
        return updated

    # ------------------------------------------------------------------ POST /documents/{id}/retry

    async def retry(self, document_id: str, caller: str) -> dict[str, Any]:
        doc = await self.get(document_id, caller)
        stage = failed_stage(doc)
        if stage is None:
            raise Conflict("only a failed document can be retried", status=doc["status"])
        if not await self.limiter.acquire(caller, doc["_id"]):
            raise RateLimited()
        updated = await self.repo.requeue_failed(doc, Stage(stage), utcnow())
        if updated is None:
            latest = await self.repo.get_owned(doc["_id"], caller)
            if latest is not None and latest["status"] not in ACTIVE_STATUSES:
                await self.limiter.release(caller, doc["_id"])
            raise Conflict("document changed while retrying; re-read and try again")
        logger.info("document retry queued", extra={"document_id": document_id, "stage": stage})
        return updated
