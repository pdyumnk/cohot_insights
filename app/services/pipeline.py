"""Two-stage simulated pipeline: processing (summary) -> enriching (tags).

Workers poll MongoDB and claim one stage of one document at a time with an atomic
`find_one_and_update` that hands out a lease (expiry) and a fencing token. Every later
write by that worker is filtered on (content_version, lease_token), so:

* two workers can never run the same stage of the same document concurrently with both
  results landing — the claim is atomic, and an expired-and-reclaimed lease changes the token;
* a PATCH (which bumps content_version and clears the token in one write) silently voids
  whatever an in-flight worker computes for the old content;
* a crashed worker's job becomes claimable again when its lease expires — no startup reset
  needed, so any number of worker processes can run.

Each stage retries with exponential backoff (+ jitter) up to `max_stage_attempts`; only then
is the document marked failed, with the failure recorded on that stage. A failure at
enriching leaves the processing summary in place, and resuming (retry endpoint, or lease
expiry) runs enriching only.
"""

import asyncio
import logging
import random
import uuid
from datetime import timedelta
from typing import Any

from pymongo.errors import PyMongoError

from app.core.config import Settings
from app.domain import Stage, build_result, utcnow
from app.repositories.documents import DocumentRepository
from app.services import mock_ai
from app.services.content_cache import ContentCache
from app.services.rate_limiter import ActiveJobLimiter

logger = logging.getLogger("cohort.pipeline")

STAGE_SECONDS: dict[Stage, tuple[float, float]] = {
    Stage.PROCESSING: (10, 20),
    Stage.ENRICHING: (5, 15),
}


class PipelineWorker:
    def __init__(
        self,
        repo: DocumentRepository,
        limiter: ActiveJobLimiter,
        cache: ContentCache,
        settings: Settings,
        name: str = "worker",
    ):
        self.repo = repo
        self.limiter = limiter
        self.cache = cache
        self.settings = settings
        self.name = name

    # ------------------------------------------------------------------ loop

    async def run_forever(self) -> None:
        logger.info("worker started", extra={"worker": self.name})
        while True:
            try:
                if not await self.run_once():
                    await asyncio.sleep(self.settings.worker_poll_interval_s)
            except PyMongoError:
                logger.exception("worker database error; backing off", extra={"worker": self.name})
                await asyncio.sleep(1)

    async def run_once(self) -> bool:
        """Claim and run one stage. Enriching first: finishing started work frees slots sooner."""
        for stage in (Stage.ENRICHING, Stage.PROCESSING):
            token = uuid.uuid4().hex
            now = utcnow()
            doc = await self.repo.claim(stage, token, now, now + self._lease())
            if doc is not None:
                await self.run_stage(stage, doc, token)
                return True
        return False

    # ------------------------------------------------------------------ one stage

    async def run_stage(self, stage: Stage, doc: dict[str, Any], token: str) -> None:
        log = {"worker": self.name, "document_id": str(doc["_id"]), "stage": stage.value,
               "content_version": doc["content_version"]}

        if stage is Stage.ENRICHING:
            processing = doc["stages"]["processing"]
            # Invariant (guaranteed by PATCH resetting both stages atomically): the summary
            # we enrich was produced from this exact content version.
            if processing["content_version"] != doc["content_version"] or processing["summary"] is None:
                logger.error("enriching claimed without a current summary", extra=log)
                await self._fail(stage, doc, token, "no summary for the current content version", log)
                return

        first_attempt = doc["stages"][stage]["attempts"] + 1  # continues the count after a lease takeover
        for attempt in range(first_attempt, self.settings.max_stage_attempts + 1):
            now = utcnow()
            if not await self.repo.start_attempt(doc, stage, token, attempt, now, now + self._lease()):
                logger.info("lease lost or content changed; abandoning stage", extra=log)
                return

            low, high = STAGE_SECONDS[stage]
            await asyncio.sleep(random.uniform(low, high) * self.settings.stage_time_scale)

            if random.random() < self.settings.failure_rate:
                error = f"simulated {stage.value} failure (attempt {attempt})"
                logger.warning("stage attempt failed", extra={**log, "attempt": attempt})
                if attempt >= self.settings.max_stage_attempts:
                    break
                if not await self.repo.record_attempt_error(doc, stage, token, error, utcnow()):
                    logger.info("content changed during backoff; abandoning stage", extra=log)
                    return
                await asyncio.sleep(self._backoff(attempt))
                continue

            await self._succeed(stage, doc, token, log)
            return

        await self._fail(stage, doc, token, f"{stage.value} failed after {self.settings.max_stage_attempts} attempts", log)

    async def _succeed(self, stage: Stage, doc: dict[str, Any], token: str, log: dict) -> None:
        now = utcnow()
        if stage is Stage.PROCESSING:
            summary = mock_ai.summarize(doc["content"])
            if await self.repo.complete_processing(doc, token, summary, now):
                logger.info("processing completed", extra=log)
            else:
                logger.info("dropped processing result for superseded content", extra=log)
            return

        summary = doc["stages"]["processing"]["summary"]
        tags = mock_ai.extract_tags(summary)
        published = build_result(
            version=doc["content_version"], content_hash=doc["content_hash"],
            summary=summary, tags=tags, source="pipeline", now=now,
        )
        if await self.repo.complete_enriching(doc, token, published, now):
            logger.info("document completed", extra=log)
            await self.limiter.release(doc["user_id"], doc["_id"])
            await self.cache.set(doc["content_hash"], summary, tags)
        else:
            logger.info("dropped enriching result for superseded content", extra=log)

    async def _fail(self, stage: Stage, doc: dict[str, Any], token: str, error: str, log: dict) -> None:
        if await self.repo.fail_stage(doc, stage, token, error, utcnow()):
            logger.warning("document failed", extra={**log, "error": error})
            await self.limiter.release(doc["user_id"], doc["_id"])

    # ------------------------------------------------------------------ timing

    def _lease(self) -> timedelta:
        return timedelta(seconds=self.settings.stage_lease_s)

    def _backoff(self, attempt: int) -> float:
        base = self.settings.retry_base_delay_s * (2 ** (attempt - 1))
        return base * random.uniform(0.5, 1.5) * self.settings.stage_time_scale


async def run_workers(worker_factory, count: int) -> list[asyncio.Task]:
    return [asyncio.create_task(worker_factory(f"worker-{i}").run_forever(), name=f"worker-{i}") for i in range(count)]
