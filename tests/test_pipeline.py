"""Two-stage pipeline: failures, retries, claims and leases."""

import asyncio
from datetime import timedelta

from conftest import TEST_SETTINGS, drain, get_doc, hdr, submit

from app.domain import Stage, utcnow
from app.services import pipeline


def worker_with(container, **overrides):
    worker = container.worker("custom")
    worker.settings = TEST_SETTINGS.model_copy(update=overrides)
    return worker


async def test_enriching_failure_keeps_summary_and_retry_skips_processing(api, container):
    document_id = (await submit(api, "ann", "enrichment will fail for this content")).json()["document_id"]
    await container.worker("ok").run_once()  # processing succeeds
    after_processing = await get_doc(api, "ann", document_id)
    assert after_processing["status"] == "enriching"

    await worker_with(container, failure_rate=1.0).run_once()  # enriching fails every attempt
    failed = await get_doc(api, "ann", document_id)
    assert failed["status"] == "failed" and failed["failed_stage"] == "enriching"
    assert failed["stages"]["enriching"]["state"] == "failed"
    assert failed["stages"]["enriching"]["attempts"] == TEST_SETTINGS.max_stage_attempts
    assert "enriching failed" in failed["stages"]["enriching"]["error"]
    assert failed["stages"]["processing"]["state"] == "completed"  # partial progress preserved
    assert failed["result"] is None
    stored = await container.resources.documents.find_one({"user_id": "ann"})
    assert stored["stages"]["processing"]["summary"]

    retried = await api.post(f"/documents/{document_id}/retry", headers=hdr("ann"))
    assert retried.status_code == 202
    assert retried.json()["status"] == "enriching"  # resumes at the failed stage

    assert await drain(container) == 1  # only enriching ran
    done = await get_doc(api, "ann", document_id)
    assert done["status"] == "completed" and done["result"]["content_version"] == 1
    assert done["stages"]["processing"]["finished_at"] == after_processing["stages"]["processing"]["finished_at"]
    assert done["stages"]["processing"]["attempts"] == after_processing["stages"]["processing"]["attempts"]


async def test_processing_failure_reports_failed_stage(api, container):
    document_id = (await submit(api, "ben", "processing will fail")).json()["document_id"]
    await worker_with(container, failure_rate=1.0).run_once()
    body = await get_doc(api, "ben", document_id)
    assert body["status"] == "failed" and body["failed_stage"] == "processing"
    assert body["stages"]["enriching"]["state"] == "pending"
    # retrying a non-failed document is a conflict
    await api.post(f"/documents/{document_id}/retry", headers=hdr("ben"))
    again = await api.post(f"/documents/{document_id}/retry", headers=hdr("ben"))
    assert again.status_code == 409


async def test_stage_retries_with_backoff_then_succeeds(api, container, monkeypatch):
    document_id = (await submit(api, "cal", "flaky first attempt")).json()["document_id"]
    rolls = iter([0.0, 0.99])  # attempt 1 fails (< failure_rate), attempt 2 succeeds
    monkeypatch.setattr(pipeline.random, "random", lambda: next(rolls))
    await worker_with(container, failure_rate=0.5).run_once()
    body = await get_doc(api, "cal", document_id)
    assert body["status"] == "enriching"
    assert body["stages"]["processing"]["attempts"] == 2
    assert body["stages"]["processing"]["state"] == "completed" and body["stages"]["processing"]["error"] is None


async def test_two_workers_cannot_claim_the_same_stage(api, container):
    await submit(api, "dee", "only one job")
    now = utcnow()
    lease = now + timedelta(seconds=60)
    claims = await asyncio.gather(*[container.repo.claim(Stage.PROCESSING, f"t{i}", now, lease) for i in range(5)])
    assert sum(c is not None for c in claims) == 1


async def test_expired_lease_is_reclaimed_and_old_holder_is_fenced(api, container):
    document_id = (await submit(api, "eli", "worker crashes mid-stage")).json()["document_id"]
    now = utcnow()
    crashed = await container.repo.claim(Stage.PROCESSING, "dead-worker", now, now - timedelta(seconds=1))
    assert crashed is not None

    # A second worker picks it up because the lease has expired, and completes it.
    assert await drain(container) == 2
    assert (await get_doc(api, "eli", document_id))["status"] == "completed"

    # The "crashed" worker wakes up and tries to write: its token no longer matches.
    assert await container.repo.complete_processing(crashed, "dead-worker", "zombie summary", utcnow()) is False
    assert "zombie" not in (await get_doc(api, "eli", document_id))["result"]["summary"]


async def test_live_lease_is_not_stolen(api, container):
    await submit(api, "fin", "long running stage")
    now = utcnow()
    assert await container.repo.claim(Stage.PROCESSING, "holder", now, now + timedelta(seconds=60))
    assert await container.repo.claim(Stage.PROCESSING, "thief", now, now + timedelta(seconds=60)) is None
