"""PATCH semantics and the no-mixed-version guarantee."""

import asyncio

from conftest import assert_no_mixed_version, drain, get_doc, hdr, submit

from app.domain import Stage, utcnow


async def _completed(api, container, user, content):
    document_id = (await submit(api, user, content)).json()["document_id"]
    await drain(container)
    return document_id


async def test_patch_marks_old_result_stale_and_reprocesses(api, container):
    document_id = await _completed(api, container, "bob", "original content about apples and orchards")
    v1 = await get_doc(api, "bob", document_id)

    patched = await api.patch(
        f"/documents/{document_id}", json={"content": "fresh content about oranges and groves"}, headers=hdr("bob")
    )
    assert patched.status_code == 200
    body = patched.json()
    assert body["status"] == "queued" and body["content_version"] == 2
    assert body["content_hash"] != v1["content_hash"]
    # Never told the old summary is current:
    assert body["result"] is None and body["result_is_current"] is False
    # ...but it is still available, explicitly labelled with the version it describes:
    assert body["stale_result"]["content_version"] == 1
    assert body["stale_result"]["content_hash"] == v1["content_hash"]
    assert body["stages"]["processing"]["state"] == body["stages"]["enriching"]["state"] == "pending"
    assert_no_mixed_version(body)

    await drain(container)
    done = await get_doc(api, "bob", document_id)
    assert done["result"]["content_version"] == 2 and done["stale_result"] is None
    assert "oranges" in done["result"]["summary"] and "apples" not in done["result"]["summary"]
    assert "apples" not in done["result"]["tags"]
    assert_no_mixed_version(done)


async def test_reader_never_sees_mixed_versions_at_any_step(api, container):
    """Advance the pipeline one stage at a time and PATCH between stages; check every snapshot."""
    worker = container.worker("stepper")
    document_id = (await submit(api, "cara", "version one text about rivers")).json()["document_id"]
    snapshots = [await get_doc(api, "cara", document_id)]
    await worker.run_once()  # processing v1 -> enriching
    snapshots.append(await get_doc(api, "cara", document_id))
    await api.patch(f"/documents/{document_id}", json={"content": "version two text about mountains"}, headers=hdr("cara"))
    snapshots.append(await get_doc(api, "cara", document_id))
    await worker.run_once()  # processing v2
    snapshots.append(await get_doc(api, "cara", document_id))
    await worker.run_once()  # enriching v2 -> completed
    snapshots.append(await get_doc(api, "cara", document_id))

    for snapshot in snapshots:
        assert_no_mixed_version(snapshot)
    final = snapshots[-1]
    assert final["content_version"] == 2 and "mountains" in final["result"]["summary"]
    assert "rivers" not in " ".join(final["result"]["tags"])


async def test_in_flight_processing_for_old_version_is_dropped(api, container):
    document_id = (await submit(api, "dan", "old text about tigers")).json()["document_id"]
    worker = container.worker("slow")
    now = utcnow()
    claimed = await container.repo.claim(Stage.PROCESSING, "tok-old", now, now + worker._lease())
    assert str(claimed["_id"]) == document_id

    # PATCH lands while the worker is mid-stage.
    await api.patch(f"/documents/{document_id}", json={"content": "new text about lions"}, headers=hdr("dan"))
    await worker.run_stage(Stage.PROCESSING, claimed, "tok-old")  # finishes computing v1's summary

    stored = await container.resources.documents.find_one({"_id": claimed["_id"]})
    assert stored["content_version"] == 2 and stored["status"] == "queued"
    assert stored["stages"]["processing"]["summary"] is None  # v1 summary never landed

    await drain(container)
    final = await get_doc(api, "dan", document_id)
    assert "lions" in final["result"]["summary"] and "tigers" not in final["result"]["summary"]


async def test_in_flight_enriching_for_old_version_is_dropped(api, container):
    document_id = (await submit(api, "eve", "old enrich text about comets")).json()["document_id"]
    worker = container.worker("w")
    await worker.run_once()  # processing v1 done; now waiting for enriching
    now = utcnow()
    claimed = await container.repo.claim(Stage.ENRICHING, "tok-e", now, now + worker._lease())
    await api.patch(f"/documents/{document_id}", json={"content": "new text about planets"}, headers=hdr("eve"))
    await worker.run_stage(Stage.ENRICHING, claimed, "tok-e")

    body = await get_doc(api, "eve", document_id)
    assert body["status"] == "queued" and body["content_version"] == 2
    assert body["result"] is None and body["stale_result"] is None  # v1 tags were never published
    assert_no_mixed_version(body)


async def test_concurrent_workers_patches_and_readers_never_mix(api, container):
    """Stress: real concurrent workers, repeated PATCHes, a poller checking every read."""
    document_id = (await submit(api, "fay", "content generation 0 alpha")).json()["document_id"]
    workers = [asyncio.create_task(container.worker(f"w{i}").run_forever()) for i in range(3)]
    reads = 0
    try:
        async def poll():
            nonlocal reads
            while True:
                assert_no_mixed_version(await get_doc(api, "fay", document_id))
                reads += 1
                await asyncio.sleep(0.002)

        poller = asyncio.create_task(poll())
        for generation in range(1, 8):
            await asyncio.sleep(0.015)
            response = await api.patch(
                f"/documents/{document_id}", json={"content": f"content generation {generation} word{generation}"},
                headers=hdr("fay"),
            )
            assert response.status_code == 200
        for _ in range(500):
            body = await get_doc(api, "fay", document_id)
            if body["status"] == "completed":
                break
            await asyncio.sleep(0.01)
        poller.cancel()
        await asyncio.gather(poller, return_exceptions=True)
    finally:
        for task in workers:
            task.cancel()
        await asyncio.gather(*workers, return_exceptions=True)

    final = await get_doc(api, "fay", document_id)
    assert final["status"] == "completed" and final["content_version"] == 8
    assert "word7" in final["result"]["summary"] and final["result"]["tags"] == ["content", "generation", "word7"]
    assert reads > 10
    assert_no_mixed_version(final)


async def test_racing_patches_with_expected_version_one_wins(api, container):
    document_id = (await submit(api, "gus", "racing content")).json()["document_id"]
    results = await asyncio.gather(
        api.patch(f"/documents/{document_id}", json={"content": "A wins", "expected_version": 1}, headers=hdr("gus")),
        api.patch(f"/documents/{document_id}", json={"content": "B wins", "expected_version": 1}, headers=hdr("gus")),
    )
    assert sorted(r.status_code for r in results) == [200, 409]
    winner = next(r for r in results if r.status_code == 200).json()
    loser = next(r for r in results if r.status_code == 409).json()
    assert loser["detail"]["content_version"] == 2
    body = await get_doc(api, "gus", document_id)
    assert body["content_version"] == 2 and body["content"] == winner["content"]


async def test_stale_expected_version_is_rejected(api, container):
    document_id = (await submit(api, "hal", "v1")).json()["document_id"]
    await api.patch(f"/documents/{document_id}", json={"content": "v2"}, headers=hdr("hal"))
    stale = await api.patch(
        f"/documents/{document_id}", json={"content": "v3", "expected_version": 1}, headers=hdr("hal")
    )
    assert stale.status_code == 409 and stale.json()["detail"]["content_version"] == 2


async def test_patch_with_identical_content_is_a_noop(api, container):
    document_id = await _completed(api, container, "ian", "unchanged content here")
    response = await api.patch(f"/documents/{document_id}", json={"content": "unchanged content here"}, headers=hdr("ian"))
    assert response.status_code == 200
    body = response.json()
    assert body["content_version"] == 1 and body["status"] == "completed" and body["result_is_current"]
