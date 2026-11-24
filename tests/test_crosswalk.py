"""client_doc_ref: uniqueness, repeat-submission policy, index-backed lookup."""

import asyncio

from conftest import drain, get_doc, hdr, submit

from app.repositories.documents import ref_filter


async def test_repeat_same_content_is_idempotent(api, container):
    first = await submit(api, "ivy", "partner content v1", ref="cms-1")
    assert first.status_code == 201
    replay = await submit(api, "ivy", "partner content v1", ref="cms-1")
    assert replay.status_code == 200
    assert replay.json()["document_id"] == first.json()["document_id"]
    assert await container.resources.documents.count_documents({}) == 1


async def test_repeat_different_content_is_rejected_with_pointer(api, container):
    first = (await submit(api, "ivy", "partner content v1", ref="cms-2")).json()
    conflict = await submit(api, "ivy", "partner content DIFFERENT", ref="cms-2")
    assert conflict.status_code == 409
    detail = conflict.json()["detail"]
    assert detail["document_id"] == first["document_id"] and detail["content_version"] == 1
    # The original is untouched.
    assert (await get_doc(api, "ivy", first["document_id"]))["content"] == "partner content v1"


async def test_same_ref_from_another_user_conflicts_without_leaking(api, container):
    await submit(api, "ivy", "partner content", ref="cms-3")
    other = await submit(api, "jay", "partner content", ref="cms-3")
    assert other.status_code == 409
    assert "document_id" not in other.json()["detail"]


async def test_out_of_order_old_retry_cannot_revert_a_patch(api, container):
    """Partner sends v1 (POST), then v2 (PATCH); a delayed retry of the v1 POST arrives last."""
    document_id = (await submit(api, "kai", "partner v1 text", ref="cms-4")).json()["document_id"]
    await api.patch(
        f"/documents/{document_id}", json={"content": "partner v2 text", "expected_version": 1}, headers=hdr("kai")
    )
    late = await submit(api, "kai", "partner v1 text", ref="cms-4")
    assert late.status_code == 409 and late.json()["detail"]["content_version"] == 2
    body = (await api.get("/documents/by-ref/cms-4", headers=hdr("kai"))).json()
    assert body["content"] == "partner v2 text" and body["content_version"] == 2


async def test_concurrent_first_submissions_of_one_ref(api, container):
    responses = await asyncio.gather(*[submit(api, "lou", "same partner doc", ref="cms-5") for _ in range(4)])
    codes = [r.status_code for r in responses]
    # Exactly one creates. The others are replays (200) — or, if they lost the race for a
    # slot before the winner's insert was visible, a retryable 429. Never a second document.
    assert codes.count(201) == 1 and set(codes) <= {200, 201, 429}
    assert len({r.json()["document_id"] for r in responses if r.status_code in (200, 201)}) == 1
    assert await container.resources.documents.count_documents({"client_doc_ref": "cms-5"}) == 1
    retry = await submit(api, "lou", "same partner doc", ref="cms-5")
    assert retry.status_code == 200
    # losers released their rate-limit slot: the user still has room for two more
    assert (await submit(api, "lou", "another doc 1")).status_code == 201
    assert (await submit(api, "lou", "another doc 2")).status_code == 201


async def test_by_ref_lookup_and_404(api, container):
    document_id = (await submit(api, "mia", "lookup me", ref="cms-6")).json()["document_id"]
    await drain(container)
    found = await api.get("/documents/by-ref/cms-6", headers=hdr("mia"))
    assert found.status_code == 200
    assert found.json()["document_id"] == document_id and found.json()["result_is_current"]
    assert (await api.get("/documents/by-ref/nope", headers=hdr("mia"))).status_code == 404
    assert (await api.get("/documents/by-ref/bad%20ref", headers=hdr("mia"))).status_code == 422


async def test_documents_without_ref_do_not_collide(api, container):
    assert (await submit(api, "ned", "no ref one")).status_code == 201
    assert (await submit(api, "ned", "no ref two")).status_code == 201


async def test_by_ref_uses_the_partial_unique_index(api, container):
    await submit(api, "ola", "indexed", ref="cms-7")
    plan = await container.resources.documents.find({**ref_filter("cms-7"), "user_id": "ola"}).explain()
    winning = str(plan["queryPlanner"]["winningPlan"])
    assert "client_doc_ref_unique" in winning and "COLLSCAN" not in winning
