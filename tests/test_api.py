"""Endpoints, validation, ownership and listing."""

from conftest import drain, get_doc, hdr, submit


async def test_health(api, container):
    response = await api.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok", "checks": {"mongodb": "ok", "redis": "ok"}}


async def test_submit_validation_and_identity(api, container):
    assert (await submit(api, "val", "   ")).status_code == 422
    missing_header = await api.post("/documents", json={"user_id": "a", "title": "t", "content": "x"})
    assert missing_header.status_code == 422
    mismatch = await api.post(
        "/documents", json={"user_id": "alice", "title": "t", "content": "x"}, headers=hdr("mallory")
    )
    assert mismatch.status_code == 403


async def test_submit_then_poll_to_completion(api, container):
    response = await submit(api, "alice", "MongoDB and Redis power the insights pipeline for cohorts")
    assert response.status_code == 201
    created = response.json()
    assert created["status"] == "queued" and created["content_version"] == 1

    assert await drain(container) == 2  # processing, then enriching
    body = await get_doc(api, "alice", created["document_id"])
    assert body["status"] == "completed" and body["failed_stage"] is None
    assert body["result_is_current"] is True
    assert body["result"]["content_version"] == 1 and body["result"]["source"] == "pipeline"
    assert body["result"]["summary"] and body["result"]["tags"]
    assert body["stages"]["processing"]["content_version"] == body["stages"]["enriching"]["content_version"] == 1
    assert body["stages"]["processing"]["state"] == body["stages"]["enriching"]["state"] == "completed"


async def test_non_owner_cannot_distinguish_foreign_from_missing(api, container):
    document_id = (await submit(api, "owner", "private notes", ref="ref-own")).json()["document_id"]
    foreign = await api.get(f"/documents/{document_id}", headers=hdr("intruder"))
    missing = await api.get("/documents/000000000000000000000000", headers=hdr("intruder"))
    malformed = await api.get("/documents/not-an-object-id", headers=hdr("intruder"))
    assert foreign.status_code == missing.status_code == malformed.status_code == 404
    assert foreign.json() == missing.json() == malformed.json()

    assert (await api.get("/documents/by-ref/ref-own", headers=hdr("intruder"))).status_code == 404
    assert (await api.get("/users/owner/documents", headers=hdr("intruder"))).status_code == 404
    patch = await api.patch(f"/documents/{document_id}", json={"content": "hijack"}, headers=hdr("intruder"))
    assert patch.status_code == 404
    retry = await api.post(f"/documents/{document_id}/retry", headers=hdr("intruder"))
    assert retry.status_code == 404
    # ...and the owner is unaffected
    assert (await get_doc(api, "owner", document_id))["content"] == "private notes"


async def test_list_newest_first_paginated_and_filtered(api, container):
    ids = [(await submit(api, "kim", f"kim document number {i}")).json()["document_id"] for i in range(3)]
    await drain(container)
    ids.append((await submit(api, "kim", "kim document number 3")).json()["document_id"])
    await submit(api, "lee", "someone else's document")

    page1 = (await api.get("/users/kim/documents?page=1&page_size=3", headers=hdr("kim"))).json()
    page2 = (await api.get("/users/kim/documents?page=2&page_size=3", headers=hdr("kim"))).json()
    assert [d["document_id"] for d in page1["items"]] == ids[::-1][:3]
    assert page1["has_more"] is True
    assert [d["document_id"] for d in page2["items"]] == [ids[0]] and page2["has_more"] is False
    assert "content" not in page1["items"][0]  # list items omit the (possibly large) content

    queued = (await api.get("/users/kim/documents?status=queued", headers=hdr("kim"))).json()
    assert [d["document_id"] for d in queued["items"]] == [ids[3]]
    completed = (await api.get("/users/kim/documents?status=completed", headers=hdr("kim"))).json()
    assert len(completed["items"]) == 3

    assert (await api.get("/users/kim/documents?status=bogus", headers=hdr("kim"))).status_code == 422
    assert (await api.get("/users/kim/documents?page_size=1000", headers=hdr("kim"))).status_code == 422


async def test_listing_uses_user_index(container):
    plan = await container.resources.documents.find({"user_id": "kim", "status": "queued"}).sort(
        [("created_at", -1), ("_id", -1)]
    ).explain()
    winning = str(plan["queryPlanner"]["winningPlan"])
    assert "user_status_created" in winning and "COLLSCAN" not in winning
