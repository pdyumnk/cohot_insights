"""Pure unit tests — no MongoDB/Redis needed."""

from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from app.domain import Status, build_result, content_fields, failed_stage, hash_content
from app.schemas.documents import DocumentCreate, to_document_out
from app.services import text_digest

NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


def stored(content: str, version: int, result_for: tuple[int, str] | None = None) -> dict:
    h = hash_content(content)
    doc = {
        "_id": "0" * 24, "user_id": "u", "title": "t", "created_at": NOW,
        **content_fields(content, h, version, None, NOW), "result": None,
    }
    if result_for:
        v, c = result_for
        doc["result"] = build_result(version=v, content_hash=hash_content(c), summary="s", tags=["x"],
                                     source="pipeline", now=NOW)
    return doc


def test_text_digest_is_deterministic():
    text = "Redis caches results while MongoDB stores documents. Redis is fast. " * 5
    assert text_digest.build_summary(text) == text_digest.build_summary(text)
    tags = text_digest.top_keywords(text_digest.build_summary(text))
    assert tags[0] == "redis" and len(tags) <= 5
    assert "while" not in tags


def test_hash_is_content_addressed():
    assert hash_content("a") == hash_content("a") != hash_content("b")
    assert hash_content("a").startswith("sha256:")


def test_result_for_current_version_is_exposed():
    doc = stored("hello", 2, result_for=(2, "hello"))
    doc["status"] = Status.COMPLETED
    out = to_document_out(doc)
    assert out.result_is_current and out.result.content_version == 2 and out.stale_result is None


def test_result_for_older_version_is_only_stale():
    out = to_document_out(stored("new content", 3, result_for=(2, "old content")))
    assert out.result is None and not out.result_is_current
    assert out.stale_result.content_version == 2


def test_version_match_but_hash_mismatch_is_not_current():
    # Belt and braces: both version AND hash must match for a result to count as current.
    out = to_document_out(stored("new content", 2, result_for=(2, "other content")))
    assert out.result is None


def test_failed_stage_is_derived_from_stage_states():
    doc = stored("x", 1)
    assert failed_stage(doc) is None
    doc["status"] = Status.FAILED
    doc["stages"]["enriching"]["state"] = "failed"
    assert failed_stage(doc) == "enriching"


def test_cache_hit_fields_publish_result_in_same_write():
    fields = content_fields("c", hash_content("c"), 4, {"summary": "s", "tags": ["t"]}, NOW)
    assert fields["status"] == "completed"
    assert fields["result"]["content_version"] == 4 == fields["content_version"]
    assert fields["stages"]["processing"]["content_version"] == fields["stages"]["enriching"]["content_version"] == 4


@pytest.mark.parametrize(
    "payload",
    [
        {"user_id": "u", "title": "t", "content": "   "},
        {"user_id": "u", "title": "", "content": "x"},
        {"user_id": "bad user", "title": "t", "content": "x"},
        {"user_id": "u", "title": "t", "content": "x", "client_doc_ref": "has space"},
        {"user_id": "u", "title": "t", "content": "bad\x00byte"},
        {"user_id": "u", "title": "t", "content": "x", "unexpected": 1},
        {"user_id": "u", "title": "t", "content": "x" * 100_001},
    ],
)
def test_create_validation(payload):
    with pytest.raises(ValidationError):
        DocumentCreate(**payload)
