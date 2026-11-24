"""Request/response models. Stored documents are converted here, and only here."""

import re
from datetime import datetime
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, field_validator

from app.domain import Stage, StageState, Status, failed_stage

# Identifiers: no whitespace or path separators, bounded length.
_ID_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9._:@-]{0,127}$"
UserId = Annotated[str, StringConstraints(pattern=_ID_PATTERN, max_length=128)]
ClientDocRef = Annotated[str, StringConstraints(pattern=_ID_PATTERN, max_length=128)]

MAX_CONTENT_CHARS = 100_000
_CONTROL_CHARS = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")


def _clean_text(value: str, field: str) -> str:
    if _CONTROL_CHARS.search(value):
        raise ValueError(f"{field} contains control characters")
    if not value.strip():
        raise ValueError(f"{field} must not be blank")
    return value


class DocumentCreate(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={
            "examples": [
                {
                    "user_id": "u1",
                    "title": "Q3 notes",
                    "content": "Revenue grew twelve percent while churn dropped across enterprise cohorts",
                    "client_doc_ref": "cms-1",
                }
            ]
        },
    )

    user_id: UserId
    title: Annotated[str, StringConstraints(min_length=1, max_length=200, strip_whitespace=True)]
    content: Annotated[str, StringConstraints(min_length=1, max_length=MAX_CONTENT_CHARS)]
    client_doc_ref: ClientDocRef | None = None

    @field_validator("title")
    @classmethod
    def _title(cls, value: str) -> str:
        return _clean_text(value, "title")

    @field_validator("content")
    @classmethod
    def _content(cls, value: str) -> str:
        return _clean_text(value, "content")


class DocumentUpdate(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={
            "examples": [{"content": "Costs fell sharply after infrastructure optimization", "expected_version": 1}]
        },
    )

    content: Annotated[str, StringConstraints(min_length=1, max_length=MAX_CONTENT_CHARS)]
    expected_version: int | None = Field(
        default=None,
        ge=1,
        description="Optimistic concurrency: the content_version you last read. "
        "If it is no longer current the PATCH is rejected with 409.",
    )

    @field_validator("content")
    @classmethod
    def _content(cls, value: str) -> str:
        return _clean_text(value, "content")


class DocumentCreated(BaseModel):
    document_id: str
    status: Status
    content_version: int
    client_doc_ref: str | None = None


class StageOut(BaseModel):
    state: StageState
    content_version: int | None = Field(description="Content version this stage last completed for.")
    attempts: int
    error: str | None
    started_at: datetime | None
    finished_at: datetime | None


class StagesOut(BaseModel):
    processing: StageOut
    enriching: StageOut


class ResultOut(BaseModel):
    content_version: int
    content_hash: str
    summary: str
    tags: list[str]
    source: Literal["pipeline", "cache"]
    completed_at: datetime


class DocumentListItem(BaseModel):
    document_id: str
    title: str
    client_doc_ref: str | None
    status: Status
    failed_stage: Stage | None = Field(description="Which stage failed, when status is 'failed'.")
    content_version: int
    content_hash: str
    stages: StagesOut
    result_is_current: bool = Field(
        description="True iff `result` exists and was derived from the current content_version."
    )
    result: ResultOut | None = Field(
        description="Summary + tags for the CURRENT content. Null until the current version completes."
    )
    stale_result: ResultOut | None = Field(
        description="Last completed result for an OLDER content version, kept while the new "
        "version is reprocessed. Never describes the current content."
    )
    created_at: datetime
    updated_at: datetime


class DocumentOut(DocumentListItem):
    user_id: str
    content: str


class DocumentPage(BaseModel):
    items: list[DocumentListItem]
    page: int
    page_size: int
    has_more: bool


class ErrorOut(BaseModel):
    detail: Any


def _split_result(doc: dict[str, Any]) -> tuple[dict | None, dict | None]:
    result = doc.get("result")
    if result is None:
        return None, None
    if result["content_version"] == doc["content_version"] and result["content_hash"] == doc["content_hash"]:
        return result, None
    return None, result


def _common(doc: dict[str, Any]) -> dict[str, Any]:
    current, stale = _split_result(doc)
    return {
        "document_id": str(doc["_id"]),
        "title": doc["title"],
        "client_doc_ref": doc.get("client_doc_ref"),
        "status": doc["status"],
        "failed_stage": failed_stage(doc),
        "content_version": doc["content_version"],
        "content_hash": doc["content_hash"],
        "stages": {
            name: {key: doc["stages"][name][key] for key in StageOut.model_fields}
            for name in ("processing", "enriching")
        },
        "result_is_current": current is not None,
        "result": current,
        "stale_result": stale,
        "created_at": doc["created_at"],
        "updated_at": doc["updated_at"],
    }


def to_document_out(doc: dict[str, Any]) -> DocumentOut:
    return DocumentOut(**_common(doc), user_id=doc["user_id"], content=doc["content"])


def to_list_item(doc: dict[str, Any]) -> DocumentListItem:
    return DocumentListItem(**_common(doc))
