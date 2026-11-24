"""Document states, the stored-document shape, and the helpers that build it.

Stored document (one MongoDB document per submitted document):

    {
      _id, user_id, title, client_doc_ref?,          # identity (immutable after POST)
      content, content_hash, content_version,        # the *current* content
      status,                                        # queued|processing|enriching|completed|failed
      stages: {
        processing: {state, content_version, attempts, error, started_at, finished_at, summary},
        enriching:  {state, content_version, attempts, error, started_at, finished_at},
      },
      result: null | {content_version, content_hash, summary, tags, source, completed_at},
      lease_token, lease_until,                      # worker claim (fencing token + expiry)
      created_at, updated_at,
    }

`result` is the only place a reader-visible summary/tags pair lives, and it is always
written as a single sub-document stamped with the content_version/content_hash it was
derived from. See README "Schema & Staleness Design".
"""

import hashlib
from datetime import datetime, timezone
from enum import StrEnum
from typing import Any


class Status(StrEnum):
    QUEUED = "queued"
    PROCESSING = "processing"
    ENRICHING = "enriching"
    COMPLETED = "completed"
    FAILED = "failed"


class StageState(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class Stage(StrEnum):
    PROCESSING = "processing"
    ENRICHING = "enriching"


ACTIVE_STATUSES: tuple[str, ...] = (Status.QUEUED, Status.PROCESSING, Status.ENRICHING)
TERMINAL_STATUSES: tuple[str, ...] = (Status.COMPLETED, Status.FAILED)


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def hash_content(content: str) -> str:
    return "sha256:" + hashlib.sha256(content.encode("utf-8")).hexdigest()


def _stage(state: StageState, version: int | None = None) -> dict[str, Any]:
    return {
        "state": state.value,
        "content_version": version,
        "attempts": 0,
        "error": None,
        "started_at": None,
        "finished_at": None,
    }


def fresh_stages() -> dict[str, Any]:
    """Both stages reset — used on POST and on every PATCH."""
    return {
        Stage.PROCESSING: {**_stage(StageState.PENDING), "summary": None},
        Stage.ENRICHING: _stage(StageState.PENDING),
    }


def cached_stages(version: int, summary: str, now: datetime) -> dict[str, Any]:
    """Both stages marked complete for `version` when the result came from the content cache."""
    processing = {**_stage(StageState.COMPLETED, version), "summary": summary, "finished_at": now}
    enriching = {**_stage(StageState.COMPLETED, version), "finished_at": now}
    return {Stage.PROCESSING: processing, Stage.ENRICHING: enriching}


def build_result(
    *, version: int, content_hash: str, summary: str, tags: list[str], source: str, now: datetime
) -> dict[str, Any]:
    return {
        "content_version": version,
        "content_hash": content_hash,
        "summary": summary,
        "tags": tags,
        "source": source,  # "pipeline" | "cache"
        "completed_at": now,
    }


def content_fields(
    content: str,
    content_hash: str,
    version: int,
    cached: dict[str, Any] | None,
    now: datetime,
) -> dict[str, Any]:
    """Every field that changes when the content changes, as ONE $set.

    This is the heart of the staleness guarantee: content, its hash, its version, the
    pipeline position, both stage records and the lease are replaced together in a single
    single-document write. With a cache hit the published `result` is written in that same
    write too; without one, the previous `result` is left in place but is now detectably
    stale (its content_version no longer equals the document's).
    """
    fields: dict[str, Any] = {
        "content": content,
        "content_hash": content_hash,
        "content_version": version,
        "lease_token": None,
        "lease_until": None,
        "updated_at": now,
    }
    if cached:
        fields["status"] = Status.COMPLETED.value
        fields["stages"] = cached_stages(version, cached["summary"], now)
        fields["result"] = build_result(
            version=version,
            content_hash=content_hash,
            summary=cached["summary"],
            tags=cached["tags"],
            source="cache",
            now=now,
        )
    else:
        fields["status"] = Status.QUEUED.value
        fields["stages"] = fresh_stages()
    return fields


def failed_stage(doc: dict[str, Any]) -> str | None:
    if doc["status"] != Status.FAILED:
        return None
    for name in (Stage.PROCESSING, Stage.ENRICHING):
        if doc["stages"][name]["state"] == StageState.FAILED:
            return name.value
    return None
