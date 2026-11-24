"""Structured (JSON-lines) logging with a per-request id carried in a contextvar."""

import json
import logging
from contextvars import ContextVar
from datetime import datetime, timezone

request_id_var: ContextVar[str | None] = ContextVar("request_id", default=None)

_RESERVED = set(vars(logging.makeLogRecord({})).keys()) | {"message", "asctime"}


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        entry = {
            "ts": datetime.fromtimestamp(record.created, timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        request_id = request_id_var.get()
        if request_id:
            entry["request_id"] = request_id
        # Anything passed via `extra={...}` becomes a top-level field.
        for key, value in vars(record).items():
            if key not in _RESERVED and not key.startswith("_"):
                entry[key] = value
        if record.exc_info:
            entry["exc"] = self.formatException(record.exc_info)
        return json.dumps(entry, default=str)


def setup_logging(level: str) -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    logging.basicConfig(level=level.upper(), handlers=[handler], force=True)
    # uvicorn's access log duplicates our request log line
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
