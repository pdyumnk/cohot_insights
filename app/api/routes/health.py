import logging
from typing import Annotated

from fastapi import APIRouter, Depends, Response, status
from pymongo.errors import PyMongoError
from redis.exceptions import RedisError

from app.api.deps import get_container
from app.container import Container

router = APIRouter(tags=["health"])
logger = logging.getLogger("cohort.health")


@router.get("/health")
async def health(response: Response, container: Annotated[Container, Depends(get_container)]):
    """MongoDB is required (503 if down). Redis has fallbacks, so its loss is reported as 'degraded'."""
    checks: dict[str, str] = {}
    try:
        await container.resources.db.command("ping")
        checks["mongodb"] = "ok"
    except PyMongoError as exc:
        logger.warning("health: mongodb unreachable", extra={"error": str(exc)})
        checks["mongodb"] = "down"
    try:
        await container.resources.redis.ping()
        checks["redis"] = "ok"
    except RedisError as exc:
        logger.warning("health: redis unreachable", extra={"error": str(exc)})
        checks["redis"] = "down"

    if checks["mongodb"] == "down":
        overall = "down"
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    elif checks["redis"] == "down":
        overall = "degraded"
    else:
        overall = "ok"
    return {"status": overall, "checks": checks}
