import asyncio
import logging
import time
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse, RedirectResponse
from pymongo.errors import PyMongoError

from app.api.routes import documents, health, users
from app.container import build_container
from app.core.config import Settings, get_settings
from app.core.logging import request_id_var, setup_logging
from app.db.connections import connect, ensure_indexes
from app.services.documents import Conflict, DocumentNotFound, Forbidden, RateLimited
from app.services.pipeline import run_workers

logger = logging.getLogger("cohort.api")


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()
    setup_logging(settings.log_level)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        resources = connect(settings)
        await ensure_indexes(resources)
        container = build_container(settings, resources)
        app.state.container = container
        workers = []
        if settings.embedded_workers:
            workers = await run_workers(container.worker, settings.worker_concurrency)
        logger.info("api started", extra={"embedded_workers": len(workers)})
        try:
            yield
        finally:
            for task in workers:
                task.cancel()
            await asyncio.gather(*workers, return_exceptions=True)
            await resources.close()

    app = FastAPI(
        title=settings.app_name,
        version="1.0.0",
        lifespan=lifespan,
        description="Ingests documents, runs a two-stage simulated pipeline, and serves versioned results.",
    )
    @app.get("/", include_in_schema=False)
    async def root():
        return RedirectResponse(url="/docs")

    app.include_router(health.router)
    app.include_router(documents.router)
    app.include_router(users.router)
    _install_middleware(app)
    _install_error_handlers(app)
    return app


def _install_middleware(app: FastAPI) -> None:
    @app.middleware("http")
    async def request_context(request: Request, call_next):
        request_id = request.headers.get("X-Request-Id") or uuid.uuid4().hex
        token = request_id_var.set(request_id)
        start = time.perf_counter()
        try:
            response = await call_next(request)
            response.headers["X-Request-Id"] = request_id
            logger.info(
                "request",
                extra={
                    "method": request.method,
                    "path": request.url.path,
                    "status": response.status_code,
                    "duration_ms": round((time.perf_counter() - start) * 1000, 1),
                },
            )
            return response
        finally:
            request_id_var.reset(token)


def _error(code: int, detail) -> JSONResponse:
    return JSONResponse(status_code=code, content={"detail": detail})


def _install_error_handlers(app: FastAPI) -> None:
    @app.exception_handler(DocumentNotFound)
    async def _not_found(_: Request, __: DocumentNotFound):
        # Identical body for missing, foreign and malformed ids: existence is never confirmed.
        return _error(status.HTTP_404_NOT_FOUND, "Document not found")

    @app.exception_handler(Forbidden)
    async def _forbidden(_: Request, exc: Forbidden):
        return _error(status.HTTP_403_FORBIDDEN, str(exc))

    @app.exception_handler(Conflict)
    async def _conflict(_: Request, exc: Conflict):
        return _error(status.HTTP_409_CONFLICT, {"message": exc.message, **exc.details})

    @app.exception_handler(RateLimited)
    async def _rate_limited(_: Request, __: RateLimited):
        return JSONResponse(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            content={"detail": "Too many documents in progress; wait for one to complete"},
            headers={"Retry-After": "10"},
        )

    @app.exception_handler(PyMongoError)
    async def _mongo_down(request: Request, exc: PyMongoError):
        logger.error("mongodb error", extra={"path": request.url.path, "error": repr(exc)})
        return _error(status.HTTP_503_SERVICE_UNAVAILABLE, "Database unavailable; retry later")


app = create_app()
