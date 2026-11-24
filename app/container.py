"""Builds the object graph once per process (API or worker)."""

from dataclasses import dataclass

from app.core.config import Settings
from app.db.connections import Resources
from app.repositories.documents import DocumentRepository
from app.services.content_cache import ContentCache
from app.services.documents import DocumentService
from app.services.pipeline import PipelineWorker
from app.services.rate_limiter import ActiveJobLimiter


@dataclass
class Container:
    settings: Settings
    resources: Resources
    repo: DocumentRepository
    limiter: ActiveJobLimiter
    cache: ContentCache
    documents: DocumentService

    def worker(self, name: str) -> PipelineWorker:
        return PipelineWorker(self.repo, self.limiter, self.cache, self.settings, name=name)


def build_container(settings: Settings, resources: Resources) -> Container:
    repo = DocumentRepository(resources.documents)
    limiter = ActiveJobLimiter(
        resources.redis, repo, limit=settings.max_active_per_user, ttl_s=settings.active_set_ttl_s
    )
    cache = ContentCache(resources.redis, repo, ttl_s=settings.cache_ttl_s)
    return Container(
        settings=settings,
        resources=resources,
        repo=repo,
        limiter=limiter,
        cache=cache,
        documents=DocumentService(repo, limiter, cache),
    )
