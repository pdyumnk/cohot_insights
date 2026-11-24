"""Worker process entrypoint: `python -m app.worker`.

Runs `WORKER_CONCURRENCY` pipeline loops. Scale out with `docker compose up --scale worker=N`;
leases make concurrent worker processes safe.
"""

import asyncio
import logging
import signal

from app.container import build_container
from app.core.config import get_settings
from app.core.logging import setup_logging
from app.db.connections import connect, ensure_indexes
from app.services.pipeline import run_workers

logger = logging.getLogger("cohort.worker")


async def main() -> None:
    settings = get_settings()
    setup_logging(settings.log_level)
    resources = connect(settings)
    await ensure_indexes(resources)
    container = build_container(settings, resources)

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)

    tasks = await run_workers(container.worker, settings.worker_concurrency)
    logger.info("worker process started", extra={"concurrency": len(tasks)})
    await stop.wait()
    logger.info("shutting down; unfinished stages resume after their lease expires")
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    await resources.close()


if __name__ == "__main__":
    asyncio.run(main())
