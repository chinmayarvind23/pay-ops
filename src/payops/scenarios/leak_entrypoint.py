"""Run the normal risk application with one explicitly selected bounded allocation worker."""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from threading import Event, Thread

import uvicorn
from fastapi import FastAPI

from payops.sandbox.entrypoint import create_app as sandbox_app
from payops.scenarios.leak_workload import allocate_leak, validate_container


def create_app() -> FastAPI:
    """Validate operator startup before serving; preserve the application's normal lifespan."""
    validate_container()
    app = sandbox_app()
    original_lifespan = app.router.lifespan_context

    @asynccontextmanager
    async def lifespan(application: FastAPI) -> AsyncGenerator[None]:
        """Stop and join the owned worker before releasing the application's dependencies."""
        stop = Event()
        failures: list[BaseException] = []

        def work() -> None:
            """Surface worker failures during shutdown."""
            try:
                allocate_leak(stop)
            except BaseException as error:
                failures.append(error)

        worker = Thread(target=work, daemon=True, name="synthetic-risk-retention")
        async with original_lifespan(application):
            worker.start()
            try:
                yield
            finally:
                stop.set()
                worker.join(timeout=5)
                if worker.is_alive():
                    raise RuntimeError("synthetic risk worker did not stop")
                if failures:
                    raise RuntimeError("synthetic risk worker failed") from failures[0]

    app.router.lifespan_context = lifespan
    return app


def main() -> None:
    """Expose only the existing synthetic risk HTTP application on the fixed container port."""
    uvicorn.run(create_app(), host="0.0.0.0", port=8080)


if __name__ == "__main__":
    main()
