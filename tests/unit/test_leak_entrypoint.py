"""Check risk service lifecycle and worker failures without experimental memory load."""

import asyncio
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from threading import Event
from unittest.mock import patch

import pytest
from fastapi import FastAPI

from payops.scenarios import leak_entrypoint as entry


@pytest.mark.parametrize("failure", [False, True])
def test_owned_worker_lifecycle_and_failure_visibility(failure: bool) -> None:
    """Real thread teardown precedes dependency shutdown and surfaces allocation errors."""
    events: list[str] = []
    started = Event()

    @asynccontextmanager
    async def original(app: FastAPI) -> AsyncGenerator[None]:
        """Track the underlying application resources around the worker's whole lifetime."""
        events.append("app-start")
        try:
            yield
        finally:
            events.append("app-stop")

    def allocate(stop: Event) -> None:
        """Wait on the actual shutdown signal without allocating experimental memory."""
        events.append("worker-start")
        started.set()
        if failure:
            raise ValueError("fixture allocation failure")
        assert stop.wait(2)
        events.append("worker-stop")

    async def exercise(app: FastAPI) -> None:
        """Wait for thread admission before leaving the actual application lifespan."""
        async with app.router.lifespan_context(app):
            assert started.wait(2)

    with (
        patch.object(entry, "validate_container"),
        patch.object(entry, "sandbox_app", return_value=FastAPI(lifespan=original)),
        patch.object(entry, "allocate_leak", side_effect=allocate),
    ):
        app = entry.create_app()
        if failure:
            with pytest.raises(RuntimeError, match="worker failed"):
                asyncio.run(exercise(app))
        else:
            asyncio.run(exercise(app))
    assert events == (
        ["app-start", "worker-start", "app-stop"]
        if failure
        else ["app-start", "worker-start", "worker-stop", "app-stop"]
    )


def test_invalid_startup_never_constructs_http_application() -> None:
    """The host/container guard executes before the server can expose any routes."""
    with (
        patch.object(entry, "validate_container", side_effect=ValueError),
        patch.object(entry, "sandbox_app") as app,
    ):
        with pytest.raises(ValueError):
            entry.create_app()
    app.assert_not_called()


def test_fixed_server_binding() -> None:
    """No request or environment field can select a command or alternate server binding."""
    with (
        patch.object(entry, "create_app", return_value=FastAPI()),
        patch.object(entry.uvicorn, "run") as run,
    ):
        entry.main()
    assert run.call_args.kwargs == {"host": "0.0.0.0", "port": 8080}
