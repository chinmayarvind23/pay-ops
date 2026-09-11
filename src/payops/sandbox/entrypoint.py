"""Deployment startup configuration is separate from untrusted simulation requests."""

import os

from fastapi import FastAPI
from pydantic import TypeAdapter

from payops.sandbox.models import FaultConfig, Role, SandboxConfig
from payops.sandbox.runtime import FaultState
from payops.sandbox.service import create_service
from payops.sandbox.tracing import configure_tracing


def create_app() -> FastAPI:
    """Fail startup on invalid role, peer origin or bounded fault configuration."""
    role = TypeAdapter[Role](Role).validate_python(
        os.environ.get("PAYOPS_SANDBOX_ROLE", "payments")
    )
    config = SandboxConfig.model_validate_json(os.environ.get("PAYOPS_SANDBOX_CONFIG", "{}"))
    fault = FaultConfig.model_validate_json(os.environ.get("PAYOPS_SANDBOX_FAULT", "{}"))
    configure_tracing(role)
    return create_service(role, config, faults=FaultState(fault))
