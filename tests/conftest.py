"""Portable isolated test directories without Windows junction cleanup hazards."""

from collections.abc import Iterator
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest


@pytest.fixture
def tmp_path() -> Iterator[Path]:
    """Avoid pytest's current-directory symlinks rejected by hardened Windows mounts."""
    with TemporaryDirectory(prefix="payops-test-") as directory:
        yield Path(directory)
