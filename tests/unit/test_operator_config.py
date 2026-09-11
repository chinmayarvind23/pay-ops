"""Local configuration never impersonates public identity or discovers a credential implicitly."""

from datetime import timedelta
from pathlib import Path
from typing import Any

import certifi
import pytest
from pydantic import SecretStr

from payops.contracts import utc_now
from payops.memory.data_clients import ElasticsearchConfig
from payops.operator_config import (
    LocalAuthority,
    LocalGrant,
    SecretReference,
    local_path,
    native_account,
    read_bounded,
)


def grant(path: Path, **changes: Any) -> Path:
    """Write an explicit short-lived local account grant, with no model-controllable roles."""
    now = utc_now()
    value = LocalGrant.model_validate(
        {
            "account": "fixture-os",
            "enabled": True,
            "issued_at": now - timedelta(seconds=1),
            "expires_at": now + timedelta(hours=1),
            **changes,
        }
    )
    path.write_text(value.model_dump_json(), encoding="utf-8")
    return path


@pytest.mark.parametrize(
    "changes",
    [
        {"enabled": False},
        {"account": "foreign-os"},
        {"issued_at": utc_now() - timedelta(hours=2), "expires_at": utc_now() - timedelta(hours=1)},
        {"issued_at": utc_now() + timedelta(hours=1), "expires_at": utc_now() + timedelta(hours=2)},
    ],
)
def test_current_grant_scope_account_and_lifetime(tmp_path: Path, changes: dict[str, Any]) -> None:
    """Malformed or no-longer-current local grants cannot create an authenticated principal."""
    authority = LocalAuthority(grant(tmp_path / "grant", **changes), account=lambda: "fixture-os")
    assert authority.principal() is None and not authority.allowed()
    with pytest.raises(PermissionError, match="current local operator grant"):
        authority.require()


@pytest.mark.parametrize(
    "content",
    [
        b'{"account":"a","account":"b"}',
        b'{"unknown":true}',
        b" " * 4097,
        b'{"ignored":1e400}',
        b"[]",
    ],
)
def test_grant_duplicate_unknown_oversized_and_nonfinite_fail_closed(
    tmp_path: Path,
    content: bytes,
) -> None:
    """The local grant is re-read as bounded strict JSON, not cached as permanent authority."""
    path = tmp_path / "grant"
    path.write_bytes(content)
    authority = LocalAuthority(path, account=lambda: "fixture-os")
    assert authority.principal() is None


def test_os_account_change_closed_host_and_missing_grant(tmp_path: Path) -> None:
    """The initiating process account is retained independently of later grant-file contents."""
    account = "fixture-os"
    path = grant(tmp_path / "grant")
    authority = LocalAuthority(path, account=lambda: account)
    principal = authority.principal()
    assert principal is not None and principal.roles == ("responder",)
    assert principal.namespaces == ("payops-sandbox",)
    account = "changed-os"
    assert not authority.allowed()
    account = "fixture-os"
    authority.closed = True
    assert not authority.allowed()
    authority.closed = False
    path.unlink()
    assert not authority.allowed()


@pytest.mark.parametrize("advance", [2, 9, -1])
def test_slow_identity_crossing_expiry_or_freshness_cannot_refresh_grant(
    tmp_path: Path,
    advance: int,
) -> None:
    """Current time is rechecked after account lookup, and verified_at remains start-bound."""
    now = utc_now()
    path = grant(
        tmp_path / "grant",
        issued_at=now - timedelta(minutes=1),
        expires_at=now + timedelta(seconds=1 if advance == 2 else 60),
    )
    calls = 0

    def account() -> str:
        """Only the verification lookup advances time, leaving constructor binding deterministic."""
        nonlocal calls, now
        calls += 1
        if calls > 1:
            now += timedelta(seconds=advance)
        return "fixture-os"

    authority = LocalAuthority(path, account=account, clock=lambda: now)
    assert authority.principal() is None


@pytest.mark.parametrize(
    "changes",
    [
        {"role": "executor"},
        {"namespace": "kube-system"},
        {"enabled": "true"},
        {"expires_at": utc_now() + timedelta(days=1)},
    ],
)
def test_grant_cannot_expand_authority_or_lifetime(tmp_path: Path, changes: dict[str, Any]) -> None:
    """A local CLI grant has only its fixed read role and an explicitly bounded lifetime."""
    with pytest.raises(ValueError):
        grant(tmp_path / "grant", **changes)


@pytest.mark.parametrize(
    "reference",
    [
        {},
        {"environment": "A", "file": "/some/file"},
        {"environment": "invalid-name"},
        {"file": "relative"},
    ],
)
def test_secret_reference_has_one_explicit_source(reference: dict[str, Any]) -> None:
    """No fallback environment name or adjacent file is selected after a malformed reference."""
    with pytest.raises(ValueError):
        SecretReference.model_validate(reference)


def test_secret_loading_is_explicit_bounded_and_not_in_representation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A named environment variable and a named file are the only supported credential sources."""
    monkeypatch.setenv("EXPLICIT_TEST_KEY", "synthetic-private-key")
    secret = SecretReference(environment="EXPLICIT_TEST_KEY").load()
    assert secret.get_secret_value() == "synthetic-private-key"
    assert "synthetic-private-key" not in repr(secret)
    path = tmp_path / "secret"
    path.write_bytes(b"explicit-file-key\r\n")
    assert SecretReference(file=path).load().get_secret_value() == "explicit-file-key"
    monkeypatch.delenv("EXPLICIT_TEST_KEY")
    with pytest.raises(ValueError):
        SecretReference(environment="EXPLICIT_TEST_KEY").load()
    monkeypatch.setenv("EXPLICIT_TEST_KEY", "x" * 4097)
    with pytest.raises(ValueError):
        SecretReference(environment="EXPLICIT_TEST_KEY").load()
    path.write_bytes(b"x" * 4097)
    with pytest.raises(ValueError):
        SecretReference(file=path).load()


def test_paths_are_local_after_resolution_and_reads_are_bounded(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Local symlink spelling cannot authorize a resolved UNC target."""
    for path in [Path("relative"), Path("\\\\server\\share\\grant")]:
        with pytest.raises(ValueError):
            local_path(path)
    with monkeypatch.context() as patch:

        def resolve(path: Path) -> Path:
            """Model an explicitly local alias that resolves to a remote authority file."""
            return Path("\\\\server\\share\\grant")

        patch.setattr(Path, "resolve", resolve)
        with pytest.raises(ValueError, match="resolved path"):
            local_path(tmp_path / "grant")
    with pytest.raises(ValueError):
        read_bounded(tmp_path / "missing", 10)
    path = tmp_path / "small"
    path.write_bytes(b"123")
    assert read_bounded(path, 3) == b"123"
    with pytest.raises(ValueError):
        read_bounded(path, 2)


def test_native_identity_ignores_user_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    """This read-only OS identity smoke test never prints the actual account value."""
    before = native_account()
    monkeypatch.setenv("USER", "spoofed-name")
    monkeypatch.setenv("USERNAME", "spoofed-name")
    assert native_account() == before and before != "spoofed-name"


def test_native_windows_failure_and_posix_uid(monkeypatch: pytest.MonkeyPatch) -> None:
    """Platform alternatives fail closed without falling back to caller-supplied account strings."""

    class Function:
        """Model the documented native API failure without changing a real Windows token."""

        argtypes: Any = None
        restype: Any = None

        def __call__(self, *args: Any) -> int:
            """A failed account API must not manufacture an empty operator principal."""
            return 0

    class Library:
        """Only the named account function is provided by this local fixture."""

        GetUserNameExW = Function()

    with monkeypatch.context() as patch:

        def library(*args: Any, **kwargs: Any) -> Library:
            """Supply a deterministic native API failure on any test platform."""
            return Library()

        patch.setattr("payops.operator_config.sys.platform", "win32")
        patch.setattr("payops.operator_config.ctypes.WinDLL", library, raising=False)
        with pytest.raises(PermissionError):
            native_account()
    with monkeypatch.context() as patch:
        patch.setattr("payops.operator_config.sys.platform", "linux")
        patch.setattr("payops.operator_config.os.getuid", lambda: 123, raising=False)
        assert native_account() == "posix-uid:123"


def test_retrieval_only_config_needs_no_unrelated_database_credentials(tmp_path: Path) -> None:
    """The subset preserves TLS/secret bounds without unrelated database credentials."""
    config = ElasticsearchConfig(
        ca_file=Path(certifi.where()), elastic_password=SecretStr("synthetic-elastic-key")
    )
    assert not hasattr(config, "postgres_password") and not hasattr(config, "redis_password")
    assert "synthetic-elastic-key" not in repr(config)
    for updates in [
        {"ca_file": tmp_path / "missing"},
        {"elastic_password": "short"},
        {"elastic_host": "foreign.invalid"},
        {"elastic_port": 0},
    ]:
        with pytest.raises(ValueError):
            ElasticsearchConfig.model_validate({**config.model_dump(), **updates})
