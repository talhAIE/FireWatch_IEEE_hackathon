"""Private storage transport, immutability, bounded reads and credential-safe failures."""

import hashlib

import httpx
import pytest
from pydantic import ValidationError

from firewatch.config import Settings
from firewatch.evidence.files import AuditFailure, ReadBudget
from firewatch.evidence.storage import (
    StorageError,
    StorageIntegrityError,
    SupabaseStore,
    source_bytes,
)


def config(root, **overrides):
    return Settings(
        _env_file=None,
        database_url="postgresql+psycopg://test:password@localhost:1/firewatch_test",
        admin_api_token="test-only-" + "x" * 40,
        supabase_url="https://synthetic-project.supabase.co",
        supabase_secret_key="synthetic-storage-unit-key",
        raw_data_dir=root / "raw",
        replay_bundle_dir=root / "replays",
        **overrides,
    )


def test_upload_is_private_verified_and_idempotent_without_overwrites(tmp_path):
    content = b"synthetic immutable evidence"
    checksum = hashlib.sha256(content).hexdigest()
    objects, calls = {}, []

    def handle(request):
        calls.append(request.method)
        assert request.headers["apikey"] == "synthetic-storage-unit-key"
        assert "authorization" not in request.headers
        assert checksum + ".bin" in request.url.path
        if request.method == "POST":
            assert request.headers["x-upsert"] == "false"
            assert request.url.path not in objects
            objects[request.url.path] = request.content
            return httpx.Response(201)
        key = request.url.path.replace("/authenticated", "")
        return (
            httpx.Response(200, content=objects[key])
            if key in objects
            else httpx.Response(400, json={"statusCode": "404", "error": "not_found"})
        )

    with SupabaseStore(
        config(tmp_path, supabase_storage_enabled=True), transport=httpx.MockTransport(handle)
    ) as store:
        store.put(checksum, content)
        store.put(checksum, content)
    assert calls.count("POST") == 1


@pytest.mark.parametrize("status", [301, 401, 403, 429, 500])
def test_failures_do_not_follow_redirects_or_expose_provider_messages(tmp_path, status):
    with SupabaseStore(
        config(tmp_path, supabase_storage_enabled=True),
        transport=httpx.MockTransport(
            lambda _: httpx.Response(
                status,
                headers={"Location": "https://untrusted.invalid"},
                text="synthetic-storage-unit-key",
            )
        ),
    ) as store:
        with pytest.raises(StorageError) as exc:
            store.get("a" * 64)
        assert "synthetic-storage-unit-key" not in str(exc.value)
        assert exc.value.__context__ is None


def test_corrupt_existing_remote_bytes_are_never_replaced(tmp_path):
    content = b"expected"
    checksum = hashlib.sha256(content).hexdigest()
    calls = []

    def handler(request):
        calls.append(request.method)
        return httpx.Response(200, content=b"corrupt")

    with SupabaseStore(
        config(tmp_path, supabase_storage_enabled=True), transport=httpx.MockTransport(handler)
    ) as store:
        with pytest.raises(StorageIntegrityError):
            store.put(checksum, content)
    assert calls == ["GET"]


def test_download_and_object_names_are_bounded(tmp_path):
    with SupabaseStore(
        config(tmp_path, supabase_storage_enabled=True),
        transport=httpx.MockTransport(lambda _: httpx.Response(200, content=b"x" * 100)),
    ) as store:
        with pytest.raises(StorageError, match="bounds"):
            store.get("a" * 64, limit=10)
        with pytest.raises(StorageError, match="checksum"):
            store.get("../other")


@pytest.mark.parametrize(
    "url",
    [
        "http://project.supabase.co",
        "https://evil.invalid",
        "https://project.supabase.co/path",
        "https://user:pass@project.supabase.co",
    ],
)
def test_enabled_store_restricts_hosts_and_requires_credentials(tmp_path, url):
    with pytest.raises(ValidationError):
        config(tmp_path, supabase_storage_enabled=True).model_validate(
            config(tmp_path).model_dump() | {"supabase_storage_enabled": True, "supabase_url": url}
        )


def test_local_corruption_never_falls_back_and_foreign_locator_uses_only_pin(tmp_path, monkeypatch):
    content = b"evidence"
    checksum = hashlib.sha256(content).hexdigest()
    settings = config(tmp_path, supabase_storage_enabled=True)
    calls = []

    def get(self, digest, **kwargs):
        calls.append(digest)
        return content

    monkeypatch.setattr(SupabaseStore, "get", get)
    assert source_bytes("Z:/foreign/file", checksum, settings, ReadBudget()) == content
    assert calls == [checksum]
    settings.raw_data_dir.mkdir()
    path = settings.raw_data_dir / "source"
    path.write_bytes(b"corrupt")
    with pytest.raises(AuditFailure) as exc:
        source_bytes(str(path), checksum, settings, ReadBudget())
    assert exc.value.mismatch
    assert calls == [checksum]


def test_disabled_storage_never_performs_network_io(tmp_path, monkeypatch):
    monkeypatch.setattr(httpx.Client, "stream", lambda *a, **k: pytest.fail("network I/O"))
    with pytest.raises(AuditFailure):
        source_bytes(str(tmp_path / "raw/missing"), "a" * 64, config(tmp_path), ReadBudget())


def test_storage_env_file_is_private():
    import subprocess

    assert (
        subprocess.run(["git", "check-ignore", ".env.supabase"], capture_output=True).returncode
        == 0
    )
