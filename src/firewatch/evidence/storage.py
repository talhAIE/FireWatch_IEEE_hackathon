"""Private, content-addressed source storage. No caller URLs, public links or overwrites."""

import hashlib
import re
import time
from pathlib import Path

import httpx

from firewatch.config import Settings
from firewatch.evidence.files import MAX_FILE_BYTES, AuditFailure, ReadBudget
from firewatch.logging import install_secret_redaction


class StorageError(RuntimeError):
    """Fixed diagnostics only: never retain a request, credential or response body."""


class StorageIntegrityError(StorageError):
    """Existing evidence bytes disagree with their immutable name."""


class SupabaseStore:
    def __init__(self, settings: Settings, *, transport: httpx.BaseTransport | None = None):
        if not settings.supabase_storage_enabled or not settings.supabase_secret_key:
            raise StorageError("Private evidence storage is not configured")
        self.bucket = settings.supabase_bucket
        self._redact = install_secret_redaction((settings.supabase_secret_key.get_secret_value(),))
        self.client = httpx.Client(
            base_url=str(settings.supabase_url),
            headers={"apikey": settings.supabase_secret_key.get_secret_value()},
            timeout=httpx.Timeout(15, connect=5),
            follow_redirects=False,
            transport=transport,
        )

    def close(self) -> None:
        self.client.close()
        self._redact()

    def __enter__(self) -> "SupabaseStore":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def _path(self, checksum: str, *, download: bool) -> str:
        if not re.fullmatch(r"[0-9a-f]{64}", checksum):
            raise StorageError("Invalid evidence checksum")
        route = "authenticated/" if download else ""
        return f"/storage/v1/object/{route}{self.bucket}/sources/{checksum}.bin"

    def get(
        self, checksum: str, *, limit: int = MAX_FILE_BYTES, deadline: float | None = None
    ) -> bytes | None:
        path = self._path(checksum, download=True)
        started = time.monotonic()
        deadline = min(deadline, started + 30) if deadline is not None else started + 30
        if deadline <= started:
            raise StorageError("Private evidence download exceeded its bounds")
        try:
            with self.client.stream("GET", path, timeout=min(15, deadline - started)) as response:
                if response.status_code == 404:
                    return None
                if response.status_code == 400:
                    # Storage's REST gateway can wrap a 404 in HTTP 400.
                    body = b""
                    for chunk in response.iter_bytes():
                        body += chunk
                        if len(body) > 65536 or time.monotonic() > deadline:
                            raise StorageError("Private evidence download failed")
                    import json

                    try:
                        if str(json.loads(body).get("statusCode")) == "404":
                            return None
                    except (ValueError, AttributeError):
                        pass
                if response.status_code != 200:
                    raise StorageError("Private evidence download failed")
                chunks, size = [], 0
                for chunk in response.iter_bytes():
                    size += len(chunk)
                    if size > limit or time.monotonic() > deadline:
                        raise StorageError("Private evidence download exceeded its bounds")
                    chunks.append(chunk)
                content = b"".join(chunks)
        except httpx.HTTPError:
            raise StorageError("Private evidence transport failed") from None
        if hashlib.sha256(content).hexdigest() != checksum:
            raise StorageIntegrityError("Private evidence checksum mismatch")
        return content

    def put(self, checksum: str, content: bytes) -> None:
        path = self._path(checksum, download=False)
        if len(content) > 32 * 1024 * 1024 or hashlib.sha256(content).hexdigest() != checksum:
            raise StorageError("Private evidence upload bytes are invalid")
        if self.get(checksum) is not None:
            return
        try:
            # Duplicate/racing writes are checked by downloading the existing bytes.
            # Never use upsert: a corrupt object must not be repaired silently.
            with self.client.stream(
                "POST",
                path,
                content=content,
                headers={"Content-Type": "application/octet-stream", "x-upsert": "false"},
            ) as response:
                if response.status_code not in {200, 201, 400, 409}:
                    raise StorageError("Private evidence upload failed")
        except httpx.HTTPError:
            raise StorageError("Private evidence transport failed") from None
        if self.get(checksum) is None:
            raise StorageError("Private evidence upload could not be verified")

    def check_bucket(self) -> bool:
        """Read configuration only; no object upload or database write."""
        try:
            response = self.client.get(f"/storage/v1/bucket/{self.bucket}")
            if response.status_code != 200 or len(response.content) > 65536:
                return False
            data = response.json()
            return bool(data.get("id") == self.bucket and data.get("public") is False)
        except (httpx.HTTPError, ValueError, AttributeError):
            return False


def source_bytes(
    path: str, checksum: str, settings: Settings, budget: ReadBudget, *, remote: bool = True
) -> bytes:
    """Prefer trusted local bytes; remote fallback is only for absent bytes, never corruption."""
    if not re.fullmatch(r"[0-9a-f]{64}", checksum):
        raise AuditFailure("metadata_mismatch", mismatch=True)
    roots = [settings.raw_data_dir, settings.replay_bundle_dir]
    original = Path(path)
    content = None
    if settings.source_artifact_dir is not None:
        mirror = settings.source_artifact_dir / f"{checksum}.bin"
        try:
            content = budget.read(mirror, [settings.source_artifact_dir])
        except AuditFailure as exc:
            if exc.reason != "missing_file":
                raise
    if content is None:
        try:
            content = budget.read(original, roots)
        except AuditFailure as exc:
            # Foreign workstation locators are never followed on this host.
            if (
                not settings.supabase_storage_enabled
                and settings.source_artifact_dir is not None
                and exc.reason == "unsafe_path"
            ):
                raise AuditFailure("missing_file") from None
            if (
                not remote
                or not settings.supabase_storage_enabled
                or exc.reason not in {"missing_file", "unsafe_path"}
            ):
                raise
            try:
                with SupabaseStore(settings) as store:
                    content = store.get(
                        checksum,
                        limit=min(MAX_FILE_BYTES, budget.remaining),
                        deadline=budget.remote_deadline,
                    )
            except StorageIntegrityError:
                raise AuditFailure("checksum_mismatch", mismatch=True) from None
            except StorageError:
                raise AuditFailure("read_failed") from None
            if content is None:
                raise AuditFailure("missing_file") from None
            budget.remaining -= len(content)
    if hashlib.sha256(content).hexdigest() != checksum:
        raise AuditFailure("checksum_mismatch", mismatch=True)
    return content


def preserve_source(path: str, checksum: str, settings: Settings) -> None:
    if settings.supabase_storage_enabled:
        content = source_bytes(path, checksum, settings, ReadBudget(), remote=False)
        with SupabaseStore(settings) as store:
            store.put(checksum, content)
