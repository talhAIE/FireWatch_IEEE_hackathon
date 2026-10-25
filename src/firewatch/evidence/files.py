"""Bounded reads from trusted local artifact roots."""

import stat
import time
from dataclasses import dataclass, field
from pathlib import Path

MAX_FILE_BYTES = 64 * 1024 * 1024
MAX_TOTAL_BYTES = 256 * 1024 * 1024


class AuditFailure(Exception):
    def __init__(self, reason: str, *, mismatch: bool = False) -> None:
        self.reason = reason
        self.mismatch = mismatch


@dataclass
class ReadBudget:
    remaining: int = MAX_TOTAL_BYTES
    remote_deadline: float = field(default_factory=lambda: time.monotonic() + 30)

    def read(self, path: Path, roots: list[Path], limit: int = MAX_FILE_BYTES) -> bytes:
        """Restrict server-owned paths before opening; bound actual reads, not just stat size."""
        try:
            absolute = path.absolute()
            if ".." in path.parts or any(
                p.is_symlink() or p.is_junction() for p in (absolute, *absolute.parents)
            ):
                raise AuditFailure("unsafe_path")
            resolved = absolute.resolve()
            if not any(resolved.is_relative_to(root.resolve()) for root in roots):
                raise AuditFailure("unsafe_path")
            info = resolved.stat()
            if not stat.S_ISREG(info.st_mode):
                raise AuditFailure("unsafe_path")
            allowed = min(limit, self.remaining)
            if info.st_size > allowed:
                raise AuditFailure("size_limit")
            with resolved.open("rb") as stream:
                content = stream.read(allowed + 1)
            self.remaining -= len(content)
            if len(content) > allowed:
                raise AuditFailure("size_limit")
            return content
        except FileNotFoundError:
            raise AuditFailure("missing_file") from None
        except (OSError, ValueError, RuntimeError):
            raise AuditFailure("read_failed") from None
