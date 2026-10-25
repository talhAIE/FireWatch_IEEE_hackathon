"""Process-wide log record redaction, including third-party exception/access logs."""

import logging
import re
import traceback
from collections.abc import Callable
from threading import RLock
from typing import Any
from urllib.parse import quote, quote_plus

from sqlalchemy.engine import make_url

from firewatch.config import Settings

_lock = RLock()
_registrations: dict[object, tuple[str, ...]] = {}
_previous: Any = None


def redact(message: str, values: tuple[str, ...] = ()) -> str:
    for value in sorted(set(values), key=len, reverse=True):
        message = message.replace(value, "[redacted]")
    message = re.sub(r"(?i)\b(?:Bearer|Basic)\s+[^\s,'\"}\]]+", "Bearer [redacted]", message)
    message = re.sub(
        r"(?i)(?:https?://|postgres(?:ql)?(?:\+psycopg)?://)[^\s'\"<>]+", "[redacted-url]", message
    )
    message = re.sub(
        r"(?i)/api/(?:area|country|data_availability)/[^\s'\"<>]+", "[redacted-firms-path]", message
    )
    message = re.sub(
        r"(?i)\b(?:authorization|admin_api_token|firms_map_key|map_key|chain_private_key|private_key|password|database_url)"
        r"['\"]?\s*[:=]\s*['\"]?[^\s,'\"}\]]+",
        "credential=[redacted]",
        message,
    )
    return message


def _factory(*args: Any, **kwargs: Any) -> logging.LogRecord:
    with _lock:
        original = _previous
        values = tuple(value for group in _registrations.values() for value in group)
    record: logging.LogRecord = original(*args, **kwargs)
    # Uvicorn's access formatter consumes the original five arguments. Preserve
    # their types/shape, but remove the entire untrusted request target and queries.
    if record.name == "uvicorn.access" and isinstance(record.args, tuple) and len(record.args) == 5:
        client, method, _, version, status = record.args
        record.args = (
            redact(str(client), values),
            redact(str(method), values),
            "[request-target-redacted]",
            redact(str(version), values),
            status,
        )
        record.msg = redact(str(record.msg), values)
    else:
        record.msg = redact(record.getMessage(), values)
        record.args = ()
    if record.exc_info:
        record.exc_text = redact("".join(traceback.format_exception(*record.exc_info)), values)
        record.exc_info = None
    if record.stack_info:
        record.stack_info = redact(record.stack_info, values)
    return record


def install_redaction(settings: Settings) -> Callable[[], None]:
    """Register each app lifetime; restore the previous factory after the last app."""
    global _previous
    raw = [settings.admin_api_token.get_secret_value(), settings.database_url.get_secret_value()]
    password = make_url(settings.database_url.get_secret_value()).password
    if password:
        raw.append(password)
    for secret in (
        settings.firms_map_key,
        settings.chain_private_key,
        settings.postgres_password,
        settings.supabase_secret_key,
    ):
        if secret is not None:
            raw.append(secret.get_secret_value())
    if settings.chain_private_key is not None:
        key = settings.chain_private_key.get_secret_value().removeprefix("0x").removeprefix("0X")
        raw.extend([key.lower(), key.upper(), "0x" + key.lower(), "0X" + key.upper()])
    return install_secret_redaction(tuple(raw))


def install_secret_redaction(raw: tuple[str, ...]) -> Callable[[], None]:
    """Protect standalone provider clients without requiring DB/operator settings."""
    global _previous
    values = tuple(
        {
            variant
            for value in raw
            if value
            for variant in (value, quote(value, safe=""), quote_plus(value))
        }
    )
    identifier = object()
    with _lock:
        if not _registrations:
            _previous = logging.getLogRecordFactory()
            logging.setLogRecordFactory(_factory)
        _registrations[identifier] = values

    def close() -> None:
        with _lock:
            _registrations.pop(identifier, None)
            if not _registrations and logging.getLogRecordFactory() is _factory:
                logging.setLogRecordFactory(_previous)

    return close
