from __future__ import annotations

MAX_ERROR_CHARACTERS = 2048


def safe_exception_text(exc: BaseException) -> str:
    """Return a diagnostic identifier without persisting an exception message.

    Exception messages frequently contain URLs, response bodies, credentials, or
    personal data supplied by downstream adapters. The exception type is enough
    for the durable runtime record; detailed diagnostics remain the downstream
    adapter's responsibility and must be logged with its own redaction policy.
    """

    return f"{type(exc).__name__}: operation failed"


def bounded_error_text(error: str) -> str:
    """Bound caller-supplied diagnostic text to prevent unbounded durable records."""

    if len(error) <= MAX_ERROR_CHARACTERS:
        return error
    suffix = "...<truncated>"
    return error[: MAX_ERROR_CHARACTERS - len(suffix)] + suffix
