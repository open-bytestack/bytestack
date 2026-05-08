"""Bytestack SDK error types."""


class Error(Exception):
    """Base error type for Bytestack SDK operations."""


class StackClosed(Error):
    """Attempted to call put() after close()."""


class StackFull(Error):
    """Data file would exceed the configured size limit."""

    def __init__(self, current: int, max_size: int) -> None:
        self.current = current
        self.max_size = max_size
        super().__init__(f"stack full: {current} bytes >= {max_size} byte limit")


class SerializeError(Error):
    """Serialization failure."""


class MagicMismatch(Error):
    """Magic number validation failure."""


class ChecksumMismatch(Error):
    """CRC-32C mismatch."""


class StackIncomplete(Error):
    """One or more stack files are missing."""


class ControllerError(Error):
    """Controller gRPC error (connection or allocation)."""


class Internal(Error):
    """Unexpected internal condition."""


class InvalidStack(StackIncomplete):
    """Backward-compatible alias for incomplete stack errors."""
