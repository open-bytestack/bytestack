use std::fmt;

/// Error types for the bytestack SDK writer.
#[derive(Debug)]
pub enum Error {
    /// Underlying filesystem I/O failure.
    Io(std::io::Error),
    /// Serialization failure (bincode / JSON).
    Serialize(String),
    /// Magic number validation failure.
    MagicMismatch {
        expected: u64,
        got: u64,
        context: &'static str,
    },
    /// CRC-32C validation failure.
    ChecksumMismatch { expected: u32, actual: u32 },
    /// Attempted to call `put()` after `close()`.
    StackClosed,
    /// One or more stack files are missing.
    StackIncomplete(String),
    /// Data file would exceed the configured size limit.
    StackFull {
        /// Accumulated raw data bytes so far.
        current: usize,
        /// Maximum allowed raw data bytes.
        max: usize,
    },
    /// Controller gRPC error (connection or allocation).
    Controller(String),
    /// Unexpected internal condition.
    Internal(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(e) => write!(f, "I/O error: {}", e),
            Error::Serialize(e) => write!(f, "serialization error: {}", e),
            Error::MagicMismatch {
                expected,
                got,
                context,
            } => write!(
                f,
                "magic mismatch in {}: expected {}, got {}",
                context, expected, got
            ),
            Error::ChecksumMismatch { expected, actual } => write!(
                f,
                "checksum mismatch: expected {:#x}, got {:#x}",
                expected, actual
            ),
            Error::StackClosed => write!(f, "stack is closed, no further writes allowed"),
            Error::StackIncomplete(e) => write!(f, "stack incomplete: {}", e),
            Error::StackFull { current, max } => {
                write!(f, "stack full: {} bytes >= {} byte limit", current, max)
            }
            Error::Controller(e) => write!(f, "controller error: {}", e),
            Error::Internal(e) => write!(f, "internal error: {}", e),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io(e)
    }
}

/// Convenience alias.
pub type Result<T> = std::result::Result<T, Error>;
