use std::fmt;

/// Error types for the bytestack SDK writer.
#[derive(Debug)]
pub enum Error {
    /// Underlying filesystem I/O failure.
    Io(std::io::Error),
    /// Serialization failure (bincode / JSON).
    Serialize(String),
    /// Attempted to call `put()` after `close()`.
    StackClosed,
    /// Data file would exceed the configured size limit.
    StackFull {
        /// Accumulated raw data bytes so far.
        current: usize,
        /// Maximum allowed raw data bytes.
        max: usize,
    },
    /// Invalid or missing stack files in the directory.
    InvalidStack(String),
    /// Controller gRPC error (connection or allocation).
    Controller(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(e) => write!(f, "I/O error: {}", e),
            Error::Serialize(e) => write!(f, "serialization error: {}", e),
            Error::StackClosed => write!(f, "stack is closed, no further writes allowed"),
            Error::StackFull { current, max } => {
                write!(f, "stack full: {} bytes >= {} byte limit", current, max)
            }
            Error::InvalidStack(e) => write!(f, "invalid stack: {}", e),
            Error::Controller(e) => write!(f, "controller error: {}", e),
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
