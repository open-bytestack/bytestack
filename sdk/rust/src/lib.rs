//! # bytestack-sdk
//!
//! A standalone Rust SDK for writing [Bytestack](https://github.com/dashjay/bytestack)
//! stacks on the local filesystem.
//!
//! This crate implements the **writer** portion of the Bytestack RFC —
//! opening a local directory, writing records via [`LocalWriter::put`], and
//! closing the stack.  The on-disk format (`.data` / `.idx` / `.meta` files,
//! binary layout, CRC-32C, alignment) is identical to the
//! [reference implementation](https://github.com/dashjay/bytestack/tree/master/core).
//!
//! # Quick start — local mode
//!
//! ```ignore
//! use bytestack_sdk::LocalWriter;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let mut writer = LocalWriter::open("/tmp/my-bytestack", None).await?;
//!
//!     let id = writer.put(b"Hello, world!", "greeting.txt", None).await?;
//!     println!("index_id: {}", id);
//!
//!     writer.close().await?;
//!     Ok(())
//! }
//! ```
//!
//! # Quick start — controller mode
//!
//! ```ignore
//! use bytestack_sdk::LocalWriter;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let mut writer = LocalWriter::open("/tmp/my-bytestack", Some("http://localhost:8080")).await?;
//!
//!     let id = writer.put(b"Hello, world!", "greeting.txt", None).await?;
//!     println!("index_id: {}", id);
//!
//!     writer.close().await?;
//!     Ok(())
//! }
//! ```

pub mod error;
pub mod writer;
pub mod writer_s3;

use error::Result;
pub use writer::LocalWriter;
pub use writer_s3::S3Writer;

pub enum Writer {
    Local(LocalWriter),
    S3(S3Writer),
}

impl Writer {
    pub async fn put(
        &mut self,
        data: Vec<u8>,
        filename: impl Into<String>,
        extra_meta: Option<Vec<u8>>,
    ) -> Result<String> {
        let filename = filename.into();
        match self {
            Writer::Local(writer) => writer.put(data, filename, extra_meta).await,
            Writer::S3(writer) => writer.put(data, filename, extra_meta).await,
        }
    }

    pub async fn close(&mut self) -> Result<()> {
        match self {
            Writer::Local(writer) => writer.close().await,
            Writer::S3(writer) => writer.close().await,
        }
    }
}

pub async fn open_writer(path: &str, controller: Option<&str>) -> Result<Writer> {
    if path.starts_with("s3://") {
        let controller = controller.ok_or_else(|| {
            error::Error::Internal("s3 writers require a controller address".to_string())
        })?;
        return Ok(Writer::S3(S3Writer::open(path, controller).await?));
    }

    Ok(Writer::Local(LocalWriter::open(path, controller).await?))
}
