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

pub use writer::LocalWriter;
