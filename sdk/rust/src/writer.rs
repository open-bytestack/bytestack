//! Local directory writer for the bytestack storage format.
//!
//! A [`LocalWriter`] manages one **stack** (`.data` / `.idx` / `.meta` triplet)
//! inside a local directory.  Stack IDs are allocated either from a remote
//! **Controller** (gRPC) or locally via a timestamp.
//!
//! # Local mode (no controller)
//!
//! The file names use the current Unix timestamp as the stack identifier
//! (e.g. `0x{timestamp}.data`).  All binary headers carry `u64::MAX`
//! as the `stack_id` field to mark the data as *generated locally*.
//!
//! Before uploading to S3 the user MUST connect to a Controller, obtain a real
//! `stack_id`, rename the files, and patch the headers.
//!
//! # Controller mode
//!
//! The stack_id is obtained from the Controller's `next_stack_id` RPC.
//! Both file names and binary headers use the real stack_id.

use std::path::{Path, PathBuf};
use std::str::FromStr;

use bytestack::types::{
    DataMagicHeader, DataRecord, IndexMagicHeader, IndexRecord, MetaMagicHeader, MetaRecord,
};
use bytestack::utils;
use proto::controller::controller_client::ControllerClient;
use tokio::io::AsyncWriteExt;
use tonic::transport::Endpoint;

use crate::error::{Error, Result};

/// Sentinel written into magic headers when the stack was created locally
/// (no Controller).  `u64::MAX` = 0xFFFF_FFFF_FFFF_FFFF.
const LOCAL_STACK_ID: u64 = u64::MAX;

/// Default maximum raw data bytes per `.data` file (5 GiB).
const DEFAULT_MAX_DATA_BYTES: usize = 5 * 1024 * 1024 * 1024;

/// A sequential writer that produces one bytestack on the local filesystem.
///
/// # Lifecycle
///
/// ```ignore
/// // Local mode (timestamp-based naming, u64::MAX in headers):
/// let mut w = LocalWriter::open("/tmp/mystack", None).await?;
///
/// // Controller mode (real stack_id from gRPC):
/// let mut w = LocalWriter::open("/tmp/mystack", Some("http://localhost:8080")).await?;
///
/// let id = w.put(b"hello", "greeting.txt", None).await?;
/// w.close().await?;
/// // w.put(...) would return Error::StackClosed
/// ```
pub struct LocalWriter {
    dir: PathBuf,
    /// Stack identifier used for **file naming**.
    file_stack_id: u64,
    /// Stack identifier written into binary headers (`u64::MAX` in local mode).
    header_stack_id: u64,
    data_file: Option<tokio::fs::File>,
    idx_file: Option<tokio::fs::File>,
    meta_file: Option<tokio::fs::File>,
    data_offset: u64,
    meta_offset: u64,
    total_raw_bytes: usize,
    max_data_bytes: usize,
}

impl LocalWriter {
    /// Open (or create) a stack inside `dir`.
    ///
    /// * `controller` — `None` for local mode (timestamp + `u64::MAX`),
    ///   `Some(addr)` to obtain a real stack_id from the Controller gRPC
    ///   service at `addr` (e.g. `"http://localhost:8080"`).
    pub async fn open(path: impl AsRef<Path>, controller: Option<&str>) -> Result<Self> {
        let dir = path.as_ref().to_path_buf();
        tokio::fs::create_dir_all(&dir).await?;

        let (file_stack_id, header_stack_id) = match controller {
            Some(addr) => {
                let sid = get_stack_id_from_controller(addr).await?;
                (sid, sid)
            }
            None => {
                let ts = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_err(|e| Error::Io(std::io::Error::new(std::io::ErrorKind::Other, e)))?
                    .as_secs();
                (ts, LOCAL_STACK_ID)
            }
        };

        let max_data_bytes = DEFAULT_MAX_DATA_BYTES;

        let data_path = dir.join(format!("0x{:04x}.data", file_stack_id));
        let idx_path = dir.join(format!("0x{:04x}.idx", file_stack_id));
        let meta_path = dir.join(format!("0x{:04x}.meta", file_stack_id));

        let mut data_file = tokio::fs::File::create(&data_path).await?;
        let mut idx_file = tokio::fs::File::create(&idx_path).await?;
        let mut meta_file = tokio::fs::File::create(&meta_path).await?;

        // --- Write magic headers ----------------------------------------

        // Data file: 16-byte DataMagicHeader (bincode) + 4080 zero padding.
        let dh_bytes = bincode::serialize(&DataMagicHeader::new(header_stack_id))
            .map_err(|e| Error::Serialize(e.to_string()))?;
        debug_assert_eq!(dh_bytes.len(), 16);
        data_file.write_all(&dh_bytes).await?;
        data_file.write_all(&[0u8; 4080]).await?;

        // Index file: 16-byte IndexMagicHeader (bincode).
        let ih_bytes = bincode::serialize(&IndexMagicHeader::new(header_stack_id))
            .map_err(|e| Error::Serialize(e.to_string()))?;
        debug_assert_eq!(ih_bytes.len(), 16);
        idx_file.write_all(&ih_bytes).await?;

        // Meta file: JSON MetaMagicHeader + newline.
        let mh_str = serde_json::to_string(&MetaMagicHeader::new(header_stack_id))
            .map_err(|e| Error::Serialize(e.to_string()))?;
        let mh_line = format!("{}\n", mh_str);
        let mh_len = mh_line.len();
        meta_file.write_all(mh_line.as_bytes()).await?;

        Ok(Self {
            dir,
            file_stack_id,
            header_stack_id,
            data_file: Some(data_file),
            idx_file: Some(idx_file),
            meta_file: Some(meta_file),
            data_offset: 4096u64,
            meta_offset: mh_len as u64,
            total_raw_bytes: 0,
            max_data_bytes,
        })
    }

    /// The stack identifier used for **file naming** (`0x{stack_id}.{suffix}`).
    ///
    /// In local mode this is a Unix timestamp; in controller mode it is the
    /// real stack_id returned by the Controller.
    pub fn stack_id(&self) -> u64 {
        self.file_stack_id
    }

    /// The stack identifier written into binary headers.
    ///
    /// In local mode this is `u64::MAX`; in controller mode it equals
    /// [`stack_id`](Self::stack_id).
    pub fn header_stack_id(&self) -> u64 {
        self.header_stack_id
    }

    /// The local directory path this writer is writing into.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// The maximum raw payload bytes allowed before rotation is needed.
    pub fn max_data_bytes(&self) -> usize {
        self.max_data_bytes
    }

    /// The raw payload bytes written so far (excluding headers / padding).
    pub fn total_raw_bytes(&self) -> usize {
        self.total_raw_bytes
    }

    /// Write one record into the stack.
    ///
    /// Returns the **index_id** — a globally unique string of the form
    /// `{stack_id},{hex_offset}{hex_cookie}` that can be used later to
    /// retrieve the data via bsserver or the reader SDK.
    pub async fn put(
        &mut self,
        data: Vec<u8>,
        filename: impl Into<String>,
        extra_meta: Option<Vec<u8>>,
    ) -> Result<String> {
        let data_file = self.data_file.as_mut().ok_or(Error::StackClosed)?;
        let idx_file = self.idx_file.as_mut().ok_or(Error::StackClosed)?;
        let meta_file = self.meta_file.as_mut().ok_or(Error::StackClosed)?;

        // --- Size guard ------------------------------------------------
        let new_total = self.total_raw_bytes + data.len();
        if new_total > self.max_data_bytes {
            return Err(Error::StackFull {
                current: new_total,
                max: self.max_data_bytes,
            });
        }

        // --- Build records ---------------------------------------------
        let filename = filename.into();
        let extra = extra_meta.unwrap_or_default();

        let cookie: u32 = rand::random();
        let crc32c = utils::CASTAGNOLI.checksum(&data);

        // MetaRecord is JSON-serialized; pre-compute length for offset tracking.
        let mr = MetaRecord::new(
            utils::current_time(),
            self.data_offset,
            cookie,
            data.len() as u32,
            filename,
            extra,
        );
        let mr_json =
            serde_json::to_vec(&mr).map_err(|e| Error::Serialize(e.to_string()))?;
        let mr_line_len = mr_json.len() + 1; // +1 for trailing newline

        // IndexRecord uses bincode (fixed-size, 28 bytes).
        let ir = IndexRecord::new(
            cookie,
            self.data_offset,
            data.len() as u32,
            self.meta_offset,
            mr_line_len as u32,
        );
        let index_id = ir.index_id();

        // DataRecord handles alignment internally.
        let dr = DataRecord::new(cookie, data.len() as u32, crc32c, data);

        // --- Write order: index → meta → data --------------------------
        // NOTE: matches the reference implementation for on-disk
        // compatibility.  A future revision should reorder to data-first
        // for crash-safety.

        // 1. Index record.
        let ir_bytes =
            bincode::serialize(&ir).map_err(|e| Error::Serialize(e.to_string()))?;
        debug_assert_eq!(ir_bytes.len(), 28);
        idx_file.write_all(&ir_bytes).await?;

        // 2. Meta record (JSON + newline).
        meta_file.write_all(&mr_json).await?;
        meta_file.write_all(b"\n").await?;
        self.meta_offset += mr_line_len as u64;

        // 3. Data record (header + payload + padding).
        let dr_header_bytes =
            bincode::serialize(&dr.header).map_err(|e| Error::Serialize(e.to_string()))?;
        debug_assert_eq!(dr_header_bytes.len(), 20);
        data_file.write_all(&dr_header_bytes).await?;
        data_file.write_all(&dr.data).await?;
        data_file.write_all(&dr.padding).await?;
        self.data_offset += dr.size() as u64;

        self.total_raw_bytes += dr.data.len();

        Ok(format!("{},{}", self.file_stack_id, index_id))
    }

    /// Flush all buffers and close the three stack files.
    ///
    /// After this call the writer is **closed** — any further `put()` returns
    /// [`Error::StackClosed`].  Calling `close()` multiple times is safe (the
    /// second call is a no-op).
    pub async fn close(&mut self) -> Result<()> {
        async fn flush_and_close(f: &mut Option<tokio::fs::File>) -> Result<()> {
            if let Some(file) = f.as_mut() {
                file.flush().await?;
                file.sync_all().await?;
            }
            f.take();
            Ok(())
        }

        flush_and_close(&mut self.data_file).await?;
        flush_and_close(&mut self.idx_file).await?;
        flush_and_close(&mut self.meta_file).await?;

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Controller interaction
// ---------------------------------------------------------------------------

/// Obtain a fresh `stack_id` from the Controller gRPC service at `addr`.
pub(crate) async fn get_stack_id_from_controller(addr: &str) -> Result<u64> {
    let endpoint =
        Endpoint::from_str(addr).map_err(|e| Error::Controller(e.to_string()))?;
    let mut cli = ControllerClient::connect(endpoint)
        .await
        .map_err(|e| Error::Controller(e.to_string()))?;
    let req = tonic::Request::new(());
    let resp = cli
        .next_stack_id(req)
        .await
        .map_err(|e| Error::Controller(e.to_string()))?;
    Ok(resp.get_ref().stack_id)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn tmpdir() -> PathBuf {
        let dir =
            std::env::temp_dir().join(format!("bst_sdk_test_{}", rand::random::<u64>()));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    #[tokio::test]
    async fn test_local_mode_headers_use_u64_max() {
        // Local mode → binary headers should carry u64::MAX,
        // file names should carry the timestamp.
        let dir = tmpdir();
        let mut w = LocalWriter::open(&dir, None).await.unwrap();

        // File stack_id should be a timestamp (reasonably large).
        let sid = w.stack_id();
        assert!(
            sid > 1_700_000_000,
            "expected a Unix timestamp, got {}",
            sid
        );
        // Header stack_id must be u64::MAX.
        assert_eq!(w.header_stack_id(), u64::MAX);

        w.put(b"hello".to_vec(), "t.txt", None).await.unwrap();
        w.close().await.unwrap();

        // Files should be named with the timestamp, not u64::MAX.
        assert!(
            dir.join(format!("0x{:04x}.data", sid)).exists(),
            "data file should use timestamp-based name"
        );
        assert!(
            !dir.join("0xffffffffffffffff.data").exists(),
            "data file must NOT be named with u64::MAX"
        );

        // Verify the binary header actually contains u64::MAX.
        let data_bytes = std::fs::read(dir.join(format!("0x{:04x}.data", sid))).unwrap();
        // bytes 8-15 are stack_id (little-endian).
        let stored: [u8; 8] = data_bytes[8..16].try_into().unwrap();
        assert_eq!(u64::from_le_bytes(stored), u64::MAX);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_put_and_close_local() {
        let dir = tmpdir();
        let mut w = LocalWriter::open(&dir, None).await.unwrap();
        let sid = w.stack_id();

        let id = w
            .put(b"hello world".to_vec(), "test.txt", None)
            .await
            .unwrap();
        assert!(id.starts_with(&format!("{},", sid)));

        let id2 = w
            .put(b"foobar".to_vec(), "bar.txt", Some(b"{}".to_vec()))
            .await
            .unwrap();
        assert!(id2.starts_with(&format!("{},", sid)));
        assert_ne!(id, id2);

        w.close().await.unwrap();

        assert!(dir.join(format!("0x{:04x}.data", sid)).exists());
        assert!(dir.join(format!("0x{:04x}.idx", sid)).exists());
        assert!(dir.join(format!("0x{:04x}.meta", sid)).exists());

        // put() after close must fail.
        let result = w.put(b"nope".to_vec(), "late.txt", None).await;
        assert!(matches!(result, Err(Error::StackClosed)));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_size_limit_enforced() {
        let dir = tmpdir();
        let mut w = LocalWriter::open(&dir, None).await.unwrap();

        w.max_data_bytes = 100;

        let large = vec![0u8; 90];
        w.put(large, "large.txt", None).await.unwrap();

        let overflow = vec![0u8; 20];
        let result = w.put(overflow, "overflow.txt", None).await;
        assert!(matches!(result, Err(Error::StackFull { .. })));

        w.close().await.unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_controller_connect_failure() {
        // Trying to connect to a non-existent Controller should give
        // a Controller error (not panic).
        let dir = tmpdir();
        let result = LocalWriter::open(&dir, Some("http://127.0.0.1:1")).await;
        assert!(result.is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
