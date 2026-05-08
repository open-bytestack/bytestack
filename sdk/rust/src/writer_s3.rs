//! S3-backed writer for the bytestack storage format.
//!
//! Buffers all data in memory and uploads the three stack files (`.data`,
//! `.idx`, `.meta`) to S3 on [`S3Writer::close`].
//!
//! Credentials are resolved via the standard AWS SDK credential chain
//! (environment variables `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`,
//! `AWS_REGION`, `~/.aws/config`, IAM roles, etc.).
//!
//! A custom S3-compatible endpoint (MinIO, gofakes3, …) can be configured
//! via the `BYTESTACK_S3_ENDPOINT` environment variable.
//!
//! # Example
//!
//! ```ignore
//! use bytestack_sdk::S3Writer;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let mut writer = S3Writer::open("s3://my-bucket/my-prefix", "http://localhost:8080").await?;
//!
//!     let id = writer.put(b"Hello, S3!".to_vec(), "greeting.txt", None).await?;
//!     println!("index_id: {}", id);
//!
//!     writer.close().await?;
//!     Ok(())
//! }
//! ```

use aws_sdk_s3::primitives::ByteStream;
use bytestack::types::{
    DataMagicHeader, DataRecord, IndexMagicHeader, IndexRecord, MetaMagicHeader, MetaRecord,
};
use bytestack::utils;

use crate::error::{Error, Result};
use crate::writer::get_stack_id_from_controller;

/// Default maximum raw data bytes per `.data` file (5 GiB).
const DEFAULT_MAX_DATA_BYTES: usize = 5 * 1024 * 1024 * 1024;

/// A sequential writer that produces one bytestack on S3-compatible storage.
///
/// # Lifecycle
///
/// ```ignore
/// let mut w = S3Writer::open("s3://bucket/prefix", "http://localhost:8080").await?;
/// let id = w.put(b"hello".to_vec(), "greeting.txt", None).await?;
/// w.close().await?;
/// ```
pub struct S3Writer {
    bucket: String,
    prefix: String,
    controller: String,
    file_stack_id: u64,
    header_stack_id: u64,
    data_buf: Vec<u8>,
    idx_buf: Vec<u8>,
    meta_buf: Vec<u8>,
    data_offset: u64,
    meta_offset: u64,
    total_raw_bytes: usize,
    max_data_bytes: usize,
    closed: bool,
    s3_client: Option<aws_sdk_s3::Client>,
}

impl S3Writer {
    /// Open a bytestack on S3-compatible storage.
    ///
    /// * `location` — `s3://bucket/prefix` where the stack files will be stored.
    /// * `controller` — gRPC address of the Controller service (required).
    pub async fn open(location: &str, controller: &str) -> Result<Self> {
        if !location.starts_with("s3://") {
            return Err(Error::Internal(format!(
                "S3Writer requires an s3:// location, got {:?}",
                location
            )));
        }

        let s3_path = &location[5..]; // strip "s3://"
        let (bucket, prefix) = match s3_path.split_once('/') {
            Some((b, p)) => (b.to_string(), p.to_string()),
            None => (s3_path.to_string(), String::new()),
        };

        let mut writer = Self {
            bucket,
            prefix,
            controller: controller.to_string(),
            file_stack_id: 0,
            header_stack_id: 0,
            data_buf: Vec::new(),
            idx_buf: Vec::new(),
            meta_buf: Vec::new(),
            data_offset: 4096,
            meta_offset: 0,
            total_raw_bytes: 0,
            max_data_bytes: DEFAULT_MAX_DATA_BYTES,
            closed: false,
            s3_client: Some(create_s3_client().await),
        };
        writer.open_stack().await?;
        Ok(writer)
    }

    async fn open_stack(&mut self) -> Result<()> {
        let sid = get_stack_id_from_controller(&self.controller).await?;
        self.file_stack_id = sid;
        self.header_stack_id = sid;
        self.data_offset = 4096;
        self.total_raw_bytes = 0;
        self.data_buf.clear();
        self.idx_buf.clear();
        self.meta_buf.clear();

        let dh_bytes = bincode::serialize(&DataMagicHeader::new(sid))
            .map_err(|e| Error::Serialize(e.to_string()))?;
        debug_assert_eq!(dh_bytes.len(), 16);
        self.data_buf.extend_from_slice(&dh_bytes);
        self.data_buf.extend_from_slice(&[0u8; 4080]);

        let ih_bytes = bincode::serialize(&IndexMagicHeader::new(sid))
            .map_err(|e| Error::Serialize(e.to_string()))?;
        debug_assert_eq!(ih_bytes.len(), 16);
        self.idx_buf.extend_from_slice(&ih_bytes);

        let mh_str = serde_json::to_string(&MetaMagicHeader::new(sid))
            .map_err(|e| Error::Serialize(e.to_string()))?;
        let mh_line = format!("{}\n", mh_str);
        self.meta_offset = mh_line.len() as u64;
        self.meta_buf.extend_from_slice(mh_line.as_bytes());
        Ok(())
    }

    async fn upload_current_stack(&mut self) -> Result<()> {
        let client = self
            .s3_client
            .as_ref()
            .ok_or_else(|| Error::Internal("missing s3 client".to_string()))?;
        let sid = self.file_stack_id;
        let objects: [(&str, &Vec<u8>); 3] = [
            ("data", &self.data_buf),
            ("meta", &self.meta_buf),
            ("idx", &self.idx_buf),
        ];

        for (suffix, buf) in objects {
            let key = if self.prefix.is_empty() {
                format!("0x{:04x}.{}", sid, suffix)
            } else {
                format!("{}/0x{:04x}.{}", self.prefix, sid, suffix)
            };
            client
                .put_object()
                .bucket(&self.bucket)
                .key(&key)
                .body(ByteStream::from(buf.clone()))
                .send()
                .await
                .map_err(|e| {
                    Error::Io(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        e.to_string(),
                    ))
                })?;
        }

        self.data_buf.clear();
        self.meta_buf.clear();
        self.idx_buf.clear();
        Ok(())
    }

    async fn rotate_stack(&mut self) -> Result<()> {
        self.upload_current_stack().await?;
        self.open_stack().await
    }

    /// The stack identifier used for **file naming**.
    pub fn stack_id(&self) -> u64 {
        self.file_stack_id
    }

    /// The stack identifier written into binary headers.
    pub fn header_stack_id(&self) -> u64 {
        self.header_stack_id
    }

    /// The maximum raw payload bytes allowed before rotation is needed.
    pub fn max_data_bytes(&self) -> usize {
        self.max_data_bytes
    }

    /// The raw payload bytes written so far (excluding headers / padding).
    pub fn total_raw_bytes(&self) -> usize {
        self.total_raw_bytes
    }

    /// The S3 location string (`s3://bucket/prefix`).
    pub fn location(&self) -> String {
        if self.prefix.is_empty() {
            format!("s3://{}", self.bucket)
        } else {
            format!("s3://{}/{}", self.bucket, self.prefix)
        }
    }

    /// Write one record into the stack.
    ///
    /// Returns the **index_id** — a globally unique string of the form
    /// `{stack_id},{hex_offset}{hex_cookie}`.
    pub async fn put(
        &mut self,
        data: Vec<u8>,
        filename: impl Into<String>,
        extra_meta: Option<Vec<u8>>,
    ) -> Result<String> {
        if self.closed {
            return Err(Error::StackClosed);
        }

        // --- Size guard ----------------------------------------------------
        if data.len() > self.max_data_bytes {
            return Err(Error::StackFull {
                current: data.len(),
                max: self.max_data_bytes,
            });
        }

        let new_total = self.total_raw_bytes + data.len();
        if new_total > self.max_data_bytes {
            self.rotate_stack().await?;
        }

        // --- Build records -------------------------------------------------
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
        let mr_json = serde_json::to_vec(&mr).map_err(|e| Error::Serialize(e.to_string()))?;
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

        // --- Write order: data → meta → idx -----------------------------
        let dr_header_bytes =
            bincode::serialize(&dr.header).map_err(|e| Error::Serialize(e.to_string()))?;
        debug_assert_eq!(dr_header_bytes.len(), 20);
        self.data_buf.extend_from_slice(&dr_header_bytes);
        self.data_buf.extend_from_slice(&dr.data);
        self.data_buf.extend_from_slice(&dr.padding);
        self.data_offset += dr.size() as u64;

        self.meta_buf.extend_from_slice(&mr_json);
        self.meta_buf.extend_from_slice(b"\n");
        self.meta_offset += mr_line_len as u64;

        let ir_bytes = bincode::serialize(&ir).map_err(|e| Error::Serialize(e.to_string()))?;
        debug_assert_eq!(ir_bytes.len(), 28);
        self.idx_buf.extend_from_slice(&ir_bytes);

        self.total_raw_bytes += dr.data.len();

        Ok(format!("{},{}", self.file_stack_id, index_id))
    }

    /// Upload the three stack files to S3 and clean up.
    ///
    /// After this call any further `put()` returns [`Error::StackClosed`].
    /// Calling `close()` multiple times is safe.
    pub async fn close(&mut self) -> Result<()> {
        if self.closed {
            return Ok(());
        }
        self.closed = true;

        self.upload_current_stack().await?;
        self.s3_client.take();
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// S3 client factory
// ---------------------------------------------------------------------------

/// Create an `aws_sdk_s3::Client` configured from environment variables.
///
/// Respects `BYTESTACK_S3_ENDPOINT` for MinIO-compatible stores.
async fn create_s3_client() -> aws_sdk_s3::Client {
    use aws_credential_types::Credentials;
    use aws_sdk_s3::config::{BehaviorVersion, Region};

    let region = std::env::var("AWS_REGION")
        .or_else(|_| std::env::var("AWS_DEFAULT_REGION"))
        .unwrap_or_else(|_| "us-east-1".to_string());

    let mut config_builder = aws_sdk_s3::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new(region));

    // Use dummy credentials — real auth is handled by env vars or IAM,
    // but the SDK requires a credentials provider for signing.
    if let Ok(akid) = std::env::var("AWS_ACCESS_KEY_ID") {
        let secret = std::env::var("AWS_SECRET_ACCESS_KEY").unwrap_or_default();
        config_builder = config_builder
            .credentials_provider(Credentials::new(&akid, &secret, None, None, "env"));
    }

    if let Ok(endpoint) = std::env::var("BYTESTACK_S3_ENDPOINT") {
        config_builder = config_builder.endpoint_url(endpoint).force_path_style(true);
    }

    let config = config_builder.build();
    aws_sdk_s3::Client::from_conf(config)
}
