//! Integration test for S3Writer using the Go e2e binary.
//!
//! Usage:
//!
//! ```bash
//! # Build the e2e binary first:
//! go build -o /tmp/e2e-server ./sdk/golang/e2e/cmd/e2e/
//!
//! # Run from workspace root:
//! cargo test --package bytestack-sdk --test s3_integration_test -- --nocapture
//! ```

use std::io::BufRead;
use std::process::{Child, Command, Stdio};

use aws_sdk_s3::config::BehaviorVersion;
use bytestack_sdk::S3Writer;

const BUCKET: &str = "bst-test-bucket";

struct E2eFixture {
    child: Child,
    endpoint_url: String,
    controller_addr: String,
}

impl E2eFixture {
    fn start() -> Self {
        let binary =
            std::env::var("E2E_BIN").unwrap_or_else(|_| "/tmp/e2e-server".to_string());

        let mut child = Command::new(&binary)
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("failed to start e2e-server binary; build it with `go build -o /tmp/e2e-server ./sdk/golang/e2e/cmd/e2e/`");

        let stdout = child.stdout.take().expect("e2e stdout not captured");
        let mut reader = std::io::BufReader::new(stdout);
        let mut line = String::new();
        reader
            .read_line(&mut line)
            .expect("failed to read e2e startup line");
        drop(reader); // no longer need stdout

        let info: serde_json::Value =
            serde_json::from_str(&line).expect("failed to parse e2e startup JSON");

        let endpoint_url = info["s3_endpoint"]
            .as_str()
            .expect("missing s3_endpoint")
            .to_string();
        let controller_addr = info["controller_addr"]
            .as_str()
            .expect("missing controller_addr")
            .to_string();

        E2eFixture {
            child,
            endpoint_url,
            controller_addr,
        }
    }

    fn s3_client(&self) -> aws_sdk_s3::Client {
        use aws_credential_types::Credentials;
        use aws_sdk_s3::config::Region;
        let config = aws_sdk_s3::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .endpoint_url(&self.endpoint_url)
            .force_path_style(true)
            .region(Region::new("us-east-1"))
            .credentials_provider(Credentials::new("dummy", "dummy", None, None, "test"))
            .build();
        aws_sdk_s3::Client::from_conf(config)
    }
}

impl Drop for E2eFixture {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_s3_writer_put_and_close() {
    let fixture = E2eFixture::start();
    let client = fixture.s3_client();

    // Create bucket.
    client.create_bucket().bucket(BUCKET).send().await.unwrap();

    // Set env vars for the S3Writer's internal client.
    std::env::set_var("AWS_ACCESS_KEY_ID", "dummy-access-key");
    std::env::set_var("AWS_SECRET_ACCESS_KEY", "dummy-secret-key");
    std::env::set_var("AWS_REGION", "us-east-1");
    std::env::set_var("BYTESTACK_S3_ENDPOINT", &fixture.endpoint_url);

    let location = format!("s3://{}/stacks", BUCKET);
    // tonic needs the http:// scheme prefix.
    let controller = format!("http://{}", fixture.controller_addr);

    let mut w = S3Writer::open(&location, &controller).await.unwrap();
    assert_eq!(w.location(), location);

    let sid = w.stack_id();
    // Mock controller returns sequential IDs starting from 1.
    assert_eq!(sid, 1, "expected stack_id=1, got {}", sid);

    // --- Write two records --------------------------------------------------
    let id1 = w
        .put(b"hello s3".to_vec(), "greeting.txt", None)
        .await
        .unwrap();
    assert!(id1.starts_with(&format!("{},", sid)));

    let id2 = w
        .put(
            b"second record".to_vec(),
            "second.txt",
            Some(b"{\"k\":\"v\"}".to_vec()),
        )
        .await
        .unwrap();
    assert!(id2.starts_with(&format!("{},", sid)));
    assert_ne!(id1, id2, "two puts must return different index_ids");

    w.close().await.unwrap();

    // --- Verify objects exist ----------------------------------------------
    for suffix in ["data", "idx", "meta"] {
        let key = format!("stacks/0x{sid:04x}.{suffix}");
        let result = client
            .head_object()
            .bucket(BUCKET)
            .key(&key)
            .send()
            .await;
        assert!(
            result.is_ok(),
            "object s3://{}/{} not found: {:?}",
            BUCKET,
            key,
            result.err()
        );
    }

    // --- Verify headers contain real stack_id (not u64::MAX) ---------------
    for suffix in ["data", "idx"] {
        let key = format!("stacks/0x{sid:04x}.{suffix}");
        let obj = client
            .get_object()
            .bucket(BUCKET)
            .key(&key)
            .send()
            .await
            .unwrap();
        let body = obj.body.collect().await.unwrap().into_bytes();
        let hdr = &body[..16];
        let stored_id = u64::from_le_bytes(hdr[8..16].try_into().unwrap());
        assert_eq!(
            stored_id, sid,
            "{} header stack_id = {:#x}, want {:#x}",
            suffix, stored_id, sid
        );
    }

    // --- Verify meta file header -------------------------------------------
    let meta_key = format!("stacks/0x{sid:04x}.meta");
    let meta_obj = client
        .get_object()
        .bucket(BUCKET)
        .key(&meta_key)
        .send()
        .await
        .unwrap();
    let meta_body = meta_obj.body.collect().await.unwrap().into_bytes();
    // Meta file is JSON-lines; first line is the header.
    let first_newline = meta_body.iter().position(|&b| b == b'\n').unwrap();
    let meta_header: serde_json::Value =
        serde_json::from_slice(&meta_body[..first_newline]).unwrap();
    assert_eq!(
        meta_header["stack_id"].as_u64().unwrap(),
        sid,
        "meta header stack_id mismatch"
    );

    // --- Put after close must fail -----------------------------------------
    let result = w.put(b"late".to_vec(), "late.txt", None).await;
    assert!(
        result.is_err(),
        "put after close should have returned an error"
    );
}
