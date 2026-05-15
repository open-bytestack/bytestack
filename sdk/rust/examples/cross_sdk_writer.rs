use bytestack_sdk::error::Error;
use bytestack_sdk::LocalWriter;

fn seq_bytes(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 256) as u8).collect()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = std::env::args()
        .nth(1)
        .ok_or("usage: cross_sdk_writer <output-dir>")?;

    let mut writer = LocalWriter::open(out_dir, None).await?;
    let mut ids = Vec::new();

    ids.push(writer.put(Vec::new(), "empty.bin", None).await?);
    ids.push(
        writer
            .put(b"hello world".to_vec(), "hello.txt", None)
            .await?,
    );
    ids.push(
        writer
            .put(
                seq_bytes(4076),
                "aligned.bin",
                Some(vec![0, 1, 2, 250, 255]),
            )
            .await?,
    );
    ids.push(
        writer
            .put(
                seq_bytes(6000),
                "unicode-λ.txt",
                Some(br#"{"k":"v"}"#.to_vec()),
            )
            .await?,
    );

    println!("stack_id={}", writer.stack_id());
    println!("header_stack_id={}", writer.header_stack_id());
    println!("total_raw_bytes={}", writer.total_raw_bytes());

    writer.close().await?;

    let late_result = writer.put(b"late".to_vec(), "late.txt", None).await;
    println!(
        "put_after_close={}",
        matches!(late_result, Err(Error::StackClosed))
    );
    for id in ids {
        println!("id={}", id);
    }

    Ok(())
}
