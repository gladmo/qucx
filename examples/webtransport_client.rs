//! WebTransport echo client example.
//!
//! Connects to the qucx echo server on port 9005 at path /wt via WebTransport
//! (HTTP/3), sends a datagram, and prints the echoed reply.
//!
//! Certificate verification is disabled because the server uses a self-signed
//! certificate. Do not do this in production.
//!
//! Run the echo server first:
//!   cargo run --example echo_server
//!
//! Then in another terminal:
//!   cargo run --example webtransport_client

use std::time::Duration;

use wtransport::{ClientConfig, Endpoint};

#[tokio::main]
async fn main() {
    let url = "https://127.0.0.1:9005/wt";
    println!("[wt_client] connecting to {url}");

    // Skip certificate verification — the server uses a self-signed cert.
    let config = ClientConfig::builder()
        .with_bind_default()
        .with_no_cert_validation()
        .build();

    let connection = Endpoint::client(config)
        .unwrap()
        .connect(url)
        .await
        .expect("WebTransport connect failed");

    println!("[wt_client] connected");

    // ── Send / receive via datagrams ─────────────────────────────────────────
    let payload = b"Hello from WebTransport client!";
    connection.send_datagram(payload).expect("send_datagram failed");
    println!("[wt_client] sent datagram ({} bytes)", payload.len());

    // Wait for the echo datagram
    let echo = tokio::time::timeout(Duration::from_secs(5), connection.receive_datagram())
        .await
        .expect("timed out waiting for echo datagram")
        .expect("receive_datagram error");

    println!(
        "[wt_client] echo received (datagram): {:?}",
        String::from_utf8_lossy(&echo)
    );

    // ── Send / receive via bidirectional stream ───────────────────────────────
    let (mut send, mut recv) = connection
        .open_bi()
        .await
        .expect("open_bi failed")
        .await
        .expect("open_bi await failed");

    let stream_payload = b"Hello via bidirectional stream!";
    send.write_all(stream_payload).await.expect("stream write failed");
    println!("[wt_client] sent stream data ({} bytes)", stream_payload.len());

    let mut buf = vec![0u8; 65536];
    match recv.read(&mut buf).await {
        Ok(Some(n)) => println!(
            "[wt_client] echo received (stream): {:?}",
            String::from_utf8_lossy(&buf[..n])
        ),
        Ok(None) => println!("[wt_client] stream closed by server"),
        Err(e) => eprintln!("[wt_client] stream read error: {e}"),
    }

    connection.close(wtransport::VarInt::from_u32(0), b"done");
}
