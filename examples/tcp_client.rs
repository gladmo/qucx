//! TCP echo client example.
//!
//! Connects to the qucx echo server on port 8080, sends a message with
//! 4-byte big-endian length-prefix framing, and prints the echoed reply.
//!
//! Run the echo server first:
//!   cargo run --example echo_server
//!
//! Then in another terminal:
//!   cargo run --example tcp_client

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[tokio::main]
async fn main() {
    let addr = "127.0.0.1:8080";
    println!("[tcp_client] connecting to {addr}");

    let mut stream = TcpStream::connect(addr).await.expect("connect failed");

    let payload = b"Hello from TCP client!";
    let len = payload.len() as u32;

    // Send: 4-byte length prefix + payload
    stream.write_all(&len.to_be_bytes()).await.unwrap();
    stream.write_all(payload).await.unwrap();
    println!("[tcp_client] sent {} bytes", payload.len());

    // Receive: 4-byte length prefix + echoed payload
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await.unwrap();
    let resp_len = u32::from_be_bytes(len_buf) as usize;

    let mut resp = vec![0u8; resp_len];
    stream.read_exact(&mut resp).await.unwrap();

    println!(
        "[tcp_client] echo received: {:?}",
        String::from_utf8_lossy(&resp)
    );
}
