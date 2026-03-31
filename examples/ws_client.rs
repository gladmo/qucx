//! WebSocket echo client example.
//!
//! Connects to the qucx echo server on port 9002 at path /ws, sends a binary
//! WebSocket message, and prints the echoed reply.
//!
//! Run the echo server first:
//!   cargo run --example echo_server
//!
//! Then in another terminal:
//!   cargo run --example ws_client

use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::{connect_async, tungstenite::Message as WsMsg};

#[tokio::main]
async fn main() {
    let url = "ws://127.0.0.1:9002/ws";
    println!("[ws_client] connecting to {url}");

    let (mut ws, _) = connect_async(url).await.expect("WebSocket connect failed");

    let payload = bytes::Bytes::from_static(b"Hello from WebSocket client!");
    ws.send(WsMsg::Binary(payload.clone())).await.unwrap();
    println!("[ws_client] sent {} bytes", payload.len());

    // Receive echoed message
    if let Some(Ok(msg)) = ws.next().await {
        let data = msg.into_data();
        println!(
            "[ws_client] echo received: {:?}",
            String::from_utf8_lossy(&data)
        );
    }

    ws.close(None).await.ok();
}
