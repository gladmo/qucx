/// Integration tests for the qucx framework.
///
/// Each test binds a plugin on an ephemeral port, connects a client,
/// sends a message, and verifies the echo response.
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio::time::timeout;

use qucx::{Server, TcpPlugin, WebSocketPlugin};

/// Poll until a TCP connect to `addr` succeeds, meaning the server is ready.
async fn wait_for_tcp_listener(addr: std::net::SocketAddr) {
    for _ in 0..50 {
        if TcpStream::connect(addr).await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("server at {addr} did not become ready in time");
}

/// Bind a `TcpPlugin` echo server on a random port, connect with a raw
/// TCP client, send a framed message, and assert the echo matches.
#[tokio::test]
async fn test_tcp_echo() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener); // release so TcpPlugin can bind the same port

    let (ready_tx, ready_rx) = oneshot::channel::<()>();
    let ready_tx = Arc::new(tokio::sync::Mutex::new(Some(ready_tx)));

    // Spawn the server
    tokio::spawn(async move {
        let ready_tx = ready_tx.clone();
        Server::builder()
            .bind(TcpPlugin::new(), addr)
            .handler(move |ctx| {
                let ready_tx = ready_tx.clone();
                async move {
                    // Signal ready on first message
                    if let Some(tx) = ready_tx.lock().await.take() {
                        let _ = tx.send(());
                    }
                    ctx.sender.send(ctx.message.data).await
                }
            })
            .run()
            .await
            .ok();
    });

    // Wait until the server is actually listening
    wait_for_tcp_listener(addr).await;

    // Connect and send a length-prefixed message
    let payload = b"hello qucx tcp";
    let mut stream = TcpStream::connect(addr).await.unwrap();

    let len = payload.len() as u32;
    stream.write_all(&len.to_be_bytes()).await.unwrap();
    stream.write_all(payload).await.unwrap();

    // Wait for server ready signal (first message received)
    timeout(Duration::from_secs(2), ready_rx).await.unwrap().unwrap();

    // Read the echo back (length-prefixed)
    let mut len_buf = [0u8; 4];
    timeout(Duration::from_secs(2), stream.read_exact(&mut len_buf))
        .await
        .unwrap()
        .unwrap();
    let resp_len = u32::from_be_bytes(len_buf) as usize;

    let mut resp_data = vec![0u8; resp_len];
    timeout(Duration::from_secs(2), stream.read_exact(&mut resp_data))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(resp_data, payload);
}

/// Bind a `WebSocketPlugin` echo server, connect with a WebSocket client,
/// send a binary frame, and assert the echo matches.
#[tokio::test]
async fn test_websocket_echo() {
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::{connect_async, tungstenite::Message as WsMsg};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    let (ready_tx, ready_rx) = oneshot::channel::<()>();
    let ready_tx = Arc::new(tokio::sync::Mutex::new(Some(ready_tx)));

    tokio::spawn(async move {
        let ready_tx = ready_tx.clone();
        Server::builder()
            .bind(WebSocketPlugin::new(), addr)
            .handler(move |ctx| {
                let ready_tx = ready_tx.clone();
                async move {
                    if let Some(tx) = ready_tx.lock().await.take() {
                        let _ = tx.send(());
                    }
                    ctx.sender.send(ctx.message.data).await
                }
            })
            .run()
            .await
            .ok();
    });

    // Wait until the TCP listener for WebSocket is ready
    wait_for_tcp_listener(addr).await;

    let url = format!("ws://{addr}");
    let (mut ws, _) = timeout(Duration::from_secs(2), connect_async(&url))
        .await
        .unwrap()
        .unwrap();

    let payload = Bytes::from_static(b"hello qucx websocket");
    ws.send(WsMsg::Binary(payload.clone())).await.unwrap();

    // Wait for server to process
    timeout(Duration::from_secs(2), ready_rx).await.unwrap().unwrap();

    // Read the echo
    let msg = timeout(Duration::from_secs(2), ws.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();

    let echo_data = match msg {
        WsMsg::Binary(b) => b,
        other => panic!("unexpected message type: {other:?}"),
    };
    assert_eq!(echo_data, payload);
}

/// Verify that multiple protocols can run concurrently with a single handler.
#[tokio::test]
async fn test_multi_protocol_shared_handler() {
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::{connect_async, tungstenite::Message as WsMsg};

    let tcp_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let tcp_addr = tcp_listener.local_addr().unwrap();
    drop(tcp_listener);

    let ws_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ws_addr = ws_listener.local_addr().unwrap();
    drop(ws_listener);

    tokio::spawn(async move {
        Server::builder()
            .bind(TcpPlugin::new(), tcp_addr)
            .bind(WebSocketPlugin::new(), ws_addr)
            .handler(|ctx| async move { ctx.sender.send(ctx.message.data).await })
            .run()
            .await
            .ok();
    });

    // Wait for both servers to be ready before connecting
    wait_for_tcp_listener(tcp_addr).await;
    wait_for_tcp_listener(ws_addr).await;

    // TCP client
    let tcp_payload = b"tcp-msg";
    let mut tcp_stream = TcpStream::connect(tcp_addr).await.unwrap();
    tcp_stream
        .write_all(&(tcp_payload.len() as u32).to_be_bytes())
        .await
        .unwrap();
    tcp_stream.write_all(tcp_payload).await.unwrap();

    let mut len_buf = [0u8; 4];
    timeout(Duration::from_secs(2), tcp_stream.read_exact(&mut len_buf))
        .await
        .unwrap()
        .unwrap();
    let mut echo = vec![0u8; u32::from_be_bytes(len_buf) as usize];
    timeout(Duration::from_secs(2), tcp_stream.read_exact(&mut echo))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(echo, tcp_payload);

    // WebSocket client
    let url = format!("ws://{ws_addr}");
    let (mut ws, _) = timeout(Duration::from_secs(2), connect_async(&url))
        .await
        .unwrap()
        .unwrap();
    let ws_payload = Bytes::from_static(b"ws-msg");
    ws.send(WsMsg::Binary(ws_payload.clone())).await.unwrap();
    let ws_echo = timeout(Duration::from_secs(2), ws.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(ws_echo.into_data(), ws_payload);
}
