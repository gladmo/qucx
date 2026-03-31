//! Connection registry example.
//!
//! Starts the echo server on all five protocols and maintains a registry of
//! every active connection.  A background task sends a "ping" heartbeat to
//! every registered connection every 20 seconds and removes any connection
//! that fails to accept the write (i.e. the peer has disconnected).
//!
//! Run this example in one terminal:
//!   cargo run --example connection_registry
//!
//! Then connect with any of the per-protocol client examples:
//!   cargo run --example tcp_client
//!   cargo run --example ws_client
//!   cargo run --example quic_client
//!   cargo run --example kcp_client

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::Mutex;

use qucx::{
    ConnectionId, ConnectionSink, Context, KcpPlugin, QuicPlugin, Result, Server, TcpPlugin,
    WebSocketPlugin, WebTransportPlugin,
};

/// Shared registry: maps each active connection ID to its send handle.
type Registry = Arc<Mutex<HashMap<ConnectionId, Arc<dyn ConnectionSink>>>>;

/// Background heartbeat task.
///
/// Every `interval` seconds it sends a small "ping" payload to every
/// registered connection.  Any connection whose `send()` returns an error is
/// considered dead and is removed from the registry.
async fn heartbeat_task(registry: Registry, interval: Duration) {
    let ping_payload = Bytes::from_static(b"ping");
    loop {
        tokio::time::sleep(interval).await;

        let mut reg = registry.lock().await;
        let snapshot: Vec<(ConnectionId, Arc<dyn ConnectionSink>)> =
            reg.iter().map(|(id, sink)| (*id, Arc::clone(sink))).collect();

        let mut dead: Vec<ConnectionId> = Vec::new();
        for (id, sink) in snapshot {
            if sink.send(ping_payload.clone()).await.is_err() {
                dead.push(id);
            }
        }

        for id in &dead {
            reg.remove(id);
            println!("[registry] connection {id} removed (heartbeat failed)");
        }

        println!(
            "[registry] heartbeat complete — {} active connection(s)",
            reg.len()
        );
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // rustls 0.23+ requires an explicit CryptoProvider when multiple backends
    // are compiled in.  Install the ring backend before any plugin starts.
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("failed to install ring CryptoProvider");

    let registry: Registry = Arc::new(Mutex::new(HashMap::new()));

    // Spawn the heartbeat background task (20-second interval).
    let registry_hb = Arc::clone(&registry);
    tokio::spawn(heartbeat_task(registry_hb, Duration::from_secs(20)));

    println!("Starting connection-registry server on all protocols:");
    println!("  TCP           → 0.0.0.0:9001");
    println!("  WebSocket     → 0.0.0.0:9002  (path: /ws)");
    println!("  QUIC          → 0.0.0.0:9003");
    println!("  KCP (UDP)     → 0.0.0.0:9004");
    println!("  WebTransport  → 0.0.0.0:9005  (path: /wt)");
    println!("Heartbeat interval: 20 s");

    Server::builder()
        .bind(TcpPlugin::new(), "0.0.0.0:9001")
        .bind(WebSocketPlugin::new(), "0.0.0.0:9002")
        .bind(QuicPlugin::new(), "0.0.0.0:9003")
        .bind(KcpPlugin::new(), "0.0.0.0:9004")
        .bind(WebTransportPlugin::new(), "0.0.0.0:9005")
        .handler(move |ctx: Context| {
            let registry = Arc::clone(&registry);
            async move {
                let id = ctx.message.id;
                let protocol = ctx.message.protocol;
                let sink = Arc::clone(&ctx.sender);

                // Register the connection the first time we see it.
                {
                    let mut reg = registry.lock().await;
                    if !reg.contains_key(&id) {
                        reg.insert(id, Arc::clone(&sink));
                        println!(
                            "[registry] registered connection {id} ({protocol:?}) — \
                             total: {}",
                            reg.len()
                        );
                    }
                }

                // Echo the payload back to the sender.
                if let Err(e) = ctx.sender.send(ctx.message.data).await {
                    // Send failed — remove from registry.
                    let mut reg = registry.lock().await;
                    if reg.remove(&id).is_some() {
                        println!(
                            "[registry] connection {id} removed (send error: {e}) — \
                             total: {}",
                            reg.len()
                        );
                    }
                }

                Ok(())
            }
        })
        .run()
        .await
}
