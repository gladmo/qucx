use qucx::{KcpPlugin, QuicPlugin, Server, TcpPlugin, WebSocketPlugin, WebTransportPlugin};

#[tokio::main]
async fn main() -> qucx::Result<()> {
    // rustls 0.23+ requires an explicit CryptoProvider when multiple backends are
    // available. Install the ring backend before any TLS-using plugin starts.
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("failed to install ring CryptoProvider");

    println!("Starting echo server on all protocols:");
    println!("  TCP           → 0.0.0.0:9001");
    println!("  WebSocket     → 0.0.0.0:9002  (path: /ws)");
    println!("  QUIC          → 0.0.0.0:9003");
    println!("  KCP (UDP)     → 0.0.0.0:9004");
    println!("  WebTransport  → 0.0.0.0:9005  (path: /wt)");

    Server::builder()
        .bind(TcpPlugin::new(), "0.0.0.0:9001")
        .bind(WebSocketPlugin::new(), "0.0.0.0:9002")
        .bind(QuicPlugin::new(), "0.0.0.0:9003")
        .bind(KcpPlugin::new(), "0.0.0.0:9004")
        .bind(WebTransportPlugin::new(), "0.0.0.0:9005")
        .handler(|ctx| async move {
            println!(
                "[{:?}] conn={} {} bytes",
                ctx.message.protocol,
                ctx.message.id,
                ctx.message.data.len()
            );
            ctx.sender.send(ctx.message.data).await
        })
        .run()
        .await
}
