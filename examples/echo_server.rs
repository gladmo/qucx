use qucx::{KcpPlugin, QuicPlugin, Server, TcpPlugin, WebSocketPlugin, WebTransportPlugin};

#[tokio::main]
async fn main() -> qucx::Result<()> {
    println!("Starting echo server on all protocols:");
    println!("  TCP           → 0.0.0.0:8080");
    println!("  WebSocket     → 0.0.0.0:8081");
    println!("  QUIC          → 0.0.0.0:8443");
    println!("  KCP (UDP)     → 0.0.0.0:9090");
    println!("  WebTransport  → 0.0.0.0:4433");

    Server::builder()
        .bind(TcpPlugin::new(), "0.0.0.0:8080")
        .bind(WebSocketPlugin::new(), "0.0.0.0:8081")
        .bind(QuicPlugin::new(), "0.0.0.0:8443")
        .bind(KcpPlugin::new(), "0.0.0.0:9090")
        .bind(WebTransportPlugin::new(), "0.0.0.0:4433")
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
