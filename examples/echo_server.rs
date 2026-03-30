use qucx::{Server, TcpPlugin, WebSocketPlugin};

#[tokio::main]
async fn main() -> qucx::Result<()> {
    Server::builder()
        .bind(TcpPlugin::new(), "0.0.0.0:8080")
        .bind(WebSocketPlugin::new(), "0.0.0.0:8081")
        .handler(|ctx| async move {
            ctx.sender.send(ctx.message.data).await
        })
        .run()
        .await
}
