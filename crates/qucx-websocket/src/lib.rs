use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message as WsMessage;

use qucx_core::{
    ConnectionId, ConnectionSink, Context, Error, Handler, Message, ProtocolKind, ProtocolPlugin,
    Result,
};

static CONNECTION_COUNTER: AtomicU64 = AtomicU64::new(1);

pub struct WebSocketPlugin;

impl WebSocketPlugin {
    pub fn new() -> Self {
        WebSocketPlugin
    }
}

impl Default for WebSocketPlugin {
    fn default() -> Self {
        Self::new()
    }
}

struct WsSink {
    id: ConnectionId,
    tx: mpsc::Sender<Bytes>,
}

#[async_trait]
impl ConnectionSink for WsSink {
    fn id(&self) -> ConnectionId {
        self.id
    }

    fn protocol(&self) -> ProtocolKind {
        ProtocolKind::WebSocket
    }

    async fn send(&self, data: Bytes) -> Result<()> {
        self.tx.send(data).await.map_err(|_| Error::ConnectionClosed)
    }

    async fn close(&self) -> Result<()> {
        Ok(())
    }
}

async fn handle_connection(
    stream: tokio::net::TcpStream,
    handler: Handler,
) -> Result<()> {
    let id = CONNECTION_COUNTER.fetch_add(1, Ordering::Relaxed);
    let ws_stream = tokio_tungstenite::accept_async(stream)
        .await
        .map_err(|e| Error::Protocol(e.to_string()))?;

    let (mut ws_sender, mut ws_receiver) = ws_stream.split();
    let (tx, mut rx) = mpsc::channel::<Bytes>(64);

    let sink = Arc::new(WsSink { id, tx });

    // Spawn writer task
    tokio::spawn(async move {
        while let Some(data) = rx.recv().await {
            if let Err(e) = ws_sender.send(WsMessage::Binary(data)).await {
                eprintln!("[qucx-websocket] send error: {e}");
                break;
            }
        }
    });

    while let Some(msg) = ws_receiver.next().await {
        let msg = msg.map_err(|e| Error::Protocol(e.to_string()))?;
        let data = match msg {
            WsMessage::Binary(b) => b,
            WsMessage::Text(t) => Bytes::from(t),
            WsMessage::Close(_) => break,
            _ => continue,
        };
        let message = Message {
            id,
            protocol: ProtocolKind::WebSocket,
            data,
        };
        let ctx = Context {
            message,
            sender: Arc::clone(&sink) as Arc<dyn ConnectionSink>,
        };
        let handler = handler.clone();
        tokio::spawn(async move {
            let _ = handler(ctx).await;
        });
    }
    Ok(())
}

#[async_trait]
impl ProtocolPlugin for WebSocketPlugin {
    fn kind(&self) -> ProtocolKind {
        ProtocolKind::WebSocket
    }

    async fn serve(&self, addr: SocketAddr, handler: Handler) -> Result<()> {
        let listener = TcpListener::bind(addr).await?;
        loop {
            let (stream, _) = listener.accept().await?;
            let handler = handler.clone();
            tokio::spawn(async move {
                let _ = handle_connection(stream, handler).await;
            });
        }
    }
}
