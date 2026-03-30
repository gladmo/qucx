use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;

use qucx_core::{
    ConnectionId, ConnectionSink, Context, Handler, Message, ProtocolKind, ProtocolPlugin,
    Result, Error,
};

static CONNECTION_COUNTER: AtomicU64 = AtomicU64::new(1);

pub struct TcpPlugin;

impl TcpPlugin {
    pub fn new() -> Self {
        TcpPlugin
    }
}

impl Default for TcpPlugin {
    fn default() -> Self {
        Self::new()
    }
}

struct TcpSink {
    id: ConnectionId,
    writer: Arc<Mutex<tokio::net::tcp::OwnedWriteHalf>>,
}

#[async_trait]
impl ConnectionSink for TcpSink {
    fn id(&self) -> ConnectionId {
        self.id
    }

    fn protocol(&self) -> ProtocolKind {
        ProtocolKind::Tcp
    }

    async fn send(&self, data: Bytes) -> Result<()> {
        let mut writer = self.writer.lock().await;
        let len = data.len() as u32;
        writer.write_all(&len.to_be_bytes()).await?;
        writer.write_all(&data).await?;
        Ok(())
    }

    async fn close(&self) -> Result<()> {
        let mut writer = self.writer.lock().await;
        writer.shutdown().await?;
        Ok(())
    }
}

async fn handle_connection(stream: TcpStream, handler: Handler) -> Result<()> {
    let id = CONNECTION_COUNTER.fetch_add(1, Ordering::Relaxed);
    let (mut reader, writer) = stream.into_split();
    let writer = Arc::new(Mutex::new(writer));
    let sink = Arc::new(TcpSink { id, writer });

    loop {
        let mut len_buf = [0u8; 4];
        match reader.read_exact(&mut len_buf).await {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(Error::Io(e)),
        }
        let len = u32::from_be_bytes(len_buf) as usize;
        let mut data = vec![0u8; len];
        match reader.read_exact(&mut data).await {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(Error::Io(e)),
        }
        let message = Message {
            id,
            protocol: ProtocolKind::Tcp,
            data: Bytes::from(data),
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
impl ProtocolPlugin for TcpPlugin {
    fn kind(&self) -> ProtocolKind {
        ProtocolKind::Tcp
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
