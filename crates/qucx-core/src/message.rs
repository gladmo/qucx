use std::sync::Arc;
use bytes::Bytes;
use async_trait::async_trait;
use crate::error::Result;

pub type ConnectionId = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ProtocolKind {
    Tcp,
    WebSocket,
    Quic,
    Kcp,
    WebTransport,
}

#[derive(Debug, Clone)]
pub struct Message {
    pub id: ConnectionId,
    pub protocol: ProtocolKind,
    pub data: Bytes,
}

#[async_trait]
pub trait ConnectionSink: Send + Sync {
    fn id(&self) -> ConnectionId;
    fn protocol(&self) -> ProtocolKind;
    async fn send(&self, data: Bytes) -> Result<()>;
    async fn close(&self) -> Result<()>;
}

pub struct Context {
    pub message: Message,
    pub sender: Arc<dyn ConnectionSink>,
}

pub type BoxFuture<'a, T> = std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send + 'a>>;
pub type Handler = Arc<dyn Fn(Context) -> BoxFuture<'static, Result<()>> + Send + Sync>;
