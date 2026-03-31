use std::net::SocketAddr;
use async_trait::async_trait;
use crate::{error::Result, message::{Handler, ProtocolKind}};

#[async_trait]
pub trait ProtocolPlugin: Send + Sync + 'static {
    fn kind(&self) -> ProtocolKind;
    async fn serve(&self, addr: SocketAddr, handler: Handler) -> Result<()>;
}
