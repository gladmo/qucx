use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::Arc;
use std::future::Future;
use crate::{
    error::Result,
    message::{Context, Handler},
    plugin::ProtocolPlugin,
};

pub struct ServerBuilder {
    entries: Vec<(Box<dyn ProtocolPlugin>, SocketAddr)>,
}

pub struct Server {
    entries: Vec<(Box<dyn ProtocolPlugin>, SocketAddr)>,
    handler: Handler,
}

impl Default for ServerBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ServerBuilder {
    pub fn new() -> Self {
        ServerBuilder { entries: Vec::new() }
    }

    pub fn bind<P: ProtocolPlugin>(mut self, plugin: P, addr: impl ToSocketAddrs + std::fmt::Debug) -> Self {
        let resolved = addr
            .to_socket_addrs()
            .unwrap_or_else(|e| panic!("could not resolve address {addr:?}: {e}"))
            .next()
            .unwrap_or_else(|| panic!("address {addr:?} resolved to no socket addresses"));
        self.entries.push((Box::new(plugin), resolved));
        self
    }

    pub fn handler<F, Fut>(self, f: F) -> Server
    where
        F: Fn(Context) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        let f = Arc::new(f);
        let handler: Handler = Arc::new(move |ctx| {
            let f = f.clone();
            Box::pin(async move { f(ctx).await })
        });
        Server { entries: self.entries, handler }
    }
}

impl Server {
    pub fn builder() -> ServerBuilder {
        ServerBuilder::new()
    }

    pub async fn run(self) -> Result<()> {
        let handler = self.handler;
        let mut handles = Vec::new();
        for (plugin, addr) in self.entries {
            let handler = handler.clone();
            handles.push(tokio::spawn(async move {
                plugin.serve(addr, handler).await
            }));
        }
        for handle in handles {
            handle.await.map_err(|e| crate::error::Error::Protocol(e.to_string()))??;
        }
        Ok(())
    }
}
