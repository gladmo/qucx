use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use quinn::{Endpoint, RecvStream, SendStream, ServerConfig};
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
use tokio::sync::Mutex;

use qucx_core::{
    ConnectionId, ConnectionSink, Context, Error, Handler, Message, ProtocolKind, ProtocolPlugin,
    Result,
};

static CONNECTION_COUNTER: AtomicU64 = AtomicU64::new(1);

pub struct QuicPlugin;

impl QuicPlugin {
    pub fn new() -> Self {
        QuicPlugin
    }
}

impl Default for QuicPlugin {
    fn default() -> Self {
        Self::new()
    }
}

struct QuicSink {
    id: ConnectionId,
    send: Arc<Mutex<SendStream>>,
}

#[async_trait]
impl ConnectionSink for QuicSink {
    fn id(&self) -> ConnectionId {
        self.id
    }

    fn protocol(&self) -> ProtocolKind {
        ProtocolKind::Quic
    }

    async fn send(&self, data: Bytes) -> Result<()> {
        let mut send = self.send.lock().await;
        let len = data.len() as u32;
        send.write_all(&len.to_be_bytes())
            .await
            .map_err(|e| Error::Protocol(e.to_string()))?;
        send.write_all(&data)
            .await
            .map_err(|e| Error::Protocol(e.to_string()))?;
        Ok(())
    }

    async fn close(&self) -> Result<()> {
        let mut send = self.send.lock().await;
        send.finish().map_err(|e| Error::Protocol(e.to_string()))?;
        Ok(())
    }
}

fn make_server_config() -> std::result::Result<ServerConfig, Box<dyn std::error::Error + Send + Sync>> {
    let certified_key = rcgen::generate_simple_self_signed(vec!["localhost".into()])?;
    let cert_der = CertificateDer::from(certified_key.cert);
    let priv_key = PrivatePkcs8KeyDer::from(certified_key.key_pair.serialize_der());
    let server_config = ServerConfig::with_single_cert(vec![cert_der], priv_key.into())?;
    Ok(server_config)
}

async fn handle_stream(
    id: ConnectionId,
    send: SendStream,
    mut recv: RecvStream,
    handler: Handler,
) -> Result<()> {
    let sink = Arc::new(QuicSink {
        id,
        send: Arc::new(Mutex::new(send)),
    });

    loop {
        let mut len_buf = [0u8; 4];
        match recv.read_exact(&mut len_buf).await {
            Ok(()) => {}
            Err(_) => break,
        }
        let len = u32::from_be_bytes(len_buf) as usize;
        const MAX_MSG: usize = 64 * 1024 * 1024; // 64 MiB
        if len > MAX_MSG {
            return Err(Error::Protocol(format!("message too large: {len} bytes")));
        }
        let mut data_buf = vec![0u8; len];
        match recv.read_exact(&mut data_buf).await {
            Ok(()) => {}
            Err(_) => break,
        }
        let message = Message {
            id,
            protocol: ProtocolKind::Quic,
            data: Bytes::from(data_buf),
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
impl ProtocolPlugin for QuicPlugin {
    fn kind(&self) -> ProtocolKind {
        ProtocolKind::Quic
    }

    async fn serve(&self, addr: SocketAddr, handler: Handler) -> Result<()> {
        let server_config = make_server_config()
            .map_err(|e| Error::Protocol(e.to_string()))?;
        let endpoint = Endpoint::server(server_config, addr)?;

        loop {
            let incoming = match endpoint.accept().await {
                Some(inc) => inc,
                None => break,
            };
            let handler = handler.clone();
            tokio::spawn(async move {
                let conn = match incoming.accept() {
                    Ok(connecting) => match connecting.await {
                        Ok(c) => c,
                        Err(_) => return,
                    },
                    Err(_) => return,
                };
                loop {
                    let (send, recv) = match conn.accept_bi().await {
                        Ok(streams) => streams,
                        Err(_) => break,
                    };
                    let id = CONNECTION_COUNTER.fetch_add(1, Ordering::Relaxed);
                    let handler = handler.clone();
                    tokio::spawn(async move {
                        let _ = handle_stream(id, send, recv, handler).await;
                    });
                }
            });
        }
        Ok(())
    }
}
