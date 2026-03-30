use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use wtransport::{Connection, Endpoint, Identity, ServerConfig, VarInt};

use qucx_core::{
    ConnectionId, ConnectionSink, Context, Error, Handler, Message, ProtocolKind, ProtocolPlugin,
    Result,
};

static CONNECTION_COUNTER: AtomicU64 = AtomicU64::new(1);

/// WebTransport protocol plugin.
///
/// Accepts WebTransport sessions over HTTP/3 (QUIC). Uses a self-signed TLS
/// certificate generated at startup. Both datagrams and bidirectional streams
/// are supported; each produces a [`Message`] delivered to the shared handler.
///
/// **TLS note:** clients must accept the self-signed certificate. In a browser
/// use the `serverCertificateHashes` option in the `WebTransport` constructor,
/// or disable certificate verification in native clients.
pub struct WebTransportPlugin;

impl WebTransportPlugin {
    pub fn new() -> Self {
        WebTransportPlugin
    }
}

impl Default for WebTransportPlugin {
    fn default() -> Self {
        Self::new()
    }
}

// ── ConnectionSink implementation ─────────────────────────────────────────────

/// Sink that sends replies back via WebTransport datagrams on the session
/// connection.
struct WtDatagramSink {
    id: ConnectionId,
    connection: Connection,
}

#[async_trait]
impl ConnectionSink for WtDatagramSink {
    fn id(&self) -> ConnectionId {
        self.id
    }

    fn protocol(&self) -> ProtocolKind {
        ProtocolKind::WebTransport
    }

    async fn send(&self, data: Bytes) -> Result<()> {
        self.connection
            .send_datagram(data.as_ref())
            .map_err(|e| Error::Protocol(e.to_string()))
    }

    async fn close(&self) -> Result<()> {
        self.connection.close(VarInt::from_u32(0), b"");
        Ok(())
    }
}

/// Sink that sends replies back on a specific bidirectional stream.
struct WtStreamSink {
    id: ConnectionId,
    tx: Arc<tokio::sync::Mutex<wtransport::stream::SendStream>>,
}

#[async_trait]
impl ConnectionSink for WtStreamSink {
    fn id(&self) -> ConnectionId {
        self.id
    }

    fn protocol(&self) -> ProtocolKind {
        ProtocolKind::WebTransport
    }

    async fn send(&self, data: Bytes) -> Result<()> {
        let mut tx = self.tx.lock().await;
        tx.write_all(&data)
            .await
            .map_err(|e| Error::Protocol(e.to_string()))
    }

    async fn close(&self) -> Result<()> {
        Ok(())
    }
}

// ── Connection handler ─────────────────────────────────────────────────────────

async fn handle_connection(connection: Connection, handler: Handler) {
    let id = CONNECTION_COUNTER.fetch_add(1, Ordering::Relaxed);
    let dgram_sink: Arc<dyn ConnectionSink> = Arc::new(WtDatagramSink {
        id,
        connection: connection.clone(),
    });

    loop {
        tokio::select! {
            // Accept bidirectional streams opened by the client
            result = connection.accept_bi() => {
                match result {
                    Ok((send, mut recv)) => {
                        let stream_id = CONNECTION_COUNTER.fetch_add(1, Ordering::Relaxed);
                        let stream_sink: Arc<dyn ConnectionSink> = Arc::new(WtStreamSink {
                            id: stream_id,
                            tx: Arc::new(tokio::sync::Mutex::new(send)),
                        });
                        let handler = handler.clone();
                        tokio::spawn(async move {
                            let mut buf = vec![0u8; 65536];
                            loop {
                                match recv.read(&mut buf).await {
                                    Ok(Some(n)) => {
                                        let data = Bytes::copy_from_slice(&buf[..n]);
                                        let message = Message {
                                            id: stream_id,
                                            protocol: ProtocolKind::WebTransport,
                                            data,
                                        };
                                        let ctx = Context {
                                            message,
                                            sender: Arc::clone(&stream_sink),
                                        };
                                        let handler = handler.clone();
                                        tokio::spawn(async move {
                                            let _ = handler(ctx).await;
                                        });
                                    }
                                    Ok(None) => break, // stream closed by peer
                                    Err(_) => break,
                                }
                            }
                        });
                    }
                    Err(_) => break,
                }
            }
            // Receive datagrams
            result = connection.receive_datagram() => {
                match result {
                    Ok(dgram) => {
                        let data = dgram.payload();
                        let message = Message {
                            id,
                            protocol: ProtocolKind::WebTransport,
                            data,
                        };
                        let ctx = Context {
                            message,
                            sender: Arc::clone(&dgram_sink),
                        };
                        let handler = handler.clone();
                        tokio::spawn(async move {
                            let _ = handler(ctx).await;
                        });
                    }
                    Err(_) => break,
                }
            }
        }
    }
}

// ── ProtocolPlugin implementation ─────────────────────────────────────────────

#[async_trait]
impl ProtocolPlugin for WebTransportPlugin {
    fn kind(&self) -> ProtocolKind {
        ProtocolKind::WebTransport
    }

    async fn serve(&self, addr: SocketAddr, handler: Handler) -> Result<()> {
        let identity = Identity::self_signed(["localhost", "127.0.0.1", "::1"])
            .map_err(|e| Error::Protocol(e.to_string()))?;

        let config = ServerConfig::builder()
            .with_bind_address(addr)
            .with_identity(identity)
            .build();

        let server = Endpoint::server(config).map_err(Error::Io)?;

        loop {
            let incoming = server.accept().await;
            let handler = handler.clone();
            tokio::spawn(async move {
                let session_request = match incoming.await {
                    Ok(req) => req,
                    Err(e) => {
                        eprintln!("[qucx-webtransport] session handshake error: {e}");
                        return;
                    }
                };
                let connection = match session_request.accept().await {
                    Ok(conn) => conn,
                    Err(e) => {
                        eprintln!("[qucx-webtransport] session accept error: {e}");
                        return;
                    }
                };
                handle_connection(connection, handler).await;
            });
        }
    }
}
