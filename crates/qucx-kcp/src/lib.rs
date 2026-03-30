use std::collections::HashMap;
use std::io::Write;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use bytes::Bytes;
use kcp::{get_conv, Kcp};
use tokio::net::UdpSocket;
use tokio::sync::{mpsc, Mutex};

use qucx_core::{
    ConnectionId, ConnectionSink, Context, Error, Handler, Message, ProtocolKind, ProtocolPlugin,
    Result,
};

static CONNECTION_COUNTER: AtomicU64 = AtomicU64::new(1);

fn now_ms() -> u32 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u32
}

/// Output buffer that sends KCP output packets to a channel.
struct KcpOutput {
    tx: std::sync::mpsc::SyncSender<Vec<u8>>,
}

impl Write for KcpOutput {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let _ = self.tx.try_send(buf.to_vec());
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

struct KcpSink {
    id: ConnectionId,
    kcp_tx: mpsc::Sender<Bytes>,
}

#[async_trait]
impl ConnectionSink for KcpSink {
    fn id(&self) -> ConnectionId {
        self.id
    }

    fn protocol(&self) -> ProtocolKind {
        ProtocolKind::Kcp
    }

    async fn send(&self, data: Bytes) -> Result<()> {
        self.kcp_tx
            .send(data)
            .await
            .map_err(|_| Error::ConnectionClosed)
    }

    async fn close(&self) -> Result<()> {
        Ok(())
    }
}

struct KcpSessionArgs {
    id: ConnectionId,
    conv: u32,
    peer: SocketAddr,
    socket: Arc<UdpSocket>,
    input_rx: mpsc::Receiver<Vec<u8>>,
    send_rx: mpsc::Receiver<Bytes>,
    handler: Handler,
    sink: Arc<KcpSink>,
}

/// Runs a single KCP session.
async fn run_kcp_session(args: KcpSessionArgs) {
    let KcpSessionArgs {
        id,
        conv,
        peer,
        socket,
        mut input_rx,
        mut send_rx,
        handler,
        sink,
    } = args;

    let (out_tx, mut out_rx) = tokio::sync::mpsc::unbounded_channel::<Vec<u8>>();

    // Wrap in a sync channel for the KCP output (KCP calls Write synchronously)
    let (sync_tx, sync_rx) = std::sync::mpsc::sync_channel::<Vec<u8>>(256);
    let kcp_output = KcpOutput { tx: sync_tx };
    let kcp = Arc::new(Mutex::new(Kcp::new(conv, kcp_output)));

    // Bridge sync_rx to out_tx
    {
        let out_tx = out_tx.clone();
        std::thread::spawn(move || {
            while let Ok(pkt) = sync_rx.recv() {
                let _ = out_tx.send(pkt);
            }
        });
    }

    // Spawn UDP writer task
    {
        let socket = Arc::clone(&socket);
        tokio::spawn(async move {
            while let Some(pkt) = out_rx.recv().await {
                let _ = socket.send_to(&pkt, peer).await;
            }
        });
    }

    // KCP update ticker
    let kcp_ticker = Arc::clone(&kcp);
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(10));
        loop {
            interval.tick().await;
            let mut k = kcp_ticker.lock().await;
            let _ = k.update(now_ms());
        }
    });

    loop {
        tokio::select! {
            // Receive a UDP packet from the peer and feed it into KCP
            Some(raw) = input_rx.recv() => {
                let mut k = kcp.lock().await;
                if k.input(&raw).is_err() {
                    continue;
                }
                let _ = k.update(now_ms());
                // Try to read complete messages from KCP
                loop {
                    let mut buf = vec![0u8; 65536];
                    match k.recv(&mut buf) {
                        Ok(n) => {
                            let data = Bytes::copy_from_slice(&buf[..n]);
                            let message = Message {
                                id,
                                protocol: ProtocolKind::Kcp,
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
                        Err(kcp::Error::RecvQueueEmpty) | Err(kcp::Error::ExpectingFragment) => break,
                        Err(_) => break,
                    }
                }
            }
            // Application wants to send data over KCP
            Some(data) = send_rx.recv() => {
                let mut k = kcp.lock().await;
                if k.send(&data).is_ok() {
                    let _ = k.flush();
                }
            }
            else => break,
        }
    }
}

pub struct KcpPlugin;

impl KcpPlugin {
    pub fn new() -> Self {
        KcpPlugin
    }
}

impl Default for KcpPlugin {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ProtocolPlugin for KcpPlugin {
    fn kind(&self) -> ProtocolKind {
        ProtocolKind::Kcp
    }

    async fn serve(&self, addr: SocketAddr, handler: Handler) -> Result<()> {
        let socket = Arc::new(UdpSocket::bind(addr).await?);
        // Map from (conv, peer) to session input channel
        let mut sessions: HashMap<(u32, SocketAddr), mpsc::Sender<Vec<u8>>> = HashMap::new();

        let mut buf = vec![0u8; 65536];
        loop {
            let (n, peer) = socket.recv_from(&mut buf).await?;
            let packet = buf[..n].to_vec();

            if packet.len() < kcp::KCP_OVERHEAD {
                continue;
            }
            let conv = get_conv(&packet);
            let key = (conv, peer);

            if let std::collections::hash_map::Entry::Vacant(e) = sessions.entry(key) {
                let id = CONNECTION_COUNTER.fetch_add(1, Ordering::Relaxed);
                let (input_tx, input_rx) = mpsc::channel::<Vec<u8>>(256);
                let (kcp_send_tx, kcp_send_rx) = mpsc::channel::<Bytes>(256);

                let sink = Arc::new(KcpSink {
                    id,
                    kcp_tx: kcp_send_tx,
                });

                let socket_clone = Arc::clone(&socket);
                let handler = handler.clone();
                let sink_clone = Arc::clone(&sink);

                tokio::spawn(async move {
                    run_kcp_session(KcpSessionArgs {
                        id,
                        conv,
                        peer,
                        socket: socket_clone,
                        input_rx,
                        send_rx: kcp_send_rx,
                        handler,
                        sink: sink_clone,
                    })
                    .await;
                });

                e.insert(input_tx);
            }

            if let Some(tx) = sessions.get(&key) {
                if tx.send(packet).await.is_err() {
                    sessions.remove(&key);
                }
            }
        }
    }
}
