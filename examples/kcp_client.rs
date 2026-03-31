//! KCP echo client example.
//!
//! Sends a KCP-framed message to the qucx echo server on UDP port 9004 and
//! waits for the echoed reply.
//!
//! Run the echo server first:
//!   cargo run --example echo_server
//!
//! Then in another terminal:
//!   cargo run --example kcp_client

use std::io::Write;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use kcp::Kcp;
use tokio::net::UdpSocket;
use tokio::sync::Mutex;

fn now_ms() -> u32 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u32
}

/// Simple output buffer that collects KCP output packets.
struct PendingOutput(Vec<Vec<u8>>);

impl Write for PendingOutput {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.push(buf.to_vec());
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    let server_addr: SocketAddr = "127.0.0.1:9004".parse().unwrap();
    println!("[kcp_client] sending to {server_addr}");

    // Bind a local UDP socket
    let socket = Arc::new(UdpSocket::bind("0.0.0.0:0").await.unwrap());
    socket.connect(server_addr).await.unwrap();

    const CONV: u32 = 1;

    // Channel to communicate pending outbound UDP packets from KCP
    let (out_tx, mut out_rx) = tokio::sync::mpsc::unbounded_channel::<Vec<u8>>();

    let (sync_tx, sync_rx) = std::sync::mpsc::sync_channel::<Vec<u8>>(64);
    let pending = PendingOutput(Vec::new());
    let _ = pending; // we use the sync channel bridge below

    // Build KCP with a synchronous output that feeds into the channel
    struct ChanOutput(std::sync::mpsc::SyncSender<Vec<u8>>);
    impl Write for ChanOutput {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            let _ = self.0.try_send(buf.to_vec());
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    let kcp = Arc::new(Mutex::new(Kcp::new(CONV, ChanOutput(sync_tx))));

    // Bridge sync_rx → async out_tx
    {
        let out_tx = out_tx.clone();
        std::thread::spawn(move || {
            while let Ok(pkt) = sync_rx.recv() {
                let _ = out_tx.send(pkt);
            }
        });
    }

    // Send queued KCP output packets over UDP
    {
        let socket = Arc::clone(&socket);
        tokio::spawn(async move {
            while let Some(pkt) = out_rx.recv().await {
                let _ = socket.send(&pkt).await;
            }
        });
    }

    // KCP update loop
    {
        let kcp = Arc::clone(&kcp);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(10));
            loop {
                interval.tick().await;
                let mut k = kcp.lock().await;
                let _ = k.update(now_ms());
            }
        });
    }

    // Send the payload via KCP.
    // update() must be called first to mark the KCP state as initialised;
    // it also triggers flush() internally, so an explicit flush() call is
    // not required afterwards.
    let payload = b"Hello from KCP client!";
    {
        let mut k = kcp.lock().await;
        k.send(payload).unwrap();
        // update() sets the internal `updated` flag and calls flush() for us.
        k.update(now_ms()).unwrap();
    }
    println!("[kcp_client] sent {} bytes", payload.len());

    // Receive loop — feed incoming UDP packets into KCP and check for messages
    let mut buf = vec![0u8; 65536];
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if tokio::time::Instant::now() > deadline {
            eprintln!("[kcp_client] timed out waiting for echo");
            break;
        }
        let n = match tokio::time::timeout(Duration::from_millis(100), socket.recv(&mut buf)).await
        {
            Ok(Ok(n)) => n,
            Ok(Err(e)) => {
                eprintln!("[kcp_client] recv error: {e}");
                break;
            }
            Err(_) => continue, // timeout — keep waiting
        };

        let mut k = kcp.lock().await;
        if k.input(&buf[..n]).is_err() {
            continue;
        }
        k.update(now_ms()).unwrap();

        let mut recv_buf = vec![0u8; 65536];
        if let Ok(m) = k.recv(&mut recv_buf) {
            println!(
                "[kcp_client] echo received: {:?}",
                String::from_utf8_lossy(&recv_buf[..m])
            );
            break;
        }
    }
}
