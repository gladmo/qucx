# qucx

A unified protocol framework written in Rust. `qucx` abstracts **TCP**, **WebSocket**, **QUIC**, and **KCP** as interchangeable plugins so you can run multiple transports simultaneously with a single handler.

## Overview

When you start a `qucx` server you bind one or more protocol plugins to ports. Clients connect using whichever protocol they support. Every inbound message – regardless of transport – is normalized into a single `Message` type and delivered to your shared handler function. You write the business logic once.

```
┌────────────────────────────────────────────────────┐
│                  qucx Server                       │
│                                                    │
│  TCP :8080   ──┐                                   │
│  WebSocket :8081 ─┼──► unified Message ──► handler │
│  QUIC :8443  ──┤                                   │
│  KCP  :9090  ──┘                                   │
└────────────────────────────────────────────────────┘
```

## Quick Start

Add to your `Cargo.toml`:

```toml
[dependencies]
qucx = { path = "." }           # or from crates.io once published
tokio = { version = "1", features = ["full"] }
```

Echo server example (TCP + WebSocket):

```rust
use qucx::{Server, TcpPlugin, WebSocketPlugin};

#[tokio::main]
async fn main() -> qucx::Result<()> {
    Server::builder()
        .bind(TcpPlugin::new(), "0.0.0.0:8080")
        .bind(WebSocketPlugin::new(), "0.0.0.0:8081")
        .handler(|ctx| async move {
            // ctx.message.protocol tells you where it came from
            // ctx.message.data is the raw payload
            // ctx.sender echoes back to the same client
            ctx.sender.send(ctx.message.data).await
        })
        .run()
        .await
}
```

Run the bundled example:

```bash
cargo run --example echo_server
```

## Protocols

| Plugin | Transport | Crate |
|--------|-----------|-------|
| `TcpPlugin` | TCP with 4-byte length-prefix framing | `qucx-tcp` |
| `WebSocketPlugin` | WebSocket (binary or text frames) | `qucx-websocket` |
| `QuicPlugin` | QUIC (self-signed TLS, per-stream messages) | `qucx-quic` |
| `KcpPlugin` | KCP over UDP (reliable, low-latency) | `qucx-kcp` |

## Feature Flags

All plugins are enabled by default. Disable unused ones to reduce binary size:

```toml
[dependencies]
qucx = { path = ".", default-features = false, features = ["tcp", "websocket"] }
```

Available features: `tcp`, `websocket`, `quic`, `kcp`.

## Architecture

```
qucx              – top-level re-exports
├── qucx-core     – Message, Context, ConnectionSink, ProtocolPlugin, Server
├── qucx-tcp      – TCP plugin
├── qucx-websocket– WebSocket plugin (tokio-tungstenite)
├── qucx-quic     – QUIC plugin (quinn + rcgen)
└── qucx-kcp      – KCP over UDP plugin
```

### Core types

- **`Message`** – unified message containing `id` (connection ID), `protocol` (which transport), and `data` (`Bytes`).
- **`Context`** – passed to your handler; contains a `Message` and a `sender` (`Arc<dyn ConnectionSink>`) for sending replies.
- **`ConnectionSink`** – trait with `send(data)` and `close()` methods; each plugin provides its own implementation.
- **`ProtocolPlugin`** – trait implemented by each transport plugin; requires `kind()` and `serve(addr, handler)`.
- **`Server`** – runs all plugins concurrently via `tokio::spawn`.

## Writing a Custom Plugin

Implement `ProtocolPlugin` from `qucx-core`:

```rust
use qucx_core::{ProtocolPlugin, ProtocolKind, Handler, Result};
use async_trait::async_trait;
use std::net::SocketAddr;

pub struct MyPlugin;

#[async_trait]
impl ProtocolPlugin for MyPlugin {
    fn kind(&self) -> ProtocolKind {
        ProtocolKind::Tcp // reuse an existing variant or extend the enum
    }

    async fn serve(&self, addr: SocketAddr, handler: Handler) -> Result<()> {
        // bind, accept connections, call handler(ctx) for each message
        todo!()
    }
}
```

## License

MIT