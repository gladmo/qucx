//! QUIC echo client example.
//!
//! Connects to the qucx echo server on port 9003 using QUIC. Sends a message
//! with 4-byte big-endian length-prefix framing on a bidirectional stream and
//! prints the echoed reply.
//!
//! The server uses a self-signed certificate, so certificate verification is
//! disabled for this example.
//!
//! Run the echo server first:
//!   cargo run --example echo_server
//!
//! Then in another terminal:
//!   cargo run --example quic_client

use std::sync::Arc;

use quinn::{ClientConfig, Endpoint};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::DigitallySignedStruct;

/// A no-op TLS verifier that accepts any certificate — for demos only.
#[derive(Debug)]
struct SkipCertVerification;

impl ServerCertVerifier for SkipCertVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        vec![
            rustls::SignatureScheme::RSA_PSS_SHA256,
            rustls::SignatureScheme::RSA_PSS_SHA384,
            rustls::SignatureScheme::RSA_PSS_SHA512,
            rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
            rustls::SignatureScheme::ECDSA_NISTP384_SHA384,
            rustls::SignatureScheme::ECDSA_NISTP521_SHA512,
            rustls::SignatureScheme::ED25519,
        ]
    }
}

#[tokio::main]
async fn main() {
    // Install the ring crypto provider so rustls knows which backend to use.
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");

    let server_addr: std::net::SocketAddr = "127.0.0.1:9003".parse().unwrap();
    println!("[quic_client] connecting to {server_addr}");

    // Build a client config that skips TLS certificate verification
    let tls_config = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(SkipCertVerification))
        .with_no_client_auth();

    let client_config = ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(tls_config).unwrap(),
    ));

    let mut endpoint = Endpoint::client("0.0.0.0:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(client_config);

    let connection = endpoint
        .connect(server_addr, "localhost")
        .unwrap()
        .await
        .expect("QUIC connect failed");

    println!("[quic_client] connected");

    // Open a bidirectional stream
    let (mut send, mut recv) = connection.open_bi().await.unwrap();

    let payload = b"Hello from QUIC client!";
    let len = payload.len() as u32;

    // Send: 4-byte length prefix + payload
    send.write_all(&len.to_be_bytes()).await.unwrap();
    send.write_all(payload).await.unwrap();
    println!("[quic_client] sent {} bytes", payload.len());

    // Receive: 4-byte length prefix + echoed payload
    let mut len_buf = [0u8; 4];
    recv.read_exact(&mut len_buf).await.unwrap();
    let resp_len = u32::from_be_bytes(len_buf) as usize;

    let mut resp = vec![0u8; resp_len];
    recv.read_exact(&mut resp).await.unwrap();

    println!(
        "[quic_client] echo received: {:?}",
        String::from_utf8_lossy(&resp)
    );

    connection.close(0u32.into(), b"done");
}
