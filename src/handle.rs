//! Network layer: establishes and drives a single WebSocket or TCP connection.
//!
//! Owns: `ConnectionHandle` (read/write halves), `SendHandle` (unified send
//! channel enum over WS/TCP sinks), connect + handshake logic. Reads are split
//! into an independent tokio task; magic numbers come from `config`.

use anyhow::{anyhow, Result};
use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::net::TcpStream;
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};
use native_tls::TlsConnector;

use crate::config::{TCP_BUFFER_SIZE, TCP_MAX_MESSAGE_SIZE, TCP_RECV_TIMEOUT_SECS, TCP_NODELAY, SOCKET_RECV_BUFFER_SIZE, SOCKET_SEND_BUFFER_SIZE};
use crate::utils::{convert_to_ws_url, parse_tcp_addr, PacketData, ProtocolType};

/// Unified send channel
#[derive(Debug)]
pub enum SendHandle {
    /// WebSocket send channel
    WebSocket(futures_util::stream::SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>),
    /// TCP send channel
    Tcp(WriteHalf<TcpStream>),
}

/// Unified receive channel
pub enum RecvHandle {
    /// WebSocket receive channel
    WebSocket(futures_util::stream::SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>),
    /// TCP receive channel (with a reusable buffer)
    Tcp {
        reader: ReadHalf<TcpStream>,
        buffer: Vec<u8>,
    },
}

/// Connection handler - creates send and receive channels
pub struct ConnectionHandle;

impl ConnectionHandle {
    /// Establish a new connection and return send and receive channels
    pub async fn connect(url: &str, protocol: ProtocolType, ignore_ssl: bool) -> Result<(SendHandle, RecvHandle)> {
        match protocol {
            ProtocolType::WebSocket => {
                let ws_url = convert_to_ws_url(url);

                // If SSL certificate verification needs to be ignored
                let stream = if ignore_ssl && (ws_url.starts_with("wss://") || ws_url.starts_with("https://")) {
                    // Create a TLS connector that accepts invalid certificates
                    let tls_connector = TlsConnector::builder()
                        .danger_accept_invalid_certs(true)
                        .danger_accept_invalid_hostnames(true)
                        .build()
                        .map_err(|e| anyhow!("Failed to create TLS connector: {}", e))?;

                    let connector = tokio_tungstenite::Connector::NativeTls(tls_connector);

                    tokio_tungstenite::connect_async_tls_with_config(
                        &ws_url,
                        None,
                        false,
                        Some(connector),
                    )
                    .await
                    .map(|(stream, _)| stream)
                    .map_err(|e| anyhow!("WebSocket connection failed: {}", e))?
                } else {
                    // Normal secure connection
                    connect_async(&ws_url)
                        .await
                        .map(|(stream, _)| stream)
                        .map_err(|e| anyhow!("WebSocket connection failed: {}", e))?
                };

                let (write, read) = stream.split();
                Ok((SendHandle::WebSocket(write), RecvHandle::WebSocket(read)))
            }
            ProtocolType::Tcp => {
                let (host, port) = parse_tcp_addr(url)?;
                let socket_addr = format!("{}:{}", host, port);

                // Connect with tokio first, then configure socket options with socket2::SockRef.
                // Enlarging the receive/send buffers + disabling Nagle alleviates the backpressure
                // cascade caused by the client not consuming fast enough (receive window narrows ->
                // server send buffer accumulates -> client send buffer fills and becomes unwritable).
                let stream = TcpStream::connect(&socket_addr)
                    .await
                    .map_err(|e| anyhow!("TCP connection failed: {}", e))?;

                // Set socket options on the already-connected tokio stream
                let sock_ref = socket2::SockRef::from(&stream);
                let _ = sock_ref.set_nodelay(TCP_NODELAY);
                let _ = sock_ref.set_recv_buffer_size(SOCKET_RECV_BUFFER_SIZE);
                let _ = sock_ref.set_send_buffer_size(SOCKET_SEND_BUFFER_SIZE);

                let (read, write) = tokio::io::split(stream);
                Ok((
                    SendHandle::Tcp(write),
                    RecvHandle::Tcp {
                        reader: read,
                        buffer: vec![0u8; TCP_BUFFER_SIZE],
                    },
                ))
            }
        }
    }
}

impl SendHandle {
    /// Send data
    pub async fn send(&mut self, data: &PacketData) -> Result<()> {
        match self {
            SendHandle::WebSocket(write) => {
                let message = match data {
                    PacketData::Text(text) => Message::Text(text.to_string()),
                    PacketData::Binary(bytes) => {
                        // Check whether this is a special control frame
                        if bytes.len() == 1 {
                            match bytes[0] {
                                0x09 => return write.send(Message::Ping(vec![])).await.map_err(|e| e.into()),
                                0x0A => return write.send(Message::Pong(vec![])).await.map_err(|e| e.into()),
                                0x08 => return write.send(Message::Close(None)).await.map_err(|e| e.into()),
                                _ => Message::Binary(bytes.to_vec()),
                            }
                        } else {
                            Message::Binary(bytes.to_vec())
                        }
                    }
                };

                write.send(message).await.map_err(|e| anyhow!("WebSocket send failed: {}", e))
            }
            SendHandle::Tcp(write) => {
                let bytes_to_send: Vec<u8> = match data {
                    PacketData::Text(text) => {
                        let mut bytes = text.as_bytes().to_vec();
                        bytes.push(b'\n');
                        bytes
                    }
                    PacketData::Binary(data) => data.to_vec(),
                };

                // Check the send data size limit
                if bytes_to_send.len() > TCP_MAX_MESSAGE_SIZE {
                    return Err(anyhow!("TCP send data exceeds max limit {} bytes", TCP_MAX_MESSAGE_SIZE));
                }

                write.write_all(&bytes_to_send).await.map_err(|e| anyhow!("TCP send failed: {}", e))?;
                write.flush().await.map_err(|e| anyhow!("TCP flush failed: {}", e))?;
                Ok(())
            }
        }
    }

    /// Close the send channel
    pub async fn close(&mut self) -> Result<()> {
        match self {
            SendHandle::WebSocket(write) => {
                write.close().await.map_err(|e| anyhow!("WebSocket close failed: {}", e))
            }
            SendHandle::Tcp(_) => {
                // TCP WriteHalf has no close method
                Ok(())
            }
        }
    }
}

impl RecvHandle {
    /// Receive data
    pub async fn receive(&mut self) -> Result<Option<PacketData>> {
        match self {
            RecvHandle::WebSocket(read) => {
                match read.next().await {
                    Some(Ok(message)) => {
                        // Check the WebSocket message size limit
                        let msg_size = match &message {
                            Message::Text(t) => t.len(),
                            Message::Binary(b) => b.len(),
                            _ => 0,
                        };
                        if msg_size > TCP_MAX_MESSAGE_SIZE {
                            return Err(anyhow!("WebSocket message exceeds max limit {} bytes", TCP_MAX_MESSAGE_SIZE));
                        }

                        match message {
                            Message::Text(text) => Ok(Some(PacketData::text(text))),
                            Message::Binary(data) => Ok(Some(PacketData::binary(data))),
                            Message::Ping(_) => Ok(Some(PacketData::binary(vec![0x09]))),
                            Message::Pong(_) => Ok(Some(PacketData::binary(vec![0x0A]))),
                            Message::Close(_) => Ok(Some(PacketData::binary(vec![0x08]))),
                            Message::Frame(_) => Ok(None),
                        }
                    }
                    Some(Err(e)) => Err(anyhow!("WebSocket receive error: {}", e)),
                    None => Ok(None),
                }
            }
            RecvHandle::Tcp { reader, buffer } => {
                // Defensive check: ensure the buffer size is correct
                // This branch should theoretically never trigger unless external code
                // incorrectly modified the buffer
                if buffer.len() < TCP_BUFFER_SIZE {
                    tracing::warn!(
                        "TCP buffer size mismatch: expected {}, got {}. Resizing...",
                        TCP_BUFFER_SIZE,
                        buffer.len()
                    );
                    buffer.resize(TCP_BUFFER_SIZE, 0);
                }

                loop {
                    match tokio::time::timeout(
                        tokio::time::Duration::from_secs(TCP_RECV_TIMEOUT_SECS),
                        reader.read(&mut buffer[..TCP_BUFFER_SIZE]),
                    )
                    .await
                    {
                        Ok(Ok(0)) => return Ok(None),
                        Ok(Ok(n)) => {
                            // Check the receive data size limit
                            if n > TCP_MAX_MESSAGE_SIZE {
                                return Err(anyhow!("TCP receive data exceeds max limit {} bytes", TCP_MAX_MESSAGE_SIZE));
                            }
                            // Use trim_end to handle all types of line endings
                            let text = String::from_utf8_lossy(&buffer[..n]);
                            let text = text.trim_end();
                            return Ok(Some(PacketData::text(text)));
                        }
                        Ok(Err(e)) => return Err(anyhow!("TCP receive error: {}", e)),
                        Err(_) => continue, // Retry after timeout, waiting for response data from the send side
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tcp_max_message_size() {
        // Verify constant values (use a const block to avoid clippy warnings)
        const _: () = assert!(TCP_MAX_MESSAGE_SIZE == 1024 * 1024);
        const _: () = assert!(TCP_BUFFER_SIZE == 128 * 1024);
        const _: () = assert!(TCP_RECV_TIMEOUT_SECS == 5);
    }
}