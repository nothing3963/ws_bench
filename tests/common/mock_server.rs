use anyhow::Result;
use futures_util::{SinkExt, StreamExt};
use portpicker::pick_unused_port;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::accept_async;

/// Mock WebSocket Echo server
pub struct MockWebSocketServer {
    port: u16,
    shutdown_tx: broadcast::Sender<()>,
}

impl MockWebSocketServer {
    /// Start a new WebSocket server
    pub async fn start() -> Result<Self> {
        let port = pick_unused_port()
            .ok_or_else(|| anyhow::anyhow!("无法找到可用的端口"))?;

        let listener = TcpListener::bind(format!("127.0.0.1:{}", port)).await?;

        let (shutdown_tx, _) = broadcast::channel(1);
        let shutdown_tx_clone = shutdown_tx.clone();

        // Spawn the server task
        tokio::spawn(async move {
            let mut shutdown_rx = shutdown_tx_clone.subscribe();
            loop {
                tokio::select! {
                    result = listener.accept() => {
                        match result {
                            Ok((stream, _)) => {
                                tokio::spawn(async move {
                                    let _ = handle_websocket_connection(stream).await;
                                });
                            }
                            Err(_) => break,
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        break;
                    }
                }
            }
        });

        Ok(MockWebSocketServer { port, shutdown_tx })
    }

    /// Get the server's WebSocket URL
    pub fn url(&self) -> String {
        format!("ws://127.0.0.1:{}", self.port)
    }

    /// Get the TCP address for testing
    pub fn addr(&self) -> String {
        format!("127.0.0.1:{}", self.port)
    }
}

impl Drop for MockWebSocketServer {
    fn drop(&mut self) {
        let _ = self.shutdown_tx.send(());
    }
}

/// Handle a WebSocket connection
async fn handle_websocket_connection(stream: TcpStream) -> Result<()> {
    let ws_stream = accept_async(stream).await?;
    let (mut ws_sender, mut ws_receiver) = ws_stream.split();

    // Echo mode: send back whatever is received
    while let Some(msg_result) = ws_receiver.next().await {
        match msg_result {
            Ok(msg) => {
                if msg.is_close() {
                    let _ = ws_sender.send(Message::Close(None)).await;
                    break;
                } else if !msg.is_ping() && !msg.is_pong() {
                    // Echo the message
                    if ws_sender.send(msg).await.is_err() {
                        break;
                    }
                }
            }
            Err(_) => break,
        }
    }
    Ok(())
}

/// Mock TCP Echo server
pub struct MockTcpServer {
    port: u16,
    shutdown_tx: broadcast::Sender<()>,
}

impl MockTcpServer {
    /// Start a new TCP server
    pub async fn start() -> Result<Self> {
        let port = pick_unused_port()
            .ok_or_else(|| anyhow::anyhow!("无法找到可用的端口"))?;

        let listener = TcpListener::bind(format!("127.0.0.1:{}", port)).await?;

        let (shutdown_tx, _) = broadcast::channel(1);
        let shutdown_tx_clone = shutdown_tx.clone();

        // Spawn the server task
        tokio::spawn(async move {
            let mut shutdown_rx = shutdown_tx_clone.subscribe();
            loop {
                tokio::select! {
                    result = listener.accept() => {
                        match result {
                            Ok((stream, _)) => {
                                tokio::spawn(async move {
                                    let _ = handle_tcp_connection(stream).await;
                                });
                            }
                            Err(_) => break,
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        break;
                    }
                }
            }
        });

        Ok(MockTcpServer { port, shutdown_tx })
    }

    /// Get the server's TCP address
    pub fn addr(&self) -> String {
        format!("127.0.0.1:{}", self.port)
    }
}

impl Drop for MockTcpServer {
    fn drop(&mut self) {
        let _ = self.shutdown_tx.send(());
    }
}

/// Handle a TCP connection
async fn handle_tcp_connection(stream: TcpStream) -> Result<()> {
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    let mut line = String::new();

    // Echo mode: send back whatever is received
    loop {
        line.clear();
        match reader.read_line(&mut line).await {
            Ok(0) => break, // Connection closed
            Ok(_) => {
                writer.write_all(line.as_bytes()).await?;
                writer.flush().await?;
            }
            Err(_) => break,
        }
    }
    Ok(())
}
