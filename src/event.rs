//! Event contract between producers (client tasks) and the `ConsoleHandler`.
//!
//! Owns: `ClientEvent` enum, `create_event_channel()` (mpsc, capacity
//! `EVENT_CHANNEL_CAPACITY`), and `BatchEventSender` — buffers events and only
//! writes to the channel every `EVENT_BATCH_SIZE` items to reduce channel
//! pressure under high concurrency.

use std::sync::Arc;
use tokio::sync::mpsc;

use crate::config::EVENT_CHANNEL_CAPACITY;

/// Client event types
#[derive(Debug, Clone)]
pub enum ClientEvent {
    /// Connection succeeded
    ConnectionSuccess {
        client_id: usize,
        url: String,
    },
    /// Connection failed
    ConnectionFailed {
        client_id: usize,
        error: String,
    },
    /// Send succeeded
    SendSuccess {
        client_id: usize,
        data: Arc<str>,
    },
    /// Send failed
    SendFailed {
        client_id: usize,
        error: String,
    },
    /// Receive succeeded
    ReceiveSuccess {
        client_id: usize,
        data: Arc<str>,
    },
    /// Receive error
    ReceiveError {
        client_id: usize,
        error: String,
    },
    /// Connection closed
    ConnectionClosed {
        client_id: usize,
        reason: String,
    },
    /// All packets sent
    AllPacketsSent {
        client_id: usize,
    },
    /// Client send task finished (emitted on both success and failure)
    ClientSendFinished {
        client_id: usize,
        success: bool,
    },
    /// All connection attempts completed (regardless of success or failure)
    AllConnectionsCompleted,
    /// Print statistics
    PrintStats,
    /// Connection passively disconnected (not initiated by the user)
    PassiveDisconnected {
        client_id: usize,
    },
}

/// Create the event channel
pub fn create_event_channel() -> (mpsc::Sender<ClientEvent>, mpsc::Receiver<ClientEvent>) {
    mpsc::channel(EVENT_CHANNEL_CAPACITY)
}
