//! Client task: `run_client()` — one task per simulated connection.
//!
//! Owns: `ClientConfig`, the global `AtomicUsize` counters
//! (`GLOBAL_SEND_COUNT` / `GLOBAL_RECV_COUNT`, Relaxed ordering), batched event
//! emission via `BatchEventSender` (flushes every `EVENT_BATCH_SIZE` events).
//! The send half waits for the `start_send` broadcast signal before sending in
//! wait mode (`-w`); immediate mode just sleeps `CONNECTION_STABILIZE_DELAY_MS`.
//!
//! Lock-free: each send task exclusively owns its `SendHandle` (no `Arc<Mutex>`).

use anyhow::Result;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use tokio::runtime::Handle;
use tokio::sync::{broadcast, mpsc::Sender, Semaphore};
use tokio::task::JoinHandle;
use tokio::time::{sleep, Duration};

use crate::config::{
    EVENT_BATCH_SIZE, YIELD_INTERVAL, RECV_YIELD_INTERVAL, VERBOSE_LOGGING_THRESHOLD,
    CONNECTION_STABILIZE_DELAY_MS, EVENT_DROP_REPORT_THRESHOLD,
    EVENT_DROP_MAX_LIMIT, DEFAULT_CONNECT_INTERVAL_MS, DEFAULT_CONNECT_CONCURRENCY,
};
use crate::event::ClientEvent;
use crate::handle::{ConnectionHandle, SendHandle};
use crate::utils::{detect_protocol, PacketData, ProtocolType};

// Global handle to the dedicated receive runtime (set by main at startup)
// Receive tasks are spawned onto this separate runtime's thread pool, isolated from the
// default runtime where send tasks run, completely avoiding send tasks crowding out
// receive tasks' worker threads and starving reception.
static RECV_RUNTIME: OnceLock<Handle> = OnceLock::new();

/// Set up the dedicated receive runtime (called once at main startup)
pub fn set_recv_runtime(handle: Handle) {
    let _ = RECV_RUNTIME.set(handle);
}

/// Spawn a receive task onto the dedicated runtime (if set), otherwise fall back to the
/// current runtime. Test code does not call set_recv_runtime and takes the fallback path,
/// preserving the original compatibility.
fn spawn_recv_task<F>(future: F) -> JoinHandle<F::Output>
where
    F: std::future::Future + Send + 'static,
    F::Output: Send + 'static,
{
    match RECV_RUNTIME.get() {
        Some(handle) => handle.spawn(future),
        None => tokio::spawn(future),
    }
}

/// Global connection-completed counter (counts both success and failure, used by wait_all mode)
pub static CONN_COMPLETED_COUNT: AtomicUsize = AtomicUsize::new(0);

/// Global connection-success counter (only counts successful connections)
pub static CONN_SUCCESS_COUNT: AtomicUsize = AtomicUsize::new(0);

/// Global send-message counter (used for high-concurrency mode statistics)
pub static GLOBAL_SEND_COUNT: AtomicUsize = AtomicUsize::new(0);

/// Global receive-message counter (used for high-concurrency mode statistics)
pub static GLOBAL_RECV_COUNT: AtomicUsize = AtomicUsize::new(0);

/// Global receive-error counter (used to count receive errors)
pub static GLOBAL_RECV_ERROR_COUNT: AtomicUsize = AtomicUsize::new(0);

/// Client configuration
#[derive(Debug, Clone)]
pub struct ClientConfig {
    /// Target URL
    pub url: String,
    /// Protocol type
    pub protocol: ProtocolType,
    /// Number of packets to send
    pub packet_count: usize,
    /// Number of packets per batch send
    pub batch_size: usize,
    /// Send interval (milliseconds)
    pub interval_ms: u64,
    /// Data to send
    pub data: PacketData,
    /// Whether to wait for all connections to complete before sending
    pub wait_all: bool,
    /// Whether to ignore SSL certificate verification
    pub ignore_ssl: bool,
    /// Connection stabilization delay (milliseconds), wait time before sending data
    pub stabilize_delay_ms: u64,
    /// Stagger delay between consecutive connection attempts (milliseconds).
    /// Client i waits `i * connect_interval_ms` before connecting, spreading the
    /// ephemeral-port allocation over time instead of one burst.
    pub connect_interval_ms: u64,
    /// Maximum number of simultaneous in-flight connection attempts.
    /// `None` = unlimited (global semaphore is not used).
    pub connect_concurrency: Option<Arc<Semaphore>>,
}

impl ClientConfig {
    /// Create a new client configuration
    pub fn new(url: String) -> Self {
        let protocol = detect_protocol(&url);
        ClientConfig {
            url,
            protocol,
            packet_count: 1,
            batch_size: 1,
            interval_ms: 1,
            data: PacketData::text(""),
            wait_all: false,
            ignore_ssl: false,
            stabilize_delay_ms: CONNECTION_STABILIZE_DELAY_MS,
            connect_interval_ms: DEFAULT_CONNECT_INTERVAL_MS,
            connect_concurrency: None,
        }
    }

    /// Set the packet count
    pub fn with_packet_count(mut self, count: usize) -> Self {
        self.packet_count = count;
        self
    }

    /// Set the batch size
    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.batch_size = size;
        self
    }

    /// Set the send interval
    pub fn with_interval(mut self, interval_ms: u64) -> Self {
        self.interval_ms = interval_ms;
        self
    }

    /// Set the data to send
    pub fn with_data(mut self, data: PacketData) -> Self {
        self.data = data;
        self
    }

    /// Set whether to wait for all connections to complete
    pub fn with_wait_all(mut self, wait_all: bool) -> Self {
        self.wait_all = wait_all;
        self
    }

    /// Set whether to ignore SSL certificate verification
    pub fn with_ignore_ssl(mut self, ignore_ssl: bool) -> Self {
        self.ignore_ssl = ignore_ssl;
        self
    }

    /// Set the connection stabilization delay (milliseconds)
    ///
    /// # Reserved for extension
    /// This method is reserved for future CLI argument extension, allowing users to
    /// customize the connection stabilization delay.
    #[allow(dead_code)]
    pub fn with_stabilize_delay(mut self, delay_ms: u64) -> Self {
        self.stabilize_delay_ms = delay_ms;
        self
    }

    /// Set the stagger delay (milliseconds) between consecutive connection attempts.
    /// Client i sleeps `i * interval_ms` before connecting.
    #[allow(dead_code)]
    pub fn with_connect_interval(mut self, interval_ms: u64) -> Self {
        self.connect_interval_ms = interval_ms;
        self
    }

    /// Set the maximum number of simultaneous in-flight connection attempts.
    /// Passing `None` disables the cap.
    #[allow(dead_code)]
    pub fn with_connect_concurrency(mut self, concurrency: Option<Arc<Semaphore>>) -> Self {
        self.connect_concurrency = concurrency;
        self
    }
}

/// Send result
enum SendResult {
    /// Send completed
    Completed,
    /// Disconnected by user
    Disconnected,
    /// Send failed
    Failed,
}

/// Batch event sender (reduces channel pressure)
///
/// By batching and aggregating ordinary events, it significantly reduces the frequency of
/// sending events to the main thread, thereby lowering channel contention and CPU overhead
/// in high-concurrency scenarios.
///
/// # How it works
/// - Ordinary events (SendSuccess/ReceiveSuccess) are buffered and sent in batches once
///   batch_size is reached
/// - Important events (connect, disconnect, completion, etc.) are sent immediately without
///   waiting for a batch
/// - The number of dropped events is reported periodically (when channel pressure is too high)
///
/// # Optimization features
/// - Smart event aggregation: high-concurrency mode uses global atomic counters to avoid
///   sending detailed events
/// - Adaptive batching: behavior is adjusted dynamically based on verbose logging mode
/// - Memory efficient: uses pre-allocated buffers to avoid frequent allocations
pub struct BatchEventSender {
    sender: Sender<ClientEvent>,
    buffer: Vec<ClientEvent>,
    batch_size: usize,
    dropped_count: usize,
    /// Whether verbose logging is enabled (affects the event sending strategy)
    verbose_mode: bool,
}

impl BatchEventSender {
    /// Create a new batch event sender
    ///
    /// # Parameters
    /// - `sender`: the event send channel
    /// - `batch_size`: batch buffer size; once this count is reached, it flushes automatically
    /// - `verbose_mode`: whether verbose logging mode is enabled (affects the event aggregation strategy)
    pub fn new(sender: Sender<ClientEvent>, batch_size: usize, verbose_mode: bool) -> Self {
        Self {
            sender,
            buffer: Vec::with_capacity(batch_size),
            batch_size,
            dropped_count: 0,
            verbose_mode,
        }
    }

    /// Send an event (smart aggregation)
    #[inline]
    pub fn emit(&mut self, event: ClientEvent) {
        // Important events are handled with priority (sent even if the drop limit is exceeded)
        if matches!(
            event,
            ClientEvent::ConnectionSuccess { .. }
                | ClientEvent::ConnectionFailed { .. }
                | ClientEvent::AllPacketsSent { .. }
                | ClientEvent::ClientSendFinished { .. }
                | ClientEvent::PassiveDisconnected { .. }
        ) {
            self.flush();
            if let Err(e) = self.sender.try_send(event) {
                tracing::warn!("Failed to send important event: {:?}", e);
            }
            return;
        }

        // High-concurrency mode: use global atomic counters for statistics, do not send detailed events
        if !self.verbose_mode {
            match event {
                ClientEvent::SendSuccess { .. } => {
                    GLOBAL_SEND_COUNT.fetch_add(1, Ordering::Relaxed);
                    return;
                }
                ClientEvent::ReceiveSuccess { .. } => {
                    GLOBAL_RECV_COUNT.fetch_add(1, Ordering::Relaxed);
                    return;
                }
                _ => {}
            }
        }

        // Verbose logging mode: ordinary events check the drop limit
        if self.dropped_count >= EVENT_DROP_MAX_LIMIT {
            tracing::error!(
                "BatchEventSender exceeded max drop limit ({}), ignoring non-critical events",
                EVENT_DROP_MAX_LIMIT
            );
            return;
        }

        self.buffer.push(event);
        if self.buffer.len() >= self.batch_size {
            self.flush();
        }
    }

    /// Flush the buffer
    #[inline]
    pub fn flush(&mut self) {
        for event in self.buffer.drain(..) {
            if self.sender.try_send(event).is_err() {
                self.dropped_count += 1;
            }
        }
        // Periodically report dropped events
        if self.dropped_count > 0 && self.dropped_count.is_multiple_of(EVENT_DROP_REPORT_THRESHOLD) {
            tracing::warn!(
                "BatchEventSender dropped {} events due to channel backpressure",
                self.dropped_count
            );
        }
    }
}

impl Drop for BatchEventSender {
    fn drop(&mut self) {
        self.flush();
    }
}

/// Run a client (full-duplex mode, lock-free optimized version)
/// Runs a single client connection
///
/// This function manages the full lifecycle of a client:
/// 1. Establish the connection (WebSocket or TCP)
/// 2. Start an independent receive task to handle server responses
/// 3. Execute the send logic according to the configuration (supports wait_all mode)
/// 4. Gracefully close the connection and clean up resources
///
/// # Parameters
/// - `client_id`: unique client identifier
/// - `config`: client configuration (URL, data, send parameters, etc.)
/// - `event_sender`: event send channel, used to report status to the main thread
/// - `disconnect_rx`: disconnect signal receiver, used to respond to user interrupts
/// - `start_send_rx`: start-send signal receiver (used in wait_all mode)
///
/// # Flow
/// - Connectivity test mode (packet_count = 0): returns immediately after a successful connection
/// - Normal mode: decides whether to wait for a signal before sending based on the wait_all setting
pub async fn run_client(
    client_id: usize,
    config: ClientConfig,
    event_sender: Sender<ClientEvent>,
    mut disconnect_rx: broadcast::Receiver<()>,
    mut start_send_rx: broadcast::Receiver<()>,
) -> Result<()> {
    // Connection rate limiting:
    // 1) Concurrency cap: acquire a semaphore permit so at most N connects are in flight.
    //    The permit is released (dropped) as soon as the connection resolves, so it bounds
    //    concurrent CONNECT attempts only, not the number of established connections.
    // 2) Stagger: client i waits i * connect_interval_ms before connecting, spreading the
    //    ephemeral-port allocation over time instead of one instantaneous burst.
    // Both are no-ops when the user leaves the defaults (interval=0, concurrency=unlimited).
    let connect_permit = if let Some(sem) = &config.connect_concurrency {
        Some(sem.acquire().await.expect("connect semaphore closed"))
    } else {
        None
    };
    if config.connect_interval_ms > 0 {
        let stagger = (client_id as u64).saturating_mul(config.connect_interval_ms);
        if stagger > 0 {
            sleep(Duration::from_millis(stagger)).await;
        }
    }

    // Establish the connection and obtain independent send and receive channels
    let connect_outcome = ConnectionHandle::connect(&config.url, config.protocol, config.ignore_ssl).await;
    // Release the concurrency permit now — the connect attempt is finished, whether it
    // succeeded or failed. This frees the slot for the next client's connect attempt.
    drop(connect_permit);
    let (mut send_handle, mut recv_handle) = match connect_outcome {
        Ok((send, recv)) => {
            // Connection success event (blocking send, ensures accurate statistics)
            if let Err(e) = event_sender
                .send(ClientEvent::ConnectionSuccess {
                    client_id,
                    url: config.url.clone(),
                })
                .await {
                tracing::warn!("[Client {}] Failed to send ConnectionSuccess event: {:?}", client_id, e);
            }

            // Increment the counters immediately after a successful connection (before waiting for a signal!)
            // Relaxed ordering is used for performance (counters do not require strict synchronization)
            CONN_COMPLETED_COUNT.fetch_add(1, Ordering::Relaxed);
            CONN_SUCCESS_COUNT.fetch_add(1, Ordering::Relaxed);

            (send, recv)
        }
        Err(e) => {
            // Connection failure event (blocking send, ensures accurate statistics)
            if let Err(evt_err) = event_sender
                .send(ClientEvent::ConnectionFailed {
                    client_id,
                    error: e.to_string(),
                })
                .await {
                tracing::warn!("[Client {}] Failed to send ConnectionFailed event: {:?}", client_id, evt_err);
            }

            // Failed connections must also increment the completed counter, otherwise wait_all mode
            // would wait forever. Note: the success counter is not incremented.
            CONN_COMPLETED_COUNT.fetch_add(1, Ordering::Relaxed);

            return Ok(());
        }
    };

    // Only format the data for display in verbose mode
    let verbose_mode = client_id < VERBOSE_LOGGING_THRESHOLD;
    let data_display = if verbose_mode {
        Arc::from(config.data.format_display())
    } else {
        Arc::from("")
    };

    // Create a batch event sender (reduces channel pressure; high-concurrency mode uses global atomic counters)
    let mut batch_sender = BatchEventSender::new(event_sender.clone(), EVENT_BATCH_SIZE, verbose_mode);

    // Start the receive task (runs independently, lock-free)
    let mut recv_disconnect_rx = disconnect_rx.resubscribe();
    let recv_event_sender = event_sender.clone();
    let recv_task = spawn_recv_task(async move {
        let mut passive_disconnect = false;
        // Receive counter: used to yield the CPU once every N messages, avoiding a
        // high-frequency client starving other tasks on the same worker
        let mut recv_count: usize = 0;
        loop {
            tokio::select! {
                _ = recv_disconnect_rx.recv() => {
                    break;
                }
                result = recv_handle.receive() => {
                    match result {
                        Ok(Some(data)) => {
                            recv_count = recv_count.wrapping_add(1);
                            // Yield the CPU once every RECV_YIELD_INTERVAL messages to
                            // guarantee that in a 1000-connection scenario all recv tasks are
                            // scheduled fairly, avoiding a high-frequency client's tight loop
                            // crowding out the worker and causing other connections to consume late.
                            if recv_count % RECV_YIELD_INTERVAL == 0 {
                                tokio::task::yield_now().await;
                            }

                            if verbose_mode {
                                // Verbose mode: send the event to the console
                                let display = Arc::from(data.format_display());
                                if let Err(e) = recv_event_sender
                                    .try_send(ClientEvent::ReceiveSuccess {
                                        client_id,
                                        data: display,
                                    }) {
                                    tracing::warn!("[Client {}] Failed to send ReceiveSuccess event: {:?}", client_id, e);
                                }
                            } else {
                                // High-concurrency mode: directly increment the global atomic counter
                                GLOBAL_RECV_COUNT.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        Ok(None) => {
                            passive_disconnect = true;
                            break;
                        }
                        Err(e) => {
                            // Increment the global error counter
                            GLOBAL_RECV_ERROR_COUNT.fetch_add(1, Ordering::Relaxed);

                            if let Err(evt_err) = recv_event_sender
                                .try_send(ClientEvent::ReceiveError {
                                    client_id,
                                    error: e.to_string(),
                                }) {
                                tracing::warn!("[Client {}] Failed to send ReceiveError event: {:?}", client_id, evt_err);
                            }
                            passive_disconnect = true;
                            break;
                        }
                    }
                }
            }
        }
        if passive_disconnect {
            // Passive disconnect event (blocking send, ensures accurate connection statistics)
            if let Err(e) = recv_event_sender
                .send(ClientEvent::PassiveDisconnected { client_id })
                .await {
                tracing::warn!("[Client {}] Failed to send PassiveDisconnected event: {:?}", client_id, e);
            }
        }
    });
    let recv_abort = recv_task.abort_handle();

    // If packet_count is 0 (connectivity test), skip the send logic
    if config.packet_count == 0 {
        // Notify send completion immediately (no data was actually sent)
        if let Err(e) = event_sender
            .send(ClientEvent::ClientSendFinished {
                client_id,
                success: true,
            })
            .await {
            tracing::warn!("[Client {}] Failed to send ClientSendFinished event: {:?}", client_id, e);
        }

        // Continue waiting for a disconnect signal or a receive error
        let user_disconnected = tokio::select! {
            _ = disconnect_rx.recv() => {
                true
            }
            _ = recv_task => {
                false
            }
        };

        if let Err(e) = send_handle.close().await {
            tracing::warn!("[Client {}] Failed to close send handle: {:?}", client_id, e);
        }

        if user_disconnected {
            // Connection closed event (blocking send, ensures accurate connection statistics)
            if let Err(e) = event_sender
                .send(ClientEvent::ConnectionClosed {
                    client_id,
                    reason: "Disconnected by user".to_string(),
                })
                .await {
                tracing::warn!("[Client {}] Failed to send ConnectionClosed event: {:?}", client_id, e);
            }
        }

        return Ok(());
    }

    // Decide whether to wait for a signal based on the wait_all parameter
    if config.wait_all {
        // Wait for the start-send signal
        tokio::select! {
            _ = start_send_rx.recv() => {}
            _ = disconnect_rx.recv() => {
                recv_abort.abort();
                if let Err(e) = recv_task.await {
                    tracing::warn!("[Client {}] Receive task error: {:?}", client_id, e);
                }
                if let Err(e) = send_handle.close().await {
                    tracing::warn!("[Client {}] Failed to close send handle: {:?}", client_id, e);
                }
                if let Err(e) = event_sender
                    .send(ClientEvent::ConnectionClosed {
                        client_id,
                        reason: "Disconnected before sending started".to_string(),
                    })
                    .await {
                    tracing::warn!("[Client {}] Failed to send ConnectionClosed event: {:?}", client_id, e);
                }
                return Ok(());
            }
        }
    }
    // If wait_all is false, start sending immediately after a successful connection without waiting for a signal

    // After a successful connection, wait before sending data to ensure the connection is stable
    sleep(Duration::from_millis(config.stabilize_delay_ms)).await;

    // Execute the send logic (exclusive ownership of send_handle, lock-free)
    let send_result = do_send_optimized(
        client_id,
        &mut send_handle,
        &mut batch_sender,
        &config,
        &data_display,
        &mut disconnect_rx,
    )
    .await;

    // Handle the send result
    match send_result {
        SendResult::Disconnected => {
            recv_abort.abort();
            if let Err(e) = recv_task.await {
                tracing::debug!("[Client {}] Receive task cancelled: {:?}", client_id, e);
            }
            if let Err(e) = send_handle.close().await {
                tracing::warn!("[Client {}] Failed to close send handle: {:?}", client_id, e);
            }
            if let Err(e) = event_sender
                .send(ClientEvent::ConnectionClosed {
                    client_id,
                    reason: "Disconnected by user".to_string(),
                })
                .await {
                tracing::warn!("[Client {}] Failed to send ConnectionClosed event: {:?}", client_id, e);
            }
            return Ok(());
        }
        SendResult::Failed => {
            if let Err(e) = event_sender
                .send(ClientEvent::ClientSendFinished {
                    client_id,
                    success: false,
                })
                .await {
                tracing::warn!("[Client {}] Failed to send ClientSendFinished event: {:?}", client_id, e);
            }
            recv_abort.abort();
            if let Err(e) = recv_task.await {
                tracing::debug!("[Client {}] Receive task cancelled: {:?}", client_id, e);
            }
            if let Err(e) = send_handle.close().await {
                tracing::warn!("[Client {}] Failed to close send handle: {:?}", client_id, e);
            }
            if let Err(e) = event_sender
                .send(ClientEvent::ConnectionClosed {
                    client_id,
                    reason: "Send failed".to_string(),
                })
                .await {
                tracing::warn!("[Client {}] Failed to send ConnectionClosed event: {:?}", client_id, e);
            }
            return Ok(());
        }
        SendResult::Completed => {}
    }

    // Flush the batch sender
    batch_sender.flush();

    // Send complete; notify the main thread (blocking send ensures important events are not lost)
    if let Err(e) = event_sender
        .send(ClientEvent::AllPacketsSent { client_id })
        .await {
        tracing::warn!("[Client {}] Failed to send AllPacketsSent event: {:?}", client_id, e);
    }

    if let Err(e) = event_sender
        .send(ClientEvent::ClientSendFinished {
            client_id,
            success: true,
        })
        .await {
        tracing::warn!("[Client {}] Failed to send ClientSendFinished event: {:?}", client_id, e);
    }

    // Continue waiting for a disconnect signal or for the receive task to end naturally
    let user_disconnected = tokio::select! {
        _ = disconnect_rx.recv() => {
            // The user sent a disconnect signal
            true
        }
        _ = recv_task => {
            // The receive task ended naturally (passive disconnect or error)
            false
        }
    };

    // Only close the send channel on a user-initiated disconnect or when receiving ends.
    // Note: do not close immediately after sending completes, otherwise it would close the
    // connection and stop receiving.
    if let Err(e) = send_handle.close().await {
        tracing::warn!("[Client {}] Failed to close send handle: {:?}", client_id, e);
    }

    // If the user disconnected, the receive task may still be running and needs to be aborted
    if user_disconnected {
        recv_abort.abort();
    }

    // Connection closed event (blocking send, ensures accurate connection statistics)
    let reason = if user_disconnected {
        "Disconnected by user".to_string()
    } else {
        "Connection closed by server".to_string()
    };

    if let Err(e) = event_sender
        .send(ClientEvent::ConnectionClosed {
            client_id,
            reason,
        })
        .await {
        tracing::warn!("[Client {}] Failed to send ConnectionClosed event: {:?}", client_id, e);
    }

    Ok(())
}

/// Execute the send logic (optimized version, exclusive ownership of the send handle, lock-free)
///
/// This function is responsible for batch-sending packets and has the following optimizations:
/// - Exclusive ownership of the send handle, no lock contention
/// - Batch event aggregation to reduce channel pressure
/// - Periodically yields the CPU to avoid blocking other tasks
/// - Non-blocking checks of the disconnect signal to guarantee responsiveness
/// - Flushes events before each batch interval to ensure the console displays in real time
///
/// # Parameters
/// - `client_id`: unique client identifier
/// - `send_handle`: send handle (WebSocket or TCP)
/// - `batch_sender`: batch event sender, used to aggregate send success/failure events
/// - `config`: client configuration
/// - `data_display`: formatted data string (used for logging)
/// - `disconnect_rx`: disconnect signal receiver
///
/// # Returns
/// - `SendResult::Completed`: all packets sent successfully
/// - `SendResult::Disconnected`: disconnected by the user
/// - `SendResult::Failed`: an error occurred during sending
async fn do_send_optimized(
    client_id: usize,
    send_handle: &mut SendHandle,
    batch_sender: &mut BatchEventSender,
    config: &ClientConfig,
    data_display: &Arc<str>,
    disconnect_rx: &mut broadcast::Receiver<()>,
) -> SendResult {
    let mut packets_sent = 0;

    while packets_sent < config.packet_count {
        let batch_end = packets_sent.saturating_add(config.batch_size).min(config.packet_count);
        let batch_size = batch_end - packets_sent;

        for i in 0..batch_size {
            // Non-blocking check of the disconnect signal
            match disconnect_rx.try_recv() {
                Ok(()) | Err(broadcast::error::TryRecvError::Closed) => {
                    return SendResult::Disconnected;
                }
                _ => {}
            }

            // Periodically yield the CPU (based on the global send count)
            // Note: the send task and receive task have already been isolated via dual runtimes
            // (see set_recv_runtime in main.rs): receiving runs on a separate thread pool and is
            // not crowded out by the tight send loop. yield_now here is only for fair round-robin
            // scheduling among multiple send tasks; there is no need to over-yield via sleep(0).
            let global_idx = packets_sent + i;
            if global_idx > 0 && global_idx % YIELD_INTERVAL == 0 {
                tokio::task::yield_now().await;
            }

            // Send directly, no lock needed
            match send_handle.send(&config.data).await {
                Ok(()) => {
                    batch_sender.emit(ClientEvent::SendSuccess {
                        client_id,
                        data: data_display.clone(),
                    });
                }
                Err(e) => {
                    batch_sender.flush();
                    if let Err(evt_err) = batch_sender.sender.try_send(ClientEvent::SendFailed {
                        client_id,
                        error: e.to_string(),
                    }) {
                        tracing::warn!("[Client {}] Failed to send SendFailed event: {:?}", client_id, evt_err);
                    }
                    return SendResult::Failed;
                }
            }
        }

        packets_sent += batch_size;

        // Flush the event buffer to ensure the console displays messages immediately
        // (unaffected by the send interval)
        batch_sender.flush();

        // Send interval (only wait when more batches remain)
        if packets_sent < config.packet_count {
            if config.interval_ms > 0 {
                sleep(Duration::from_millis(config.interval_ms)).await;
            } else {
                // When interval_ms = 0, force a suspend after each batch to actually release the
                // worker thread, avoiding yield_now() not releasing the worker and starving the
                // receive task.
                tokio::time::sleep(Duration::from_millis(0)).await;
            }
        }
    }

    SendResult::Completed
}
