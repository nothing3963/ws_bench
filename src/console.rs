//! Output & statistics consumer. `ConsoleHandler` drains the event channel on
//! the main thread, aggregates connection/send/recv stats and prints progress.
//!
//! Owns: verbose-log vs. high-performance mode switching (threshold
//! `VERBOSE_LOGGING_THRESHOLD`: ≤10 connections print every message, above that
//! only global atomic counters + periodic summary), the `console_print!` macro
//! (async print, avoids blocking the event loop), and log-mode toggling.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::Ordering;
use tokio::sync::mpsc::Receiver;
use tokio::time::{interval, Duration, Interval};

use crate::client::{GLOBAL_SEND_COUNT, GLOBAL_RECV_COUNT};
use crate::config::{
    CONSOLE_PROGRESS_THRESHOLD, STATS_PRINT_INTERVAL_SECS, VERBOSE_LOGGING_THRESHOLD,
};
use crate::event::ClientEvent;

/// Asynchronous print macro, avoids blocking the event loop
///
/// # Usage
/// ```text
/// async_println!("[Client {}] Message", client_id);
/// ```
///
/// # How it works
/// Executes println! in a background task, preventing synchronous printing from blocking the
/// event loop.
/// Suitable for verbose log output; critical error information should still use a synchronous
/// println!.
macro_rules! async_println {
    ($($arg:tt)*) => {
        {
            let msg = format!($($arg)*);
            tokio::spawn(async move {
                println!("{}", msg);
            });
        }
    };
}

/// Console event handler
pub struct ConsoleHandler {
    /// Event receiver
    receiver: Receiver<ClientEvent>,
    /// Client connection status (client_id -> is_active)
    connection_status: HashMap<usize, bool>,
    /// Set of closed connection IDs (prevents double counting)
    closed_clients: HashSet<usize>,
    /// Set of client IDs that finished sending (prevents double counting)
    finished_clients: HashSet<usize>,
    /// Statistics
    stats: ConsoleStats,
    /// Total number of connections initiated
    total_connections: usize,
    /// Whether all connections have completed
    all_connections_completed: bool,
    /// Timer (used to print statistics every 5 seconds after sending completes)
    stats_timer: Option<Interval>,
    /// Whether verbose logging is enabled (disabled by default to improve performance)
    verbose_logging: bool,
    /// Batch print buffer (used to reduce println calls)
    pending_events: usize,
    /// Whether "all connections closed" has been printed
    all_closed_printed: bool,
    /// Send-completion notification channel (oneshot)
    all_finished_tx: Option<tokio::sync::oneshot::Sender<()>>,
    /// Whether this is a connectivity test only
    connectivity_test_only: bool,
}

/// Console statistics
#[derive(Debug, Default)]
struct ConsoleStats {
    /// Successful connection count
    connections_success: usize,
    /// Failed connection count
    connections_failed: usize,
    /// Successful send count
    send_success: usize,
    /// Failed send count
    send_failed: usize,
    /// Successful receive count
    receive_success: usize,
    /// Receive error count
    receive_error: usize,
}

impl ConsoleHandler {
    /// Create a new console handler
    pub fn new(
        receiver: Receiver<ClientEvent>,
        total_connections: usize,
        all_finished_tx: tokio::sync::oneshot::Sender<()>,
        connectivity_test_only: bool,
    ) -> Self {
        // Enable verbose logging when the connection count is at or below the threshold;
        // otherwise disable it to improve performance
        let verbose_logging = total_connections <= VERBOSE_LOGGING_THRESHOLD;

        // Start the timer immediately so statistics are printed periodically from the beginning
        let stats_timer = Some(interval(Duration::from_secs(STATS_PRINT_INTERVAL_SECS)));

        ConsoleHandler {
            receiver,
            connection_status: HashMap::with_capacity(total_connections),
            closed_clients: HashSet::with_capacity(total_connections),
            finished_clients: HashSet::with_capacity(total_connections),
            stats: ConsoleStats::default(),
            total_connections,
            all_connections_completed: false,
            stats_timer,
            verbose_logging,
            pending_events: 0,
            all_closed_printed: false,
            all_finished_tx: Some(all_finished_tx),
            connectivity_test_only,
        }
    }

    /// Run the event handling loop
    pub async fn run(mut self) {
        loop {
            tokio::select! {
                // Handle events
                event = self.receiver.recv() => {
                    match event {
                        Some(event) => {
                            self.handle_event(event);
                        }
                        None => {
                            // Channel closed, exit
                            break;
                        }
                    }
                }
                // Handle the timer (if started)
                _ = async {
                    if let Some(ref mut timer) = self.stats_timer {
                        timer.tick().await;
                    } else {
                        // If there is no timer, wait forever
                        std::future::pending::<()>().await;
                    }
                } => {
                    // Timer fired, print statistics (but stop once all connections are closed)
                    if !self.all_closed_printed {
                        self.print_detailed_stats();
                    }
                }
            }
        }
    }

    /// Handle a single event
    fn handle_event(&mut self, event: ClientEvent) {
        match event {
            ClientEvent::ConnectionSuccess { client_id, url } => {
                self.connection_status.insert(client_id, true);
                self.stats.connections_success += 1;
                if self.verbose_logging {
                    async_println!("[Client {}] Connected to {}", client_id, url);
                }
                self.check_all_connections_completed();
            }
            ClientEvent::ConnectionFailed { client_id, error } => {
                self.connection_status.insert(client_id, false);
                self.stats.connections_failed += 1;
                // Failures are always printed (synchronously, because they are critical error info)
                println!("[Client {}] Connection failed: {}", client_id, error);
                self.check_all_connections_completed();
            }
            ClientEvent::SendSuccess { client_id, data } => {
                // Do not process send events in connectivity test mode
                if self.connectivity_test_only {
                    return;
                }
                self.stats.send_success += 1;
                self.pending_events += 1;
                if self.verbose_logging {
                    async_println!("[Client {}] Sent: {}", client_id, data);
                } else if self.pending_events >= CONSOLE_PROGRESS_THRESHOLD {
                    // Print progress once every certain number of events
                    self.print_detailed_stats();
                    self.pending_events = 0;
                }
            }
            ClientEvent::SendFailed { client_id, error } => {
                // Do not process send events in connectivity test mode
                if self.connectivity_test_only {
                    return;
                }
                self.stats.send_failed += 1;
                // Failures are always printed (synchronously, because they are critical error info)
                println!("[Client {}] Send failed: {}", client_id, error);
            }
            ClientEvent::ReceiveSuccess { client_id, data } => {
                // Do not process receive events in connectivity test mode
                if self.connectivity_test_only {
                    return;
                }
                self.stats.receive_success += 1;
                self.pending_events += 1;
                if self.verbose_logging {
                    async_println!("[Client {}] Received: {}", client_id, data);
                } else if self.pending_events >= CONSOLE_PROGRESS_THRESHOLD {
                    // Print progress once every certain number of events
                    self.print_detailed_stats();
                    self.pending_events = 0;
                }
            }
            ClientEvent::ReceiveError { client_id, error } => {
                // Do not process receive events in connectivity test mode
                if self.connectivity_test_only {
                    return;
                }
                self.stats.receive_error += 1;
                // Errors are always printed (synchronously, because they are critical error info)
                println!("[Client {}] Receive error: {}", client_id, error);
            }
            ClientEvent::ConnectionClosed { client_id, reason } => {
                // Use a HashSet to prevent double counting
                if self.closed_clients.insert(client_id) {
                    // This is the first time this connection is closed; update its status
                    if let Some(status) = self.connection_status.get_mut(&client_id) {
                        *status = false;
                    }
                }
                if self.verbose_logging {
                    async_println!("[Client {}] Disconnected: {}", client_id, reason);
                }
                // Check whether all connections are closed
                self.check_all_connections_closed();
            }
            ClientEvent::AllPacketsSent { client_id } => {
                if self.verbose_logging {
                    async_println!("[Client {}] All packets sent", client_id);
                }
            }
            ClientEvent::ClientSendFinished { client_id, success } => {
                // Use a HashSet to prevent double counting
                if !self.finished_clients.insert(client_id) {
                    // Duplicate ClientSendFinished event, ignore it
                    tracing::warn!("Duplicate ClientSendFinished event for client {}", client_id);
                    return;
                }

                // This event is only used for main-thread statistics; the console does not need to
                // display it
                if self.verbose_logging {
                    if success {
                        async_println!("[Client {}] Send finished successfully", client_id);
                    } else {
                        async_println!("[Client {}] Send finished with failure", client_id);
                    }
                }

                // Check whether all clients have finished sending
                if self.finished_clients.len() >= self.total_connections {
                    // Print statistics (the timer was already started at initialization)
                    self.print_detailed_stats();

                    // Notify the main thread that all clients have finished sending
                    if let Some(tx) = self.all_finished_tx.take() {
                        if let Err(e) = tx.send(()) {
                            tracing::warn!("Failed to send all_finished signal: {:?}", e);
                        }
                    }
                }
            }
            ClientEvent::AllConnectionsCompleted => {
                if !self.all_connections_completed {
                    self.all_connections_completed = true;
                    self.print_connection_stats();
                }
            }
            ClientEvent::PrintStats => {
                // Do not process PrintStats events once all connections are closed
                if self.all_closed_printed {
                    return;
                }
                // Print statistics (the timer was already started at initialization)
                self.print_detailed_stats();
            }
            ClientEvent::PassiveDisconnected { client_id } => {
                // Use a HashSet to prevent double counting
                if self.closed_clients.insert(client_id) {
                    // This is the first time this connection is closed; update its status
                    if let Some(status) = self.connection_status.get_mut(&client_id) {
                        *status = false;
                    }
                }
                if self.verbose_logging {
                    async_println!("[Client {}] Connection closed by server", client_id);
                }
                // Check whether all connections are closed
                self.check_all_connections_closed();
            }
        }
    }

    /// Check whether all connections have completed
    fn check_all_connections_completed(&mut self) {
        let completed = self.stats.connections_success + self.stats.connections_failed;
        if completed >= self.total_connections && !self.all_connections_completed {
            self.all_connections_completed = true;
            self.print_connection_stats();
        }
    }

    /// Check whether all connections are closed
    fn check_all_connections_closed(&mut self) {
        // If already printed, return immediately
        if self.all_closed_printed {
            return;
        }

        // Calculate the number of successfully established connections
        let successful_connections = self.stats.connections_success;

        // If there are no successful connections, no need to wait for closures
        if successful_connections == 0 {
            return;
        }

        // Use the HashSet length to check whether all successful connections are closed
        if self.closed_clients.len() >= successful_connections {
            // All connections are closed, print final statistics (printed only once)
            self.all_closed_printed = true;

            // Stop the timer
            self.stats_timer = None;

            println!("\n>>> All connections closed <<<");
            self.print_detailed_stats();
        }
    }

    /// Get the number of active connections
    fn get_active_connections(&self) -> usize {
        self.connection_status.values().filter(|&&v| v).count()
    }

    /// Print connection statistics (after all connections complete)
    fn print_connection_stats(&self) {
        let active = self.get_active_connections();
        println!("\n>>> Active connections:{} / Total connections:{} <<<\n", active, self.total_connections);
    }

    /// Print detailed statistics (after all sends complete, every 5 seconds)
    fn print_detailed_stats(&self) {
        let active = self.get_active_connections();

        if self.connectivity_test_only {
            // Connectivity test: show only connection statistics
            println!(
                ">>> Active connections:{} / Total connections:{} <<<",
                active, self.total_connections
            );
        } else {
            // Normal mode: show full statistics
            // In high-concurrency mode read from global atomic counters; in verbose mode read from
            // local statistics
            let send_count = if self.verbose_logging {
                self.stats.send_success
            } else {
                GLOBAL_SEND_COUNT.load(Ordering::Relaxed) + self.stats.send_success
            };

            let recv_count = if self.verbose_logging {
                self.stats.receive_success
            } else {
                GLOBAL_RECV_COUNT.load(Ordering::Relaxed) + self.stats.receive_success
            };

            println!(
                ">>> Active connections:{} / Total connections:{} / Messages sent:{} / Messages received:{} <<<",
                active, self.total_connections, send_count, recv_count
            );
        }
    }
}

/// Print configuration information (standalone function)
pub fn print_config(
    clients: usize,
    packets: usize,
    batch: usize,
    interval: u64,
    data: &str,
    url: &str,
    connect_interval_ms: u64,
    connect_concurrency: usize,
) {
    println!("Socket Client Test Tool");
    println!("-------------------");
    println!("Number of clients: {}", clients);
    println!("Packets per client: {}", packets);
    println!("Batch send size: {}", batch);
    println!("Send interval: {} ms", interval);
    println!("Data content: {}", data);
    println!("Connection URL: {}", url);
    if connect_interval_ms > 0 {
        println!("Connect stagger interval: {} ms", connect_interval_ms);
    }
    if connect_concurrency > 0 {
        println!("Connect concurrency limit: {}", connect_concurrency);
    }
    println!("-------------------");
    println!("Log output samples:");
}

/// Simple console output function
pub fn print_banner() {
    println!("WevSocket Bench Test Tool v0.1.0");
    println!("Supports WebSocket and TCP Socket protocols");
    println!();
}

/// Print completion information
pub fn print_completion() {
    println!("\n===================");
    println!(">>> All data sent <<<");
    println!(">>> Waiting to receive data... <<<");
    println!(">>> Press [Enter] to exit <<<");
    println!("===================\n");
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn test_console_handler_verbose_mode() {
        let (tx, rx) = mpsc::channel(100);
        let (finished_tx, _finished_rx) = tokio::sync::oneshot::channel();
        let handler = ConsoleHandler::new(rx, 5, finished_tx, false); // 5 <= 10, verbose logging should be enabled

        // Verify that verbose_logging is enabled
        assert!(handler.verbose_logging);
        drop(tx);
    }

    #[tokio::test]
    async fn test_console_handler_non_verbose_mode() {
        let (tx, rx) = mpsc::channel(100);
        let (finished_tx, _finished_rx) = tokio::sync::oneshot::channel();
        let handler = ConsoleHandler::new(rx, 100, finished_tx, false); // 100 > 10, verbose logging should be disabled

        // Verify that verbose_logging is disabled
        assert!(!handler.verbose_logging);
        drop(tx);
    }

    #[tokio::test]
    async fn test_console_handler_event_handling() {
        let (tx, rx) = mpsc::channel(100);
        let (finished_tx, _finished_rx) = tokio::sync::oneshot::channel();
        let handler = ConsoleHandler::new(rx, 2, finished_tx, false);

        // Send test events
        tx.send(ClientEvent::ConnectionSuccess {
            client_id: 0,
            url: "ws://test".to_string(),
        })
        .await
        .unwrap();

        tx.send(ClientEvent::SendSuccess {
            client_id: 0,
            data: std::sync::Arc::from("test"),
        })
        .await
        .unwrap();

        drop(tx);

        // Run the handler until the channel closes
        handler.run().await;
    }

    #[test]
    fn test_print_functions() {
        // Verify that the print functions do not panic
        print_banner();
        print_config(10, 100, 5, 10, "test data", "ws://localhost:8080", 0, 0);
        print_completion();
    }

    #[test]
    fn test_constants_usage() {
        // Verify constant values are correct (use a const block to avoid clippy warnings)
        const _: () = assert!(VERBOSE_LOGGING_THRESHOLD > 0);
        const _: () = assert!(CONSOLE_PROGRESS_THRESHOLD > 0);
        const _: () = assert!(STATS_PRINT_INTERVAL_SECS > 0);
    }
}
