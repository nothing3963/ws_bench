//! Binary entry point. Parses CLI args (clap), wires up the event channels and
//! spawns the connection tasks plus the `ConsoleHandler`. See `AGENTS.md` for the
//! full architecture diagram.
//!
//! Owns: argument parsing, task orchestration, signal handling.

use anyhow::{anyhow, Result};
use clap::Parser;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::io::AsyncBufReadExt;
use tokio::signal;
use tokio::sync::broadcast;
use tokio::task::JoinSet;

mod client;
mod config;
mod console;
mod event;
mod handle;
mod utils;

use client::{run_client, set_recv_runtime, ClientConfig, CONN_COMPLETED_COUNT, CONN_SUCCESS_COUNT, GLOBAL_SEND_COUNT, GLOBAL_RECV_COUNT, GLOBAL_RECV_ERROR_COUNT};
use console::{print_banner, print_completion, print_config, ConsoleHandler};
use event::{create_event_channel, ClientEvent};
use utils::{parse_packet_data, validate_url, PacketData};
use config::{
    CONNECTION_POLL_INTERVAL_MS, CONNECTION_COMPLETED_NOTIFY_DELAY_MS,
    CONNECTION_STABILIZE_DELAY_MS, FINAL_STATS_DELAY_MS, TASK_CLEANUP_TIMEOUT_SECS,
};

/// Socket client testing tool - supports WebSocket and TCP Socket
#[derive(Parser, Debug)]
#[command(name = "ws_bench")]
#[command(version = "0.1.0")]
#[command(about = "WebSocket/TCP Socket client test tool")]
#[command(long_about = r#"
A console TCPSocket/WebSocket test tool written in Rust
Supports concurrent connections, batch sending, and both binary and text data

Examples:
  # basic send
  socket_bench -d "hello" ws://localhost:8080

  # TCP connection test
  socket_bench -c 10 -d "test" 127.0.0.1:8080

  # batch send binary data
  socket_bench -c 5 -n 100 -t 10 -d "[ff ff 00]" ws://echo.websocket.org

  # wait for all connections before sending
  socket_bench -c 10 -n 100 -w -d "batch test" ws://localhost:8080

  # ignore SSL cert errors (self-signed)
  socket_bench -i -d "test" wss://localhost:8443

  # connectivity test (no data sent)
  socket_bench -c 100 ws://localhost:8080

  # connectivity test (wait for all connections)
  socket_bench -c 100 -w ws://localhost:8080
"#)]
struct Args {
    /// Number of client connections
    #[arg(short, long, default_value = "1")]
    clients: usize,

    /// Number of packets sent per connection
    #[arg(short = 'n', long, default_value = "1")]
    packets: usize,

    /// Number of packets sent per send operation
    #[arg(short, long, default_value = "1")]
    batch: usize,

    /// Interval between sends (milliseconds)
    #[arg(short = 't', long, default_value = "1")]
    interval: u64,

    /// Packet content to send (optional; connectivity is tested only when omitted)
    #[arg(short = 'd', long)]
    body: Option<String>,

    /// Wait for all connections to complete before sending data
    #[arg(short = 'w', long)]
    wait_all: bool,

    /// Ignore SSL certificate verification errors (for self-signed certs or test environments)
    #[arg(short = 'i', long)]
    ignore_ssl: bool,

    /// Server address (supports ws://, wss://, http://, https://, or host:port)
    #[arg(required = true)]
    url: String,

    /// Send runtime (main runtime) worker thread count, 0 = use CPU core count
    #[arg(long = "send-workers", default_value = "0")]
    send_workers: usize,

    /// Receive runtime worker thread count, 0 = use CPU core count (minimum 2)
    #[arg(long = "recv-workers", default_value = "0")]
    recv_workers: usize,

    /// Stagger delay between consecutive connection attempts (milliseconds).
    /// Client i waits i * this value before connecting. Spreads the ephemeral-port
    /// allocation over time to avoid exhausting local source ports in one burst.
    #[arg(long = "connect-interval", default_value = "0")]
    connect_interval: u64,

    /// Maximum number of simultaneous in-flight connection attempts (0 = unlimited).
    /// Bounds concurrent CONNECT attempts (not established connections) via a semaphore.
    #[arg(long = "connect-concurrency", default_value = "100")]
    connect_concurrency: usize,
}

fn main() -> Result<()> {
    // Parse arguments first so the send runtime's thread count can be derived from them
    let args = Args::parse();

    let send_workers = if args.send_workers == 0 {
        std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4)
    } else {
        args.send_workers
    };

    // Manually build the send runtime (main runtime); the thread count is controlled by --send-workers.
    // Send threads run at low priority: under the dual-runtime architecture, this lets the OS scheduler
    // favor receive threads during CPU contention, further ensuring the consumption rate keeps up with the
    // send rate (avoiding accumulation in the kernel receive buffer).
    let send_rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(send_workers)
        .thread_name("send-worker")
        .enable_all()
        .on_thread_start(|| {
            // Lower the send thread priority (failure does not affect operation)
            let _ = thread_priority::set_thread_priority(
                thread_priority::thread_native_id(),
                thread_priority::ThreadPriority::Min,
            );
        })
        .build()
        .expect("Failed to build send runtime");

    send_rt.block_on(async_main(args))
}

async fn async_main(args: Args) -> Result<()> {
    // Initialize the tracing logging framework
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| {
                    // Only show logs at warn level and above (suppresses most logs)
                    "ws_bench=warn".into()
                })
        )
        .with_target(false)
        .init();

    // Print the banner
    print_banner();

    // Determine whether this is a connectivity test only
    let connectivity_test_only = args.body.is_none();

    // If it is a connectivity test, set packets to 0
    let actual_packets = if connectivity_test_only {
        0
    } else {
        args.packets
    };

    // Validate parameters
    if args.clients == 0 {
        return Err(anyhow!("Number of clients must be greater than 0"));
    }
    if !connectivity_test_only && args.packets == 0 {
        return Err(anyhow!("Number of packets must be greater than 0"));
    }
    if args.batch == 0 {
        return Err(anyhow!("Batch size must be greater than 0"));
    }

    // Reset the atomic counters
    CONN_COMPLETED_COUNT.store(0, Ordering::Relaxed);
    CONN_SUCCESS_COUNT.store(0, Ordering::Relaxed);
    GLOBAL_SEND_COUNT.store(0, Ordering::Relaxed);
    GLOBAL_RECV_COUNT.store(0, Ordering::Relaxed);
    GLOBAL_RECV_ERROR_COUNT.store(0, Ordering::Relaxed);

    // Validate and parse the URL
    let validated_url = validate_url(&args.url)?;

    // Parse the packet content (if provided)
    let packet_data = if let Some(body) = &args.body {
        parse_packet_data(body)?
    } else {
        // Use empty text for a connectivity test
        PacketData::text("")
    };

    // Print configuration information
    let data_display = if connectivity_test_only {
        "Connectivity test (no data sent)"
    } else {
        &packet_data.format_display()
    };

    print_config(
        args.clients,
        actual_packets,
        args.batch,
        args.interval,
        data_display,
        &validated_url,
        args.connect_interval,
        args.connect_concurrency,
    );

    if args.wait_all {
        println!("Mode: wait for all connections to complete before sending");
    } else {
        println!("Mode: send immediately after each connection succeeds");
    }

    // Create the event channel
    let (event_sender, event_receiver) = create_event_channel();

    // Create a oneshot channel to wait for all clients to finish sending
    let (all_finished_tx, all_finished_rx) = tokio::sync::oneshot::channel::<()>();

    // Start the console handler
    let console_handler = ConsoleHandler::new(
        event_receiver,
        args.clients,
        all_finished_tx,
        connectivity_test_only,
    );

    // Run the ConsoleHandler on a dedicated single-threaded runtime and set that thread to low
    // priority, preventing synchronous println! calls and event handling from occupying the
    // worker threads used by the high-concurrency send/receive tasks.
    let console_handle = std::thread::Builder::new()
        .name("console-handler".into())
        .spawn(move || {
            // Try to set this thread to low priority (failure does not affect operation)
            if let Err(e) = thread_priority::set_thread_priority(
                thread_priority::thread_native_id(),
                thread_priority::ThreadPriority::Min,
            ) {
                tracing::debug!("Failed to set console thread priority: {:?}", e);
            }

            // Single-threaded runtime: the ConsoleHandler and the async_println! tasks it
            // spawns internally all run serially on this thread
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("Failed to build console runtime");
            rt.block_on(console_handler.run());
        })
        .expect("Failed to spawn console thread");

    // Create the disconnect signal broadcast channel
    let (disconnect_tx, _disconnect_rx) = broadcast::channel::<()>(1);

    // Create the start-send signal broadcast channel
    let (start_send_tx, _start_send_rx) = broadcast::channel::<()>(1);

    // Create the client configuration
    // Connection rate limiting (defaults preserve the original burst behavior):
    // - connect_interval: per-client stagger delay (client i waits i * interval).
    // - connect_concurrency: cap on simultaneous in-flight CONNECT attempts.
    let connect_semaphore = if args.connect_concurrency > 0 {
        Some(Arc::new(tokio::sync::Semaphore::new(args.connect_concurrency)))
    } else {
        None
    };

    let config = ClientConfig::new(validated_url)
        .with_packet_count(actual_packets)
        .with_batch_size(args.batch)
        .with_interval(args.interval)
        .with_data(packet_data)
        .with_wait_all(args.wait_all)
        .with_ignore_ssl(args.ignore_ssl)
        .with_connect_interval(args.connect_interval)
        .with_connect_concurrency(connect_semaphore);

    if args.ignore_ssl {
        println!("⚠️  Warning: SSL certificate verification is disabled");
    }

    // Wrap the total client count in an Arc for use in closures
    let total_clients = Arc::new(args.clients);
    let wait_all_flag = args.wait_all;
    let connectivity_test_flag = connectivity_test_only;

    // Create the client task collection
    let mut join_set = JoinSet::new();

    // Dual-runtime isolation: create a dedicated runtime (separate thread pool) for receive tasks.
    // Send tasks run on the main tokio runtime; receive tasks run on this dedicated recv runtime.
    // The two are physically isolated - no matter how busy sending gets, the receive thread pool
    // always has idle capacity to consume the kernel receive buffer, fundamentally avoiding a
    // high server send-buffer watermark caused by "sending starving reception".
    {
        let default_workers = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);
        let recv_workers = if args.recv_workers == 0 {
            std::cmp::max(2, default_workers)
        } else {
            args.recv_workers
        };
        let recv_rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(recv_workers)
            .thread_name("recv-worker")
            .enable_all()
            .build()
            .expect("Failed to build recv runtime");
        set_recv_runtime(recv_rt.handle().clone());
        // Intentionally do not shutdown: let the receive runtime exit with the process
        // (dropping it early would cancel all recv tasks)
        std::mem::forget(recv_rt);
    }

    // Start the connection monitoring task (sends the start signal in wait_all mode)
    if wait_all_flag {
        let total = total_clients.clone();
        let start_tx = start_send_tx.clone();
        let evt_sender = event_sender.clone();

        join_set.spawn(async move {
            // Wait for all connections to complete (including successes and failures)
            loop {
                let completed = CONN_COMPLETED_COUNT.load(Ordering::Relaxed);
                if completed >= *total {
                    break;
                }
                tokio::time::sleep(tokio::time::Duration::from_millis(CONNECTION_POLL_INTERVAL_MS)).await;
            }

            // Short delay to ensure all counters have finished updating (avoids race conditions)
            tokio::time::sleep(tokio::time::Duration::from_millis(CONNECTION_STABILIZE_DELAY_MS)).await;

            // Get the success count
            let success_count = CONN_SUCCESS_COUNT.load(Ordering::Relaxed);
            let failed_count = *total - success_count;

            // Send the connection-completed event (blocking send, ensures the event is not lost)
            if let Err(e) = evt_sender
                .send(ClientEvent::AllConnectionsCompleted)
                .await {
                tracing::warn!("Failed to send AllConnectionsCompleted: {:?}", e);
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(CONNECTION_COMPLETED_NOTIFY_DELAY_MS)).await;

            // Broadcast the start-send signal
            if connectivity_test_flag {
                if failed_count > 0 {
                    println!(
                        "\n>>> Connection complete: success {} / failed {} / total {} <<<\n",
                        success_count, failed_count, *total
                    );
                } else {
                    println!("\n>>> All connections established (connectivity test complete) <<<\n");
                }
            } else {
                if failed_count > 0 {
                    println!(
                        "\n>>> Connection complete: success {} / failed {} / total {} <<<",
                        success_count, failed_count, *total
                    );
                }
                if success_count > 0 {
                    println!("\n>>> Starting to send data... <<<\n");
                    if let Err(e) = start_tx.send(()) {
                        tracing::error!("Failed to send start signal: {:?}", e);
                    }
                } else {
                    println!("\n>>> All connections failed, cannot send data <<<\n");
                }
            }
        });
    }

    // Start all client tasks
    for client_id in 0..args.clients {
        let sender = event_sender.clone();
        let client_config = config.clone();
        let disconnect_rx = disconnect_tx.subscribe();
        let start_send_rx = start_send_tx.subscribe();

        join_set.spawn(async move {
            let _ = run_client(
                client_id,
                client_config,
                sender,
                disconnect_rx,
                start_send_rx,
            )
            .await;
        });
    }

    // Wait for all clients to finish sending or for a user interrupt
    let all_sent = tokio::select! {
        result = all_finished_rx => {
            // Wait for the ConsoleHandler to notify that all clients have finished sending
            result.is_ok()
        }
        _ = async {
            while let Some(result) = join_set.join_next().await {
                if let Err(e) = result {
                    tracing::error!("Client task error: {}", e);
                }
            }
        } => {
            false
        }
        _ = signal::ctrl_c() => {
            println!("\nReceived interrupt signal, shutting down...");
            if let Err(e) = disconnect_tx.send(()) {
                tracing::warn!("Failed to send disconnect signal: {:?}", e);
            }
            join_set.abort_all();
            false
        }
    };

    // If it is a connectivity test, exit directly once all connections complete
    if connectivity_test_only {
        // Send the disconnect signal
        if let Err(e) = disconnect_tx.send(()) {
            tracing::warn!("Failed to send disconnect signal: {:?}", e);
        }
    } else if all_sent {
        // All clients have finished sending
        print_completion();

        let stdin = tokio::io::stdin();
        let mut reader = tokio::io::BufReader::new(stdin);
        let mut line = String::new();

        tokio::select! {
            _ = reader.read_line(&mut line) => {
                if let Err(e) = disconnect_tx.send(()) {
                    tracing::warn!("Failed to send disconnect signal: {:?}", e);
                }
            }
            _ = signal::ctrl_c() => {
                println!("\nReceived interrupt signal, shutting down...");
                if let Err(e) = disconnect_tx.send(()) {
                    tracing::warn!("Failed to send disconnect signal: {:?}", e);
                }
            }
        }
    }

    // Wait for all client tasks to complete (with a timeout)
    tracing::debug!("Waiting for all client tasks to complete...");
    let timeout_duration = tokio::time::Duration::from_secs(TASK_CLEANUP_TIMEOUT_SECS);
    let wait_result = tokio::time::timeout(
        timeout_duration,
        async {
            while let Some(result) = join_set.join_next().await {
                if let Err(e) = result {
                    tracing::error!("Client task error: {}", e);
                }
            }
        },
    )
    .await;

    if wait_result.is_err() {
        tracing::warn!("Some tasks did not finish within the timeout, forcing exit");
        println!("Some tasks did not finish within the timeout, forcing exit...");
        join_set.abort_all();
    } else {
        tracing::debug!("All client tasks completed");
    }

    // Print final statistics
    tracing::debug!("Sending PrintStats event");
    if let Err(e) = event_sender.try_send(ClientEvent::PrintStats) {
        tracing::warn!("Failed to send PrintStats: {:?}", e);
    }
    tokio::time::sleep(tokio::time::Duration::from_millis(FINAL_STATS_DELAY_MS)).await;

    // Close the event sender
    tracing::debug!("Dropping event_sender");
    drop(event_sender);

    // Wait for the console handler to complete (blocking wait, since this is at the end of main
    // and the process is about to exit)
    tracing::debug!("Waiting for console handler to finish...");
    if let Err(e) = console_handle.join() {
        tracing::error!("Console handler thread panicked: {:?}", e);
    }
    tracing::debug!("Console handler finished");

    Ok(())
}