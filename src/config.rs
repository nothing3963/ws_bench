//! Single source of truth for every performance-related magic number: buffer
//! sizes, timeouts, channel capacities, batch sizes, CPU yield intervals,
//! verbose-logging threshold, drop-report thresholds. Tuning these directly
//! affects throughput and memory. Grouped by subsystem (TCP / socket / event /
//! console / client).

// ============ TCP-related constants ============

/// TCP receive buffer size (bytes, application-layer read buffer)
pub const TCP_BUFFER_SIZE: usize = 128 * 1024;

/// TCP receive maximum message size limit (bytes, prevents memory exhaustion attacks)
pub const TCP_MAX_MESSAGE_SIZE: usize = 1024 * 1024; // 1MB

/// TCP receive timeout (seconds)
pub const TCP_RECV_TIMEOUT_SECS: u64 = 5;

/// TCP_NODELAY switch (disables the Nagle algorithm)
///
/// # Purpose
/// The Nagle algorithm batches and delays small outbound packets (typically 40-200ms),
/// which significantly increases RTT in high-frequency echo scenarios, slowing down
/// echo arrival at the client. This in turn narrows the client's TCP receive window ->
/// server send buffer accumulates -> server stops reading client input -> client send
/// buffer fills up -> send() becomes unwritable.
///
/// Once disabled, every small packet is sent immediately, shortening echo RTT and
/// speeding up the client's consumption rate.
pub const TCP_NODELAY: bool = true;

/// SO_RCVBUF kernel receive buffer size (bytes, advisory value, OS may adjust)
///
/// # Purpose
/// Enlarges the client's kernel receive buffer so the kernel can absorb more echo
/// bursts without triggering window narrowing. When the client's consumption speed
/// keeps up, a large buffer can smooth out brief processing jitter; when consumption
/// lags behind, the window narrows more slowly, giving the application more reaction time.
///
/// Note: the Linux kernel doubles this value (including overhead), so the effective
/// value may be larger than the configured one.
/// A value that is too large may consume excessive kernel memory
/// (1000 connections x 256KB = 256MB), so a trade-off is required.
pub const SOCKET_RECV_BUFFER_SIZE: usize = 256 * 1024;

/// SO_SNDBUF kernel send buffer size (bytes, advisory value)
///
/// # Purpose
/// Enlarges the client's kernel send buffer so that burst-sent data lands in the kernel
/// buffer instead of blocking application-layer send() calls. Alleviates the "send buffer
/// full" problem (but this only defers it; the root cause is still consumption speed).
pub const SOCKET_SEND_BUFFER_SIZE: usize = 128 * 1024;

// ============ Event handling constants ============

/// Event channel capacity (supports extremely high concurrency scenarios)
///
/// # Capacity design notes
///
/// **Basis for the calculation**:
/// - Maximum concurrent connections: 2000+
/// - Packets per connection: 1000+
/// - Events per packet: 2 (SendSuccess + ReceiveSuccess)
/// - Theoretical total events: 4,000,000+
///
/// **Optimization mechanisms**:
/// - Batch event sender: 200 events/batch (verbose mode)
/// - Smart event aggregation: high-concurrency mode uses global atomic counters and does
///   not emit detailed events
/// - Critical events first: connect/disconnect/completion events are flushed and sent
///   immediately
///
/// **Actual channel pressure**:
/// - Verbose mode (<=10 connections): one channel call per 200 events
/// - High-concurrency mode (>10 connections): critical events only, about 6,000 events
/// - A 1,000,000 capacity can buffer 50-5000 batches, absorbing burst peaks
///
/// **Memory overhead**:
/// - Each event is about 100 bytes
/// - 1,000,000 capacity ~= 100 MB (in extreme cases)
/// - Typical actual usage is < 20 MB
pub const EVENT_CHANNEL_CAPACITY: usize = 1_000_000;

/// Batch event send buffer size
///
/// # Performance trade-off
/// - Larger value: less channel pressure, but higher latency
/// - Smaller value: lower latency, but more channel pressure
/// - 200 is the optimal balance point for the 2000-connection scenario
/// - Combined with a forced flush before the send interval, this guarantees timeliness
pub const EVENT_BATCH_SIZE: usize = 200;

/// Console progress print threshold (print once every N events)
pub const CONSOLE_PROGRESS_THRESHOLD: usize = 10000;

/// Verbose logging mode threshold (verbose logging is enabled when connection count <= this value)
pub const VERBOSE_LOGGING_THRESHOLD: usize = 10;

// ============ Performance tuning constants ============

/// CPU yield frequency (yield the CPU once every N packets sent)
///
/// # Yield mechanism notes
///
/// The send task and receive task have already been isolated via dual runtimes
/// (see `set_recv_runtime` in main.rs): receiving runs on a separate thread pool and is
/// not crowded out by the tight send loop. `yield_now()` here is only used for fair
/// round-robin scheduling among multiple send tasks within the same runtime, so there is
/// no need for excessive yielding via sleep(0) at the cost of throughput.
///
/// # Tuning
///
/// - Smaller value: fairer scheduling among multiple send tasks
/// - Larger value: higher throughput for a single send task
pub const YIELD_INTERVAL: usize = 10;

/// Receive-path CPU yield frequency (yield the CPU once every N messages received)
///
/// # Background
///
/// In high-concurrency mode (>10 connections), the receive path only performs atomic
/// counting (`fetch_add`) with no await points. A single high-frequency client's recv task
/// can form a CPU-intensive tight loop on the worker thread, crowding out the scheduling
/// opportunities of other clients' recv tasks on the same worker.
///
/// # Relationship with the dual runtime
///
/// The receive task runs on a dedicated recv runtime (see `set_recv_runtime` in main.rs)
/// and is already physically isolated from the send task, so it will no longer be crowded
/// out by sending. The yield here is only used for fair round-robin scheduling among
/// multiple recv tasks within the recv runtime (1000 connections = 1000 recv tasks
/// sharing the recv pool).
///
/// # Tuning
///
/// - Smaller value: fairer scheduling among recv tasks
/// - Larger value: higher throughput for a single recv task
pub const RECV_YIELD_INTERVAL: usize = 10;

/// Statistics print interval (seconds)
pub const STATS_PRINT_INTERVAL_SECS: u64 = 5;

// ============ Connection behavior constants ============

/// Wait time after a successful connection before sending data (milliseconds)
/// Used to ensure the connection is stable and avoid failures caused by sending immediately
pub const CONNECTION_STABILIZE_DELAY_MS: u64 = 300;

/// Connection monitoring poll interval (milliseconds)
/// In wait_all mode, the interval at which all connections are checked for completion
pub const CONNECTION_POLL_INTERVAL_MS: u64 = 10;

/// Connection completion notification delay (milliseconds)
/// A short delay after the AllConnectionsCompleted event is sent
pub const CONNECTION_COMPLETED_NOTIFY_DELAY_MS: u64 = 100;

/// Delay before printing final statistics (milliseconds)
/// Ensures all events have been processed
pub const FINAL_STATS_DELAY_MS: u64 = 100;

// ============ Timeout and cleanup constants ============

/// Task cleanup timeout (seconds)
/// The maximum time to wait for all client tasks to complete
pub const TASK_CLEANUP_TIMEOUT_SECS: u64 = 5;

// ============ Batch event drop reporting threshold ============

/// Batch event sender drop report threshold
/// Prints a warning once every time this number of events has been dropped
pub const EVENT_DROP_REPORT_THRESHOLD: usize = 1000;

/// Batch event sender maximum drop limit
/// Once this limit is exceeded, an error is logged and no new events are accepted
pub const EVENT_DROP_MAX_LIMIT: usize = 10000;

// ============ Connection rate-limiting constants ============

/// Default stagger delay between consecutive connection attempts (milliseconds)
///
/// # Purpose
/// When thousands of connections target the SAME server IP:port, every connection needs a
/// unique local ephemeral source port. Spawning all connection tasks at once makes the OS
/// allocate the whole ephemeral-port pool in a single burst; once exhausted, further
/// connects fail with "address not available". Staggering each client's connect by
/// `client_id * interval` spreads the port allocation over time, giving TIME_WAIT ports
/// time to be recycled and avoiding transient SYN floods.
///
/// # Default
/// 0 = no stagger (all clients connect immediately), preserving the original behavior
/// unless the user explicitly opts in via `--connect-interval`.
pub const DEFAULT_CONNECT_INTERVAL_MS: u64 = 0;

/// Default maximum number of in-flight connection attempts (0 = unlimited)
///
/// # Purpose
/// Caps how many connects are simultaneously in progress via a global semaphore. Prevents
/// a burst of SYNs from overwhelming a slow server's listen backlog. 0 = unlimited.
///
/// # Default
/// 100 = a conservative cap applied out of the box so that high client counts (e.g. >2000)
/// do not exhaust local ephemeral ports / the server backlog in one instantaneous burst.
/// The user can override it via `--connect-concurrency` (set 0 to disable the cap).
pub const DEFAULT_CONNECT_CONCURRENCY: usize = 100;
