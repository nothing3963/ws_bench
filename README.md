# WebSocket/TCP Socket Client Stress Testing Tool

A high-performance WebSocket/TCP Socket stress testing tool implemented in Rust, supporting high-concurrency connections, batch sending, binary and text data, and many other features.

## Features

- ✅ **High concurrency**: Supports 2000+ concurrent connections, event channel capacity of 1,000,000
- ✅ **Dual protocol support**: Automatically detects and supports WebSocket and native TCP Socket
- ✅ **Batch data sending**: Each connection can be configured with packet count and batch size
- ✅ **Send interval control**: Configurable time interval between batches (millisecond level)
- ✅ **Multiple data formats**:
  - Text data: plain text or JSON format
  - Binary data: hexadecimal format, such as `[ff ff 00]`
  - WebSocket control frames: Ping `[09]`, Pong `[0a]`, Close `[08]`
- ✅ **Smart log mode**:
  - Verbose log mode (≤10 connections): shows every message
  - High-performance mode (>10 connections): uses global atomic counters, only shows summary
- ✅ **Full-duplex communication**: Sending and receiving run independently and concurrently without blocking each other
- ✅ **Flexible sending modes**:
  - Wait mode (-w): all connections are established first, then sending starts uniformly
  - Immediate mode (default): sending starts as soon as a connection succeeds
  - Pre-send delay: each connection automatically waits 300ms before sending data to ensure connection stability
- ✅ **Connectivity testing**: when `-d` is not provided, only tests the connection without sending data
- ✅ **Robust error handling**: sending failures immediately stop that connection, keeping statistics accurate and reliable
- ✅ **Real-time statistics**: automatically shows active connections, sent/received message counts, and other real-time data every 5 seconds
- ✅ **Logging framework integration**: uses the tracing framework, log level controllable via environment variables

## Performance Characteristics

- **High-concurrency optimization**:
  - Event channel capacity: 1,000,000 (supports extreme concurrency scenarios)
  - Batch event sending: aggregates 200 events per batch, reducing channel pressure by 99.5%
  - Smart event aggregation: high-concurrency mode uses global atomic counters to avoid emitting detailed events
  - Asynchronous log printing: verbose logs are async and do not block the event loop

- **Lock-free design**:
  - The send task exclusively owns SendHandle, eliminating Arc<Mutex> lock contention
  - Global atomic counters for statistics: uses Relaxed ordering, zero lock overhead

- **Memory optimization**:
  - TCP buffer reuse: 32KB buffer reuse avoids allocating memory on every receive
  - Arc optimization: PacketData uses Arc<str>/Arc<[u8]>, cloning is O(1)
  - Pre-allocated capacity: HashMap/HashSet pre-allocation reduces rehashing overhead

- **CPU optimization**:
  - Pre-compiled regex: uses once_cell to pre-compile regular expressions
  - CPU yield strategy: yields the CPU every 10 packets to avoid starving the receive task
  - Smart logging: automatically switches to high-performance mode under high concurrency, reducing console I/O

- **Performance targets**:
  - Supports 2000+ concurrent connections
  - 4,000,000+ events throughput
  - Statistics 100% accurate (zero critical event loss)
  - Memory usage < 500 MB (2000-connection scenario)

## Safety Features

- **Message size limit**: TCP/WebSocket messages are capped at 1MB to prevent memory exhaustion attacks
- **Receive timeout protection**: TCP receive has a 5-second timeout
- **Task cleanup timeout**: 5-second timeout protection, forcibly cleans up leftover tasks

## Installation

### Prerequisites

- Rust 1.70 or later
- Cargo package manager

### Build from source

```bash
# Build the release version
cargo build --release

# The compiled binary is located at
target/release/ws_bench.exe  # Windows
target/release/ws_bench      # Linux/macOS
```

## Usage

### Basic syntax

```bash
# Send data mode
ws_bench [OPTIONS] -d <BODY> <URL>

# Connectivity test mode
ws_bench [OPTIONS] <URL>
```

### Options

| Option | Long option | Default | Description |
|--------|-------------|---------|-------------|
| `-c` | `--clients` | 1 | Number of client connections |
| `-n` | `--packets` | 1 | Number of packets each connection sends |
| `-b` | `--batch` | 1 | Number of packets per batch |
| `-t` | `--interval` | 1 | Interval between sends (milliseconds) |
| `-d` | `--body` | optional | Packet content to send (when omitted, only connectivity is tested) |
| `-w` | `--wait-all` | false | Wait for all connections to complete before starting to send |
| `-i` | `--ignore-ssl` | false | Ignore SSL certificate validation errors (for self-signed certificates) |
| | `--send-workers` | 0 | Send runtime worker thread count (0 = use CPU core count) |
| | `--recv-workers` | 0 | Receive runtime worker thread count (0 = use CPU core count, minimum 2) |
| | `--connect-interval` | 0 | Stagger delay between consecutive connection attempts (ms). Client *i* waits *i* × value before connecting; spreads ephemeral-port allocation over time (0 = no stagger) |
| | `--connect-concurrency` | 100 | Max simultaneous in-flight connection attempts; bounds concurrent CONNECTs (not established connections) via a semaphore (0 = unlimited) |
| `<URL>` | - | **required** | Server address |

### Connection rate limiting

When creating very large numbers of connections (e.g. >2000) from a single machine, the local ephemeral-port pool can be exhausted in one instantaneous burst, causing `EADDRNOTAVAIL` / connection failures. The `--connect-interval` and `--connect-concurrency` options spread the connection attempts over time and bound how many are in flight at once:

```bash
# Stagger each connection 5 ms apart (10,000 connections spread over ~50 s)
ws_bench -c 10000 --connect-interval 5 ws://server:8080

# Allow at most 100 connection attempts in flight at any moment
ws_bench -c 10000 --connect-concurrency 100 ws://server:8080

# Combine both (the concurrency limit is on by default; interval is off by default)
ws_bench -c 10000 --connect-interval 2 --connect-concurrency 50 ws://server:8080
```

For the maximum single-machine connection count, also raise the OS ephemeral-port range (Windows, run as Administrator): `netsh int ipv4 set dynamicport tcp start=1025 num=64510`.

### Supported URL formats

| Format | Protocol | Example |
|--------|----------|---------|
| `ws://host:port` | WebSocket | `ws://localhost:8080` |
| `wss://host:port` | WebSocket Secure | `wss://echo.websocket.org` |
| `http://host:port` | WebSocket (auto-converted) | `http://localhost:8080` |
| `https://host:port` | WebSocket Secure (auto-converted) | `https://localhost:8080` |
| `tcp://host:port` | TCP Socket | `tcp://127.0.0.1:8080` |
| `host:port` | TCP Socket | `127.0.0.1:8080` |

## Examples

### 1. Connectivity test

```bash
# Test a single connection
ws_bench ws://localhost:8080

# Test 100 concurrent connections (start immediately, no waiting)
ws_bench -c 100 ws://localhost:8080

# Test 100 concurrent connections (wait for all connections to finish before exiting)
ws_bench -c 100 -w ws://localhost:8080

# TCP connectivity test
ws_bench -c 50 127.0.0.1:8080
```

**Characteristics**:
- No packets are sent
- Only tests whether the connection can be established
- Suitable for bulk-testing server availability
- Automatically exits when all connections complete

### 2. Basic data sending

```bash
# Single connection sending text
ws_bench -d "hello" ws://localhost:8080

# Single connection sending binary
ws_bench -d "[ff ff 00]" ws://localhost:8080

# TCP Socket
ws_bench -d "test" 127.0.0.1:8080
```

### 3. Small-scale concurrency (verbose logs)

```bash
# 10 connections, each sending 100 messages (send immediately upon connection)
ws_bench -c 10 -n 100 -d "test" ws://localhost:8080

# 10 connections, wait for all connections before sending
ws_bench -c 10 -n 100 -w -d "test" ws://localhost:8080
```

**Output characteristics** (connections ≤ 10):
- Shows each connection being established
- Shows every message sent and received
- Convenient for debugging and observing details

### 4. Stress test (high-performance mode)

```bash
# 2000 connections, each sending 1000 messages, 5ms interval (send immediately)
ws_bench -c 2000 -n 1000 -t 5 -d "stress test" ws://localhost:8080

# 2000 connections, wait for all connections before sending uniformly (simulates burst traffic)
ws_bench -c 2000 -n 1000 -t 5 -w -d "stress test" ws://localhost:8080
```

**Output characteristics** (connections > 10):
- Only shows connection statistics and errors
- Prints progress every 10,000 events
- Performance optimized, reduced console I/O

### 5. Batch sending test

```bash
# 100 connections, each sending 500 messages, batch of 10, 100ms interval between batches
ws_bench -c 100 -n 500 -b 10 -t 100 -d "batch test" ws://localhost:8080
```

**Batch notes**:
- `-n 500`: 500 messages sent in total
- `-b 10`: 10 messages per batch
- `-t 100`: 100ms interval between batches
- 50 batches in total (500 ÷ 10)

### 6. Sending mode comparison

```bash
# Immediate mode: sending starts as soon as a connection succeeds (default)
# Simulates real users coming online one by one
ws_bench -c 1000 -n 100 -d "immediate" ws://localhost:8080

# Wait mode: sending starts uniformly after all connections complete
# Simulates burst traffic / flash-sale scenarios
ws_bench -c 1000 -n 100 -w -d "synchronized" ws://localhost:8080
```

**Differences**:
- **Immediate mode**: earlier connections start sending first, sending is spread out over time
- **Wait mode**: all clients start sending simultaneously, producing a higher instantaneous traffic peak

### 7. SSL certificate handling

```bash
# Connect to a WSS server with a self-signed certificate
ws_bench -i -d "test" wss://localhost:8443

# Ignore certificate errors to connect to a test environment
ws_bench -c 10 -i -d "test" wss://test.example.com/ws

# Connect to production (without -i, validates the certificate)
ws_bench -c 10 -d "test" wss://prod.example.com/ws
```

**Security warning**:
- ⚠️ The `-i` option disables SSL certificate validation, for testing environments only
- ⚠️ Use a valid SSL certificate in production
- ⚠️ A warning message is displayed when used

### 8. Sending binary data

```bash
# Hexadecimal format
ws_bench -c 5 -n 100 -d "[ff ff 00 ab cd]" ws://localhost:8080

# WebSocket Ping
ws_bench -d "[09]" ws://localhost:8080

# WebSocket Pong
ws_bench -d "[0a]" ws://localhost:8080

# WebSocket Close
ws_bench -d "[08]" ws://localhost:8080
```

### 9. Extreme stress test

## Run Flow

### Phase 1: Establishing connections

```text
Socket client testing tool
Supports WebSocket and TCP Socket protocols

Socket client testing tool
-------------------
Clients: 2000
Packets per client: 1000
Batch size: 1
Send interval: 5 ms
Data: "test"
URL: ws://localhost:8080
-------------------
Log output example:
Mode: send immediately upon connection

[Client 0] Connected to ws://localhost:8080  (verbose mode)
[Client 1] Connected to ws://localhost:8080  (verbose mode)
...
```

**Note**: each successful connection automatically waits 300ms before sending data to ensure connection stability.

### Phase 2: Starting to send data

**Immediate mode (default)**:
```text
[Client 0] Connected, will start sending after 300ms
[Client 1] Connected, will start sending after 300ms
...
```

**Wait mode (using the -w option)**:
```text
>>> Active connections:2000 / Total connections:2000 <<<

>>> All connections established, starting to send data... <<<

(all clients wait 300ms before sending)
```

### Phase 3: Concurrent sending and receiving

**Verbose log mode** (connections ≤ 10):
```text
[Client 0] Sent: "test"
[Client 0] Received: "echo: test"
[Client 1] Sent: "test"
[Client 1] Received: "echo: test"
...
```

**High-performance mode** (connections > 10):
```text
>>> Active connections:2000 / Total connections:2000 / Sent messages:100000 / Received messages:95000 <<<
>>> Active connections:2000 / Total connections:2000 / Sent messages:500000 / Received messages:480000 <<<
...
```

### Phase 4: Sending complete, waiting for receives

```text
===================
>>> All data sent <<<
>>> Waiting to receive data... <<<
>>> Press [Enter] to close the program <<<
===================

>>> Active connections:2000 / Total connections:2000 / Sent messages:2000000 / Received messages:1950000 <<<
(statistics printed every 10 seconds)
```

### Phase 5: User exit

```text
(press Enter or Ctrl+C)

>>> All connections closed <<<
>>> Active connections:0 / Total connections:2000 / Sent messages:2000000 / Received messages:2000000 <<<
```

## Log Modes

### Verbose log mode (connections ≤ 10)

**Characteristics**:
- ✅ Shows each connection being established
- ✅ Shows every message sent
- ✅ Shows every message received
- ✅ Shows each connection closing

**Use cases**: debugging, development, functional testing

### High-performance mode (connections > 10)

**Characteristics**:
- ✅ Only shows connection failures and errors
- ✅ Prints progress every 10,000 events
- ✅ Automatically prints statistics every 10 seconds
- ❌ Does not show successful send/receive messages

**Use cases**: stress testing, performance testing, production environments

## Statistics

### Connection statistics
```text
>>> Active connections:2000 / Total connections:2000 <<<
```
- **Active connections**: the number of currently active connections
- **Total connections**: the total number of connections initially established

### Detailed statistics
```text
>>> Active connections:1850 / Total connections:2000 / Sent messages:1800000 / Received messages:1750000 <<<
```
- **Active connections**: currently active connections (closed ones are not counted)
- **Total connections**: the total number of connections
- **Sent messages**: the number of successfully sent messages (failed ones are not counted)
- **Received messages**: the number of successfully received messages

### Failure handling

**Send failure**:
```text
[Client 123] Send failed: Connection reset by peer
[Client 123] Connection closed: Send failed
```
- A send failure immediately stops that client
- It is not counted in the sent statistics
- It does not affect other clients

**Connection failure**:
```text
[Client 0] Connection failed: Connection refused
```
- Connection failures are always printed (even in high-performance mode)
- They are not counted as active connections

## Data Formats

### Text data

```bash
# Plain text
ws_bench -d "hello world" ws://localhost:8080

# JSON format
ws_bench -d '{"key": "value"}' ws://localhost:8080
```

### Binary data

```bash
# Hexadecimal format (space separated)
ws_bench -d "[ff ff 00]" ws://localhost:8080

# Comma separated also works
ws_bench -d "[00,01,02,03]" ws://localhost:8080

# Full example
ws_bench -d "[1a 2b 3c 4d 5e 6f]" ws://localhost:8080
```

### WebSocket control frames

```bash
# Ping frame
ws_bench -d "[09]" ws://localhost:8080

# Pong frame
ws_bench -d "[0a]" ws://localhost:8080

# Close frame
ws_bench -d "[08]" ws://localhost:8080
```

## Performance Tuning

### Adjusting the send rate

```bash
# Lower the send rate (increase interval)
ws_bench -c 2000 -n 1000 -t 50 -d "test" ws://localhost:8080

# Raise the send rate (decrease interval)
ws_bench -c 2000 -n 1000 -t 1 -d "test" ws://localhost:8080

# Fastest rate (no interval, but still yields)
ws_bench -c 2000 -n 1000 -t 0 -d "test" ws://localhost:8080
```

### Adjusting the batch size

```bash
# Batch sending can reduce network round trips
# Send 10 messages every 100ms instead of 1 message every 10ms
ws_bench -c 1000 -n 5000 -b 10 -t 100 -d "test" ws://localhost:8080
```

### System tuning

**Linux**:
```bash
# Increase the file descriptor limit
ulimit -n 65535

# Check the current limit
ulimit -n
```

**Windows**:
requires modifying the registry:
```
HKEY_LOCAL_MACHINE\SYSTEM\CurrentControlSet\Services\Tcpip\Parameters
- MaxUserPort = 65534
- TcpTimedWaitDelay = 30
```

## FAQ

### Q1: Why don't I see logs at high concurrency?

**A**: This is a performance optimization. Above 10 connections, the tool automatically switches to high-performance mode to reduce console I/O overhead.

### Q2: How do I see real-time progress?

**A**: In high-performance mode, the program automatically prints statistics every 10,000 events or every 10 seconds.

### Q3: Connection fails with "Too many open files"

**A**: Increase the system file descriptor limit (see system tuning above).

### Q4: The program doesn't exit after sending finishes?

**A**: This is normal. After sending completes, the program continues to receive data. Press Enter or Ctrl+C to exit.

### Q5: How do I know it truly finished?

**A**:
- Seeing `All data sent` → all clients successfully sent all their packets
- Not seeing this message → some clients failed to send or were interrupted

### Q6: Statistics show fewer sent messages than expected?

**A**: Possible causes:
- Some connections failed to send (check for "Send failed" in the logs)
- Ctrl+C was pressed during sending
- Only successful sends are counted in the statistics

## Architecture

### Module structure

```
src/
├── main.rs       # Program entry, argument parsing, flow control
├── client.rs     # Client logic, batch event sender
├── handle.rs     # Network layer, WebSocket/TCP implementation, buffer reuse
├── event.rs      # Event system, event definitions
├── console.rs    # Console output, statistics handling
├── config.rs     # Configuration constants (buffer sizes, timeouts, etc.)
└── utils.rs      # Utility functions, data parsing, pre-compiled regex
```

### Event-driven architecture

```text
┌─────────────────────────────────────────┐
│       Console Handler (main thread)     │
│  ┌─────────────────────────────────┐    │
│  │ ConsoleHandler                  │    │
│  │ - Receives and handles events   │    │
│  │ - Smart log output              │    │
│  │ - Statistics management         │    │
│  └─────────────────────────────────┘    │
└─────────────────────────────────────────┘
              ↑
              │ mpsc channel (capacity 100,000)
              │
┌─────────────────────────────────────────┐
│      Client tasks (2000+ concurrent)    │
│  ┌─────────────────────────────────┐    │
│  │ Receive task (independent spawn)│    │
│  │ - Continuously receives data    │    │
│  │ - Emits ReceiveSuccess events   │    │
│  └─────────────────────────────────┘    │
│  ┌─────────────────────────────────┐    │
│  │ Send task (main task)           │    │
│  │ - Waits for the start signal    │    │
│  │ - Sends data in batches         │    │
│  │ - yield_now() every 10 packets  │    │
│  │ - Emits SendSuccess events      │    │
│  └─────────────────────────────────┘    │
└─────────────────────────────────────────┘
```

### Key characteristics

1. **Unified send start point**: a start signal is broadcast only after all connections are established
2. **Full-duplex communication**: sending and receiving run independently and concurrently without blocking each other
3. **Lock-free design**: the send task exclusively owns SendHandle, no lock contention at high concurrency
4. **Batch event aggregation**: 100 events per batch, reducing channel pressure
5. **Smart scheduling**: yield_now() every 10 packets to avoid CPU starvation
6. **Message size limit**: a 1MB maximum to prevent memory exhaustion
7. **Atomic counters**: atomic counters track connection state, reducing event interception overhead

## Event Types

```rust
pub enum ClientEvent {
    ConnectionSuccess { client_id, url },
    ConnectionFailed { client_id, error },
    SendSuccess { client_id, data },
    SendFailed { client_id, error },
    ReceiveSuccess { client_id, data },
    ReceiveError { client_id, error },
    ConnectionClosed { client_id, reason },
    AllPacketsSent { client_id },
    ClientSendFinished { client_id, success },
    AllConnectionsCompleted,
    PrintStats,
    PassiveDisconnected { client_id },
}
```

## Dependencies

- `tokio` (1.35): async runtime
- `tokio-tungstenite` (0.21): WebSocket client implementation
- `tungstenite` (0.21): WebSocket protocol
- `futures-util` (0.3): async stream handling
- `clap` (4.4): command-line argument parsing
- `anyhow` (1.0): error handling
- `regex` (1.10): regular expression matching
- `once_cell` (1.19): lazy initialization, pre-compiled regex
- `url` (2.5): URL parsing
- `native-tls` (0.2): TLS support
- `tracing` (0.1): logging framework
- `tracing-subscriber` (0.3): log subscriber

## Testing Recommendations

### Increase load gradually

```bash
# Start small
ws_bench -c 10 -n 100 -d "test" ws://localhost:8080

# Increase gradually
ws_bench -c 100 -n 100 -d "test" ws://localhost:8080
ws_bench -c 500 -n 100 -d "test" ws://localhost:8080
ws_bench -c 1000 -n 100 -d "test" ws://localhost:8080
ws_bench -c 2000 -n 100 -d "test" ws://localhost:8080
```

### Monitor server metrics

- CPU usage
- Memory usage
- Network bandwidth
- Connection count
- Response latency

### Record test results

```bash
# Save output to a file
ws_bench -c 2000 -n 1000 -d "test" ws://localhost:8080 > test-result.log 2>&1
```

## Notes

1. **Server capacity**: adjust the connection count according to server performance to avoid overload
2. **Network bandwidth**: watch for bandwidth limits to avoid congestion
3. **File descriptors**: make sure the system allows enough file descriptors
4. **Data format**: binary data must be valid hexadecimal format
5. **Send failures**: a send failure immediately stops that connection and does not retry
6. **TCP protocol**: in TCP Socket mode, the server must support a newline-delimited text protocol
7. **Sending mode**: use the `-w` option to wait for all connections to complete before sending uniformly, which suits simulating burst traffic
8. **Connectivity testing**: when `-d` is not provided, only the connection is tested without sending data

## License

MIT License

## Author

Fire Phoenix Studio
