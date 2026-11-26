mod common;

use anyhow::Result;
use common::{EventCollector, MockTcpServer, MockWebSocketServer, count_events};
use tokio::time::{sleep, Duration};
use socket_bench::client::{run_client, ClientConfig};
use socket_bench::event::{create_event_channel, ClientEvent};
use socket_bench::utils::PacketData;

/// Single client WebSocket connection test
#[tokio::test]
async fn test_single_client_websocket_connection() {
    let server = MockWebSocketServer::start().await.expect("启动 WebSocket 服务器失败");
    let url = server.url();

    let (event_tx, mut event_rx) = create_event_channel();
    let (_disconnect_tx, disconnect_rx) = tokio::sync::broadcast::channel(1);
    let (_start_send_tx, start_send_rx) = tokio::sync::broadcast::channel(1);
    let collector = EventCollector::new();
    let collector_clone = collector.clone();

    // Send the start signal immediately
    let _ = _start_send_tx.send(());

    // Start the event collection task
    tokio::spawn(async move {
        while let Some(event) = event_rx.recv().await {
            collector_clone.add(event).await;
        }
    });

    // Configure the client: send 5 packets
    let config = ClientConfig::new(url)
        .with_packet_count(5)
        .with_batch_size(1)
        .with_interval(5)
        .with_data(PacketData::text("test"));

    // Run the client
    let client_handle = tokio::spawn(run_client(1, config, event_tx.clone(), disconnect_rx, start_send_rx));

    // Wait for the client to finish
    let _ = tokio::time::timeout(Duration::from_secs(5), client_handle).await;

    // Leave a little time for event collection
    sleep(Duration::from_millis(200)).await;

    let events = collector.get_all().await;
    println!("Collected events: {:?}", events);

    // Verify the events
    assert!(
        events.iter().any(|e| matches!(e, ClientEvent::ConnectionSuccess { .. })),
        "应该有 ConnectionSuccess 事件"
    );

    let send_success_count = count_events(&events, |e| matches!(e, ClientEvent::SendSuccess { .. }));
    assert_eq!(send_success_count, 5, "应该有 5 个 SendSuccess 事件");

    let recv_success_count =
        count_events(&events, |e| matches!(e, ClientEvent::ReceiveSuccess { .. }));
    assert_eq!(recv_success_count, 5, "应该有 5 个 ReceiveSuccess 事件（Echo）");
}

/// Concurrent multi-client connection test
#[tokio::test]
async fn test_concurrent_clients_websocket() {
    let server = MockWebSocketServer::start().await.expect("启动 WebSocket 服务器失败");
    let url = server.url();

    let (event_tx, mut event_rx) = create_event_channel();
    let (_disconnect_tx, disconnect_rx) = tokio::sync::broadcast::channel(1);
    let (start_send_tx, start_send_rx) = tokio::sync::broadcast::channel(1);
    let collector = EventCollector::new();
    let collector_clone = collector.clone();

    // Start the event collection task
    tokio::spawn(async move {
        while let Some(event) = event_rx.recv().await {
            collector_clone.add(event).await;
        }
    });

    // Start 10 concurrent clients, each sending 10 packets
    let mut handles = vec![];
    for client_id in 0..10 {
        let url = url.clone();
        let tx = event_tx.clone();
        let rx = disconnect_rx.resubscribe();
        let start_rx = start_send_rx.resubscribe();

        let handle = tokio::spawn(async move {
            let config = ClientConfig::new(url)
                .with_packet_count(10)
                .with_batch_size(2)
                .with_interval(5)
                .with_data(PacketData::text("concurrent"));

            run_client(client_id, config, tx, rx, start_rx).await
        });

        handles.push(handle);
    }

    // Wait a bit for all connections to be established
    sleep(Duration::from_millis(100)).await;
    // Send the start signal
    let _ = start_send_tx.send(());

    // Wait for all clients to finish
    for handle in handles {
        let _ = tokio::time::timeout(Duration::from_secs(10), handle).await;
    }

    // Leave a little time for event collection
    sleep(Duration::from_millis(300)).await;

    let events = collector.get_all().await;
    println!("Concurrent test - total events: {}", events.len());

    // Verify the connection success count
    let connection_success_count =
        count_events(&events, |e| matches!(e, ClientEvent::ConnectionSuccess { .. }));
    assert_eq!(
        connection_success_count, 10,
        "应该有 10 个客户端连接成功"
    );

    // Verify the send success count (at least 90, allowing some failures)
    let send_success_count = count_events(&events, |e| matches!(e, ClientEvent::SendSuccess { .. }));
    assert!(
        send_success_count >= 90,
        "应该至少有 90 个发送成功事件，实际: {}",
        send_success_count
    );
}

/// TCP Socket connection test
#[tokio::test]
async fn test_single_client_tcp_connection() {
    let server = MockTcpServer::start().await.expect("启动 TCP 服务器失败");
    let addr = server.addr();

    let (event_tx, mut event_rx) = create_event_channel();
    let (_disconnect_tx, disconnect_rx) = tokio::sync::broadcast::channel(1);
    let (_start_send_tx, start_send_rx) = tokio::sync::broadcast::channel(1);
    let collector = EventCollector::new();
    let collector_clone = collector.clone();

    // Send the start signal immediately
    let _ = _start_send_tx.send(());

    // Start the event collection task
    tokio::spawn(async move {
        while let Some(event) = event_rx.recv().await {
            collector_clone.add(event).await;
        }
    });

    // Configure the client: use TCP protocol
    let config = ClientConfig::new(addr)
        .with_packet_count(3)
        .with_batch_size(1)
        .with_interval(10)
        .with_data(PacketData::text("tcp_test"));

    // Run the client
    let client_handle = tokio::spawn(run_client(1, config, event_tx.clone(), disconnect_rx, start_send_rx));

    // Wait for the client to finish
    let _ = tokio::time::timeout(Duration::from_secs(5), client_handle).await;

    // Leave a little time for event collection
    sleep(Duration::from_millis(200)).await;

    let events = collector.get_all().await;
    println!("TCP test - total events: {}", events.len());

    // Verify connection success
    assert!(
        events.iter().any(|e| matches!(e, ClientEvent::ConnectionSuccess { .. })),
        "应该有 ConnectionSuccess 事件"
    );

    // Verify send and receive
    let send_count = count_events(&events, |e| matches!(e, ClientEvent::SendSuccess { .. }));
    assert_eq!(send_count, 3, "应该有 3 个 SendSuccess 事件");

    let recv_count = count_events(&events, |e| matches!(e, ClientEvent::ReceiveSuccess { .. }));
    assert_eq!(recv_count, 3, "应该有 3 个 ReceiveSuccess 事件");
}

/// Batch sending test
#[tokio::test]
async fn test_batch_sending() {
    let server = MockWebSocketServer::start().await.expect("启动 WebSocket 服务器失败");
    let url = server.url();

    let (event_tx, mut event_rx) = create_event_channel();
    let (_disconnect_tx, disconnect_rx) = tokio::sync::broadcast::channel(1);
    let (_start_send_tx, start_send_rx) = tokio::sync::broadcast::channel(1);
    let collector = EventCollector::new();
    let collector_clone = collector.clone();

    // Send the start signal immediately
    let _ = _start_send_tx.send(());

    // Start the event collection task
    tokio::spawn(async move {
        while let Some(event) = event_rx.recv().await {
            collector_clone.add(event).await;
        }
    });

    // Configure the client: 20 packets, batch of 5, 100ms interval
    let config = ClientConfig::new(url)
        .with_packet_count(20)
        .with_batch_size(5)
        .with_interval(100)
        .with_data(PacketData::text("batch"));

    let start = tokio::time::Instant::now();

    // Run the client
    let client_handle = tokio::spawn(run_client(1, config, event_tx.clone(), disconnect_rx, start_send_rx));

    // Wait for the client to finish
    let _ = tokio::time::timeout(Duration::from_secs(10), client_handle).await;

    let elapsed = start.elapsed();

    // Leave a little time for event collection
    sleep(Duration::from_millis(200)).await;

    let events = collector.get_all().await;

    // Verify the send count
    let send_count = count_events(&events, |e| matches!(e, ClientEvent::SendSuccess { .. }));
    assert_eq!(send_count, 20, "应该有 20 个 SendSuccess 事件");

    // Verify total time is at least 300ms (3 batches × 100ms interval)
    // Note: actual time may be slightly longer, allowing some tolerance
    println!("Batch send elapsed: {:?}", elapsed);
    assert!(
        elapsed.as_millis() >= 300,
        "批量发送应该花费至少 300ms，实际: {:?}",
        elapsed
    );
}

/// Connection failure handling test
#[tokio::test]
async fn test_connection_failure() {
    let invalid_url = "ws://127.0.0.1:1";  // Use a non-existent port

    let (event_tx, mut event_rx) = create_event_channel();
    let (_disconnect_tx, disconnect_rx) = tokio::sync::broadcast::channel(1);
    let (_start_send_tx, start_send_rx) = tokio::sync::broadcast::channel(1);
    let collector = EventCollector::new();
    let collector_clone = collector.clone();

    // Start the event collection task
    tokio::spawn(async move {
        while let Some(event) = event_rx.recv().await {
            collector_clone.add(event).await;
        }
    });

    // Configure the client
    let config = ClientConfig::new(invalid_url.to_string())
        .with_packet_count(5)
        .with_batch_size(1)
        .with_interval(10)
        .with_data(PacketData::text("test"));

    // Run the client
    let client_handle = tokio::spawn(run_client(1, config, event_tx.clone(), disconnect_rx, start_send_rx));

    // Wait for the client to finish
    let _ = tokio::time::timeout(Duration::from_secs(5), client_handle).await;

    // Leave a little time for event collection
    sleep(Duration::from_millis(200)).await;

    let events = collector.get_all().await;
    println!("Failure test - events: {:?}", events);

    // Verify that connection failure events exist
    assert!(
        events.iter().any(|e| matches!(e, ClientEvent::ConnectionFailed { .. })),
        "应该有 ConnectionFailed 事件"
    );

    // Verify that no send success events exist
    let send_count = count_events(&events, |e| matches!(e, ClientEvent::SendSuccess { .. }));
    assert_eq!(send_count, 0, "连接失败的情况下不应该有 SendSuccess 事件");
}