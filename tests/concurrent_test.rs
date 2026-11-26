mod common;

use anyhow::Result;
use common::{EventCollector, MockWebSocketServer, count_events};
use tokio::time::{sleep, Duration};
use socket_bench::client::{run_client, ClientConfig};
use socket_bench::event::{create_event_channel, ClientEvent};
use socket_bench::utils::PacketData;

/// High-concurrency stress test
/// 50 clients, each sending 100 packets
/// Verifies at least a 90% connection success rate
#[tokio::test]
#[ignore]  // Ignored by default; run with cargo test -- --ignored
async fn test_high_concurrency_stress() {
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

    println!("启动高并发压力测试: 50 个客户端 × 100 个数据包");

    // Start 50 concurrent clients, each sending 100 packets
    let mut handles = vec![];
    for client_id in 0..50 {
        let url = url.clone();
        let tx = event_tx.clone();
        let rx = disconnect_rx.resubscribe();
        let start_rx = start_send_rx.resubscribe();

        let handle = tokio::spawn(async move {
            let config = ClientConfig::new(url)
                .with_packet_count(100)
                .with_batch_size(10)
                .with_interval(1)
                .with_data(PacketData::text("stress_test"));

            run_client(client_id, config, tx, rx, start_rx).await
        });

        handles.push(handle);
    }

    // Wait a short while to let all connections establish
    sleep(Duration::from_millis(500)).await;
    // Send the start signal
    let _ = start_send_tx.send(());

    // Wait for all clients to finish
    for handle in handles {
        let _ = tokio::time::timeout(Duration::from_secs(30), handle).await;
    }

    // Leave a little time for event collection
    sleep(Duration::from_millis(500)).await;

    let events = collector.get_all().await;
    println!("压力测试 - 总事件数: {}", events.len());

    // Count successful connections
    let connection_success_count =
        count_events(&events, |e| matches!(e, ClientEvent::ConnectionSuccess { .. }));
    println!("连接成功: {}/50", connection_success_count);

    // Count successful sends
    let send_success_count = count_events(&events, |e| matches!(e, ClientEvent::SendSuccess { .. }));
    println!("发送成功: {} (期望 5000)", send_success_count);

    // Verify that at least 90% of connections succeeded (45 or more)
    assert!(
        connection_success_count >= 45,
        "至少 90% 的连接应该成功，实际成功: {}/50",
        connection_success_count
    );

    // Verify that at least 50% of packets were sent successfully
    // Some failures are allowed due to network and timing reasons
    assert!(
        send_success_count >= 2500,
        "至少 50% 的数据包应该被发送成功，实际: {}",
        send_success_count
    );

    println!("✓ 压力测试通过！");
}