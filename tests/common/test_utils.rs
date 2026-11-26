use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::{sleep, timeout, Duration};
use socket_bench::event::ClientEvent;

/// Event collector - collects all events that occur
pub struct EventCollector {
    events: Arc<Mutex<Vec<ClientEvent>>>,
}

impl EventCollector {
    /// Create a new event collector
    pub fn new() -> Self {
        EventCollector {
            events: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Add an event
    pub async fn add(&self, event: ClientEvent) {
        let mut events = self.events.lock().await;
        events.push(event);
    }

    /// Get a copy of all events
    pub async fn get_all(&self) -> Vec<ClientEvent> {
        let events = self.events.lock().await;
        events.clone()
    }

    /// Clear all events
    pub async fn clear(&self) {
        let mut events = self.events.lock().await;
        events.clear();
    }

    /// Get the number of events
    pub async fn len(&self) -> usize {
        let events = self.events.lock().await;
        events.len()
    }
}

impl Default for EventCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for EventCollector {
    fn clone(&self) -> Self {
        EventCollector {
            events: Arc::clone(&self.events),
        }
    }
}

/// Wait for a specific event to appear
/// # Parameters
/// - `events`: the event list
/// - `predicate`: a closure to determine whether an event matches
/// - `timeout_ms`: the timeout (milliseconds)
/// # Returns
/// Returns true if the event is found before the timeout, otherwise false
pub async fn wait_for_event<F>(
    events: &[ClientEvent],
    predicate: F,
    timeout_ms: u64,
) -> bool
where
    F: Fn(&ClientEvent) -> bool,
{
    let deadline = tokio::time::Instant::now() + Duration::from_millis(timeout_ms);

    loop {
        if let Some(_) = events.iter().find(|e| predicate(e)) {
            return true;
        }

        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return false;
        }

        // Re-check after waiting a short while
        sleep(Duration::from_millis(50)).await;
    }
}

/// Count the number of events matching a specific condition
pub fn count_events<F>(events: &[ClientEvent], predicate: F) -> usize
where
    F: Fn(&ClientEvent) -> bool,
{
    events.iter().filter(|e| predicate(e)).count()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_event_collector() {
        let collector = EventCollector::new();

        // Initial state is empty
        assert_eq!(collector.len().await, 0);

        // Add an event
        let event = ClientEvent::ConnectionSuccess {
            client_id: 1,
            url: "ws://test".to_string(),
        };
        collector.add(event.clone()).await;

        assert_eq!(collector.len().await, 1);
        let all = collector.get_all().await;
        assert_eq!(all.len(), 1);

        // Clear
        collector.clear().await;
        assert_eq!(collector.len().await, 0);
    }

    #[tokio::test]
    async fn test_count_events() {
        let events = vec![
            ClientEvent::ConnectionSuccess {
                client_id: 1,
                url: "ws://test".to_string(),
            },
            ClientEvent::SendSuccess {
                client_id: 1,
                data: Arc::from("test"),
            },
            ClientEvent::SendSuccess {
                client_id: 2,
                data: Arc::from("test"),
            },
        ];

        let send_count = count_events(&events, |e| matches!(e, ClientEvent::SendSuccess { .. }));
        assert_eq!(send_count, 2);

        let client1_count = count_events(&events, |e| match e {
            ClientEvent::SendSuccess { client_id, .. } => *client_id == 1,
            _ => false,
        });
        assert_eq!(client1_count, 1);
    }
}
