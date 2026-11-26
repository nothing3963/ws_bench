//! Shared helpers: URL validation and `ws://`→wss upgrade, TCP address parsing,
//! packet-data encoding (`-d` hex/utf8 parsing via `PacketData`), protocol
//! detection (`ProtocolType`), and pre-compiled regexes (`HEX_REGEX` via
//! `once_cell::Lazy` for zero per-call compile cost).

use anyhow::{anyhow, Result};
use once_cell::sync::Lazy;
use regex::Regex;
use std::sync::Arc;

/// Pre-compiled hexadecimal regular expression (performance optimization)
static HEX_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"[0-9a-fA-F]{1,2}").unwrap()
});

/// Protocol type
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ProtocolType {
    /// WebSocket protocol
    WebSocket,
    /// TCP protocol
    Tcp,
}

/// Packet data type (uses Arc to optimize clone performance)
#[derive(Debug, Clone, PartialEq)]
pub enum PacketData {
    /// Text data
    Text(Arc<str>),
    /// Binary data
    Binary(Arc<[u8]>),
}

impl PacketData {
    /// Create a text packet (convenience method)
    pub fn text(s: impl Into<String>) -> Self {
        PacketData::Text(Arc::from(s.into()))
    }

    /// Create a binary packet (convenience method)
    pub fn binary(bytes: impl Into<Vec<u8>>) -> Self {
        PacketData::Binary(Arc::from(bytes.into()))
    }

    /// Format as a display string
    pub fn format_display(&self) -> String {
        use std::fmt::Write;
        match self {
            PacketData::Text(s) => s.to_string(),
            PacketData::Binary(b) => {
                if b.is_empty() {
                    return "[]".to_string();
                }
                let mut s = String::with_capacity(b.len() * 3 + 2);
                s.push('[');
                for (i, byte) in b.iter().enumerate() {
                    if i > 0 {
                        s.push(' ');
                    }
                    write!(s, "{:02x}", byte).unwrap();
                }
                s.push(']');
                s
            }
        }
    }
}

/// Detect the protocol type
pub fn detect_protocol(url: &str) -> ProtocolType {
    let url_lower = url.to_lowercase();
    if url_lower.starts_with("ws://")
        || url_lower.starts_with("wss://")
        || url_lower.starts_with("http://")
        || url_lower.starts_with("https://")
    {
        ProtocolType::WebSocket
    } else {
        // tcp://host:port and bare host:port are both treated as TCP
        ProtocolType::Tcp
    }
}

/// Parse packet content
/// Supported formats:
/// - Text data: wrapped in quotes, e.g. "hello" or 'hello'
/// - Binary data: in hexadecimal format, e.g. [ff ff ff ff]
pub fn parse_packet_data(input: &str) -> Result<PacketData> {
    let input = input.trim();

    if input.is_empty() {
        return Err(anyhow!("Packet content cannot be empty"));
    }

    // Check whether this is binary format [xx xx xx]
    if input.starts_with('[') && input.ends_with(']') {
        // Check whether this is a special message type
        let content = &input[1..input.len() - 1];
        let content_upper = content.to_uppercase();

        if content_upper == "PING" {
            return Ok(PacketData::binary(vec![0x09])); // WebSocket Ping frame type
        }
        if content_upper == "PONG" {
            return Ok(PacketData::binary(vec![0x0A])); // WebSocket Pong frame type
        }
        if content_upper == "CLOSE" {
            return Ok(PacketData::binary(vec![0x08])); // WebSocket Close frame type
        }

        // Parse hexadecimal data
        return parse_hex_data(content);
    }

    // Check whether this is quote-wrapped text (use char_indices to avoid UTF-8 slicing issues)
    if input.len() >= 2 {
        let mut chars = input.chars();
        if let (Some(first), Some(last)) = (chars.next(), chars.next_back()) {
            if (first == '"' && last == '"') || (first == '\'' && last == '\'') {
                // Use char_indices to slice safely
                let start_idx = first.len_utf8();
                let end_idx = input.len() - last.len_utf8();
                if start_idx <= end_idx {
                    let content = &input[start_idx..end_idx];
                    return Ok(PacketData::text(content));
                }
            }
        }
    }

    // By default, treat it as plain text
    Ok(PacketData::text(input))
}

/// Parse hexadecimal data (uses a pre-compiled regex)
fn parse_hex_data(content: &str) -> Result<PacketData> {
    let content = content.trim();

    if content.is_empty() {
        return Ok(PacketData::binary(Vec::new()));
    }

    // Use the pre-compiled regular expression to match hexadecimal bytes
    let mut bytes = Vec::new();

    for cap in HEX_REGEX.find_iter(content) {
        let hex_str = cap.as_str();
        match u8::from_str_radix(hex_str, 16) {
            Ok(byte) => bytes.push(byte),
            Err(_) => return Err(anyhow!("Invalid hexadecimal data: {}", hex_str)),
        }
    }

    if bytes.is_empty() {
        return Err(anyhow!("Failed to parse hexadecimal data: {}", content));
    }

    Ok(PacketData::binary(bytes))
}

/// Validate the URL format
pub fn validate_url(url: &str) -> Result<String> {
    if url.is_empty() {
        return Err(anyhow!("URL cannot be empty"));
    }

    let protocol = detect_protocol(url);

    match protocol {
        ProtocolType::WebSocket => {
            // WebSocket URL validation
            if !url.starts_with("ws://")
                && !url.starts_with("wss://")
                && !url.starts_with("http://")
                && !url.starts_with("https://")
            {
                return Err(anyhow!("Invalid WebSocket URL format, must start with ws:// or wss://"));
            }
            Ok(url.to_string())
        }
        ProtocolType::Tcp => {
            // TCP address format validation: accept `tcp://host:port` or bare `host:port`.
            // Strip an optional `tcp://` prefix (case-insensitive) before parsing.
            let addr = if url.len() >= 6 && url[..6].to_lowercase() == "tcp://" {
                &url[6..]
            } else {
                url
            };

            let parts: Vec<&str> = addr.split(':').collect();
            if parts.len() != 2 {
                return Err(anyhow!("TCP address format error, expected host:port"));
            }

            let host = parts[0];
            let port_str = parts[1];

            if host.is_empty() {
                return Err(anyhow!("TCP address host part cannot be empty"));
            }

            match port_str.parse::<u16>() {
                Ok(port) if port > 0 => Ok(format!("{}:{}", host, port)),
                _ => Err(anyhow!("TCP address port format error, expected a number between 1-65535")),
            }
        }
    }
}

/// Convert http/https URLs to ws/wss URLs
pub fn convert_to_ws_url(url: &str) -> String {
    if url.starts_with("http://") {
        url.replacen("http://", "ws://", 1)
    } else if url.starts_with("https://") {
        url.replacen("https://", "wss://", 1)
    } else {
        url.to_string()
    }
}

/// Parse an address into host and port
pub fn parse_tcp_addr(addr: &str) -> Result<(String, u16)> {
    // Accept `tcp://host:port` or bare `host:port`; strip an optional tcp:// prefix.
    let addr = if addr.len() >= 6 && addr[..6].to_lowercase() == "tcp://" {
        &addr[6..]
    } else {
        addr
    };

    let parts: Vec<&str> = addr.split(':').collect();
    if parts.len() != 2 {
        return Err(anyhow!("TCP address format error, expected host:port"));
    }

    let host = parts[0].to_string();
    let port = parts[1].parse::<u16>()
        .map_err(|_| anyhow!("Invalid port number: {}", parts[1]))?;

    Ok((host, port))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detect_protocol() {
        assert_eq!(
            detect_protocol("ws://localhost:8080"),
            ProtocolType::WebSocket
        );
        assert_eq!(
            detect_protocol("wss://example.com"),
            ProtocolType::WebSocket
        );
        assert_eq!(detect_protocol("http://localhost"), ProtocolType::WebSocket);
        assert_eq!(
            detect_protocol("https://example.com"),
            ProtocolType::WebSocket
        );
        assert_eq!(detect_protocol("tcp://127.0.0.1:9000"), ProtocolType::Tcp);
        assert_eq!(detect_protocol("127.0.0.1:8080"), ProtocolType::Tcp);
        assert_eq!(detect_protocol("localhost:3000"), ProtocolType::Tcp);
    }

    #[test]
    fn test_parse_packet_data() {
        // Text data
        let result = parse_packet_data("\"hello\"").unwrap();
        assert_eq!(result, PacketData::text("hello"));

        let result = parse_packet_data("'world'").unwrap();
        assert_eq!(result, PacketData::text("world"));

        // Plain text (no quotes)
        let result = parse_packet_data("plaintext").unwrap();
        assert_eq!(result, PacketData::text("plaintext"));

        // Binary data
        let result = parse_packet_data("[ff ff 00 ab]").unwrap();
        assert_eq!(result, PacketData::binary(vec![0xff, 0xff, 0x00, 0xab]));

        // Special message types
        let result = parse_packet_data("[PING]").unwrap();
        assert_eq!(result, PacketData::binary(vec![0x09]));
    }

    #[test]
    fn test_validate_url() {
        assert!(validate_url("ws://localhost:8080").is_ok());
        assert!(validate_url("tcp://127.0.0.1:9000").is_ok());
        assert_eq!(
            validate_url("tcp://127.0.0.1:9000").unwrap(),
            "127.0.0.1:9000"
        );
        assert!(validate_url("127.0.0.1:8080").is_ok());
        assert!(validate_url("localhost:3000").is_ok());
        assert!(validate_url("").is_err());
        assert!(validate_url("invalid").is_err());
    }
}
