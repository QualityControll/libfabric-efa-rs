//! Simple ping-pong example demonstrating libfabric-rs basic usage
//!
//! This example shows bidirectional communication between two endpoints.
//! The client sends a "ping" message and receives a "pong" response.

use eyre::Result;
use libfabric_rs::{AddressExchangeChannel, FabricEndpoint};

const MESSAGE_SIZE: usize = 64;
const PING_COUNT: usize = 10;

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();

    if args.len() > 1 {
        // Client mode
        run_client(&args[1]).await
    } else {
        // Server mode
        run_server().await
    }
}

async fn run_client(server_addr: &str) -> Result<()> {
    println!("Ping-Pong Client");
    println!("================\n");

    // Initialize endpoint
    let mut endpoint = FabricEndpoint::new()?;

    // Connect and exchange addresses
    let mut channel = AddressExchangeChannel::connect(server_addr, None).await?;
    let peer_addr = channel.exchange(&endpoint, true).await?;
    let peer_id = endpoint.insert_peer(&peer_addr)?;

    println!("Connected to server at {}\n", server_addr);

    // Ping-pong loop
    let mut buf = vec![0u8; MESSAGE_SIZE];

    for i in 1..=PING_COUNT {
        // Send ping
        let msg = format!("PING {}", i);
        buf[..msg.len()].copy_from_slice(msg.as_bytes());
        println!("→ Sending: {}", msg);

        buf = endpoint.send_to(peer_id, buf).await?;

        // Receive pong
        buf = endpoint.recv(buf).await?;

        let response = String::from_utf8_lossy(&buf[..16]);
        let response = response.trim_end_matches('\0');
        println!("← Received: {}", response);

        // Clear buffer for next iteration
        buf.fill(0);
    }

    println!("\n✓ Completed {} ping-pongs!", PING_COUNT);
    Ok(())
}

async fn run_server() -> Result<()> {
    println!("Ping-Pong Server");
    println!("================\n");

    // Initialize endpoint
    let mut endpoint = FabricEndpoint::new()?;

    // Listen and exchange addresses
    let mut channel = AddressExchangeChannel::listen(None).await?;
    let peer_addr = channel.exchange(&endpoint, false).await?;
    let peer_id = endpoint.insert_peer(&peer_addr)?;

    println!("Waiting for ping messages...\n");

    // Ping-pong loop
    let mut buf = vec![0u8; MESSAGE_SIZE];

    for i in 1..=PING_COUNT {
        // Receive ping
        buf = endpoint.recv(buf).await?;

        let message = String::from_utf8_lossy(&buf[..16]);
        let message = message.trim_end_matches('\0');
        println!("← Received: {}", message);

        // Send pong
        buf.fill(0);
        let response = format!("PONG {}", i);
        buf[..response.len()].copy_from_slice(response.as_bytes());
        println!("→ Sending: {}", response);

        buf = endpoint.send_to(peer_id, buf).await?;
    }

    println!("\n✓ Completed {} ping-pongs!", PING_COUNT);
    Ok(())
}
