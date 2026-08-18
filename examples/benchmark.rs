// EFA RDM Benchmark Tool - replica of fi_pingpong
use clap::Parser;
use eyre::Result;
use libfabric_rs::{AddressExchangeChannel, FabricEndpoint, PeerId};
use std::time::Instant;

const WARMUP_ITERATIONS: usize = 10;

#[derive(Parser, Debug)]
#[command(author, version, about = "Async EFA RDM Benchmark Tool", long_about = None)]
struct Args {
    /// Server address (omit to run as server)
    server_addr: Option<String>,

    /// Number of iterations per size
    #[arg(short = 'I', long, default_value_t = 100)]
    iterations: usize,

    /// Minimum message size
    #[arg(short = 's', long, default_value_t = 64)]
    min_size: usize,

    /// Maximum message size
    #[arg(short = 'S', long, default_value_t = 1024 * 1024 * 1024)]
    max_size: usize,
}

async fn run_benchmark(
    endpoint: &FabricEndpoint,
    peer: PeerId,
    is_client: bool,
    min_size: usize,
    max_size: usize,
    iterations: usize,
) -> Result<()> {
    if is_client {
        println!("\nStarting EFA Bandwidth Benchmark");
        println!("==============================================\n");
        print_header();
    } else {
        println!("\nServer ready for benchmark...");
    }

    let mut size = min_size;
    while size <= max_size {
        let mut buffer = vec![0u8; size];

        // Warmup
        for _ in 0..WARMUP_ITERATIONS {
            if is_client {
                buffer = endpoint.send_to(peer, buffer).await?;
            } else {
                buffer = endpoint.recv(buffer).await?;
            }
        }

        // Benchmark
        let start = Instant::now();
        for _ in 0..iterations {
            if is_client {
                buffer = endpoint.send_to(peer, buffer).await?;
            } else {
                buffer = endpoint.recv(buffer).await?;
            }
        }
        let elapsed = start.elapsed().as_secs_f64();

        if is_client {
            print_result(size, iterations, elapsed);
        }

        if size == max_size {
            break;
        }
        size *= 2;
        if size > max_size {
            size = max_size;
        }
    }

    if is_client {
        println!("\nBenchmark complete!");
    } else {
        println!("Benchmark complete (server side)");
    }

    Ok(())
}

fn format_size(size: usize) -> String {
    if size >= 1024 * 1024 * 1024 {
        format!("{}G", size / (1024 * 1024 * 1024))
    } else if size >= 1024 * 1024 {
        format!("{}M", size / (1024 * 1024))
    } else if size >= 1024 {
        format!("{}k", size / 1024)
    } else {
        format!("{}", size)
    }
}

fn format_total(bytes: f64) -> String {
    if bytes >= 1024.0 * 1024.0 * 1024.0 {
        format!("{:.1}G", bytes / (1024.0 * 1024.0 * 1024.0))
    } else if bytes >= 1024.0 * 1024.0 {
        format!("{:.1}M", bytes / (1024.0 * 1024.0))
    } else if bytes >= 1024.0 {
        format!("{:.1}k", bytes / 1024.0)
    } else {
        format!("{:.0}", bytes)
    }
}

fn print_header() {
    println!(
        "{:<12} {:<8} {:<8} {:<12} {:<10} {:<10} {:<12} {:<12}",
        "bytes", "#sent", "#ack", "total", "time", "MB/sec", "usec/xfer", "Mxfers/sec"
    );
}

fn print_result(size: usize, iters: usize, elapsed_secs: f64) {
    let total_bytes = size as f64 * iters as f64;
    let mb_sec = (total_bytes / (1024.0 * 1024.0)) / elapsed_secs;
    let usec_per_xfer = (elapsed_secs * 1_000_000.0) / iters as f64;
    let mxfers_sec = (iters as f64 / elapsed_secs) / 1_000_000.0;

    println!(
        "{:<12} {:<8} ={:<7} {:<12} {:<10.2} {:<10.2} {:<12.2} {:<12.2}",
        format_size(size),
        iters,
        iters,
        format_total(total_bytes),
        elapsed_secs,
        mb_sec,
        usec_per_xfer,
        mxfers_sec
    );
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    println!("Async Rust EFA Benchmark");
    println!("Using tokio async runtime\n");

    // Initialize fabric endpoint
    let mut endpoint = FabricEndpoint::new()?;

    // Setup control connection and exchange addresses
    let is_client = args.server_addr.is_some();
    let peer_id = if is_client {
        let mut conn =
            AddressExchangeChannel::connect(args.server_addr.as_ref().unwrap(), None).await?;
        let peer_addr = conn.exchange(&endpoint, true).await?;
        endpoint.insert_peer(&peer_addr)?
    } else {
        let mut conn = AddressExchangeChannel::listen(None).await?;
        let peer_addr = conn.exchange(&endpoint, false).await?;
        endpoint.insert_peer(&peer_addr)?
    };

    println!("Peer configured: {:?}", peer_id);

    run_benchmark(
        &endpoint,
        peer_id,
        is_client,
        args.min_size,
        args.max_size,
        args.iterations,
    )
    .await?;

    println!("Done!");
    Ok(())
}
