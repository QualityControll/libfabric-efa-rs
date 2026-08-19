# libfabric-efa-rs

High-performance async Rust wrapper for libfabric with EFA support.

## Overview

This library provides a safe, ergonomic async interface to libfabric for RDMA communication. It uses an ownership-based API design to guarantee memory safety while maintaining zero-copy performance comparable to native C implementations.

## Features

- **Safe API**: Ownership-based design prevents undefined behavior
- **High Performance**: Throughput matching libfabric C implementation
- **Async/Await**: Full tokio integration for concurrent operations
- **Zero-Copy**: Direct hardware access without memory copies
- **EFA Support**: Works with AWS Elastic Fabric Adapter
- **Multi-Peer**: Single endpoint can communicate with multiple peers
- **Serializable Addresses**: Exchange addresses through any control plane

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
libfabric-rs = { path = "path/to/libfabric-rs" }
tokio = { version = "1.40", features = ["rt-multi-thread", "macros", "net"] }
eyre = "0.6"
```

### Prerequisites

- [Instance set up to support EFA](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/efa.html)
    - Instance type supports EFA and RDMA
    - Security Group allowing EFA traffic
    - Enabled EFA on NIC
    - Cluster placement group
- libfabric library installed
- clang/LLVM for bindgen (build-time only)

**Amazon Linux 2023:**
```bash
sudo yum install -y clang-devel
```

**Ubuntu/Debian:**
```bash
sudo apt install -y libfabric-dev clang
```

### Build Configuration

The library automatically detects libfabric using:
1. pkg-config (preferred method)
2. `LIBFABRIC_DIR` or `LIBFABRIC_PREFIX` environment variables
3. Common installation paths

## Quick Start

### Client Example

```rust
use eyre::Result;
use libfabric_rs::{AddressExchangeChannel, FabricEndpointBuilder};

#[tokio::main]
async fn main() -> Result<()> {
    let mut endpoint = FabricEndpointBuilder::new()
        .fabric_attr_prov_name(CString::new("sockets").unwrap())
        .caps(ffi::FI_MSG)
        .mode(ffi::FI_CONTEXT)
        .domain_attr_threading(ffi::fi_threading_FI_THREAD_SAFE)
        .build()?;
    
    // Exchange addresses with server
    let mut channel = AddressExchangeChannel::connect("192.168.1.100", None).await?;
    let peer_addr = channel.exchange(&endpoint, true).await?;
    let peer_id = endpoint.insert_peer(&peer_addr)?;
    
    // Send data
    let mut buf = vec![0u8; 1024];
    buf[..5].copy_from_slice(b"Hello");
    buf = endpoint.send_to(peer_id, buf).await?;
    
    Ok(())
}
```

### Server Example

```rust
use eyre::Result;
use libfabric_rs::{AddressExchangeChannel, FabricEndpointBuilder};

#[tokio::main]
async fn main() -> Result<()> {
    let mut endpoint = FabricEndpointBuilder::new()
        .fabric_attr_prov_name(CString::new("sockets").unwrap())
        .caps(ffi::FI_MSG)
        .mode(ffi::FI_CONTEXT)
        .domain_attr_threading(ffi::fi_threading_FI_THREAD_SAFE)
        .build()?;

    // Exchange addresses with client
    let mut channel = AddressExchangeChannel::listen(None).await?;
    let peer_addr = channel.exchange(&endpoint, false).await?;
    let peer_id = endpoint.insert_peer(&peer_addr)?;
    
    // Receive data
    let buf = vec![0u8; 1024];
    let buf = endpoint.recv(buf).await?;
    println!("Received {} bytes", buf.len());
    
    Ok(())
}
```

## Performance

Tested on AWS EC2 c8gn.16xlarge	instance (200 Gbps).

```
todo!  Rerun benchmark on AWS EC2 hardware!

```

## License

MIT

## Related Projects

- [libfabric](https://github.com/ofiwg/libfabric): The underlying C library

This library provides a higher-level, async-friendly abstraction over the official bindings.
