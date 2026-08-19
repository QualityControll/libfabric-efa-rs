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
Starting EFA Bandwidth Benchmark (Async Rust)
==============================================

bytes        #sent    #ack     total        time       MB/sec     usec/xfer    Mxfers/sec
64           100      =100     6.2k         0.02       0.28       220.81       0.00
128          100      =100     12.5k        0.00       3.88       31.46        0.03
256          100      =100     25.0k        0.00       7.81       31.27        0.03
512          100      =100     50.0k        0.00       15.44      31.63        0.03
1k           100      =100     100.0k       0.00       30.96      31.54        0.03
2k           100      =100     200.0k       0.00       61.33      31.85        0.03
4k           100      =100     400.0k       0.00       118.27     33.03        0.03
8k           100      =100     800.0k       0.00       223.07     35.02        0.03
16k          100      =100     1.6M         0.00       437.60     35.71        0.03
32k          100      =100     3.1M         0.00       847.63     36.87        0.03
64k          100      =100     6.2M         0.00       1599.52    39.07        0.03
128k         100      =100     12.5M        0.01       1759.83    71.03        0.01
256k         100      =100     25.0M        0.01       3178.62    78.65        0.01
512k         100      =100     50.0M        0.01       4011.76    124.63       0.01
1M           100      =100     100.0M       0.01       10565.05   94.65        0.01
2M           100      =100     200.0M       0.01       16965.04   117.89       0.01
4M           100      =100     400.0M       0.02       20856.15   191.79       0.01
8M           100      =100     800.0M       0.03       24477.27   326.83       0.00
16M          100      =100     1.6G         0.07       23987.50   667.01       0.00
32M          100      =100     3.1G         0.13       23942.71   1336.52      0.00
64M          100      =100     6.2G         0.27       23975.32   2669.41      0.00
128M         100      =100     12.5G        0.53       23971.64   5339.64      0.00
256M         100      =100     25.0G        1.07       23971.85   10679.19     0.00
512M         100      =100     50.0G        2.14       23970.91   21359.22     0.00
1G           100      =100     100.0G       4.27       23972.09   42716.34     0.00
```

## License

MIT

## Related Projects

- [libfabric](https://github.com/ofiwg/libfabric): The underlying C library

This library provides a higher-level, async-friendly abstraction over the official bindings.
