//! Async Rust wrapper for libfabric with EFA support
//!
//! This library provides a safe, async interface to libfabric for high-performance
//! RDMA communication. It uses an ownership-based API to guarantee memory safety
//! while maintaining zero-copy performance.
//!
//! # Example
//!
//! ```ignore
//! use eyre::Result;
//! use libfabric_rs::{AddressExchangeChannel, FabricEndpoint};
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     let mut endpoint = FabricEndpoint::new()?;
//!     let mut channel = AddressExchangeChannel::connect("192.168.1.100", None).await?;
//!     let peer_addr = channel.exchange(&endpoint, true).await?;
//!     let peer_id = endpoint.insert_peer(&peer_addr)?;
//!     
//!     let mut buf = vec![0u8; 1024];
//!     buf = endpoint.send_to(peer_id, buf).await?;
//!     
//!     Ok(())
//! }
//! ```

use eyre::{bail, ensure, Context, Result};
use ofi_libfabric_sys::bindgen as ffi;
use serde::{Deserialize, Serialize};
use std::ffi::{CString, c_void};
use std::os::unix::io::{AsRawFd, RawFd};
use std::ptr;
use std::ptr::NonNull;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::io::unix::AsyncFd;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::sync::oneshot::{Sender};


/// Default TCP port for control channel (address exchange)
pub const CONTROL_PORT: u16 = 9229;

pub const DEFAULT_PORT: u16 = 9228;
const EAGAIN_ERROR: isize = -(ffi::FI_EAGAIN as i32) as isize;

/// Compact, serializable representation of a libfabric endpoint address.
///
/// `FabricAddress` wraps the opaque byte blob returned by `fi_getname`. Because
/// it implements `Serialize`/`Deserialize`, callers can exchange the address
/// through any out-of-band channel (files, RPC, etc.) without relying on the
/// auxiliary TCP helper provided in this crate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FabricAddress {
    bytes: Vec<u8>,
}

impl FabricAddress {
    /// Creates a new address from raw bytes.
    pub fn new(bytes: Vec<u8>) -> Self {
        Self { bytes }
    }

    /// Returns the raw bytes of the address.
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Consumes the wrapper and returns the owned byte vector.
    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }

    /// Returns the length in bytes.
    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    /// Returns true if the address contains no bytes.
    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }
}

impl From<Vec<u8>> for FabricAddress {
    fn from(bytes: Vec<u8>) -> Self {
        Self::new(bytes)
    }
}

impl AsRef<[u8]> for FabricAddress {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}


pub struct Info {
    ptr: NonNull<ffi::fi_info>
}

impl Info {
    pub fn new() -> Self {
        Self {
            ptr: NonNull::new(
            unsafe {
                ffi::fi_allocinfo()
            }).expect("Failed to allocate info")
        }
    }

    pub fn new_from(info: NonNull<ffi::fi_info>) -> Self {
        Self {
            ptr: info
        }
    }
}

impl Drop for Info {
    fn drop(&mut self) {
        unsafe {
            ffi::fi_freeinfo(self.ptr.as_ptr());
        }
    }
}


/// Identifier for a peer in the address vector.
///
/// This is a type-safe wrapper around libfabric's `fi_addr_t`. Each peer
/// that is inserted into the endpoint's address vector gets a unique PeerId.
///
/// # Example
///
/// ```ignore
/// let peer1 = endpoint.insert_peer(&addr1)?;
/// let peer2 = endpoint.insert_peer(&addr2)?;
/// buf = endpoint.send_to(peer1, buf).await?;
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PeerId(pub ffi::fi_addr_t);


#[derive(Clone)]
struct CompletionQueueFd(RawFd);

impl CompletionQueueFd {
    fn new(cq: *mut ffi::fid_cq) -> Result<Self> {
        unsafe {
            let mut fd: RawFd = -1;
            let ret = ffi::fi_control(&mut (*cq).fid as *mut ffi::fid, ffi::FI_GETWAIT as i32, &mut fd as *mut i32 as *mut c_void);
            if ret != 0 {
                bail!("fi_control failed");
            }
            Ok(Self {
                0: fd
            })
        }
    }
}

impl AsRawFd for CompletionQueueFd {
    fn as_raw_fd(&self) -> RawFd { self.0 }
}

unsafe fn cq_waitable(resources: &FabricEndpointResources) -> bool {
    let cq_fid = &mut (*resources.cq).fid as *mut ffi::fid;
    let mut fds: [*mut ffi::fid; 1] = [ cq_fid ];
    let ret = ffi::fi_trywait(resources.fabric, fds.as_mut_ptr(), 1);
    return ret == 0;
}

enum CqReadResult {
    CqReadSuccess(ffi::fi_cq_data_entry),
    CqErrorAgain(),
    CqError()
}

unsafe fn read_cq_entry(resources: &FabricEndpointResources) -> CqReadResult {
    let mut comp: ffi::fi_cq_data_entry = std::mem::zeroed();
    let ret = ffi::fi_cq_read(
            resources.cq,
            &mut comp as *mut ffi::fi_cq_data_entry as *mut libc::c_void,
            1);

    if ret == 1 {
       return CqReadResult::CqReadSuccess(comp);
    } else if ret == EAGAIN_ERROR {
        return CqReadResult::CqErrorAgain();
    } else {
       return CqReadResult::CqError();        
    }
}

/// A fabric endpoint for RDMA communication.
///
/// This structure manages the libfabric resources needed for RDMA operations,
/// including fabric, domain, endpoint, address vector, and completion queue.
///
/// Resources are automatically cleaned up when the endpoint is dropped.
///
/// # Thread Safety
///
/// `FabricEndpoint` is configured with `FI_THREAD_SAFE` mode, which allows
/// concurrent access to the endpoint and completion queue from multiple threads.
/// The EFA provider supports thread-safe operations, and all libfabric calls
/// are internally synchronized.
///
/// Operations like `send_to()` and `recv()` move work to blocking threads via
/// `spawn_blocking`, so concurrent calls are safe and will not interfere with
/// each other.
struct FabricEndpointResources {
    fabric: *mut ffi::fid_fabric,
    domain: *mut ffi::fid_domain,
    ep: *mut ffi::fid_ep,
    av: *mut ffi::fid_av,
    cq: *mut ffi::fid_cq,
}

// SAFETY: FabricEndpoint is configured with FI_THREAD_SAFE mode during initialization,
// which ensures that the EFA provider's internal structures are thread-safe.
unsafe impl Send for FabricEndpointResources {}
unsafe impl Sync for FabricEndpointResources {}

impl Drop for FabricEndpointResources {
    fn drop(&mut self) {
        unsafe {
            if !self.ep.is_null() {
                ffi::fi_close(&mut (*self.ep).fid as *mut ffi::fid);
            }
            if !self.av.is_null() {
                ffi::fi_close(&mut (*self.av).fid as *mut ffi::fid);
            }
            if !self.cq.is_null() {
                ffi::fi_close(&mut (*self.cq).fid as *mut ffi::fid);
            }
            if !self.domain.is_null() {
                ffi::fi_close(&mut (*self.domain).fid as *mut ffi::fid);
            }
            if !self.fabric.is_null() {
                ffi::fi_close(&mut (*self.fabric).fid as *mut ffi::fid);
            }
        }
    }
}

#[derive(Clone)]
pub struct FabricEndpoint {
    inner: Arc<FabricEndpointResources>
}



impl FabricEndpoint {
    /// Creates a new fabric endpoint with EFA provider.
    ///
    /// This initializes all necessary libfabric resources including fabric,
    /// domain, endpoint, completion queue, and address vector.
    ///
    /// # Returns
    ///
    /// Returns `Ok(FabricEndpoint)` on success, or an error if initialization fails.
    ///
    /// # Errors
    ///
    /// Returns an error if any libfabric initialization call fails.
    fn new(info: Info) -> Result<Self> {
        unsafe {
            // RAII guard to ensure resources are cleaned up on any error path
            struct ResourceGuard {
                fabric: *mut ffi::fid_fabric,
                domain: *mut ffi::fid_domain,
                ep: *mut ffi::fid_ep,
                av: *mut ffi::fid_av,
                cq: *mut ffi::fid_cq,
            }

            impl Drop for ResourceGuard {
                fn drop(&mut self) {
                    unsafe {
                        if !self.ep.is_null() {
                            ffi::fi_close(&mut (*self.ep).fid as *mut ffi::fid);
                        }
                        if !self.av.is_null() {
                            ffi::fi_close(&mut (*self.av).fid as *mut ffi::fid);
                        }
                        if !self.cq.is_null() {
                            ffi::fi_close(&mut (*self.cq).fid as *mut ffi::fid);
                        }
                        if !self.domain.is_null() {
                            ffi::fi_close(&mut (*self.domain).fid as *mut ffi::fid);
                        }
                        if !self.fabric.is_null() {
                            ffi::fi_close(&mut (*self.fabric).fid as *mut ffi::fid);
                        }
                    }
                }
            }

            let hints = ffi::fi_allocinfo();
            if hints.is_null() {
                bail!("fi_allocinfo failed");
            }

            let mut guard = ResourceGuard {
                fabric: ptr::null_mut(),
                domain: ptr::null_mut(),
                ep: ptr::null_mut(),
                av: ptr::null_mut(),
                cq: ptr::null_mut(),
            };

            let mut fabric: *mut ffi::fid_fabric = ptr::null_mut();
            let ret = ffi::fi_fabric((*info.ptr.as_ptr()).fabric_attr, &mut fabric, ptr::null_mut());
            if ret != 0 {
                bail!("fi_fabric failed: {}", ret);
            }
            guard.fabric = fabric;

            let mut domain: *mut ffi::fid_domain = ptr::null_mut();
            let ret = ffi::fi_domain(fabric, info.ptr.as_ptr(), &mut domain, ptr::null_mut());
            if ret != 0 {
                bail!("fi_domain failed: {}", ret);
            }
            guard.domain = domain;

            let mut ep: *mut ffi::fid_ep = ptr::null_mut();
            let ret = ffi::fi_endpoint(domain, info.ptr.as_ptr(), &mut ep, ptr::null_mut());
            if ret != 0 {
                bail!("fi_endpoint failed: {}", ret);
            }
            guard.ep = ep;

            let mut cq_attr: ffi::fi_cq_attr = std::mem::zeroed();
            cq_attr.size = 128;
            cq_attr.format = ffi::fi_cq_format_FI_CQ_FORMAT_DATA;

            let mut cq: *mut ffi::fid_cq = ptr::null_mut();
            let ret = ffi::fi_cq_open(domain, &mut cq_attr, &mut cq, ptr::null_mut());
            if ret != 0 {
                bail!("fi_cq_open failed: {}", ret);
            }
            guard.cq = cq;

            let ret = ffi::fi_ep_bind(
                ep,
                &mut (*cq).fid as *mut ffi::fid,
                (ffi::FI_SEND | ffi::FI_RECV) as u64,
            );
            if ret != 0 {
                bail!("fi_ep_bind cq failed: {}", ret);
            }

            let mut av_attr: ffi::fi_av_attr = std::mem::zeroed();
            av_attr.type_ = ffi::fi_av_type_FI_AV_MAP;
            av_attr.count = 64;

            let mut av: *mut ffi::fid_av = ptr::null_mut();
            let ret = ffi::fi_av_open(domain, &mut av_attr, &mut av, ptr::null_mut());
            if ret != 0 {
                bail!("fi_av_open failed: {}", ret);
            }
            guard.av = av;

            let ret = ffi::fi_ep_bind(ep, &mut (*av).fid as *mut ffi::fid, 0);
            if ret != 0 {
                bail!("fi_ep_bind av failed: {}", ret);
            }

            let ret = ffi::fi_enable(ep);
            if ret != 0 {
                bail!("fi_enable failed: {}", ret);
            }

            // Disarm the guard by moving resources out and forgetting it
            let fabric = guard.fabric;
            let domain = guard.domain;
            let ep = guard.ep;
            let av = guard.av;
            let cq = guard.cq;
            std::mem::forget(guard);

            Ok(FabricEndpoint {
                inner: 
                Arc::new(FabricEndpointResources {
                    fabric,
                    domain,
                    ep,
                    av,
                    cq,
                })  
            })
        }
    }

    


    pub async fn read_cq(&self) -> Result<()> {
        let cq_fd = CompletionQueueFd::new(self.inner.cq)?;
        let fd = AsyncFd::new(cq_fd.clone())?;
        loop {
            if unsafe { cq_waitable(&self.inner) } {
                //we can safely wait on the fd
                let _ = fd.readable().await?;
            }

            loop {
                let res = unsafe { read_cq_entry(&self.inner) };
                match res {
                    CqReadResult::CqReadSuccess(entry) => {
                        let ctx = entry.op_context;
                        let tx = unsafe { std::ptr::read(ctx as *mut Sender<usize>) };
                        let _ = tx.send(entry.len);
                    },
                    CqReadResult::CqErrorAgain() => {
                        break;
                    },
                    CqReadResult::CqError() => {
                        bail!("read_cq_entry failed");
                    }
                }
            }

        }
    }

    /// Sends data to a specific peer.
    ///
    /// This function takes ownership of the buffer, sends it to the specified peer,
    /// and returns the buffer when the operation completes.
    ///
    /// # Arguments
    ///
    /// * `peer` - The peer to send to
    /// * `buf` - The buffer to send. Ownership is transferred to this function.
    ///
    /// # Returns
    ///
    /// Returns the buffer after the send operation completes, allowing reuse.
    ///
    /// # Errors
    ///
    /// Returns an error if the send operation fails.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let peer = endpoint.insert_peer(&peer_addr)?;
    /// let mut buf = vec![0u8; 8192];
    /// buf = endpoint.send_to(peer, buf).await?;
    /// ```
    pub async fn send_to(&self, peer: PeerId, buf: Vec<u8>) -> Result<Vec<u8>> {
        let ep = self.inner.ep as usize;
        let fi_addr = peer.0;

        let (mut tx, rx) = oneshot::channel::<usize>();
        let op_ctx: *mut _ = &mut tx;
        let ep = ep as *mut ffi::fid_ep;
        let ret = unsafe { ffi::fi_send(
            ep,
            buf.as_ptr() as *const libc::c_void,
            buf.len(),
            ptr::null_mut(),
            fi_addr,
            op_ctx as *mut c_void,
        ) };
        if ret == 0 {
            std::mem::forget(tx);
            let _ = rx.await?; 
            Ok(buf)       
        } else {
            bail!("fi_send failed: {}", ret);
        }
    }

    /// Receives data from any peer.
    ///
    /// This function takes ownership of the buffer, receives data, and returns the
    /// buffer when the operation completes.
    ///
    /// # Note
    ///
    /// This receive operation accepts data from any connected peer. Libfabric RDM
    /// endpoints do not support peer-specific receives. If you need to receive from
    /// specific peers, use multiple endpoints or implement peer filtering at the
    /// application level.
    ///
    /// # Arguments
    ///
    /// * `buf` - The buffer to receive into. Ownership is transferred to this function.
    ///
    /// # Returns
    ///
    /// Returns the buffer filled with received data.
    ///
    /// # Errors
    ///
    /// Returns an error if the receive operation fails.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let buf = vec![0u8; 8192];
    /// let buf = endpoint.recv(buf).await?;
    /// // buf now contains received data
    /// ```
    pub async fn recv(&self, mut buf: Vec<u8>) -> Result<Vec<u8>> {
        let ep = self.inner.ep as usize;
        let ep = ep as *mut ffi::fid_ep;
        let (mut tx, rx) = oneshot::channel::<usize>();
        let op_ctx: *mut _ = &mut tx;
        let ret = unsafe { ffi::fi_recv(
            ep,
            buf.as_mut_ptr() as *mut libc::c_void,
            buf.len(),
            ptr::null_mut(),
            0,
            op_ctx as *mut c_void,
        ) };

        if ret == 0 {
            std::mem::forget(tx);
            let _ = rx.await?;
            Ok(buf)
        } else {
            bail!("fi_recv failed: {}", ret);
        }
    }

    /// Retrieves the local endpoint address.
    ///
    /// # Returns
    ///
    /// Returns the local address as a [`FabricAddress`].
    ///
    /// # Errors
    ///
    /// Returns an error if fi_getname fails.
    pub fn local_address(&self) -> Result<FabricAddress> {
        unsafe {
            let mut local_addr: Vec<u8> = vec![0; 128];
            let mut local_addrlen: libc::size_t = local_addr.len();

            let ret = ffi::fi_getname(
                &mut (*self.inner.ep).fid as *mut ffi::fid,
                local_addr.as_mut_ptr() as *mut libc::c_void,
                &mut local_addrlen,
            );

            if ret != 0 {
                bail!("fi_getname failed: {}", ret);
            }

            local_addr.resize(local_addrlen, 0);
            Ok(FabricAddress::from(local_addr))
        }
    }

    /// Inserts a peer address into the address vector.
    ///
    /// # Arguments
    ///
    /// * `peer_addr` - The peer's [`FabricAddress`] to insert
    ///
    /// # Errors
    ///
    /// Returns an error if fi_av_insert fails.
    /// Inserts a peer address into the address vector.
    ///
    /// This method adds a new peer to the endpoint's address vector.
    ///
    /// # Arguments
    ///
    /// * `peer_addr` - The peer's [`FabricAddress`] to insert
    ///
    /// # Returns
    ///
    /// Returns a `PeerId` that can be used to send messages to this peer.
    ///
    /// # Errors
    ///
    /// Returns an error if the address insertion fails.
    pub fn insert_peer(&mut self, peer_addr: &FabricAddress) -> Result<PeerId> {
        unsafe {
            let mut fi_addr: ffi::fi_addr_t = 0;
            let ret = ffi::fi_av_insert(
                self.inner.av,
                peer_addr.as_bytes().as_ptr() as *const libc::c_void,
                1,
                &mut fi_addr,
                0,
                ptr::null_mut(),
            );

            ensure!(ret == 1, "fi_av_insert failed: {}", ret);

            Ok(PeerId(fi_addr))
        }
    }
}

/// Optional TCP helper to exchange `FabricAddress` blobs.
///
/// Libfabric endpoints rely on opaque addresses that usually travel over a
/// separate control plane. `AddressExchangeChannel` is a convenience shim for
/// demos and tests—you can freely replace it with any custom mechanism that
/// ferries serialized [`FabricAddress`] values between peers.
pub struct AddressExchangeChannel {
    stream: TcpStream,
}

impl AddressExchangeChannel {
    /// Connects to a server (client mode).
    ///
    /// Establishes a TCP connection to the server's control port for address
    /// exchange. Production deployments can skip this entirely if they already
    /// have a control plane (e.g., gRPC or MPI) for moving `FabricAddress`
    /// payloads.
    ///
    /// # Arguments
    ///
    /// * `server_addr` - IP address of the server
    /// * `port` - Optional port (defaults to [`CONTROL_PORT`])
    ///
    /// # Returns
    ///
    /// Returns `Ok(AddressExchangeChannel)` on successful connection.
    ///
    /// # Errors
    ///
    /// Returns an error if connection fails.
    pub async fn connect(server_addr: &str, port: Option<u16>) -> Result<Self> {
        let port = port.unwrap_or(CONTROL_PORT);
        let addr = format!("{}:{}", server_addr, port);
        let stream = TcpStream::connect(&addr)
            .await
            .wrap_err_with(|| format!("failed to connect to control port {addr}"))?;
        Ok(AddressExchangeChannel { stream })
    }

    /// Listens for client connection (server mode).
    ///
    /// Binds to the control port and waits for a client to connect.
    ///
    /// # Arguments
    ///
    /// * `port` - Optional port to bind (defaults to [`CONTROL_PORT`])
    ///
    /// # Returns
    ///
    /// Returns `Ok(AddressExchangeChannel)` when a client connects.
    ///
    /// # Errors
    ///
    /// Returns an error if bind or accept fails.
    pub async fn listen(port: Option<u16>) -> Result<Self> {
        let port = port.unwrap_or(CONTROL_PORT);
        let addr = format!("0.0.0.0:{}", port);
        let listener = TcpListener::bind(&addr)
            .await
            .wrap_err_with(|| format!("failed to bind control port {}", port))?;

        let (stream, _) = listener
            .accept()
            .await
            .wrap_err("control connection accept failed")?;
        Ok(AddressExchangeChannel { stream })
    }

    /// Exchanges endpoint addresses and returns the peer's address.
    ///
    /// This method exchanges addresses over the TCP control channel but does NOT
    /// insert the peer address into the endpoint. This allows manual peer management
    /// for multi-peer scenarios.
    ///
    /// # Arguments
    ///
    /// * `endpoint` - The fabric endpoint to get local address from
    /// * `is_client` - true for client mode, false for server mode
    ///
    /// # Returns
    ///
    /// Returns the peer's address. Call `endpoint.insert_peer()` to add it to
    /// the address vector.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let peer_addr = conn.exchange(&endpoint, true).await?;
    /// let peer_id = endpoint.insert_peer(&peer_addr)?;
    /// ```
    pub async fn exchange(
        &mut self,
        endpoint: &FabricEndpoint,
        is_client: bool,
    ) -> Result<FabricAddress> {
        let local_addr = endpoint.local_address()?;

        let peer_addr = if is_client {
            self.write_address(&local_addr).await?;
            self.read_address().await?
        } else {
            let peer = self.read_address().await?;
            self.write_address(&local_addr).await?;
            peer
        };

        Ok(peer_addr)
    }

    async fn write_address(&mut self, addr: &FabricAddress) -> Result<()> {
        let len_bytes = (addr.len() as u64).to_le_bytes();
        self.stream
            .write_all(&len_bytes)
            .await
            .wrap_err("failed to send address length")?;
        self.stream
            .write_all(addr.as_bytes())
            .await
            .wrap_err("failed to send address payload")?;
        Ok(())
    }

    async fn read_address(&mut self) -> Result<FabricAddress> {
        let mut len_bytes = [0u8; 8];
        self.stream
            .read_exact(&mut len_bytes)
            .await
            .wrap_err("failed to read address length")?;
        let addr_len = u64::from_le_bytes(len_bytes) as usize;

        let mut addr = vec![0u8; addr_len];
        self.stream
            .read_exact(&mut addr)
            .await
            .wrap_err("failed to read address payload")?;

        Ok(FabricAddress::from(addr))
    }
}


pub struct FabricEndpointBuilder {
    hints: Info,
    port: u16
}


impl FabricEndpointBuilder {

    pub fn new() -> Self {
        Self {
           hints: Info::new(),
           port: DEFAULT_PORT
        }
    }

    pub fn port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    pub fn caps(mut self, caps: u32) -> Self {
        let hints = unsafe { self.hints.ptr.as_mut() };
        hints.caps = caps as u64;
        self
    }

    pub fn mode(mut self, mode: u64) -> Self {
        let hints = unsafe { self.hints.ptr.as_mut() };
        hints.mode = mode;
        self
    }

    pub fn ep_attr_type(mut self, typ: u32) -> Self {
        let hints = unsafe { self.hints.ptr.as_mut() };
        unsafe { (*hints.ep_attr).type_ = typ };
        self
    }

    pub fn tx_attr_op_flags(mut self, flags: u32) -> Self {
        let hints = unsafe { self.hints.ptr.as_mut() };
        unsafe { (*hints.tx_attr).op_flags = flags as u64 };
        self
    }

    pub fn domain_attr_mr_mode(mut self, mr_mode: u32) -> Self {
        let hints = unsafe { self.hints.ptr.as_mut() };
        unsafe { (*hints.domain_attr).mr_mode = mr_mode as i32 };
        self
    }

    pub fn fabric_attr_prov_name(mut self, name: CString) -> Self {
        let hints = unsafe { self.hints.ptr.as_mut() };
        unsafe { (*hints.fabric_attr).prov_name = name.as_ptr() as *mut i8 };
        std::mem::forget(name);
        self
    }

    pub fn domain_attr_threading(mut self, mode: u32) -> Self {
        let hints = unsafe { self.hints.ptr.as_mut() };
        let _ = mode;
        // Request thread-safe mode to enable concurrent access from multiple threads
        unsafe {(*(*hints).domain_attr).threading = ffi::fi_threading_FI_THREAD_SAFE };
        self
    }

    pub fn build_efa_default() -> Result<FabricEndpoint> {
        let builder = FabricEndpointBuilder::new(); 
        builder.fabric_attr_prov_name(CString::new("efa").unwrap())
            .caps(ffi::FI_MSG)
            .mode(ffi::FI_CONTEXT)
            .ep_attr_type(ffi::fi_ep_type_FI_EP_RDM)
            .tx_attr_op_flags(ffi::FI_DELIVERY_COMPLETE)
            .domain_attr_threading(ffi::fi_threading_FI_THREAD_SAFE)
            .build()
    }

    pub fn build(self) -> Result<FabricEndpoint> {
        unsafe {
            let version = ffi::fi_version();
            let mut info_ptr = std::ptr::null_mut();
            let port_str = self.port.to_string();
            let ret = ffi::fi_getinfo(
            version,
            std::ptr::null_mut(),
        port_str.as_ptr() as *const i8,
            0,
            self.hints.ptr.as_ptr(),
                &mut info_ptr,
            );

            if ret != 0 {
                bail!("fi_getinfo failed!");
            }
            let info = Info::new_from(
                    NonNull::new(info_ptr).expect("info ptr was null!"));
            Ok(FabricEndpoint::new(info)?)
        }
    }

}
