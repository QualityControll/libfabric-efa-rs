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

use eyre::{bail, Context, Result};
use ofi_libfabric_sys::bindgen as ffi;
use serde::{Deserialize, Serialize};
use std::cell::UnsafeCell;
use std::ffi::{CString, c_void};
use std::os::unix::io::{AsRawFd, RawFd};
use std::ptr;
use std::ptr::NonNull;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::io::unix::AsyncFd;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;


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
pub struct FabricAddress(Vec<u8>);

struct Info(NonNull<ffi::fi_info>);

impl Info {
    fn new() -> Self {
        Self {
            0: NonNull::new(
            unsafe {
                ffi::fi_allocinfo()
            }).expect("Failed to allocate info")
        }
    }

    fn new_from(info: NonNull<ffi::fi_info>) -> Self {
        Self {
            0: info
        }
    }
}

impl Drop for Info {
    fn drop(&mut self) {
        unsafe { ffi::fi_freeinfo(self.0.as_ptr()); }
    }
}

struct Fabric(NonNull<ffi::fid_fabric>);

impl Fabric {
    fn new(info: &Info) -> Result<Self> {
        let mut fabric: *mut ffi::fid_fabric = ptr::null_mut();
        let ret = unsafe { ffi::fi_fabric((*info.0.as_ptr()).fabric_attr, &mut fabric, ptr::null_mut()) };
        if ret != 0 {
            bail!("fi_fabric failed: {}", ret);
        }
        Ok(Fabric {
            0: NonNull::new(fabric).unwrap()
        })
    }
}

impl Drop for Fabric {
    fn drop(&mut self) {
        unsafe { ffi::fi_close(&mut (*(self.0).as_ptr()).fid as *mut ffi::fid); }
    }
}

struct Domain(NonNull<ffi::fid_domain>);

impl Domain {
    fn new(info: &mut Info, fabric: &mut Fabric) -> Result<Self> {
        let mut domain: *mut ffi::fid_domain = ptr::null_mut();
        let ret = unsafe { ffi::fi_domain(fabric.0.as_mut(), info.0.as_ptr(), &mut domain, ptr::null_mut()) };
        if ret != 0 {
           bail!("fi_domain failed: {}", ret);
        }
        Ok(Domain {
            0: NonNull::new(domain).unwrap()
        })
    }
}

impl Drop for Domain {
    fn drop(&mut self) {
        unsafe { ffi::fi_close(&mut (*(self.0).as_ptr()).fid as *mut ffi::fid); }
    }
}


struct Endpoint(NonNull<ffi::fid_ep>);

impl Endpoint {
    fn new(info: &Info, domain: &mut Domain) -> Result<Self> {
        let mut ep: *mut ffi::fid_ep = ptr::null_mut();
        let ret = unsafe { ffi::fi_endpoint(domain.0.as_mut(), info.0.as_ptr(), &mut ep, ptr::null_mut()) };
        if ret != 0 {
           bail!("fi_endpoint failed: {}", ret);
        }
        Ok(Endpoint {
            0: NonNull::new(ep).unwrap()
        })
    }

    fn bind_av(&mut self, av: &mut AddressVector) -> Result<()> {
        let ret = unsafe { ffi::fi_ep_bind(self.0.as_mut(), &mut av.0.as_mut().fid as *mut ffi::fid, 0) };
        if ret != 0 {
            bail!("fi_ep_bind av failed: {}", ret);
        }
        Ok(())
    }

    fn bind_cq(&mut self, cq: &mut CompletionQueue) -> Result<()> {
        let ret = unsafe {ffi::fi_ep_bind(
                self.0.as_mut(),
                &mut cq.0.as_mut().fid as *mut ffi::fid,
                (ffi::FI_SEND | ffi::FI_RECV) as u64,
            ) };
        if ret != 0 {
           bail!("fi_ep_bind cq failed: {}", ret);
        }
        Ok(())
    }

    fn enable(&mut self) -> Result<()> {
        let ret = unsafe { ffi::fi_enable(self.0.as_mut()) };
        if ret != 0 {
            bail!("fi_enable failed: {}", ret);
        }
        Ok(())
    }
}

impl Drop for Endpoint {
    fn drop(&mut self) {
        unsafe { ffi::fi_close(&mut (*(self.0).as_ptr()).fid as *mut ffi::fid); }
    }
}

struct AddressVector(NonNull<ffi::fid_av>);

impl AddressVector {
    fn new(domain: &mut Domain) -> Result<Self> {
        let mut av_attr: ffi::fi_av_attr = unsafe { std::mem::zeroed() };
        av_attr.type_ = ffi::fi_av_type_FI_AV_MAP;
        av_attr.count = 64;

        let mut av: *mut ffi::fid_av = ptr::null_mut();
        let ret = unsafe { ffi::fi_av_open(domain.0.as_mut(), &mut av_attr, &mut av, ptr::null_mut()) };
        if ret != 0 {
           bail!("fi_av_open failed: {}", ret);
        }
        Ok(AddressVector {
            0: NonNull::new(av).unwrap()
        })
    }

    unsafe fn insert(&self, peer_addr: &FabricAddress) -> Result<PeerId> {
        let mut fi_addr: ffi::fi_addr_t = 0;
        let ret = ffi::fi_av_insert(
            self.0.as_ptr(),
            peer_addr.0.as_ptr() as *const libc::c_void,
            1,
            &mut fi_addr,
            0,
            ptr::null_mut(),
        );

        if ret != 1 {
            bail!("fi_av_insert failed: {}", ret);
        }
        Ok(PeerId(fi_addr))
    }

}

impl Drop for AddressVector {
    fn drop(&mut self) {
        unsafe { ffi::fi_close(&mut (*(self.0).as_ptr()).fid as *mut ffi::fid); }
    }
}


struct CompletionQueue(NonNull<ffi::fid_cq>);

enum CqReadResult {
    CqReadSuccess(ffi::fi_cq_data_entry),
    CqErrorAgain(),
    CqError()
}

pub struct SendBuffer {
    data: Box<[u8]>,
}

impl SendBuffer {
    pub fn new(data: Vec<u8>) -> Self {
        Self {
            data: data.into_boxed_slice(),
        }
    }

    pub fn as_ptr(&self) -> *const u8 {
        self.data.as_ptr()
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }
}

pub struct RecvBuffer {
    data: UnsafeCell<Box<[u8]>>,
}

// The buffer is only accessed by the caller after recv() completes.
// While an operation is outstanding, libfabric owns the mutable access.
unsafe impl Send for RecvBuffer {}
unsafe impl Sync for RecvBuffer {}

impl RecvBuffer {
    pub fn new(size: usize) -> Self {
        Self {
            data: UnsafeCell::new(vec![0; size].into_boxed_slice()),
        }
    }

    pub fn len(&self) -> usize {
        // This is read-only and doesn't conflict with libfabric
        // writing the contents.
        unsafe { (&*self.data.get()).len() }
    }

    pub fn as_slice(&self, len: usize) -> &[u8] {
        assert!(len <= self.len());

        unsafe {
            &(&*self.data.get())[..len]
        }
    }

    unsafe fn as_mut_ptr(&self) -> *mut u8 {
        (*self.data.get()).as_mut_ptr()
    }
}

enum Operation {
    Send {
        completion: oneshot::Sender<Result<Vec<u8>>>,
        buffer: Vec<u8>,
    },

    Recv {
        completion: oneshot::Sender<Result<(Vec<u8>, usize)>>,
        buffer: Vec<u8>,
    },
}


impl CompletionQueue {
    fn new(domain: &mut Domain) -> Result<Self> {
        let mut cq_attr: ffi::fi_cq_attr = unsafe { std::mem::zeroed() };
        cq_attr.size = 128;
        cq_attr.format = ffi::fi_cq_format_FI_CQ_FORMAT_DATA;
        cq_attr.wait_obj = ffi::fi_wait_obj_FI_WAIT_FD;

        let mut cq: *mut ffi::fid_cq = ptr::null_mut();
        let ret = unsafe { ffi::fi_cq_open(domain.0.as_mut(), &mut cq_attr, &mut cq, ptr::null_mut()) };
        if ret != 0 {
            bail!("fi_cq_open failed: {}", ret);
        }       
        Ok(CompletionQueue {
            0: NonNull::new(cq).unwrap()
        })
    }

    fn handle_completion(
        &self,
        entry: &ffi::fi_cq_data_entry) {
        let operation = unsafe {
        Box::from_raw(
            entry.op_context as *mut Operation)
        };

        match *operation {
            Operation::Send {
                completion,
                buffer: b,
            } => {
                let _ = completion.send(Ok(b));
            }

            Operation::Recv {
                completion,
                buffer: b,
            } => {
                let _ = completion.send(Ok((b, entry.len)));
            }
        }

        //
        // `operation` is dropped here.
        //
        // That releases its Arc<SendBuffer>.
        //
        // If the caller still has an Arc reference, the buffer remains
        // alive. Otherwise the buffer is freed here.
    }

    unsafe fn read_cq_entry(&self) -> CqReadResult {
        let mut comp: ffi::fi_cq_data_entry = std::mem::zeroed();
        let ret = ffi::fi_cq_read(
                self.0.as_ptr(),
                &mut comp as *mut ffi::fi_cq_data_entry as *mut libc::c_void,
                1);
        if ret > 0 {
            return CqReadResult::CqReadSuccess(comp);
        } else if ret == EAGAIN_ERROR {
            return CqReadResult::CqErrorAgain();
        } else {
            return CqReadResult::CqError();        
        }
    }

    fn read_cq_entries(&self) {
        loop {
            let res = unsafe { self.read_cq_entry() };
            match res {
                CqReadResult::CqReadSuccess(entry) => {
                    self.handle_completion(&entry);
                },
                CqReadResult::CqErrorAgain() => {
                    break;
                },
                CqReadResult::CqError() => {
                    break;
                }
            }
        }
    }

    unsafe fn cq_waitable(&self, fabric: &Fabric) -> bool {
        let cq_fid = &mut (*(self.0).as_ptr()).fid as *mut ffi::fid;
        let mut fds: [*mut ffi::fid; 1] = [ cq_fid ];
        let ret = ffi::fi_trywait(fabric.0.as_ptr(), fds.as_mut_ptr(), 1);
        return ret == 0;
    }

}

impl Drop for CompletionQueue {
    fn drop(&mut self) {
        unsafe { ffi::fi_close(&mut (*(self.0).as_ptr()).fid as *mut ffi::fid); }
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

struct CompletionQueueFd(RawFd);

impl CompletionQueueFd {
    fn new(cq: *mut ffi::fid_cq) -> Result<Self> {
        unsafe {
            let mut fd: RawFd = -1;
            let ret = ffi::fi_control(
                &mut (*cq).fid as *mut ffi::fid, 
                ffi::FI_GETWAIT as i32, 
                &mut fd as *mut i32 as *mut c_void);
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
struct FabricEndpointResources {
    cq: CompletionQueue,
    av: AddressVector,
    ep: Endpoint,
    //domain isn't used, but we shouldn't drop it!
    #[allow(dead_code)] 
    domain: Domain,
    fabric: Fabric,
}

// SAFETY: FabricEndpoint is configured with FI_THREAD_SAFE mode during initialization,
// which ensures that the EFA provider's internal structures are thread-safe.
unsafe impl Send for FabricEndpointResources {}
unsafe impl Sync for FabricEndpointResources {}


#[derive(Clone)]
pub struct FabricEndpoint(Arc<FabricEndpointResources>);


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
    fn new(mut info: Info) -> Result<Self> {
        let mut fabric = Fabric::new(&mut info)?;
        let mut domain = Domain::new(&mut info, &mut fabric)?;
        let mut ep = Endpoint::new(&mut info, &mut domain)?;
        let mut cq = CompletionQueue::new(&mut domain)?; 
        let mut av = AddressVector::new(&mut domain)?;

        ep.bind_cq(&mut cq)?;
        ep.bind_av(&mut av)?;
        ep.enable()?;

        Ok(FabricEndpoint {
            0: 
            Arc::new(FabricEndpointResources {
                fabric,
                domain,
                ep,
                av,
                cq,
            })  
        })
    }

    

    /// Reads the completion queue for asynchronous data transfer events. 
    ///
    /// User's submit async operations to libfabric using `send_to` and `recv`.
    /// Operations are completed via completion queue events, and async awaiters are
    /// notified using `tokio::sync::oneshot::channel` as async ops are completed.
    ///
    /// See https://manpages.debian.org/stretch/libfabric-dev/fi_trywait.3.en.html
    pub async fn read_cq(&self) -> Result<()> {
        let cq_fd = CompletionQueueFd::new(self.0.cq.0.as_ptr())?;
        let fd = AsyncFd::new(cq_fd)?;
        loop {
            if unsafe { self.0.cq.cq_waitable(&self.0.fabric) } {
                //we can safely wait on the fd
                let mut guard = fd.readable().await?;
                match guard.try_io(|_| Ok(self.0.cq.read_cq_entries())) {
                    _ => {
                    }
                }
                guard.clear_ready();
            } else {
                self.0.cq.read_cq_entries();
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
    /// 
    pub async fn send_to(
        &self,
        peer: PeerId,
        buffer: Vec<u8>) -> Result<Vec<u8>> {

        let (tx, rx) = oneshot::channel::<Result<Vec<u8>>>();
        let len = buffer.len();
        let ptr = buffer.as_ptr();

        //
        // The operation owns an Arc reference to the buffer.
        //
        // This guarantees that the buffer remains alive until the
        // libfabric operation has completed.
        //
        let operation = Box::new(Operation::Send {
            completion: tx,
            buffer: buffer,
        });

        //
        // Transfer ownership of the operation allocation to the
        // outstanding libfabric operation.
        //
        // The CQ dispatcher will eventually reclaim this Box.
        //
        let operation_ptr = Box::into_raw(operation);

        let ret = unsafe {
            ffi::fi_send(
                self.0.ep.0.as_ptr(),
                ptr as *const c_void,
                len,
                std::ptr::null_mut(),
                peer.0,
                //
                // Assuming FI_CONTEXT is not required:
                //
                // use our SendOperation allocation itself as the
                // application context.
                //
                operation_ptr as *mut c_void,
            )
        };

        if ret < 0 {
            //
            // fi_send() failed synchronously.
            //
            // No CQ completion will be generated, so the CQ cannot
            // reclaim this operation.
            //
            // Reclaim it here.
            //
            unsafe {
                drop(Box::from_raw(operation_ptr));
            }
            bail!("fi_send failed: {}", ret)
        }

        //
        // fi_send() succeeded.
        //
        // Ownership of operation_ptr is now effectively transferred
        // to the CQ dispatcher.
        //
        // DO NOT call Box::from_raw() here.
        //
        // The caller is free to drop its Arc<SendBuffer> at any time;
        // the operation's Arc keeps the actual buffer alive.
        //

        match rx.await {
            Ok(result) => result,

            Err(e) => {
                //
                // The caller dropped/cancelled the future.
                //
                // The libfabric operation is still outstanding.
                //
                // Therefore we MUST NOT reclaim operation_ptr here.
                //
                // The CQ dispatcher will do that when the completion
                // arrives.
                //
                Err(e.into())
            }
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
    pub async fn recv(
        &self,
        mut buffer: Vec<u8>,
    ) -> Result<(Vec<u8>, usize)> {
        let (tx, rx) = oneshot::channel::<Result<(Vec<u8>, usize)>>();
        let len = buffer.len(); 
        let ptr = buffer.as_mut_ptr().cast();

        let operation = Box::new(Operation::Recv {
            completion: tx,
            buffer: buffer,
        });

        //
        // libfabric will receive this pointer back in the CQ entry.
        //
        let operation_ptr = Box::into_raw(operation);

        let ret = unsafe {
            ffi::fi_recv(
                self.0.ep.0.as_ptr(),
                ptr, 
                len,
                std::ptr::null_mut(),
                0,
                operation_ptr.cast(),
            )
        };

        if ret < 0 {
            //
            // No CQ completion will arrive, so reclaim the operation.
            //
            unsafe {
                drop(Box::from_raw(operation_ptr));
            }
            bail!("fi_recv failed: {}", ret)
        }

        //
        // Successful submission:
        //
        // The CQ now owns operation_ptr.
        //
        // The caller can keep its Arc<RecvBuffer>, but MUST NOT access
        // the buffer contents until this recv() completes.
        //

        match rx.await {
            Ok(result) => result,

            Err(e) => {
                //
                // The async future was cancelled.
                //
                // The receive is still outstanding.
                //
                // DO NOT reclaim operation_ptr here.
                //
                // The CQ will reclaim it after completion.
                //
                Err(e.into())
            }
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
                &mut (*self.0.ep.0.as_ptr()).fid as *mut ffi::fid,
                local_addr.as_mut_ptr() as *mut libc::c_void,
                &mut local_addrlen,
            );

            if ret != 0 {
                bail!("fi_getname failed: {}", ret);
            }

            local_addr.resize(local_addrlen, 0);
            Ok(FabricAddress { 0: local_addr })
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
        unsafe { self.0.av.insert(peer_addr) }
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
        let len_bytes = (addr.0.len() as u64).to_le_bytes();
        self.stream
            .write_all(&len_bytes)
            .await
            .wrap_err("failed to send address length")?;
        self.stream
            .write_all(addr.0.as_slice())
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

        Ok(FabricAddress { 0: addr })
    }
}


pub struct FabricEndpointBuilder {
    hints: Info,
    port: u16
}


impl FabricEndpointBuilder {

    /// Creates a new FabricEndpointBuilder. Use this to create a FabricEndpoint based on
    /// the user's required specifications.  Note that the default control_port `DEFAULT_PORT` is
    /// set to 9228.
    ///
    /// # Arguments
    /// 
    /// None
    ///
    /// # Returns
    ///
    /// Self
    ///
    /// # Errors
    ///
    /// None
    pub fn new() -> Self {
        Self {
           hints: Info::new(),
           port: DEFAULT_PORT
        }
    }

    /// Sets the control_port for the FabricEndpoint to the port specified.
    ///
    /// # Arguments
    /// 
    /// * `self` - consumes self.
    /// * `port` - the port to set the control_port to.
    ///
    /// # Returns
    ///
    /// self 
    ///
    /// # Errors
    ///
    /// None
    pub fn port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }


    /// Sets the hints caps for the FabricEndpoint to the capabilities flag specified.
    ///
    /// # Arguments
    /// 
    /// * `self` - consumes self.
    /// * `caps` - the capabilities flag to set the caps to.  See ffi::FI_MSG.
    ///
    /// # Returns
    ///
    /// self 
    ///
    /// # Errors
    ///
    /// None
    pub fn caps(mut self, caps: u32) -> Self {
        let hints = unsafe { self.hints.0.as_mut() };
        hints.caps = caps as u64;
        self
    }

    /// Sets the hints mode for the FabricEndpoint to the mode flag specified.
    ///
    /// # Arguments
    /// 
    /// * `self` - consumes self.
    /// * `mode` - the mode flag to set the mode to.  See ffi::FI_CONTEXT.
    ///
    /// # Returns
    ///
    /// self 
    ///
    /// # Errors
    ///
    /// None
    pub fn mode(mut self, mode: u64) -> Self {
        let hints = unsafe { self.hints.0.as_mut() };
        hints.mode = mode;
        self
    }

    /// Sets the hints ep_attr->type_ for the FabricEndpoint to the typ flag specified.
    ///
    /// # Arguments
    /// 
    /// * `self` - consumes self.
    /// * `typ` - the mode flag to set the caps to.  See ffi::FI_CONTEXT.
    ///
    /// # Returns
    ///
    /// self 
    ///
    /// # Errors
    ///
    /// None
    pub fn ep_attr_type(mut self, typ: u32) -> Self {
        let hints = unsafe { self.hints.0.as_mut() };
        unsafe { (*hints.ep_attr).type_ = typ };
        self
    }

    /// Sets the hints tx_attr->op_flags for the FabricEndpoint to the flags specified.
    ///
    /// # Arguments
    /// 
    /// * `self` - consumes self.
    /// * `flags` - the tx_attr->op_flags value to set.  See ffi::FI_DELIVERY_COMPLETE.
    ///
    /// # Returns
    ///
    /// self 
    ///
    /// # Errors
    ///
    /// None
    pub fn tx_attr_op_flags(mut self, flags: u32) -> Self {
        let hints = unsafe { self.hints.0.as_mut() };
        unsafe { (*hints.tx_attr).op_flags = flags as u64 };
        self
    }

    /// Sets the hints domain_attr->mr_mode for the FabricEndpoint to the mr_mode specified.
    ///
    /// # Arguments
    /// 
    /// * `self` - consumes self.
    /// * `flags` - the domain_attr->mr_mode value to set.  See ffi::FI_MR_LOCAL.
    ///
    /// # Returns
    ///
    /// self 
    ///
    /// # Errors
    ///
    /// None
    pub fn domain_attr_mr_mode(mut self, mr_mode: u32) -> Self {
        let hints = unsafe { self.hints.0.as_mut() };
        unsafe { (*hints.domain_attr).mr_mode = mr_mode as i32 };
        self
    }

    /// Sets the hints fabric_attr->prov_name for the FabricEndpoint to the name specified.
    ///
    /// # Arguments
    /// 
    /// * `self` - consumes self.
    /// * `name` - the fabric_attr->prov_name value to set.  See efa, sockets, tcp, etc.
    ///
    /// # Returns
    ///
    /// self 
    ///
    /// # Errors
    ///
    /// None
    pub fn fabric_attr_prov_name(mut self, name: CString) -> Self {
        let hints = unsafe { self.hints.0.as_mut() };
        unsafe { (*hints.fabric_attr).prov_name = name.as_ptr() as *mut i8 };
        std::mem::forget(name);
        self
    }

    /// Builds a new FabricEndpoint using the EFA provider from libfabric.
    ///
    /// # Arguments
    /// 
    /// None
    ///
    /// # Returns
    ///
    /// Returns a FabricEndpoint if Ok.
    ///
    /// # Errors
    ///
    /// Returns an error if something fails during the process of creating fabric resources
    /// necessary for the endpoint.  The user should analyze the arguments that were used
    /// when building the FabricEndpoint.
    pub fn build_efa_default() -> Result<FabricEndpoint> {
        let builder = FabricEndpointBuilder::new(); 
        builder.fabric_attr_prov_name(CString::new("efa").unwrap())
            .caps(ffi::FI_MSG)
            .ep_attr_type(ffi::fi_ep_type_FI_EP_RDM)
            .tx_attr_op_flags(ffi::FI_DELIVERY_COMPLETE)
            .build()
    }


    /// Builds a new FabricEndpoint if possible.
    ///
    /// # Arguments
    ///
    /// * `self` - consumes self
    ///
    /// # Returns
    ///
    /// Returns a FabricEndpoint if Ok.
    ///
    /// # Errors
    ///
    /// Returns an error if something fails during the process of creating fabric resources
    /// necessary for the endpoint.  The user should analyze the arguments that were used
    /// when building the FabricEndpoint.
    pub fn build(mut self) -> Result<FabricEndpoint> {
        unsafe {
            let hints = self.hints.0.as_mut();
            (*(*hints).domain_attr).threading = ffi::fi_threading_FI_THREAD_SAFE;

            let version = ffi::fi_version();
            let mut info_ptr = std::ptr::null_mut();
            let port_str = CString::new(self.port.to_string()).unwrap();
            let ret = ffi::fi_getinfo(
                version, 
                std::ptr::null_mut(),
                port_str.as_ptr(),
                0,
                self.hints.0.as_ptr(),
                &mut info_ptr,
            );

            if ret != 0 {
                bail!("fi_getinfo failed!");
            }
            let info = Info::new_from(
                    NonNull::new(info_ptr).expect("info ptr was null!"));
            let endpoint = FabricEndpoint::new(info)?;
            let ep_clone = endpoint.clone();
            tokio::spawn(async move {
                let _ = ep_clone.read_cq().await;
            });
            Ok(endpoint)
        }
    }

}


#[cfg(test)]
mod tests {
    use super::*;

    const MESSAGE_SIZE: usize = 64;
    const PING_COUNT: usize = 10;

    async fn run_server() -> Result<()> {
        println!("Ping-Pong Server");
        println!("================\n");

        // Initialize endpoint
        let mut endpoint = FabricEndpointBuilder::new()
            .fabric_attr_prov_name(CString::new("sockets").unwrap())
            .caps(ffi::FI_MSG)
            .build()?;


        // Listen and exchange addresses
        let mut channel = AddressExchangeChannel::listen(None).await?;
        let peer_addr = channel.exchange(&endpoint, false).await?;
        let peer_id = endpoint.insert_peer(&peer_addr)?;

        println!("Waiting for ping messages...\n");

        // Ping-pong loop
        let mut buf = vec![0; MESSAGE_SIZE];

        for i in 1..=PING_COUNT {
            // Receive ping
            let result = endpoint.recv(buf).await?;
            buf = result.0;

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

    async fn run_client(server_addr: &str) -> Result<()> {
        println!("Ping-Pong Client");
        println!("================\n");

        // Initialize endpoint
        let mut endpoint = FabricEndpointBuilder::new()
            .fabric_attr_prov_name(CString::new("sockets").unwrap())
            .caps(ffi::FI_MSG)
            .build()?;

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
            let result = endpoint.recv(buf).await?;
            buf = result.0;

            let response = String::from_utf8_lossy(&buf[..16]);
            let response = response.trim_end_matches('\0');
            println!("← Received: {}", response);

            // Clear buffer for next iteration
            buf.fill(0);
        }

        println!("\n✓ Completed {} ping-pongs!", PING_COUNT);
        Ok(())
    }

    #[tokio::test]
    async fn pingpong_sockets_test() {
        let mut tasks = tokio::task::JoinSet::new();
        tasks.spawn(run_server());
        tasks.spawn(run_client("127.0.0.1"));

        let results = tasks.join_all().await;
        for res in results {
            res.expect("One of the expected tasks failed");
        }
    }
}
