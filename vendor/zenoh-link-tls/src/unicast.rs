//
// Copyright (c) 2023 ZettaScale Technology
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
//
// Contributors:
//   ZettaScale Zenoh Team, <zenoh@zettascale.tech>
//
#[cfg(all(feature = "uring", target_os = "linux"))]
use std::os::fd::RawFd;
use std::{
    convert::TryInto,
    fmt::{self, Debug},
    net::SocketAddr,
    sync::{Arc, Weak},
    time::Duration,
};

use async_trait::async_trait;
use time::OffsetDateTime;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf},
    net::{TcpListener, TcpStream},
    sync::{Mutex as AsyncMutex, Notify},
};
use tokio_rustls::{TlsAcceptor, TlsConnector, TlsStream};
use tokio_util::sync::CancellationToken;
use x509_parser::prelude::{FromDer, X509Certificate};
use zenoh_core::{bail, zasynclock};
use zenoh_link_commons::{
    get_ip_interface_names,
    tls::expiration::{LinkCertExpirationManager, LinkWithCertExpiration},
    LinkAuthId, LinkManagerUnicastTrait, LinkUnicast, LinkUnicastTrait, ListenersUnicastIP,
    NewLinkChannelSender, BIND_INTERFACE, BIND_SOCKET,
};
use zenoh_protocol::{
    core::{EndPoint, Locator, Priority},
    transport::BatchSize,
};
use zenoh_result::{zerror, ZResult};

use crate::{
    utils::{get_tls_addr, get_tls_host, get_tls_server_name, TlsClientConfig, TlsServerConfig},
    TLS_ACCEPT_THROTTLE_TIME, TLS_DEFAULT_MTU, TLS_LINGER_TIMEOUT, TLS_LOCATOR_PREFIX,
};

#[derive(Default, Debug, PartialEq, Eq, Hash)]
pub struct TlsCommonName(String);

// An already-due expiration task can run inside Arc::new_cyclic, where the
// link's Weak cannot yet be upgraded. Keep a fully initialized target alive
// and defer its link lookup until the containing Arc has been published.
struct ReadyExpiration {
    link: Weak<dyn LinkWithCertExpiration>,
    ready: Notify,
    #[cfg(test)]
    entered: std::sync::atomic::AtomicBool,
}

#[async_trait]
impl LinkWithCertExpiration for ReadyExpiration {
    async fn expire(&self) -> ZResult<()> {
        #[cfg(test)]
        self.entered
            .store(true, std::sync::atomic::Ordering::Release);
        self.ready.notified().await;
        if let Some(link) = self.link.upgrade() {
            link.expire().await
        } else {
            Ok(())
        }
    }
}

struct ExpirationRegistration {
    manager: LinkCertExpirationManager,
    target: Arc<ReadyExpiration>,
}

impl ExpirationRegistration {
    fn new(
        link: Weak<dyn LinkWithCertExpiration>,
        src_addr: SocketAddr,
        dst_addr: SocketAddr,
        expires: OffsetDateTime,
    ) -> Self {
        let target = Arc::new(ReadyExpiration {
            link,
            ready: Notify::new(),
            #[cfg(test)]
            entered: std::sync::atomic::AtomicBool::new(false),
        });
        let manager = LinkCertExpirationManager::new(
            Arc::downgrade(&target) as Weak<dyn LinkWithCertExpiration>,
            src_addr,
            dst_addr,
            TLS_LOCATOR_PREFIX,
            expires,
        );
        Self { manager, target }
    }

    fn initialized(&self) {
        // notify_one retains a permit if the one expiration task has not
        // reached the gate yet. It must be called only after Arc construction.
        self.target.ready.notify_one();
    }
}

impl Drop for ExpirationRegistration {
    fn drop(&mut self) {
        // Also release the gate if construction unwinds; the weak link then
        // cannot upgrade and the expiration task exits without retaining it.
        self.target.ready.notify_one();
    }
}

#[cfg(test)]
mod expiration_initialization_tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    struct TestLink {
        expired: AtomicBool,
        registration: ExpirationRegistration,
    }

    #[async_trait]
    impl LinkWithCertExpiration for TestLink {
        async fn expire(&self) -> ZResult<()> {
            self.expired.store(true, Ordering::Release);
            Ok(())
        }
    }

    fn wait_until(mut ready: impl FnMut() -> bool) {
        let limit = std::time::Instant::now() + Duration::from_secs(5);
        while !ready() {
            assert!(std::time::Instant::now() < limit);
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    fn registration(weak: Weak<TestLink>) -> ExpirationRegistration {
        ExpirationRegistration::new(
            weak,
            "127.0.0.1:1".parse().unwrap(),
            "127.0.0.1:2".parse().unwrap(),
            OffsetDateTime::now_utc() - time::Duration::seconds(1),
        )
    }

    #[test]
    fn already_due_expiration_waits_for_arc_initialization() {
        let link = Arc::<TestLink>::new_cyclic(|weak| {
            let registration = registration(weak.clone());
            // Force the exact race: expiry is already executing, but the
            // monitored link cannot yet be upgraded inside new_cyclic.
            wait_until(|| registration.target.entered.load(Ordering::Acquire));
            assert!(weak.upgrade().is_none());
            TestLink {
                expired: AtomicBool::new(false),
                registration,
            }
        });
        assert!(!link.expired.load(Ordering::Acquire));
        link.registration.initialized();
        zenoh_runtime::ZRuntime::Application
            .block_in_place(link.registration.manager.wait_for_expiration_task())
            .unwrap();
        assert!(link.expired.load(Ordering::Acquire));
    }

    #[test]
    fn failed_initialization_releases_the_expiration_task() {
        let mut target = Weak::<ReadyExpiration>::new();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = Arc::<TestLink>::new_cyclic(|weak| {
                let registration = registration(weak.clone());
                target = Arc::downgrade(&registration.target);
                wait_until(|| registration.target.entered.load(Ordering::Acquire));
                panic!("controlled initialization failure");
            });
        }));
        assert!(result.is_err());
        wait_until(|| target.upgrade().is_none());
    }
}

pub struct LinkUnicastTls {
    // TLS reads and writes both mutate the same rustls connection. Tokio's
    // split halves serialize each poll of that state, releasing the shared
    // lock on Pending so an idle read cannot block a write or shutdown.
    // The outer locks preserve ordering within each direction across awaits.
    reader: AsyncMutex<ReadHalf<TlsStream<TcpStream>>>,
    writer: AsyncMutex<WriteHalf<TlsStream<TcpStream>>>,
    // The source socket address of this link (address used on the local host)
    src_addr: SocketAddr,
    src_locator: Locator,
    // The destination socket address of this link (address used on the local host)
    dst_addr: SocketAddr,
    dst_locator: Locator,
    auth_identifier: LinkAuthId,
    mtu: BatchSize,
    expiration_manager: Option<ExpirationRegistration>,
}

impl LinkUnicastTls {
    fn new(
        socket: TlsStream<TcpStream>,
        src_addr: SocketAddr,
        dst_addr: SocketAddr,
        auth_identifier: LinkAuthId,
        expiration_manager: Option<ExpirationRegistration>,
    ) -> LinkUnicastTls {
        let (tcp_stream, _) = socket.get_ref();
        // Set the TLS nodelay option
        if let Err(err) = tcp_stream.set_nodelay(true) {
            tracing::warn!(
                "Unable to set NODEALY option on TLS link {} => {}: {}",
                src_addr,
                dst_addr,
                err
            );
        }

        // Set the TLS linger option
        #[allow(deprecated)]
        if let Err(err) = tcp_stream.set_linger(Some(Duration::from_secs(
            (*TLS_LINGER_TIMEOUT).try_into().unwrap(),
        ))) {
            tracing::warn!(
                "Unable to set LINGER option on TLS link {} => {}: {}",
                src_addr,
                dst_addr,
                err
            );
        }

        // Compute the MTU
        // See IETF RFC6691: https://datatracker.ietf.org/doc/rfc6691/
        let header = match src_addr.ip() {
            std::net::IpAddr::V4(_) => 40,
            std::net::IpAddr::V6(_) => 60,
        };
        #[allow(unused_mut)] // mut is not needed when target_family != unix
        let mut mtu = *TLS_DEFAULT_MTU - header;

        // target limitation of socket2: https://docs.rs/socket2/latest/src/socket2/sys/unix.rs.html#1544
        #[cfg(target_family = "unix")]
        {
            let socket = socket2::SockRef::from(&tcp_stream);
            // Get the MSS and divide it by 2 to ensure we can at least fill half the MSS
            let mss = socket.mss().unwrap_or(mtu as u32) / 2;
            // Compute largest multiple of TCP MSS that is smaller of default MTU
            let mut tgt = mss;
            while (tgt + mss) < mtu as u32 {
                tgt += mss;
            }
            mtu = (mtu as u32).min(tgt) as BatchSize;
        }

        let (reader, writer) = tokio::io::split(socket);
        // Build the Tls object
        LinkUnicastTls {
            reader: AsyncMutex::new(reader),
            writer: AsyncMutex::new(writer),
            src_addr,
            src_locator: Locator::new(TLS_LOCATOR_PREFIX, src_addr.to_string(), "").unwrap(),
            dst_addr,
            dst_locator: Locator::new(TLS_LOCATOR_PREFIX, dst_addr.to_string(), "").unwrap(),
            auth_identifier,
            mtu,
            expiration_manager,
        }
    }

    fn monitored(
        socket: TlsStream<TcpStream>,
        src_addr: SocketAddr,
        dst_addr: SocketAddr,
        auth_identifier: LinkAuthId,
        expiration: Option<OffsetDateTime>,
    ) -> ZResult<Arc<Self>> {
        // Handshake validity has second precision. Do not admit a new link
        // whose monitored certificate has already reached its exact deadline.
        if expiration.is_some_and(|expires| expires <= OffsetDateTime::now_utc()) {
            bail!("Remote TLS certificate chain has expired");
        }
        let link = Arc::new_cyclic(|weak: &Weak<Self>| {
            let registration = expiration.map(|expires| {
                ExpirationRegistration::new(weak.clone(), src_addr, dst_addr, expires)
            });
            Self::new(socket, src_addr, dst_addr, auth_identifier, registration)
        });
        if let Some(registration) = &link.expiration_manager {
            registration.initialized();
        }
        Ok(link)
    }

    async fn close(&self) -> ZResult<()> {
        tracing::trace!("Closing TLS link: {}", self);
        let mut writer = zasynclock!(self.writer);
        let res = writer.flush().await;
        tracing::trace!("TLS link flush {}: {:?}", self, res);
        // Send close_notify before shutting down TCP. This uses the same
        // shared poll lock as reads; it never borrows the TLS state twice.
        let res = writer.shutdown().await;
        tracing::trace!("TLS link shutdown {}: {:?}", self, res);
        res.map_err(|e| zerror!(e).into())
    }
}

#[async_trait]
impl LinkUnicastTrait for LinkUnicastTls {
    async fn close(&self) -> ZResult<()> {
        if let Some(registration) = &self.expiration_manager {
            let expiration_manager = &registration.manager;
            if !expiration_manager.set_closing() {
                // expiration_task is closing link, return its returned ZResult to Transport
                return expiration_manager.wait_for_expiration_task().await;
            }
            // cancel the expiration task and close link
            expiration_manager.cancel_expiration_task();
            let res = self.close().await;
            let _ = expiration_manager.wait_for_expiration_task().await;
            return res;
        }
        self.close().await
    }

    async fn write(&self, buffer: &[u8], _priority: Option<Priority>) -> ZResult<usize> {
        let mut writer = zasynclock!(self.writer);
        let written = writer.write(buffer).await.map_err(|e| {
            tracing::trace!("Write error on TLS link {}: {}", self, e);
            zerror!(e)
        })?;
        // tokio-rustls may accept plaintext while encrypted bytes remain
        // buffered. Deliver the whole accepted write before releasing its
        // operation lock; an idle link has no later write to flush it.
        writer.flush().await.map_err(|e| {
            tracing::trace!("Flush error on TLS link {}: {}", self, e);
            zerror!(e)
        })?;
        Ok(written)
    }

    async fn write_all(&self, buffer: &[u8], _priority: Option<Priority>) -> ZResult<()> {
        let mut writer = zasynclock!(self.writer);
        writer.write_all(buffer).await.map_err(|e| {
            tracing::trace!("Write error on TLS link {}: {}", self, e);
            zerror!(e)
        })?;
        writer.flush().await.map_err(|e| {
            tracing::trace!("Flush error on TLS link {}: {}", self, e);
            zerror!(e).into()
        })
    }

    async fn read(&self, buffer: &mut [u8], _priority: Option<Priority>) -> ZResult<usize> {
        let mut reader = zasynclock!(self.reader);
        reader.read(buffer).await.map_err(|e| {
            tracing::trace!("Read error on TLS link {}: {}", self, e);
            zerror!(e).into()
        })
    }

    async fn read_exact(&self, buffer: &mut [u8], _priority: Option<Priority>) -> ZResult<()> {
        let mut reader = zasynclock!(self.reader);
        let _ = reader.read_exact(buffer).await.map_err(|e| {
            tracing::trace!("Read error on TLS link {}: {}", self, e);
            zerror!(e)
        })?;
        Ok(())
    }

    #[inline(always)]
    fn get_src(&self) -> &Locator {
        &self.src_locator
    }

    #[inline(always)]
    fn get_dst(&self) -> &Locator {
        &self.dst_locator
    }

    #[inline(always)]
    fn get_mtu(&self) -> BatchSize {
        self.mtu
    }

    #[inline(always)]
    fn get_interface_names(&self) -> Vec<String> {
        get_ip_interface_names(&self.src_addr)
    }

    #[inline(always)]
    fn is_reliable(&self) -> bool {
        super::IS_RELIABLE
    }

    #[inline(always)]
    fn is_streamed(&self) -> bool {
        true
    }

    #[inline(always)]
    fn get_auth_id(&self) -> &LinkAuthId {
        &self.auth_identifier
    }

    #[cfg(all(feature = "uring", target_os = "linux"))]
    fn get_fd(&self) -> ZResult<RawFd> {
        bail!("Correct FD unavailable for TLS extension")
    }
}

#[async_trait]
impl LinkWithCertExpiration for LinkUnicastTls {
    async fn expire(&self) -> ZResult<()> {
        let expiration_manager = self
            .expiration_manager
            .as_ref()
            .expect("expiration_manager should be set");
        let expiration_manager = &expiration_manager.manager;
        if expiration_manager.set_closing() {
            return self.close().await;
        }
        // Transport is already closing the link
        Ok(())
    }
}

// Dropping both split halves drops the owned TcpStream and closes its socket.
// Explicit close/expiration sends close_notify; Drop must not enter a runtime
// or manufacture an alias to a stream owned by another half.

impl fmt::Display for LinkUnicastTls {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} => {}", self.src_addr, self.dst_addr)?;
        Ok(())
    }
}

impl fmt::Debug for LinkUnicastTls {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Tls")
            .field("src", &self.src_addr)
            .field("dst", &self.dst_addr)
            .finish()
    }
}

pub struct LinkManagerUnicastTls {
    manager: NewLinkChannelSender,
    listeners: ListenersUnicastIP,
}

impl fmt::Debug for LinkManagerUnicastTls {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LinkManagerUnicastTls")
            .field("manager", &self.manager)
            .field("listeners", &self.listeners)
            .finish()
    }
}

impl LinkManagerUnicastTls {
    pub fn new(manager: NewLinkChannelSender) -> Self {
        Self {
            manager,
            listeners: ListenersUnicastIP::new(),
        }
    }
}

#[async_trait]
impl LinkManagerUnicastTrait for LinkManagerUnicastTls {
    async fn new_link(&self, endpoint: EndPoint) -> ZResult<LinkUnicast> {
        let epaddr = endpoint.address();
        let epconf = endpoint.config();

        let server_name = get_tls_server_name(&epaddr)?;
        let addr = get_tls_addr(&epaddr).await?;

        // if both `iface`, and `bind` are present, return error
        if let (Some(_), Some(_)) = (epconf.get(BIND_INTERFACE), epconf.get(BIND_SOCKET)) {
            bail!(
                "Using Config options `iface` and `bind` in conjunction is unsupported at this time {} {:?}",
                BIND_INTERFACE,
                BIND_SOCKET
            )
        }

        // Initialize the TLS Config
        let client_config = TlsClientConfig::new(&epconf)
            .await
            .map_err(|e| zerror!("Cannot create a new TLS listener to {endpoint}: {e}"))?;
        let config = Arc::new(client_config.client_config);
        let connector = TlsConnector::from(config);

        // Initialize the TcpStream
        let (tcp_stream, src_addr, dst_addr) = client_config
            .tcp_socket_config
            .new_link(&addr)
            .await
            .map_err(|e| {
                zerror!(
                    "Can not create a new TLS link bound to {:?}: {}",
                    server_name,
                    e
                )
            })?;

        // Initialize the TlsStream
        let tls_stream = connector
            .connect(server_name.to_owned(), tcp_stream)
            .await
            .map_err(|e| {
                zerror!(
                    "Can not create a new TLS link bound to {:?}: {}",
                    server_name,
                    e
                )
            })?;

        let (_, tls_conn) = tls_stream.get_ref();
        let auth_identifier = get_server_cert_common_name(tls_conn)?;
        let certchain_expiration_time = get_cert_chain_expiration(&tls_conn.peer_certificates())?
            .expect("server should have certificate chain");

        let tls_stream = TlsStream::Client(tls_stream);

        let link = LinkUnicastTls::monitored(
            tls_stream,
            src_addr,
            dst_addr,
            auth_identifier.into(),
            client_config
                .tls_close_link_on_expiration
                .then_some(certchain_expiration_time),
        )?;

        Ok(LinkUnicast::from(link as Arc<dyn LinkUnicastTrait>))
    }

    async fn new_listener(&self, endpoint: EndPoint) -> ZResult<Locator> {
        let epaddr = endpoint.address();
        let epconf = endpoint.config();

        let addr = get_tls_addr(&epaddr).await?;
        let host = get_tls_host(&epaddr)?;

        // Initialize TlsConfig
        let tls_server_config = TlsServerConfig::new(&epconf)
            .await
            .map_err(|e| zerror!("Cannot create a new TLS listener on {addr}. {e}"))?;

        // Initialize the TcpListener
        let (socket, local_addr) = tls_server_config
            .tcp_socket_config
            .new_listener(&addr)
            .map_err(|e| zerror!("Can not create a new TLS listener on {}: {}", addr, e))?;

        let local_port = local_addr.port();

        // Initialize the TlsAcceptor
        let token = self.listeners.token.child_token();

        let task = {
            let acceptor = TlsAcceptor::from(Arc::new(tls_server_config.server_config));
            let token = token.clone();
            let manager = self.manager.clone();

            async move {
                accept_task(
                    socket,
                    acceptor,
                    token,
                    manager,
                    tls_server_config.tls_handshake_timeout,
                    tls_server_config.tls_close_link_on_expiration,
                )
                .await
            }
        };

        // Update the endpoint locator address
        let locator = Locator::new(
            endpoint.protocol(),
            format!("{host}:{local_port}"),
            endpoint.metadata(),
        )?;
        let endpoint = EndPoint::new(
            locator.protocol(),
            locator.address(),
            locator.metadata(),
            endpoint.config(),
        )?;
        self.listeners
            .add_listener(endpoint, local_addr, task, token)
            .await?;

        Ok(locator)
    }

    async fn del_listener(&self, endpoint: &EndPoint) -> ZResult<()> {
        let epaddr = endpoint.address();
        let addr = get_tls_addr(&epaddr).await?;
        self.listeners.del_listener(addr).await
    }

    async fn get_listeners(&self) -> Vec<EndPoint> {
        self.listeners.get_endpoints()
    }

    /// See [`zenoh_link_commons::ListenersUnicastIP::get_locators`].
    async fn get_locators(&self) -> Vec<Locator> {
        self.listeners.get_locators()
    }

    /// See [`zenoh_link_commons::ListenersUnicastIP::get_locators_noloopback`].
    async fn get_locators_noloopback(&self) -> Vec<Locator> {
        self.listeners.get_locators_noloopback()
    }
}

async fn accept_task(
    socket: TcpListener,
    acceptor: TlsAcceptor,
    token: CancellationToken,
    manager: NewLinkChannelSender,
    tls_handshake_timeout: Duration,
    tls_close_link_on_expiration: bool,
) -> ZResult<()> {
    let src_addr = socket.local_addr().map_err(|e| {
        let e = zerror!("Can not accept TLS connections: {}", e);
        tracing::warn!("{}", e);
        e
    })?;

    let mut listener = tls_listener::builder(acceptor)
        .handshake_timeout(tls_handshake_timeout)
        .listen(socket);

    tracing::trace!("Ready to accept TLS connections on: {:?}", src_addr);
    loop {
        tokio::select! {
            _ = token.cancelled() => break,

            res = listener.accept() => {
                match res {
                    Ok((tls_stream, dst_addr)) => {
                        let (tcp_stream, tls_conn) = tls_stream.get_ref();
                        let src_addr =  match tcp_stream.local_addr()  {
                            Ok(sa) => sa,
                            Err(e) => {
                                tracing::debug!("Can not accept TLS connection: {}", e);
                                continue;
                            }
                        };
                        let auth_identifier = match get_client_cert_common_name(tls_conn) {
                            Ok(auth_id) => auth_id,
                            Err(e) => {
                                tracing::warn!("Error getting client cert common name: {e}");
                                continue;
                            }
                        };

                        // Get certificate chain expiration
                        let mut maybe_expiration_time = None;
                        if tls_close_link_on_expiration {
                            let exp = match get_cert_chain_expiration(&tls_conn.peer_certificates()) {
                                Ok(exp) => exp,
                                Err(e) => {
                                    tracing::warn!("Error getting client cert expiration: {e}");
                                    continue;
                                }
                            };
                            match exp {
                                exp @ Some(_) => maybe_expiration_time = exp,
                                None => tracing::warn!(
                                    "Cannot monitor expiration for TLS link {:?} => {:?}: client does not have certificates",
                                    src_addr,
                                    dst_addr,
                                ),
                            }
                        }

                        tracing::debug!("Accepted TLS connection on {:?}: {:?}. {:?}.", src_addr, dst_addr, auth_identifier);
                        // Create the new link object
                        let link = match LinkUnicastTls::monitored(
                            tokio_rustls::TlsStream::Server(tls_stream),
                            src_addr,
                            dst_addr,
                            auth_identifier.into(),
                            maybe_expiration_time,
                        ) {
                            Ok(link) => link,
                            Err(error) => {
                                tracing::warn!("Rejecting expired TLS connection: {error}");
                                continue;
                            }
                        };

                        // Communicate the new link to the initial transport manager
                        if let Err(e) = manager
                            .send_async(LinkUnicast::from(link as Arc<dyn LinkUnicastTrait>))
                            .await
                        {
                            tracing::error!("{}-{}: {}", file!(), line!(), e)
                        }
                    }
                    Err(e) => {
                        tracing::warn!("{}. Hint: increase the system open file limit.", e);
                        // Throttle the accept loop upon an error
                        // NOTE: This might be due to various factors. However, the most common case is that
                        //       the process has reached the maximum number of open files in the system. On
                        //       Linux systems this limit can be changed by using the "ulimit" command line
                        //       tool. In case of systemd-based systems, this can be changed by using the
                        //       "sysctl" command line tool.
                        tokio::time::sleep(Duration::from_micros(*TLS_ACCEPT_THROTTLE_TIME)).await;
                    }
                }
            }
        }
    }

    Ok(())
}

fn get_client_cert_common_name(tls_conn: &rustls::CommonState) -> ZResult<TlsAuthId> {
    if let Some(client_certs) = tls_conn.peer_certificates() {
        let (_, cert) = X509Certificate::from_der(client_certs[0].as_ref())?;
        let subject_name = &cert
            .subject
            .iter_common_name()
            .next()
            .and_then(|cn| cn.as_str().ok());

        Ok(TlsAuthId {
            auth_value: subject_name.map(|cn| cn.to_string()),
        })
    } else {
        Ok(TlsAuthId { auth_value: None })
    }
}

fn get_server_cert_common_name(tls_conn: &rustls::ClientConnection) -> ZResult<TlsAuthId> {
    let serv_certs = tls_conn.peer_certificates().unwrap();
    let mut auth_id = TlsAuthId { auth_value: None };

    // Need the first certificate in the chain so no need for looping
    if let Some(item) = serv_certs.iter().next() {
        let (_, cert) = X509Certificate::from_der(item.as_ref())?;
        let subject_name = &cert
            .subject
            .iter_common_name()
            .next()
            .and_then(|cn| cn.as_str().ok());

        auth_id = TlsAuthId {
            auth_value: subject_name.map(|cn| cn.to_string()),
        };
        return Ok(auth_id);
    }
    Ok(auth_id)
}

/// Returns the minimum value of the `not_after` field in the given certificate chain.
/// Returns `None` if the input certificate chain is empty or `None`
fn get_cert_chain_expiration(
    cert_chain: &Option<&[rustls_pki_types::CertificateDer]>,
) -> ZResult<Option<OffsetDateTime>> {
    let mut link_expiration: Option<OffsetDateTime> = None;
    if let Some(remote_certs) = cert_chain {
        for cert in *remote_certs {
            let (_, cert) = X509Certificate::from_der(cert.as_ref())?;
            let cert_expiration = cert.validity().not_after.to_datetime();
            link_expiration = link_expiration
                .map(|current_min| current_min.min(cert_expiration))
                .or(Some(cert_expiration));
        }
    }
    Ok(link_expiration)
}

struct TlsAuthId {
    auth_value: Option<String>,
}

impl Debug for TlsAuthId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Common Name: {}",
            self.auth_value.as_deref().unwrap_or("None")
        )
    }
}

impl From<TlsAuthId> for LinkAuthId {
    fn from(value: TlsAuthId) -> Self {
        LinkAuthId::Tls(value.auth_value.clone())
    }
}

#[cfg(test)]
mod duplex_tests {
    use super::*;
    use rustls_pki_types::{CertificateDer, PrivateKeyDer, ServerName};
    use std::{
        fs,
        io::BufReader,
        path::PathBuf,
        process::Command,
        sync::atomic::{AtomicU64, Ordering},
    };

    struct Certificate {
        directory: PathBuf,
        cert: CertificateDer<'static>,
        key: PrivateKeyDer<'static>,
    }

    impl Certificate {
        fn new() -> Self {
            static NEXT: AtomicU64 = AtomicU64::new(0);
            let directory = std::env::temp_dir().join(format!(
                "aos-tls-duplex-{}-{}-{}",
                std::process::id(),
                time::OffsetDateTime::now_utc().unix_timestamp_nanos(),
                NEXT.fetch_add(1, Ordering::Relaxed),
            ));
            fs::create_dir(&directory).unwrap();
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                fs::set_permissions(&directory, fs::Permissions::from_mode(0o700)).unwrap();
            }
            let output = Command::new("openssl")
                .args([
                    "req",
                    "-x509",
                    "-newkey",
                    "ec",
                    "-pkeyopt",
                    "ec_paramgen_curve:prime256v1",
                    "-nodes",
                    "-sha256",
                    "-days",
                    "2",
                    "-subj",
                    "/CN=localhost",
                    "-addext",
                    "subjectAltName=IP:127.0.0.1",
                    "-addext",
                    "basicConstraints=critical,CA:FALSE",
                    "-keyout",
                    "key.pem",
                    "-out",
                    "cert.pem",
                ])
                .current_dir(&directory)
                .output()
                .expect("OpenSSL is required for the TLS transport regression");
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            let cert = rustls_pemfile::certs(&mut BufReader::new(
                fs::File::open(directory.join("cert.pem")).unwrap(),
            ))
            .next()
            .unwrap()
            .unwrap();
            let key = rustls_pemfile::private_key(&mut BufReader::new(
                fs::File::open(directory.join("key.pem")).unwrap(),
            ))
            .unwrap()
            .unwrap();
            Self {
                directory,
                cert,
                key,
            }
        }

        async fn pair(&self) -> (Arc<LinkUnicastTls>, Arc<LinkUnicastTls>) {
            let crypto = Arc::new(rustls::crypto::ring::default_provider());
            let server = rustls::ServerConfig::builder_with_provider(crypto.clone())
                .with_safe_default_protocol_versions()
                .unwrap()
                .with_no_client_auth()
                .with_single_cert(vec![self.cert.clone()], self.key.clone_key())
                .unwrap();
            let mut roots = rustls::RootCertStore::empty();
            roots.add(self.cert.clone()).unwrap();
            let client = rustls::ClientConfig::builder_with_provider(crypto)
                .with_safe_default_protocol_versions()
                .unwrap()
                .with_root_certificates(roots)
                .with_no_client_auth();
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let accept = tokio::spawn(async move {
                let (tcp, remote) = listener.accept().await.unwrap();
                let tls = TlsAcceptor::from(Arc::new(server))
                    .accept(tcp)
                    .await
                    .unwrap();
                Arc::new(LinkUnicastTls::new(
                    TlsStream::Server(tls),
                    address,
                    remote,
                    LinkAuthId::Tls(None),
                    None,
                ))
            });
            let tcp = TcpStream::connect(address).await.unwrap();
            let local = tcp.local_addr().unwrap();
            let tls = TlsConnector::from(Arc::new(client))
                .connect(ServerName::try_from("127.0.0.1").unwrap(), tcp)
                .await
                .unwrap();
            let client = Arc::new(LinkUnicastTls::new(
                TlsStream::Client(tls),
                local,
                address,
                LinkAuthId::Tls(None),
                None,
            ));
            (client, accept.await.unwrap())
        }
    }

    impl Drop for Certificate {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.directory);
        }
    }

    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap()
    }

    #[test]
    fn simultaneous_tls_reads_and_writes_preserve_every_frame() {
        let certificate = Certificate::new();
        runtime().block_on(async {
            tokio::time::timeout(Duration::from_secs(30), async {
                let (client, server) = certificate.pair().await;
                let start = Arc::new(tokio::sync::Barrier::new(4));
                let mut senders = Vec::new();
                let mut receivers = Vec::new();
                // Each end sends and receives on separate runtime workers.
                // Compare every byte, including sequence, across TLS records.
                for (link, sending, marker) in [
                    (client.clone(), true, 0x51),
                    (server.clone(), false, 0x51),
                    (server.clone(), true, 0xa7),
                    (client.clone(), false, 0xa7),
                ] {
                    let start = start.clone();
                    let task = tokio::spawn(async move {
                        let mut frame = vec![marker; 64 * 1024];
                        start.wait().await;
                        for sequence in 0u64..256 {
                            if sending {
                                frame[..8].copy_from_slice(&sequence.to_be_bytes());
                                if sequence % 2 == 0 {
                                    link.write_all(&frame, None).await.unwrap();
                                } else {
                                    let mut offset = 0;
                                    while offset < frame.len() {
                                        let written =
                                            link.write(&frame[offset..], None).await.unwrap();
                                        assert!(written > 0);
                                        offset += written;
                                    }
                                }
                            } else {
                                link.read_exact(&mut frame, None).await.unwrap();
                                assert_eq!(&frame[..8], &sequence.to_be_bytes());
                                assert!(frame[8..].iter().all(|value| *value == marker));
                            }
                        }
                    });
                    if sending {
                        senders.push(task);
                    } else {
                        receivers.push(task);
                    }
                }
                for task in senders {
                    task.await.unwrap();
                }
                // No explicit flush, close, or subsequent write may be
                // needed to deliver the last TLS record to either receiver.
                for task in receivers {
                    task.await.unwrap();
                }
                client.close().await.unwrap();
                server.close().await.unwrap();
            })
            .await
            .expect("full-duplex TLS traffic stalled");
        });
    }

    #[test]
    fn pending_read_does_not_block_write_or_tls_shutdown() {
        let certificate = Certificate::new();
        runtime().block_on(async {
            tokio::time::timeout(Duration::from_secs(5), async {
                let (client, server) = certificate.pair().await;
                let pending_client = client.clone();
                let pending =
                    tokio::spawn(async move { pending_client.read_exact(&mut [0; 1], None).await });
                tokio::time::sleep(Duration::from_millis(30)).await;
                assert!(!pending.is_finished());
                client.write_all(b"probe", None).await.unwrap();
                let mut data = [0; 5];
                server.read_exact(&mut data, None).await.unwrap();
                assert_eq!(&data, b"probe");
                assert!(!pending.is_finished());
                client.close().await.unwrap();
                // A graceful TLS close is EOF, not UnexpectedEof from a bare
                // TCP shutdown without close_notify.
                assert_eq!(server.read(&mut [0; 1], None).await.unwrap(), 0);
                server.close().await.unwrap();
                assert!(pending.await.unwrap().is_err());
            })
            .await
            .expect("pending TLS read blocked write or close");
        });
    }

    #[test]
    fn dropping_link_closes_socket_without_entering_a_runtime() {
        let certificate = Certificate::new();
        let runtime = runtime();
        let (client, server) = runtime.block_on(certificate.pair());
        // Drop outside a Tokio enter guard. The remaining peer must observe
        // socket closure even when explicit graceful close was not called.
        drop(client);
        runtime.block_on(async {
            let result =
                tokio::time::timeout(Duration::from_secs(2), server.read(&mut [0; 1], None))
                    .await
                    .expect("dropped TLS socket remained open");
            assert!(result.is_err() || result.unwrap() == 0);
        });
    }
}
