use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use futures::{channel::oneshot, lock::Mutex};
use rand::Rng;
use url::Url;

use crate::{connection::Connection, error::ConnectionError, executor::Executor, Certificate};

/// holds connection information for a broker
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct BrokerAddress {
    /// URL we're using for connection (can be the proxy's URL)
    pub url: Url,
    /// pulsar URL for the broker we're actually contacting
    /// this must follow the IP:port format
    pub broker_url: String,
    /// true if we're connecting through a proxy
    pub proxy: bool,
}

/// configuration for reconnection exponential back off
#[derive(Debug, Clone)]
pub struct ConnectionRetryOptions {
    /// minimum delay between connection retries
    pub min_backoff: Duration,
    /// maximum delay between reconnection retries
    pub max_backoff: Duration,
    /// maximum number of connection retries
    pub max_retries: u32,
    /// time limit to establish a connection
    pub connection_timeout: Duration,
    /// keep-alive interval for each broker connection
    pub keep_alive: Duration,
    /// maximum idle time before a connection is eligible for cleanup
    pub connection_max_idle: Duration,
}

impl Default for ConnectionRetryOptions {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn default() -> Self {
        ConnectionRetryOptions {
            min_backoff: Duration::from_millis(10),
            max_backoff: Duration::from_secs(30),
            max_retries: 12u32,
            connection_timeout: Duration::from_secs(10),
            keep_alive: Duration::from_secs(60),
            connection_max_idle: Duration::from_secs(120),
        }
    }
}

/// configuration for Pulsar operation retries
#[derive(Debug, Clone)]
pub struct OperationRetryOptions {
    /// time limit to receive an answer to a Pulsar operation
    pub operation_timeout: Duration,
    /// delay between operation retries after a ServiceNotReady error
    pub retry_delay: Duration,
    /// maximum number of operation retries. None indicates infinite retries
    pub max_retries: Option<u32>,
}

impl Default for OperationRetryOptions {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn default() -> Self {
        OperationRetryOptions {
            operation_timeout: Duration::from_secs(30),
            retry_delay: Duration::from_secs(5),
            max_retries: None,
        }
    }
}

impl OperationRetryOptions {
    pub fn allow_retry(&self, current: u32) -> bool {
        self.max_retries.is_none() || current < self.max_retries.unwrap()
    }
}

/// An I/O error's kind and message; a rustls error inside it is kept (it
/// is `Clone`), so a TLS failure classifies the same for every waiter.
fn copy_io_error(e: &std::io::Error) -> std::io::Error {
    #[cfg(any(
        feature = "tokio-rustls-runtime-aws-lc-rs",
        feature = "tokio-rustls-runtime-ring",
        feature = "async-std-rustls-runtime-aws-lc-rs",
        feature = "async-std-rustls-runtime-ring"
    ))]
    if let Some(tls) = e
        .get_ref()
        .and_then(|inner| inner.downcast_ref::<rustls::Error>())
    {
        return std::io::Error::new(e.kind(), tls.clone());
    }
    std::io::Error::new(e.kind(), e.to_string())
}

/// FORK-PATCH (3rdi): a copy of a dial's failure for each caller waiting on
/// it (`ConnectionError` is not `Clone`): the same variant, and an I/O error
/// of the same kind, so retry classification matches the dialer's. An I/O
/// error keeps its kind and message, and a rustls error inside it, but
/// loses any other `source()` chain; a TLS or DNS-name error variant becomes
/// `Unexpected` with its message.
pub(crate) fn for_waiter(e: &ConnectionError) -> ConnectionError {
    match e {
        ConnectionError::Io(e) => ConnectionError::Io(copy_io_error(e)),
        ConnectionError::SlowDown => ConnectionError::SlowDown,
        ConnectionError::Disconnected => ConnectionError::Disconnected,
        ConnectionError::PulsarError(code, msg) => ConnectionError::PulsarError(*code, msg.clone()),
        ConnectionError::Unexpected(m) => ConnectionError::Unexpected(m.clone()),
        ConnectionError::Decoding(m) => ConnectionError::Decoding(m.clone()),
        ConnectionError::Encoding(m) => ConnectionError::Encoding(m.clone()),
        ConnectionError::SocketAddr(m) => ConnectionError::SocketAddr(m.clone()),
        ConnectionError::UnexpectedResponse(m) => ConnectionError::UnexpectedResponse(m.clone()),
        ConnectionError::Authentication(a) => ConnectionError::Authentication(a.clone()),
        ConnectionError::NotFound => ConnectionError::NotFound,
        ConnectionError::Canceled => ConnectionError::Canceled,
        ConnectionError::Shutdown => ConnectionError::Shutdown,
        other => ConnectionError::Unexpected(other.to_string()),
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// A fake broker for connection-pool tests: it answers every
    /// `CommandConnect` with `CommandConnected` and never answers anything
    /// else, so any request on its connections times out.
    #[cfg(any(
        feature = "tokio-runtime",
        feature = "tokio-rustls-runtime-aws-lc-rs",
        feature = "tokio-rustls-runtime-ring"
    ))]
    pub(crate) mod silent_broker {
        use futures::{SinkExt, StreamExt};
        use tokio::net::TcpListener;

        use super::*;
        use crate::{
            message::{BaseCommand, Codec, Message},
            proto::CommandConnected,
            TokioExecutor,
        };

        /// A silent broker's `pulsar://` URL, and every command it receives.
        pub(crate) async fn listen() -> (
            String,
            futures::channel::mpsc::UnboundedReceiver<BaseCommand>,
        ) {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let port = listener.local_addr().unwrap().port();
            let (seen_tx, seen_rx) = futures::channel::mpsc::unbounded();
            tokio::spawn(async move {
                while let Ok((stream, _)) = listener.accept().await {
                    let seen_tx = seen_tx.clone();
                    tokio::spawn(async move {
                        let mut framed = tokio_util::codec::Framed::new(stream, Codec);
                        while let Some(Ok(msg)) = framed.next().await {
                            let _ = seen_tx.unbounded_send(msg.command.clone());
                            if msg.command.connect.is_some() {
                                let connected = Message {
                                    command: BaseCommand {
                                        r#type: 3,
                                        connected: Some(CommandConnected::default()),
                                        ..Default::default()
                                    },
                                    payload: None,
                                };
                                if framed.send(connected).await.is_err() {
                                    return;
                                }
                            }
                        }
                    });
                }
            });
            (format!("pulsar://127.0.0.1:{port}"), seen_rx)
        }

        /// A manager on a silent broker with a 200 ms operation timeout,
        /// and the broker's address.
        pub(crate) async fn manager() -> (ConnectionManager<TokioExecutor>, BrokerAddress) {
            let (url, _) = listen().await;
            let manager = ConnectionManager::new(
                url,
                None,
                None,
                OperationRetryOptions {
                    operation_timeout: Duration::from_millis(200),
                    ..Default::default()
                },
                None,
                100,
                Arc::new(TokioExecutor),
            )
            .await
            .unwrap();
            let broker = manager.get_base_address();
            (manager, broker)
        }

        /// A manager on `url` that has not dialed yet (no broker answers
        /// there): one connection attempt, a 1 s connection timeout.
        pub(crate) fn undialed_manager(url: &str) -> ConnectionManager<TokioExecutor> {
            ConnectionManager {
                url: Url::parse(url).unwrap(),
                auth: None,
                executor: Arc::new(TokioExecutor),
                connections: Arc::new(Mutex::new(HashMap::new())),
                connection_retry_options: ConnectionRetryOptions {
                    max_retries: 0,
                    connection_timeout: Duration::from_secs(1),
                    ..Default::default()
                },
                operation_retry_options: OperationRetryOptions::default(),
                tls_options: TlsOptions::default(),
                certificate_chain: vec![],
                outbound_channel_size: 100,
                dial_panics: false,
                give_up_gate: Default::default(),
            }
        }

        /// Fails the keepalive ping on `conn`, as on a broker restart.
        pub(crate) async fn fail(conn: &Connection<TokioExecutor>) {
            assert!(conn.sender().send_ping().await.is_err());
            assert!(!conn.is_valid(), "a failed ping retires the connection");
        }
    }

    /// FORK-PATCH (3rdi): a request the broker is slow to answer fails
    /// alone; only the keepalive ping retires a connection (as the Java
    /// client does), so one slow lookup never drops every consumer on it.
    #[tokio::test]
    #[cfg(any(
        feature = "tokio-runtime",
        feature = "tokio-rustls-runtime-aws-lc-rs",
        feature = "tokio-rustls-runtime-ring"
    ))]
    async fn a_slow_request_does_not_retire_the_connection() {
        let (manager, broker) = silent_broker::manager().await;
        let conn = manager.get_connection(&broker).await.unwrap();
        let lookup = conn.sender().lookup_topic("persistent://t/n/x", false);
        assert!(lookup.await.is_err(), "the unanswered lookup times out");
        assert!(conn.is_valid(), "a request timeout retired the connection");
        let again = manager.get_connection(&broker).await.unwrap();
        assert_eq!(again.id(), conn.id());
    }

    /// FORK-PATCH (3rdi): callers replacing the same dead connection at
    /// once share one new connection instead of dialing one each.
    #[tokio::test]
    #[cfg(any(
        feature = "tokio-runtime",
        feature = "tokio-rustls-runtime-aws-lc-rs",
        feature = "tokio-rustls-runtime-ring"
    ))]
    async fn concurrent_callers_share_one_replacement_connection() {
        let (manager, broker) = silent_broker::manager().await;
        let dead = manager.get_connection(&broker).await.unwrap();
        silent_broker::fail(&dead).await;

        let (a, b) = futures::join!(
            manager.get_connection(&broker),
            manager.get_connection(&broker)
        );
        let (a, b) = (a.unwrap(), b.unwrap());
        assert_ne!(a.id(), dead.id());
        assert_eq!(a.id(), b.id(), "each caller dialed its own connection");
    }

    /// FORK-PATCH (3rdi): the caller that claimed a dial can be cancelled
    /// (an application timeout, an aborted task, a lost `select!`) without
    /// stranding the callers queued behind it: the dial runs detached.
    #[tokio::test(start_paused = true)]
    #[cfg(any(
        feature = "tokio-runtime",
        feature = "tokio-rustls-runtime-aws-lc-rs",
        feature = "tokio-rustls-runtime-ring"
    ))]
    async fn a_cancelled_dial_strands_no_later_caller() {
        // a broker that accepts the socket and never answers CommandConnect
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((stream, _)) = listener.accept().await {
                held.push(stream);
            }
        });
        let manager = silent_broker::undialed_manager(&format!("pulsar://127.0.0.1:{port}"));
        let broker = manager.get_base_address();

        {
            let first = manager.get_connection(&broker);
            futures::pin_mut!(first);
            assert!(futures::poll!(first.as_mut()).is_pending());
        } // the claiming caller is dropped mid-dial

        let second =
            tokio::time::timeout(Duration::from_secs(3600), manager.get_connection(&broker)).await;
        assert!(second.is_ok(), "a cancelled dial stranded the next caller");
    }

    /// A broker that accepts the socket and never answers CommandConnect;
    /// its `pulsar://` URL.
    #[cfg(any(
        feature = "tokio-runtime",
        feature = "tokio-rustls-runtime-aws-lc-rs",
        feature = "tokio-rustls-runtime-ring"
    ))]
    async fn mute_broker() -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((stream, _)) = listener.accept().await {
                held.push(stream);
            }
        });
        format!("pulsar://127.0.0.1:{port}")
    }

    /// FORK-PATCH (3rdi): a dial that panics fails its waiters with
    /// `Unexpected` and clears the entry, so the next caller dials anew.
    #[tokio::test]
    #[cfg(any(
        feature = "tokio-runtime",
        feature = "tokio-rustls-runtime-aws-lc-rs",
        feature = "tokio-rustls-runtime-ring"
    ))]
    async fn a_panicking_dial_fails_its_waiters_and_clears_the_entry() {
        let mut manager = silent_broker::undialed_manager(&mute_broker().await);
        manager.dial_panics = true;
        let broker = manager.get_base_address();
        let got = tokio::time::timeout(Duration::from_secs(10), manager.get_connection(&broker))
            .await
            .expect("the waiter of a panicked dial was stranded");
        assert!(
            matches!(got, Err(ConnectionError::Unexpected(_))),
            "{:?}",
            got.map(|c| c.id())
        );
        assert!(manager.connections.lock().await.get(&broker).is_none());
    }

    /// FORK-PATCH (3rdi): a detached dial every caller stopped waiting for
    /// gives up, and a caller arriving right after that gets a fresh dial,
    /// never the abandoned one's `Canceled`.
    #[tokio::test(start_paused = true)]
    #[cfg(any(
        feature = "tokio-runtime",
        feature = "tokio-rustls-runtime-aws-lc-rs",
        feature = "tokio-rustls-runtime-ring"
    ))]
    async fn a_caller_after_an_abandoned_dial_gets_a_fresh_one() {
        let mut manager = silent_broker::undialed_manager(&mute_broker().await);
        manager.connection_retry_options.max_retries = 1000;
        let (reached_tx, reached_rx) = oneshot::channel();
        let (go_tx, go_rx) = oneshot::channel();
        *manager.give_up_gate.lock().unwrap() = Some((reached_tx, go_rx));
        let broker = manager.get_base_address();

        {
            let first = manager.get_connection(&broker);
            futures::pin_mut!(first);
            assert!(futures::poll!(first.as_mut()).is_pending());
        } // the only caller is dropped

        // paused time steps through the connection timeout and the backoff
        // until the dial gives up; it holds there
        reached_rx.await.expect("the abandoned dial never gave up");

        let late = manager.get_connection(&broker);
        futures::pin_mut!(late);
        assert!(futures::poll!(late.as_mut()).is_pending());

        // the abandoned dial returns
        go_tx.send(()).unwrap();
        tokio::time::sleep(Duration::from_millis(1)).await;

        match futures::poll!(late.as_mut()) {
            std::task::Poll::Pending => {}
            std::task::Poll::Ready(Ok(_)) => {}
            std::task::Poll::Ready(Err(e)) => {
                panic!("the late caller got the abandoned dial's end: {e:?}")
            }
        }
        assert!(
            matches!(
                manager.connections.lock().await.get(&broker),
                Some(ConnectionStatus::Connecting(v)) if !v.is_empty()
            ),
            "the late caller's fresh dial lost its entry"
        );
    }

    /// FORK-PATCH (3rdi): once a connection failed, the pool hands out a
    /// new one, even after a consumer engine read the error
    /// (prophesee-service-rs #284).
    #[tokio::test]
    #[cfg(any(
        feature = "tokio-runtime",
        feature = "tokio-rustls-runtime-aws-lc-rs",
        feature = "tokio-rustls-runtime-ring"
    ))]
    async fn the_pool_replaces_a_connection_whose_error_was_read() {
        let (manager, broker) = silent_broker::manager().await;
        let dead = manager.get_connection(&broker).await.unwrap();
        silent_broker::fail(&dead).await;
        assert!(dead.error().is_some(), "the engine reads the error");

        let next = manager.get_connection(&broker).await.unwrap();
        assert_ne!(
            next.id(),
            dead.id(),
            "the pool handed out the dead connection"
        );
        assert!(next.is_valid());
    }

    #[test]
    fn test_allow_retry_no_max_retries() {
        let options = OperationRetryOptions {
            operation_timeout: Duration::from_secs(30),
            retry_delay: Duration::from_secs(5),
            max_retries: None,
        };

        // If max_retries is None, it should always allow retries
        assert!(options.allow_retry(0));
        assert!(options.allow_retry(100));
        assert!(options.allow_retry(u32::MAX));
    }

    #[test]
    fn test_allow_retry_with_max_retries() {
        let options = OperationRetryOptions {
            operation_timeout: Duration::from_secs(30),
            retry_delay: Duration::from_secs(5),
            max_retries: Some(3),
        };

        // If max_retries is set to 3, we allow retries for current < 3
        assert!(options.allow_retry(0)); // current < 3
        assert!(options.allow_retry(2)); // current < 3
        assert!(!options.allow_retry(3)); // current == 3
        assert!(!options.allow_retry(4)); // current > 3
    }

    #[test]
    fn test_allow_retry_max_retries_is_zero() {
        let options = OperationRetryOptions {
            operation_timeout: Duration::from_secs(30),
            retry_delay: Duration::from_secs(5),
            max_retries: Some(0),
        };

        // If max_retries is 0, it should not allow any retries
        assert!(!options.allow_retry(0)); // current == 0
        assert!(!options.allow_retry(1)); // current > 0
    }
}

/// configuration for TLS connections
#[derive(Debug, Clone)]
pub struct TlsOptions {
    /// contains a list of PEM encoded certificates
    pub certificate_chain: Option<Vec<u8>>,

    /// allow insecure TLS connection if set to true
    ///
    /// defaults to *false*
    pub allow_insecure_connection: bool,

    /// whether hostname verification is enabled when insecure TLS connection is allowed
    ///
    /// defaults to *true*
    pub tls_hostname_verification_enabled: bool,
}

impl Default for TlsOptions {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn default() -> Self {
        Self {
            certificate_chain: None,
            allow_insecure_connection: false,
            tls_hostname_verification_enabled: true,
        }
    }
}

enum ConnectionStatus<Exe: Executor> {
    Connected {
        conn: Arc<Connection<Exe>>,
        last_used: Instant,
    },
    Connecting(Vec<oneshot::Sender<Result<Arc<Connection<Exe>>, ConnectionError>>>),
}

/// Look up broker addresses for topics and partitioned topics
///
/// The ConnectionManager object provides a single interface to start
/// interacting with a cluster. It will automatically follow redirects
/// or use a proxy, and aggregate broker connections
#[derive(Clone)]
pub struct ConnectionManager<Exe: Executor> {
    pub url: Url,
    pub(crate) auth: Option<Arc<Mutex<Box<dyn crate::authentication::Authentication>>>>,
    pub(crate) executor: Arc<Exe>,
    connections: Arc<Mutex<HashMap<BrokerAddress, ConnectionStatus<Exe>>>>,
    connection_retry_options: ConnectionRetryOptions,
    pub(crate) operation_retry_options: OperationRetryOptions,
    pub(crate) tls_options: TlsOptions,
    certificate_chain: Vec<Certificate>,
    outbound_channel_size: usize,
    /// Test seam: the dial panics.
    #[cfg(test)]
    dial_panics: bool,
    /// Test seam: a dial that gave up signals the first sender, then waits
    /// for the second receiver before it returns.
    #[cfg(test)]
    give_up_gate: GiveUpGate,
}

#[cfg(test)]
type GiveUpGate = Arc<std::sync::Mutex<Option<(oneshot::Sender<()>, oneshot::Receiver<()>)>>>;

impl<Exe: Executor> ConnectionManager<Exe> {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn new(
        url: String,
        auth: Option<Arc<Mutex<Box<dyn crate::authentication::Authentication>>>>,
        connection_retry: Option<ConnectionRetryOptions>,
        operation_retry_options: OperationRetryOptions,
        tls: Option<TlsOptions>,
        outbound_channel_size: usize,
        executor: Arc<Exe>,
    ) -> Result<Self, ConnectionError> {
        let connection_retry_options = connection_retry.unwrap_or_default();
        let tls_options = tls.unwrap_or_default();
        let url = Url::parse(&url)
            .map_err(|e| {
                error!("error parsing URL: {:?}", e);
                ConnectionError::NotFound
            })
            .and_then(|url| {
                url.host_str().ok_or_else(|| {
                    error!("missing host for URL: {:?}", url);
                    ConnectionError::NotFound
                })?;
                Ok(url)
            })?;

        let certificate_chain = match tls_options.certificate_chain.as_ref() {
            None => vec![],
            Some(certificate_chain) => {
                let mut v = vec![];
                let certificates =
                    pem::parse_many(certificate_chain).map_err(std::io::Error::other)?;

                for cert in certificates.iter().rev() {
                    #[cfg(any(feature = "tokio-runtime", feature = "async-std-runtime"))]
                    v.push(Certificate::from_der(cert.contents()).map_err(std::io::Error::other)?);

                    #[cfg(all(
                        any(
                            feature = "tokio-rustls-runtime-aws-lc-rs",
                            feature = "tokio-rustls-runtime-ring",
                            feature = "async-std-rustls-runtime-aws-lc-rs",
                            feature = "async-std-rustls-runtime-ring"
                        ),
                        not(any(feature = "tokio-runtime", feature = "async-std-runtime"))
                    ))]
                    v.push(Certificate::from(cert.contents().to_vec()));
                }
                v
            }
        };

        if let Some(auth) = auth.clone() {
            auth.lock().await.initialize().await?;
        }

        let manager = ConnectionManager {
            url: url.clone(),
            auth,
            executor,
            connections: Arc::new(Mutex::new(HashMap::new())),
            connection_retry_options,
            operation_retry_options,
            tls_options,
            certificate_chain,
            outbound_channel_size,
            #[cfg(test)]
            dial_panics: false,
            #[cfg(test)]
            give_up_gate: Default::default(),
        };
        let broker_address = BrokerAddress {
            url: url.clone(),
            broker_url: format!("{}:{}", url.host_str().unwrap(), url.port().unwrap_or(6650)),
            proxy: false,
        };
        manager.connect(broker_address).await?;
        Ok(manager)
    }

    pub fn get_base_address(&self) -> BrokerAddress {
        BrokerAddress {
            url: self.url.clone(),
            broker_url: format!(
                "{}:{}",
                self.url.host_str().unwrap(),
                self.url.port().unwrap_or(6650)
            ),
            proxy: false,
        }
    }

    /// get an active Connection from a broker address
    ///
    /// creates a connection if not available
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn get_base_connection(&self) -> Result<Arc<Connection<Exe>>, ConnectionError> {
        let broker_address = self.get_base_address();
        self.get_connection(&broker_address).await
    }

    /// get an active Connection from a broker address
    ///
    /// creates a connection if not available
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn get_connection(
        &self,
        broker: &BrokerAddress,
    ) -> Result<Arc<Connection<Exe>>, ConnectionError> {
        trace!("Looking for connection to {}...", broker.url);
        let rx = {
            let mut conns = self.connections.lock().await;
            // FORK-PATCH (3rdi): a caller that finds no usable connection
            // claims the dial under this lock (`Connecting`), so callers
            // racing it queue for its connection instead of each dialing
            // one (upstream left the entry as it was until `connect_inner`).
            // The dial runs detached and every caller, the claimer too,
            // waits for it the same way, so cancelling any of them strands
            // no one.
            let claim = match conns.get_mut(broker) {
                None => {
                    trace!("[] no connection for {}", broker.url);
                    true
                }
                Some(ConnectionStatus::Connected { conn, last_used }) => {
                    if conn.is_valid() {
                        trace!("[connected] returning valid connection for {}", broker.url);
                        // Update last_used timestamp to prevent premature cleanup
                        *last_used = Instant::now();
                        return Ok(conn.clone());
                    } else {
                        warn!("[connected] invalid connection for {}", broker.url);
                        true
                    }
                }
                Some(ConnectionStatus::Connecting(_)) => false,
            };
            let (tx, rx) = oneshot::channel();
            if claim {
                info!("No existing connection, creating new for {}", broker.url);
                conns.insert(broker.clone(), ConnectionStatus::Connecting(vec![tx]));
                let manager = self.clone();
                let dial = broker.clone();
                let spawned = self
                    .executor
                    .spawn(Box::pin(async move { manager.dial_detached(dial).await }));
                if spawned.is_err() {
                    error!("the executor could not spawn the dial to {}", broker.url);
                    if let Some(ConnectionStatus::Connecting(v)) = conns.remove(broker) {
                        for tx in v {
                            let _ = tx.send(Err(ConnectionError::Shutdown));
                        }
                    }
                }
            } else if let Some(ConnectionStatus::Connecting(v)) = conns.get_mut(broker) {
                debug!(
                    "[connecting...] existing pending connection to {}",
                    broker.url
                );
                v.push(tx);
            }
            rx
        };

        match rx.await {
            Ok(res) => {
                debug!("Connection found for {}", broker.url);
                res
            }
            Err(_) => Err(ConnectionError::Canceled),
        }
    }

    /// Removes `broker`'s `Connecting` entry when every caller queued on it
    /// stopped waiting (its future was dropped); true when it did.
    async fn give_up_if_nobody_waits(&self, broker: &BrokerAddress) -> bool {
        let mut conns = self.connections.lock().await;
        let abandoned = matches!(
            conns.get(broker),
            Some(ConnectionStatus::Connecting(v)) if v.iter().all(|tx| tx.is_canceled())
        );
        if abandoned {
            conns.remove(broker);
        }
        abandoned
    }

    #[cfg(test)]
    async fn at_give_up_gate(&self) {
        let gate = self.give_up_gate.lock().unwrap().take();
        if let Some((reached, go)) = gate {
            let _ = reached.send(());
            let _ = go.await;
        }
    }

    /// FORK-PATCH (3rdi): dials `broker` for the callers queued on its
    /// `Connecting` entry, detached from all of them. Whatever ends the
    /// dial (an error, a panic, a failed keepalive spawn) resolves every
    /// waiter and clears the entry, so no later caller queues forever.
    async fn dial_detached(self, broker: BrokerAddress) {
        use futures::FutureExt;
        let mut gave_up = false;
        let dialed =
            std::panic::AssertUnwindSafe(self.connect_claimed(broker.clone(), true, &mut gave_up))
                .catch_unwind()
                .await;
        let err = match dialed {
            Ok(Ok(_)) => return,
            // its entry is gone (it had no waiter); any there now is a fresh
            // dial's, whether the dial then returned or panicked
            Ok(Err(_)) if gave_up => return,
            Err(_) if gave_up => return,
            Ok(Err(e)) => e,
            Err(_) => ConnectionError::Unexpected("the connection dial panicked".to_string()),
        };
        let waiting = {
            let mut conns = self.connections.lock().await;
            match conns.get(&broker) {
                Some(ConnectionStatus::Connecting(_)) => conns.remove(&broker),
                _ => None,
            }
        };
        if let Some(ConnectionStatus::Connecting(v)) = waiting {
            for tx in v {
                let _ = tx.send(Err(for_waiter(&err)));
            }
        }
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    async fn connect_inner(
        &self,
        broker: &BrokerAddress,
        claimed: bool,
        gave_up: &mut bool,
    ) -> Result<Arc<Connection<Exe>>, ConnectionError> {
        let rx = if claimed {
            None
        } else {
            match self
                .connections
                .lock()
                .await
                .entry(broker.clone())
                .or_insert_with(|| ConnectionStatus::Connecting(Vec::new()))
            {
                ConnectionStatus::Connecting(ref mut v) => {
                    if v.is_empty() {
                        None
                    } else {
                        let (tx, rx) = oneshot::channel();
                        v.push(tx);
                        Some(rx)
                    }
                }
                ConnectionStatus::Connected { .. } => None,
            }
        };
        if let Some(rx) = rx {
            return match rx.await {
                Ok(res) => res,
                Err(_) => Err(ConnectionError::Canceled),
            };
        }

        #[cfg(test)]
        if self.dial_panics {
            panic!("injected dial panic");
        }

        let proxy_url = if broker.proxy {
            Some(broker.broker_url.clone())
        } else {
            None
        };

        let mut current_backoff;
        let mut current_retries = 0u32;

        let start = std::time::Instant::now();
        let conn = loop {
            match Connection::new(
                broker.url.clone(),
                self.auth.clone(),
                proxy_url.clone(),
                &self.certificate_chain,
                self.tls_options.allow_insecure_connection,
                self.tls_options.tls_hostname_verification_enabled,
                self.connection_retry_options.connection_timeout,
                self.operation_retry_options.operation_timeout,
                self.outbound_channel_size,
                self.executor.clone(),
            )
            .await
            {
                Ok(c) => break c,
                Err(e) if e.establish_retryable() => {
                    if current_retries >= self.connection_retry_options.max_retries {
                        return Err(e);
                    }

                    let jitter = rand::thread_rng().gen_range(0..10);
                    current_backoff = std::cmp::min(
                        self.connection_retry_options.min_backoff
                            * 2u32.saturating_pow(current_retries),
                        self.connection_retry_options.max_backoff,
                    ) + self.connection_retry_options.min_backoff * jitter;
                    current_retries += 1;

                    trace!(
                        "current retries: {}, current_backoff(pow = {}): {}ms",
                        current_retries,
                        2u32.pow(current_retries - 1),
                        current_backoff.as_millis()
                    );
                    error!(
                        "connection error, retrying connection to {} after {}ms",
                        broker.url,
                        current_backoff.as_millis()
                    );
                    self.executor.delay(current_backoff).await;
                    // FORK-PATCH (3rdi): a detached dial that every caller
                    // stopped waiting for gives up. The check and the removal
                    // of its entry are one step under the pool lock, so a
                    // caller arriving after it finds no entry and starts a
                    // fresh dial, and one arriving before it keeps this dial
                    // going.
                    if claimed && self.give_up_if_nobody_waits(broker).await {
                        debug!("nobody waits for the dial to {} any more", broker.url);
                        *gave_up = true;
                        #[cfg(test)]
                        self.at_give_up_gate().await;
                        return Err(ConnectionError::Canceled);
                    }
                }
                Err(e) => {
                    return Err(e);
                }
            }
        };
        let connection_id = conn.id();
        if let Some(url) = proxy_url.as_ref() {
            info!(
                "Connected n°{} to {} via proxy {} in {}ms",
                connection_id,
                url,
                broker.url,
                (std::time::Instant::now() - start).as_millis()
            );
        } else {
            info!(
                "Connected n°{} to {} in {}ms",
                connection_id,
                broker.url,
                (std::time::Instant::now() - start).as_millis()
            );
        }
        let c = Arc::new(conn);

        Ok(c)
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    async fn connect(
        &self,
        broker: BrokerAddress,
    ) -> Result<Arc<Connection<Exe>>, ConnectionError> {
        self.connect_claimed(broker, false, &mut false).await
    }

    /// `claimed`: the caller already set this broker's entry to
    /// `Connecting` and is the one to dial.
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    async fn connect_claimed(
        &self,
        broker: BrokerAddress,
        claimed: bool,
        gave_up: &mut bool,
    ) -> Result<Arc<Connection<Exe>>, ConnectionError> {
        let c = match self.connect_inner(&broker, claimed, gave_up).await {
            // FORK-PATCH (3rdi): a dial that gave up removed its own entry;
            // one there now belongs to a fresh dial
            Err(e) if *gave_up => return Err(e),
            Err(e) => {
                // the current ConnectionStatus is Connecting, containing
                // notification channels for all the tasks waiting for the
                // reconnection. If we delete this status, they will be
                // notified that reconnection is canceled instead of getting
                // stuck
                if let Some(ConnectionStatus::Connecting(mut v)) =
                    self.connections.lock().await.remove(&broker)
                {
                    for tx in v.drain(..) {
                        // FORK-PATCH (3rdi): every waiter gets the dial's
                        // own failure (upstream sent Canceled), so it
                        // classifies the retry the same way the dialer does
                        let _ = tx.send(Err(for_waiter(&e)));
                    }
                }

                return Err(e);
            }
            Ok(c) => c,
        };

        let connection_id = c.id();
        let proxy_url = if broker.proxy {
            Some(broker.broker_url.clone())
        } else {
            None
        };

        // set up client heartbeats for the connection
        let weak_conn = Arc::downgrade(&c);
        let mut interval = self
            .executor
            .interval(self.connection_retry_options.keep_alive);
        let broker_url = broker.url.clone();
        let proxy_to_broker_url = proxy_url.clone();
        let res = self.executor.spawn(Box::pin(async move {
            use crate::futures::StreamExt;
            while let Some(()) = interval.next().await {
                let Some(strong_conn) = weak_conn.upgrade() else {
                    debug!(
                        "connection {} was dropped, stopping keepalive task",
                        connection_id
                    );
                    break;
                };
                if !strong_conn.is_valid() {
                    debug!(
                        "connection {} is not valid anymore, stopping keepalive task",
                        connection_id
                    );
                    break;
                }
                if let Some(url) = proxy_to_broker_url.as_ref() {
                    trace!(
                        "will ping connection {} to {} via proxy {}",
                        connection_id,
                        url,
                        broker_url
                    );
                } else {
                    trace!("will ping connection {} to {}", connection_id, broker_url);
                }
                if let Err(e) = strong_conn.sender().send_ping().await {
                    error!(
                        "could not ping connection {} to the server at {}: {}",
                        connection_id, broker_url, e
                    );
                }
            }
        }));
        if res.is_err() {
            error!("the executor could not spawn the keepalive future");
            return Err(ConnectionError::Shutdown);
        }

        let old = self.connections.lock().await.insert(
            broker,
            ConnectionStatus::Connected {
                conn: c.clone(),
                last_used: Instant::now(),
            },
        );
        match old {
            Some(ConnectionStatus::Connecting(mut v)) => {
                //info!("was in connecting state({} waiting)", v.len());
                for tx in v.drain(..) {
                    let _ = tx.send(Ok(c.clone()));
                }
            }
            Some(ConnectionStatus::Connected { .. }) => {
                info!("removing old connection");
            }
            None => {
                debug!("setting up new connection");
            }
        };

        Ok(c)
    }

    /// tests that all connections are valid and still used
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub(crate) async fn check_connections(&self) {
        trace!("cleaning invalid or unused connections");
        self.connections
            .lock()
            .await
            .retain(|broker, ref mut connection| match connection {
                ConnectionStatus::Connecting(_) => {
                    trace!("Retaining connection in `Connecting` state");
                    true
                }
                ConnectionStatus::Connected { conn, last_used } => {
                    let max_idle = self.connection_retry_options.connection_max_idle;
                    let idle_time = last_used.elapsed();
                    let recently_used = idle_time < max_idle;
                    let strong_count = Arc::strong_count(conn);
                    let is_valid = conn.is_valid();

                    // Keep connection if valid AND (actively held OR recently used)
                    // This allows periodic use (like topic refresh) while cleaning up truly abandoned connections
                    let should_retain = is_valid && (strong_count > 1 || recently_used);

                    trace!(
                        "checking broker {} connection {}, is_valid: {}, strong_count: {}, idle_time: {:?}, max_idle: {:?}, recently_used: {}",
                        broker.url,
                        conn.id(),
                        is_valid,
                        strong_count,
                        idle_time,
                        max_idle,
                        recently_used
                    );
                    if !should_retain {
                        info!(
                            "Removing {} connection {} to {} (max_idle: {:?}, idle_time: {:?})",
                            if is_valid { "unused" } else { "invalid" },
                            conn.id(),
                            broker.url,
                            max_idle,
                            idle_time
                        );
                    }
                    should_retain
                }
            });
    }
}
