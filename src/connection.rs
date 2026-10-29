use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Debug,
    net::SocketAddr,
    pin::Pin,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use async_trait::async_trait;
use futures::{
    self,
    channel::{mpsc, oneshot},
    future::{select, Either},
    lock::Mutex,
    pin_mut,
    task::{Context, Poll},
    Future, FutureExt, Sink, SinkExt, Stream, StreamExt,
};
use proto::MessageIdData;
use rand::{seq::SliceRandom, thread_rng};
use url::Url;
use uuid::Uuid;

use crate::{
    consumer::ConsumerOptions,
    error::{AuthenticationError, ConnectionError, SharedError},
    executor::{Executor, ExecutorKind},
    message::{
        proto::{self, command_subscribe::SubType},
        BaseCommand, Codec, Message,
    },
    producer::{self, ProducerOptions},
    Certificate,
};

pub(crate) enum Register {
    Request {
        key: RequestKey,
        resolver: oneshot::Sender<Message>,
    },
    Consumer {
        consumer_id: u64,
        resolver: mpsc::UnboundedSender<Message>,
    },
    RemoveConsumer {
        consumer_id: u64,
    },
    /// the producer is (re)created on this connection, forget an earlier close by the broker
    Producer {
        producer_id: u64,
    },
    Ping {
        resolver: oneshot::Sender<()>,
    },
    /// FORK-PATCH (3rdi): the request timed out; forget it
    CancelRequest {
        key: RequestKey,
    },
}

/// identifier for a message
#[derive(Debug, Clone, PartialEq, Ord, PartialOrd, Eq)]
pub enum RequestKey {
    RequestId(u64),
    ProducerSend { producer_id: u64, sequence_id: u64 },
    Consumer { consumer_id: u64 },
    CloseConsumer { consumer_id: u64, request_id: u64 },
    CloseProducer { producer_id: u64 },
    AuthChallenge,
}

/// ids of the producers closed by the broker on a connection
type ClosedProducers = Arc<std::sync::Mutex<BTreeSet<u64>>>;

/// Authentication parameters
#[derive(Clone)]
pub struct Authentication {
    /// Authentication kid. Use "token" for JWT
    pub name: String,
    /// Authentication data
    pub data: Vec<u8>,
}

#[async_trait]
impl crate::authentication::Authentication for Authentication {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn auth_method_name(&self) -> String {
        self.name.clone()
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    async fn initialize(&mut self) -> Result<(), AuthenticationError> {
        Ok(())
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    async fn auth_data(&mut self) -> Result<Vec<u8>, AuthenticationError> {
        Ok(self.data.clone())
    }
}

/// FORK-PATCH (3rdi): a registered request's answer, for as long as it is
/// awaited. However the request ends without its answer (a timeout, its
/// future dropped, even while it waits for room on the data channel), its
/// registration is cancelled: the receiver end goes first, then
/// `Register::CancelRequest`, so the connection's receiver forgets this
/// request's resolver and no other.
struct PendingRequest {
    response: Option<oneshot::Receiver<Message>>,
    key: Option<RequestKey>,
    registrations: mpsc::UnboundedSender<Register>,
}

impl PendingRequest {
    /// The answer came: nothing to cancel.
    fn answered(&mut self) {
        self.key = None;
    }
}

impl Drop for PendingRequest {
    fn drop(&mut self) {
        self.response.take();
        if let Some(key) = self.key.take() {
            let _ = self
                .registrations
                .unbounded_send(Register::CancelRequest { key });
        }
    }
}

pub(crate) struct Receiver<S: Stream<Item = Result<Message, ConnectionError>>> {
    inbound: Pin<Box<S>>,
    control_tx: async_channel::Sender<Message>,
    error: SharedError,
    pending_requests: BTreeMap<RequestKey, oneshot::Sender<Message>>,
    consumers: BTreeMap<u64, mpsc::UnboundedSender<Message>>,
    received_messages: BTreeMap<RequestKey, Message>,
    registrations: Pin<Box<mpsc::UnboundedReceiver<Register>>>,
    shutdown: Pin<Box<oneshot::Receiver<()>>>,
    ping: Option<oneshot::Sender<()>>,
    auth_challenge: mpsc::UnboundedSender<()>,
    closed_producers: ClosedProducers,
}

impl<S: Stream<Item = Result<Message, ConnectionError>>> Receiver<S> {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn new(
        inbound: S,
        control_tx: async_channel::Sender<Message>,
        error: SharedError,
        registrations: mpsc::UnboundedReceiver<Register>,
        shutdown: oneshot::Receiver<()>,
        auth_challenge: mpsc::UnboundedSender<()>,
        closed_producers: ClosedProducers,
    ) -> Receiver<S> {
        Receiver {
            inbound: Box::pin(inbound),
            control_tx,
            error,
            pending_requests: BTreeMap::new(),
            received_messages: BTreeMap::new(),
            consumers: BTreeMap::new(),
            registrations: Box::pin(registrations),
            shutdown: Box::pin(shutdown),
            ping: None,
            auth_challenge,
            closed_producers,
        }
    }
}

impl<S: Stream<Item = Result<Message, ConnectionError>>> Receiver<S> {
    /// Takes in every registration queued so far; false when the
    /// registration channel is closed (the connection is gone).
    fn drain_registrations(&mut self, cx: &mut Context<'_>) -> bool {
        loop {
            match self.registrations.as_mut().poll_next(cx) {
                Poll::Ready(Some(Register::Request { key, resolver })) => {
                    match self.received_messages.remove(&key) {
                        Some(msg) => {
                            let _ = resolver.send(msg);
                        }
                        None => match key {
                            // the broker already closed this producer, no receipt will come
                            RequestKey::ProducerSend { producer_id, .. }
                                if self.closed_producers.lock().unwrap().contains(&producer_id) =>
                            {
                                let _ =
                                    resolver.send(messages::close_producer(producer_id, u64::MAX));
                            }
                            key => {
                                self.pending_requests.insert(key, resolver);
                            }
                        },
                    }
                }
                Poll::Ready(Some(Register::Producer { producer_id })) => {
                    self.closed_producers.lock().unwrap().remove(&producer_id);
                }
                Poll::Ready(Some(Register::Consumer {
                    consumer_id,
                    resolver,
                })) => {
                    self.consumers.insert(consumer_id, resolver);
                }
                Poll::Ready(Some(Register::RemoveConsumer { consumer_id })) => {
                    self.consumers.remove(&consumer_id);
                }
                Poll::Ready(Some(Register::Ping { resolver })) => {
                    self.ping = Some(resolver);
                }
                Poll::Ready(Some(Register::CancelRequest { key })) => {
                    // only the cancelled request's own resolver (its
                    // receiver is gone): a resend may have registered the
                    // same key since
                    if self
                        .pending_requests
                        .get(&key)
                        .is_some_and(|resolver| resolver.is_canceled())
                    {
                        self.pending_requests.remove(&key);
                    }
                    self.received_messages.remove(&key);
                }
                Poll::Ready(None) => {
                    self.error.set(ConnectionError::Disconnected);
                    return false;
                }
                Poll::Pending => return true,
            }
        }
    }

    /// FORK-PATCH (3rdi): the resolver of the request `key` answers, if one
    /// is registered. A request registers before it is written, so a reply
    /// can only be read ahead of a registration still in the channel: the
    /// channel is drained once more before giving up on it.
    fn resolver_for(
        &mut self,
        key: &RequestKey,
        cx: &mut Context<'_>,
    ) -> Result<Option<oneshot::Sender<Message>>, ()> {
        // FORK-PATCH (3rdi): a resolver nobody waits on (its request was
        // dropped, its cancellation still in the channel) counts as absent:
        // a resend's registration may be behind it in the channel
        if let Some(resolver) = self.pending_requests.remove(key) {
            if !resolver.is_canceled() {
                return Ok(Some(resolver));
            }
        }
        if !self.drain_registrations(cx) {
            return Err(());
        }
        Ok(self
            .pending_requests
            .remove(key)
            .filter(|resolver| !resolver.is_canceled()))
    }
}

impl<S: Stream<Item = Result<Message, ConnectionError>>> Future for Receiver<S> {
    type Output = Result<(), ()>;

    #[allow(clippy::result_large_err)]
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.shutdown.as_mut().poll(cx) {
            Poll::Ready(Ok(())) | Poll::Ready(Err(futures::channel::oneshot::Canceled)) => {
                return Poll::Ready(Err(()));
            }
            Poll::Pending => {}
        }

        //Are we worried about starvation here?
        if !self.drain_registrations(cx) {
            return Poll::Ready(Err(()));
        }

        loop {
            match self.inbound.as_mut().poll_next(cx) {
                Poll::Ready(Some(Ok(msg))) => {
                    match msg {
                        Message {
                            command: BaseCommand { ping: Some(_), .. },
                            ..
                        } => {
                            if self.control_tx.try_send(messages::pong()).is_err() {
                                error!("failed to send pong: control channel closed");
                                self.error.set(ConnectionError::Disconnected);
                            }
                        }
                        Message {
                            command: BaseCommand { pong: Some(_), .. },
                            ..
                        } => {
                            if let Some(sender) = self.ping.take() {
                                let _ = sender.send(());
                            }
                        }
                        msg => match msg.request_key() {
                            Some(key @ RequestKey::RequestId(_))
                            | Some(key @ RequestKey::ProducerSend { .. }) => {
                                trace!("received this message: {:?}", msg);
                                match self.resolver_for(&key, cx) {
                                    Err(()) => return Poll::Ready(Err(())),
                                    Ok(Some(resolver)) => {
                                        // We don't care if the receiver has dropped their future
                                        let _ = resolver.send(msg);
                                    }
                                    // FORK-PATCH (3rdi): the producer's exclusive-access
                                    // answer comes unrequested; its waiter registers only
                                    // after the first answer, so it is kept for it
                                    Ok(None) if msg.command.producer_success.is_some() => {
                                        self.received_messages.insert(key, msg);
                                    }
                                    // FORK-PATCH (3rdi): every registration is in, so
                                    // nobody waits for this reply (its request timed out
                                    // and was cancelled): dropped, where upstream kept
                                    // it for the connection's lifetime
                                    Ok(None) => {
                                        debug!("dropping a reply nobody waits for: {:?}", key);
                                    }
                                }
                            }
                            Some(RequestKey::Consumer { consumer_id }) => {
                                let _ = self
                                    .consumers
                                    .get_mut(&consumer_id)
                                    .map(move |consumer| consumer.unbounded_send(msg));
                            }
                            Some(RequestKey::CloseConsumer {
                                consumer_id,
                                request_id,
                            }) => {
                                // FORK-PATCH (3rdi): a registration still in the
                                // channel is drained in before deciding (upstream's
                                // FIXME), so a reply to our own close is never taken
                                // for a broker-initiated one
                                let resolver = match self
                                    .resolver_for(&RequestKey::RequestId(request_id), cx)
                                {
                                    Err(()) => return Poll::Ready(Err(())),
                                    Ok(resolver) => resolver,
                                };
                                if let Some(resolver) = resolver {
                                    // We don't care if the receiver has dropped their future
                                    let _ = resolver.send(msg);
                                } else {
                                    let res = self
                                        .consumers
                                        .get_mut(&consumer_id)
                                        .map(move |consumer| consumer.unbounded_send(msg));

                                    if !res.as_ref().map(|r| r.is_ok()).unwrap_or(false) {
                                        error!("ConnectionReceiver: error transmitting message to consumer: {:?}", res);
                                    }
                                }
                            }
                            Some(RequestKey::CloseProducer { producer_id }) => {
                                match &msg.command.send_error {
                                    Some(e) => warn!("Producer {producer_id} send error: {e:?}"),
                                    None => {
                                        info!(
                                            "Broker notification of closed producer: {producer_id}"
                                        )
                                    }
                                }
                                self.closed_producers.lock().unwrap().insert(producer_id);
                                // no receipt will come for its pending sends: fail them so the
                                // producer reconnects and resends them right away
                                let first = RequestKey::ProducerSend {
                                    producer_id,
                                    sequence_id: 0,
                                };
                                let last = RequestKey::ProducerSend {
                                    producer_id,
                                    sequence_id: u64::MAX,
                                };
                                let keys: Vec<_> = self
                                    .pending_requests
                                    .range(first..=last)
                                    .map(|(k, _)| k.clone())
                                    .collect();
                                for key in keys {
                                    if let Some(resolver) = self.pending_requests.remove(&key) {
                                        let _ = resolver.send(msg.clone());
                                    }
                                }
                            }
                            Some(RequestKey::AuthChallenge) => {
                                debug!("Received AuthChallenge");
                                let _ = self.auth_challenge.unbounded_send(());
                            }
                            None => {
                                warn!(
                                    "Received unexpected message; dropping. Message {:?}",
                                    crate::message::redacted(&msg.command)
                                )
                            }
                        },
                    }
                }
                Poll::Ready(None) => {
                    self.error.set(ConnectionError::Disconnected);
                    return Poll::Ready(Err(()));
                }
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Some(Err(e))) => {
                    self.error.set(e);
                    return Poll::Ready(Err(()));
                }
            }
        }
    }
}

#[derive(Clone)]
/// FORK-PATCH (3rdi): 64 bits on every target (upstream's `AtomicUsize`
/// wraps at 2^32 on a 32-bit target, reusing request ids).
pub struct SerialId(Arc<AtomicU64>);

impl Default for SerialId {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn default() -> Self {
        SerialId(Arc::new(AtomicU64::new(0)))
    }
}

impl SerialId {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn new() -> Self {
        Self::default()
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn get(&self) -> u64 {
        self.0.fetch_add(1, Ordering::Relaxed)
    }
}

/// Which outbound channel a message should travel on, and the backpressure
/// behavior to apply.
#[derive(Debug, Clone, Copy)]
enum SendChannel {
    /// Unbounded control plane. Used for every command other than producer
    /// `Send` (Ping, Pong, Ack, Flow, Lookup, AuthChallenge, Subscribe and
    /// its close, GetTopicsOfNamespace, etc.). Never blocks; only fails if
    /// the sink task has exited.
    Control,
    /// Bounded data plane: wait until there is room. Used by `Connection::send`
    /// when the caller opts into producer-side backpressure.
    DataBlocking,
    /// Bounded data plane: return `ConnectionError::SlowDown` if the channel
    /// is full. Used by `Connection::send` when the caller wants to react to
    /// backpressure rather than block.
    DataTrySend,
}

/// An owned type that can send messages like a connection
//#[derive(Clone)]
pub struct ConnectionSender<Exe: Executor> {
    connection_id: Uuid,
    data_tx: async_channel::Sender<Message>,
    control_tx: async_channel::Sender<Message>,
    registrations: mpsc::UnboundedSender<Register>,
    receiver_shutdown: Option<oneshot::Sender<()>>,
    request_id: SerialId,
    error: SharedError,
    executor: Arc<Exe>,
    operation_timeout: Duration,
    closed_producers: ClosedProducers,
}

impl<Exe: Executor> ConnectionSender<Exe> {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub(crate) fn new(
        connection_id: Uuid,
        data_tx: async_channel::Sender<Message>,
        control_tx: async_channel::Sender<Message>,
        registrations: mpsc::UnboundedSender<Register>,
        receiver_shutdown: oneshot::Sender<()>,
        request_id: SerialId,
        error: SharedError,
        executor: Arc<Exe>,
        operation_timeout: Duration,
        closed_producers: ClosedProducers,
    ) -> ConnectionSender<Exe> {
        ConnectionSender {
            connection_id,
            data_tx,
            control_tx,
            registrations,
            receiver_shutdown: Some(receiver_shutdown),
            request_id,
            error,
            executor,
            operation_timeout,
            closed_producers,
        }
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub(crate) async fn send(
        &self,
        producer_id: u64,
        producer_name: String,
        sequence_id: u64,
        message: producer::ProducerMessage,
        block_if_queue_full: bool,
    ) -> Result<
        impl Future<Output = Result<proto::CommandSendReceipt, ConnectionError>>,
        ConnectionError,
    > {
        let key = RequestKey::ProducerSend {
            producer_id,
            sequence_id,
        };
        let msg = messages::send(producer_id, producer_name, sequence_id, message);
        let via = if block_if_queue_full {
            SendChannel::DataBlocking
        } else {
            SendChannel::DataTrySend
        };
        self.send_message_non_blocking(msg, key, |resp| resp.command.send_receipt, via)
            .await
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn send_ping(&self) -> Result<(), ConnectionError> {
        let (resolver, response) = oneshot::channel();
        trace!("sending ping to connection {}", self.connection_id);

        match (
            self.registrations
                .unbounded_send(Register::Ping { resolver }),
            self.control_tx.send(messages::ping()).await?,
        ) {
            (Ok(_), ()) => {
                debug!("set timeout to {:?} for ping-pong", self.operation_timeout);
                let delay_f = self.executor.delay(self.operation_timeout);
                pin_mut!(response);
                pin_mut!(delay_f);

                match select(response, delay_f).await {
                    Either::Left((res, _)) => res
                        .map_err(|oneshot::Canceled| {
                            error!("connection-sender: send ping, we have been canceled");
                            self.error.set(ConnectionError::Disconnected);
                            ConnectionError::Disconnected
                        })
                        .map(move |_| trace!("received pong from {}", self.connection_id)),
                    Either::Right(_) => {
                        error!("connection-sender: send ping, we did not received pong inside the timed out");
                        self.error.set(ConnectionError::Io(std::io::Error::new(
                            std::io::ErrorKind::TimedOut,
                            "timeout when sending ping to the Pulsar server",
                        )));
                        Err(ConnectionError::Io(std::io::Error::new(
                            std::io::ErrorKind::TimedOut,
                            "timeout when sending ping to the Pulsar server",
                        )))
                    }
                }
            }
            _ => Err(ConnectionError::Disconnected),
        }
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn lookup_topic<S: Into<String>>(
        &self,
        topic: S,
        authoritative: bool,
    ) -> Result<proto::CommandLookupTopicResponse, ConnectionError> {
        let request_id = self.request_id.get();
        let msg = messages::lookup_topic(topic.into(), authoritative, request_id);
        self.send_message(msg, RequestKey::RequestId(request_id), |resp| {
            resp.command.lookup_topic_response
        })
        .await
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn lookup_partitioned_topic<S: Into<String>>(
        &self,
        topic: S,
    ) -> Result<proto::CommandPartitionedTopicMetadataResponse, ConnectionError> {
        let request_id = self.request_id.get();
        let msg = messages::lookup_partitioned_topic(topic.into(), request_id);
        self.send_message(msg, RequestKey::RequestId(request_id), |resp| {
            resp.command.partition_metadata_response
        })
        .await
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn get_consumer_stats(
        &self,
        consumer_id: u64,
    ) -> Result<proto::CommandConsumerStatsResponse, ConnectionError> {
        let request_id = self.request_id.get();
        let msg = messages::consumer_stats(request_id, consumer_id);
        self.send_message(msg, RequestKey::RequestId(request_id), |resp| {
            resp.command.consumer_stats_response
        })
        .await
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn get_last_message_id(
        &self,
        consumer_id: u64,
    ) -> Result<proto::CommandGetLastMessageIdResponse, ConnectionError> {
        let request_id = self.request_id.get();
        let msg = messages::get_last_message_id(consumer_id, request_id);
        self.send_message(msg, RequestKey::RequestId(request_id), |resp| {
            resp.command.get_last_message_id_response
        })
        .await
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn create_producer(
        &self,
        topic: String,
        producer_id: u64,
        producer_name: Option<String>,
        options: ProducerOptions,
    ) -> Result<proto::CommandProducerSuccess, ConnectionError> {
        let request_id = self.request_id.get();
        let msg = messages::create_producer(topic, producer_name, producer_id, request_id, options);
        // if this fails, send_message below fails with Disconnected too
        let _ = self
            .registrations
            .unbounded_send(Register::Producer { producer_id });
        self.send_message(msg, RequestKey::RequestId(request_id), |resp| {
            resp.command.producer_success
        })
        .await
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn wait_for_exclusive_access(
        &self,
        request_id: u64,
    ) -> Result<proto::CommandProducerSuccess, ConnectionError> {
        self.wait_exclusive_access(RequestKey::RequestId(request_id), |resp| {
            resp.command.producer_success
        })
        .await
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn get_topics_of_namespace(
        &self,
        namespace: String,
        mode: proto::command_get_topics_of_namespace::Mode,
    ) -> Result<proto::CommandGetTopicsOfNamespaceResponse, ConnectionError> {
        let request_id = self.request_id.get();
        let msg = messages::get_topics_of_namespace(request_id, namespace, mode);
        self.send_message(msg, RequestKey::RequestId(request_id), |resp| {
            resp.command.get_topics_of_namespace_response
        })
        .await
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn close_producer(
        &self,
        producer_id: u64,
    ) -> Result<proto::CommandSuccess, ConnectionError> {
        let request_id = self.request_id.get();
        let msg = messages::close_producer(producer_id, request_id);
        self.send_message(msg, RequestKey::RequestId(request_id), |resp| {
            resp.command.success
        })
        .await
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn subscribe(
        &self,
        resolver: mpsc::UnboundedSender<Message>,
        topic: String,
        subscription: String,
        sub_type: SubType,
        consumer_id: u64,
        consumer_name: Option<String>,
        options: ConsumerOptions,
    ) -> Result<proto::CommandSuccess, ConnectionError> {
        let request_id = self.request_id.get();
        let msg = messages::subscribe(
            topic,
            subscription,
            sub_type,
            consumer_id,
            request_id,
            consumer_name,
            options,
        );
        match self.registrations.unbounded_send(Register::Consumer {
            consumer_id,
            resolver,
        }) {
            Ok(_) => {}
            Err(_) => {
                self.error.set(ConnectionError::Disconnected);
                return Err(ConnectionError::Disconnected);
            }
        }
        self.send_message(msg, RequestKey::RequestId(request_id), |resp| {
            resp.command.success
        })
        .await
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn send_flow(
        &self,
        consumer_id: u64,
        message_permits: u32,
    ) -> Result<(), ConnectionError> {
        self.control_tx
            .send(messages::flow(consumer_id, message_permits))
            .await?;
        Ok(())
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn send_ack(
        &self,
        consumer_id: u64,
        message_ids: Vec<proto::MessageIdData>,
        cumulative: bool,
    ) -> Result<(), ConnectionError> {
        self.control_tx
            .send(messages::ack(consumer_id, message_ids, cumulative))
            .await?;
        Ok(())
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn send_redeliver_unacknowleged_messages(
        &self,
        consumer_id: u64,
        message_ids: Vec<proto::MessageIdData>,
    ) -> Result<(), ConnectionError> {
        self.control_tx
            .send(messages::redeliver_unacknowleged_messages(
                consumer_id,
                message_ids,
            ))
            .await?;
        Ok(())
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn close_consumer(
        &self,
        consumer_id: u64,
    ) -> Result<proto::CommandSuccess, ConnectionError> {
        let request_id = self.request_id.get();
        let msg = messages::close_consumer(consumer_id, request_id);
        match self
            .registrations
            .unbounded_send(Register::RemoveConsumer { consumer_id })
        {
            Ok(_) => {}
            Err(_) => {
                self.error.set(ConnectionError::Disconnected);
                return Err(ConnectionError::Disconnected);
            }
        }
        self.send_message(msg, RequestKey::RequestId(request_id), |resp| {
            resp.command.success
        })
        .await
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn seek(
        &self,
        consumer_id: u64,
        message_id: Option<MessageIdData>,
        timestamp: Option<u64>,
    ) -> Result<proto::CommandSuccess, ConnectionError> {
        let request_id = self.request_id.get();
        let msg = messages::seek(consumer_id, request_id, message_id, timestamp);
        self.send_message(msg, RequestKey::RequestId(request_id), |resp| {
            resp.command.success
        })
        .await
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn unsubscribe(
        &self,
        consumer_id: u64,
    ) -> Result<proto::CommandSuccess, ConnectionError> {
        let request_id = self.request_id.get();
        let msg = messages::unsubscribe(consumer_id, request_id);
        self.send_message(msg, RequestKey::RequestId(request_id), |resp| {
            resp.command.success
        })
        .await
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn auth_challenge(
        &self,
        auth_data: Authentication,
    ) -> Result<proto::CommandSuccess, ConnectionError> {
        let msg = messages::auth_challenge(auth_data);
        self.send_message(msg, RequestKey::AuthChallenge, |resp| resp.command.success)
            .await
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    async fn send_message<R: Debug, F>(
        &self,
        msg: Message,
        key: RequestKey,
        extract: F,
    ) -> Result<R, ConnectionError>
    where
        F: FnOnce(Message) -> Option<R> + 'static,
    {
        // RPCs other than CommandSend go through the unbounded control channel so they are
        // never throttled by producer-side backpressure on the bounded data channel.
        self.send_message_non_blocking(msg, key, extract, SendChannel::Control)
            .await?
            .await
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    async fn send_message_non_blocking<R: Debug, F>(
        &self,
        msg: Message,
        key: RequestKey,
        extract: F,
        via: SendChannel,
    ) -> Result<impl Future<Output = Result<R, ConnectionError>>, ConnectionError>
    where
        F: FnOnce(Message) -> Option<R> + 'static,
    {
        let (resolver, response) = oneshot::channel();
        trace!("sending message(key = {:?}): {:?}", key, msg);

        let k = key.clone();
        let error = self.error.clone();

        self.registrations
            .unbounded_send(Register::Request {
                key: key.clone(),
                resolver,
            })
            .map_err(|e| {
                warn!(
                    "connection {} disconnected when sending the Request: {}",
                    self.connection_id, e
                );
                ConnectionError::Disconnected
            })?;
        // FORK-PATCH (3rdi): from here on, a request that ends without its
        // answer cancels its registration
        let mut pending = PendingRequest {
            response: Some(response),
            key: Some(key),
            registrations: self.registrations.clone(),
        };
        match via {
            SendChannel::Control => {
                self.control_tx.try_send(msg).map_err(|e| {
                    warn!(
                        "connection {} disconnected when sending the message: {}",
                        self.connection_id, e
                    );
                    ConnectionError::Disconnected
                })?;
            }
            SendChannel::DataBlocking => {
                self.data_tx.send(msg).await.map_err(|e| {
                    warn!(
                        "connection {} disconnected when sending the message: {}",
                        self.connection_id, e
                    );
                    ConnectionError::Disconnected
                })?;
            }
            SendChannel::DataTrySend => {
                self.data_tx.try_send(msg).map_err(|e| {
                    if e.is_full() {
                        ConnectionError::SlowDown
                    } else {
                        warn!(
                            "connection {} disconnected when sending the message: {}",
                            self.connection_id, e
                        );
                        ConnectionError::Disconnected
                    }
                })?;
            }
        };

        let connection_id = self.connection_id;
        let delay_f = self.executor.delay(self.operation_timeout);
        trace!(
            "Create timeout futures with operation timeout at {:?}",
            self.operation_timeout
        );
        let fut = async move {
            pin_mut!(delay_f);
            let answer = match pending.response.as_mut() {
                Some(response) => select(response, delay_f).await,
                None => return Err(ConnectionError::Disconnected),
            };
            match answer {
                Either::Left((Ok(message), _)) => {
                    pending.answered();
                    debug!("Received response: {:?}", message);
                    trace!("received message(key = {:?}): {:?}", k, message);
                    extract_message(message, extract)
                }
                Either::Left((Err(oneshot::Canceled), _)) => {
                    error!(
                        "response has been canceled (key = {:?}), we are disconnected",
                        k
                    );
                    error.set(ConnectionError::Disconnected);
                    Err(ConnectionError::Disconnected)
                }
                Either::Right(_) => {
                    warn!(
                        "connection {} timedout sending message to the Pulsar server",
                        connection_id
                    );
                    // FORK-PATCH (3rdi): a slow answer fails this request
                    // only. Upstream also set the connection-wide error here,
                    // retiring the connection for every user of it; as in
                    // the Java client, only a failed keepalive ping does.
                    // The request is cancelled when `pending` drops.
                    Err(ConnectionError::Io(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        format!(
                            " connection {} timedout sending message to the Pulsar server",
                            connection_id
                        ),
                    )))
                }
            }
        };

        Ok(fut)
    }

    /// wait for desired message(commandproducersuccess with ready field true)
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    async fn wait_exclusive_access<R: Debug, F>(
        &self,
        key: RequestKey,
        extract: F,
    ) -> Result<R, ConnectionError>
    where
        F: FnOnce(Message) -> Option<R>,
    {
        let (resolver, response) = oneshot::channel();

        let k = key.clone();
        let response = async {
            response
                .await
                .map_err(|oneshot::Canceled| {
                    self.error.set(ConnectionError::Disconnected);
                    ConnectionError::Disconnected
                })
                .map(move |message: Message| {
                    trace!("received message(key = {:?}): {:?}", k, message);
                    extract_message(message, extract)
                })?
        };

        match self
            .registrations
            .unbounded_send(Register::Request { key, resolver })
        {
            Ok(_) => {
                //there should be no timeout for this message
                pin_mut!(response);
                response.await
            }
            _ => Err(ConnectionError::Disconnected),
        }
    }

    pub(crate) async fn get_schema(
        &self,
        topic: &str,
        version: Option<Vec<u8>>,
    ) -> Result<proto::CommandGetSchemaResponse, ConnectionError> {
        let request_id = self.request_id.get();
        let msg = messages::get_schema(request_id, topic, version);
        self.send_message(msg, RequestKey::RequestId(request_id), |resp| {
            resp.command.get_schema_response
        })
        .await
    }
}

pub struct Connection<Exe: Executor> {
    id: Uuid,
    url: Url,
    sender: ConnectionSender<Exe>,
}

impl<Exe: Executor> Connection<Exe> {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn new(
        url: Url,
        auth_data: Option<Arc<Mutex<Box<dyn crate::authentication::Authentication>>>>,
        proxy_to_broker_url: Option<String>,
        certificate_chain: &[Certificate],
        allow_insecure_connection: bool,
        tls_hostname_verification_enabled: bool,
        connection_timeout: Duration,
        operation_timeout: Duration,
        outbound_channel_size: usize,
        executor: Arc<Exe>,
    ) -> Result<Connection<Exe>, ConnectionError> {
        if url.scheme() != "pulsar" && url.scheme() != "pulsar+ssl" {
            error!("invalid scheme: {}", url.scheme());
            return Err(ConnectionError::NotFound);
        }
        let hostname = url.host().map(|s| s.to_string());

        let tls = match url.scheme() {
            "pulsar" => false,
            "pulsar+ssl" => true,
            s => {
                error!("invalid scheme: {}", s);
                return Err(ConnectionError::NotFound);
            }
        };

        let u = url.clone();
        let addresses: Vec<SocketAddr> = match executor
            .spawn_blocking(move || {
                u.socket_addrs(|| match u.scheme() {
                    "pulsar" => Some(6650),
                    "pulsar+ssl" => Some(6651),
                    _ => None,
                })
                .map_err(|e| {
                    error!("could not look up address: {:?}", e);
                    e
                })
                .ok()
                .map(|mut v| {
                    v.shuffle(&mut thread_rng());
                    v
                })
            })
            .await
        {
            Some(Some(addresses)) if !addresses.is_empty() => addresses,
            _ => return Err(ConnectionError::NotFound),
        };

        let id = Uuid::new_v4();
        let mut errors = vec![];
        for address in addresses {
            let hostname = hostname.clone().unwrap_or_else(|| address.ip().to_string());
            debug!("Connecting to {}: {}, as {}", url, address, id);
            let sender_prepare = Connection::prepare_stream(
                id,
                address,
                hostname,
                tls,
                auth_data.clone(),
                proxy_to_broker_url.clone(),
                certificate_chain,
                allow_insecure_connection,
                tls_hostname_verification_enabled,
                executor.clone(),
                operation_timeout,
                outbound_channel_size,
            );
            let delay_f = executor.delay(connection_timeout);

            pin_mut!(sender_prepare);
            pin_mut!(delay_f);

            let sender = match select(sender_prepare, delay_f).await {
                Either::Left((Ok(res), _)) => res,
                Either::Left((Err(err), _)) => {
                    errors.push(err);
                    continue;
                }
                Either::Right(_) => {
                    errors.push(ConnectionError::Io(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "timeout connecting to the Pulsar server",
                    )));
                    continue;
                }
            };

            return Ok(Connection { id, url, sender });
        }

        let mut fatal_errors = vec![];
        let mut retryable_errors = vec![];
        for e in errors.into_iter() {
            if e.establish_retryable() {
                retryable_errors.push(e);
            } else {
                fatal_errors.push(e);
            }
        }

        if retryable_errors.is_empty() {
            error!("connection error, not retryable: {:?}", fatal_errors);
            Err(ConnectionError::Io(std::io::Error::other(
                "fatal error when connecting to the Pulsar server",
            )))
        } else {
            warn!("retry establish connection on: {:?}", retryable_errors);
            Err(retryable_errors.into_iter().next().unwrap())
        }
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    async fn prepare_auth_data(
        auth: Option<Arc<Mutex<Box<dyn crate::authentication::Authentication>>>>,
    ) -> Result<Option<Authentication>, ConnectionError> {
        match auth {
            Some(m_auth) => {
                let mut auth_guard = m_auth.lock().await;
                let name = auth_guard.auth_method_name();
                // wrap the future of auth_data() with Shared so that it implements Sync
                let data_fut = auth_guard.auth_data().shared();
                let data = data_fut.await?;
                Ok(Some(Authentication { name, data }))
            }
            None => Ok(None),
        }
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    #[allow(unused_variables)] // allow_insecure_connection and tls_hostname_verification_enabled are native-tls only
    async fn prepare_stream(
        connection_id: Uuid,
        address: SocketAddr,
        hostname: String,
        tls: bool,
        auth: Option<Arc<Mutex<Box<dyn crate::authentication::Authentication>>>>,
        proxy_to_broker_url: Option<String>,
        certificate_chain: &[Certificate],
        allow_insecure_connection: bool,
        tls_hostname_verification_enabled: bool,
        executor: Arc<Exe>,
        operation_timeout: Duration,
        outbound_channel_size: usize,
    ) -> Result<ConnectionSender<Exe>, ConnectionError> {
        match executor.kind() {
            #[cfg(feature = "tokio-runtime")]
            ExecutorKind::Tokio => {
                if tls {
                    let stream = tokio::net::TcpStream::connect(&address).await?;

                    let mut builder = native_tls::TlsConnector::builder();
                    for certificate in certificate_chain {
                        builder.add_root_certificate(certificate.clone());
                    }
                    builder.danger_accept_invalid_hostnames(
                        allow_insecure_connection || !tls_hostname_verification_enabled,
                    );
                    builder.danger_accept_invalid_certs(allow_insecure_connection);
                    let cx = builder.build()?;
                    let cx = tokio_native_tls::TlsConnector::from(cx);
                    let stream = cx
                        .connect(&hostname, stream)
                        .await
                        .map(|stream| tokio_util::codec::Framed::new(stream, Codec))?;

                    Connection::connect(
                        connection_id,
                        stream,
                        auth,
                        proxy_to_broker_url,
                        executor,
                        operation_timeout,
                        outbound_channel_size,
                    )
                    .await
                } else {
                    let stream = tokio::net::TcpStream::connect(&address)
                        .await
                        .map(|stream| tokio_util::codec::Framed::new(stream, Codec))?;

                    Connection::connect(
                        connection_id,
                        stream,
                        auth,
                        proxy_to_broker_url,
                        executor,
                        operation_timeout,
                        outbound_channel_size,
                    )
                    .await
                }
            }
            #[cfg(all(
                any(
                    feature = "tokio-rustls-runtime-aws-lc-rs",
                    feature = "tokio-rustls-runtime-ring"
                ),
                not(feature = "tokio-runtime")
            ))]
            ExecutorKind::Tokio => {
                if tls {
                    let stream = tokio::net::TcpStream::connect(&address).await?;
                    let mut root_store = rustls::RootCertStore::empty();

                    root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
                    for certificate in certificate_chain {
                        root_store.add(certificate.clone())?;
                    }

                    let config = rustls::ClientConfig::builder()
                        .with_root_certificates(root_store)
                        .with_no_client_auth();

                    let cx = tokio_rustls::TlsConnector::from(Arc::new(config));
                    let stream = cx
                        .connect(
                            rustls::pki_types::ServerName::try_from(hostname.as_str())?.to_owned(),
                            stream,
                        )
                        .await
                        .map(|stream| tokio_util::codec::Framed::new(stream, Codec))?;

                    Connection::connect(
                        connection_id,
                        stream,
                        auth,
                        proxy_to_broker_url,
                        executor,
                        operation_timeout,
                        outbound_channel_size,
                    )
                    .await
                } else {
                    let stream = tokio::net::TcpStream::connect(&address)
                        .await
                        .map(|stream| tokio_util::codec::Framed::new(stream, Codec))?;

                    Connection::connect(
                        connection_id,
                        stream,
                        auth,
                        proxy_to_broker_url,
                        executor,
                        operation_timeout,
                        outbound_channel_size,
                    )
                    .await
                }
            }
            #[cfg(all(
                not(feature = "tokio-runtime"),
                not(feature = "tokio-rustls-runtime-aws-lc-rs"),
                not(feature = "tokio-rustls-runtime-ring")
            ))]
            ExecutorKind::Tokio => {
                unimplemented!("the tokio-runtime cargo feature is not active");
            }
            #[cfg(feature = "async-std-runtime")]
            ExecutorKind::AsyncStd => {
                if tls {
                    let stream = async_std::net::TcpStream::connect(&address).await?;
                    let mut connector = async_native_tls::TlsConnector::new();
                    for certificate in certificate_chain {
                        connector = connector.add_root_certificate(certificate.clone());
                    }
                    connector = connector.danger_accept_invalid_hostnames(
                        allow_insecure_connection || !tls_hostname_verification_enabled,
                    );
                    connector = connector.danger_accept_invalid_certs(allow_insecure_connection);
                    let stream = connector
                        .connect(&hostname, stream)
                        .await
                        .map(|stream| asynchronous_codec::Framed::new(stream, Codec))?;

                    Connection::connect(
                        connection_id,
                        stream,
                        auth,
                        proxy_to_broker_url,
                        executor,
                        operation_timeout,
                        outbound_channel_size,
                    )
                    .await
                } else {
                    let stream = async_std::net::TcpStream::connect(&address)
                        .await
                        .map(|stream| asynchronous_codec::Framed::new(stream, Codec))?;

                    Connection::connect(
                        connection_id,
                        stream,
                        auth,
                        proxy_to_broker_url,
                        executor,
                        operation_timeout,
                        outbound_channel_size,
                    )
                    .await
                }
            }
            #[cfg(all(
                any(
                    feature = "async-std-rustls-runtime-aws-lc-rs",
                    feature = "async-std-rustls-runtime-ring"
                ),
                not(feature = "async-std-runtime")
            ))]
            #[allow(deprecated)]
            ExecutorKind::AsyncStd => {
                if tls {
                    let stream = async_std::net::TcpStream::connect(&address).await?;
                    let mut root_store = rustls::RootCertStore::empty();

                    root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
                    for certificate in certificate_chain {
                        root_store.add(certificate.clone())?;
                    }

                    let config = rustls::ClientConfig::builder()
                        .with_root_certificates(root_store)
                        .with_no_client_auth();

                    let connector = futures_rustls::TlsConnector::from(Arc::new(config));
                    let stream = connector
                        .connect(
                            rustls::pki_types::ServerName::try_from(hostname.as_str())?.to_owned(),
                            stream,
                        )
                        .await
                        .map(|stream| asynchronous_codec::Framed::new(stream, Codec))?;

                    Connection::connect(
                        connection_id,
                        stream,
                        auth,
                        proxy_to_broker_url,
                        executor,
                        operation_timeout,
                        outbound_channel_size,
                    )
                    .await
                } else {
                    let stream = async_std::net::TcpStream::connect(&address)
                        .await
                        .map(|stream| asynchronous_codec::Framed::new(stream, Codec))?;

                    Connection::connect(
                        connection_id,
                        stream,
                        auth,
                        proxy_to_broker_url,
                        executor,
                        operation_timeout,
                        outbound_channel_size,
                    )
                    .await
                }
            }
            #[cfg(all(
                not(feature = "async-std-runtime"),
                not(feature = "async-std-rustls-runtime-aws-lc-rs"),
                not(feature = "async-std-rustls-runtime-ring")
            ))]
            ExecutorKind::AsyncStd => {
                unimplemented!("the async-std-runtime cargo feature is not active");
            }
        }
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn connect<S>(
        connection_id: Uuid,
        mut stream: S,
        auth: Option<Arc<Mutex<Box<dyn crate::authentication::Authentication>>>>,
        proxy_to_broker_url: Option<String>,
        executor: Arc<Exe>,
        operation_timeout: Duration,
        outbound_channel_size: usize,
    ) -> Result<ConnectionSender<Exe>, ConnectionError>
    where
        S: Stream<Item = Result<Message, ConnectionError>>,
        S: Sink<Message, Error = ConnectionError>,
        S: Send + std::marker::Unpin + 'static,
    {
        let auth_data = Self::prepare_auth_data(auth.clone()).await?;
        stream
            .send({
                let msg = messages::connect(auth_data, proxy_to_broker_url);
                trace!("connection message: {:?}", msg);
                msg
            })
            .await?;

        let msg = stream.next().await;
        match msg {
            Some(Ok(Message {
                command:
                    proto::BaseCommand {
                        error: Some(error), ..
                    },
                ..
            })) => Err(ConnectionError::PulsarError(
                crate::error::server_error(error.error),
                Some(error.message),
            )),
            Some(Ok(msg)) => {
                trace!("received connection response: {:?}", msg);
                let Some(c) = msg.command.connected else {
                    return Err(ConnectionError::Unexpected(format!(
                        "Unexpected message from pulsar: {:?}",
                        crate::message::redacted(&msg.command)
                    )));
                };

                Ok(c)
            }
            Some(Err(e)) => Err(e),
            None => Err(ConnectionError::Disconnected),
        }?;

        let (mut sink, stream) = stream.split();
        // Data plane: bounded so that producer Send commands experience natural
        // backpressure when the broker can't keep up.
        let (data_tx, data_rx) = async_channel::bounded(outbound_channel_size);
        // Control plane: unbounded so that Ping/Pong/Ack/Flow/AuthChallenge/etc.
        // can never be throttled by producer queue depth.
        let (control_tx, control_rx) = async_channel::unbounded();
        let (registrations_tx, registrations_rx) = mpsc::unbounded();
        let error = SharedError::new();
        let (receiver_shutdown_tx, receiver_shutdown_rx) = oneshot::channel();
        let (auth_challenge_tx, mut auth_challenge_rx) = mpsc::unbounded();
        let closed_producers = ClosedProducers::default();

        if executor
            .spawn(Box::pin(
                Receiver::new(
                    stream,
                    control_tx.clone(),
                    error.clone(),
                    registrations_rx,
                    receiver_shutdown_rx,
                    auth_challenge_tx,
                    closed_producers.clone(),
                )
                .map(|_| ()),
            ))
            .is_err()
        {
            error!("the executor could not spawn the Receiver future");
            return Err(ConnectionError::Shutdown);
        }

        let err = error.clone();
        let res = executor.spawn(Box::pin(async move {
            loop {
                // Drain the control plane ahead of the data plane so that broker
                // keepalive pongs and other RPCs are flushed promptly even when
                // producer fan-in has saturated the data channel.
                let msg = futures::select_biased! {
                    msg = control_rx.recv().fuse() => match msg {
                        Ok(msg) => msg,
                        Err(_) => break,
                    },
                    msg = data_rx.recv().fuse() => match msg {
                        Ok(msg) => msg,
                        Err(_) => break,
                    },
                };
                if let Err(e) = sink.send(msg).await {
                    err.set(e);
                    break;
                }
            }
        }));
        if res.is_err() {
            error!("the executor could not spawn the Receiver future");
            return Err(ConnectionError::Shutdown);
        }

        if auth.is_some() {
            let auth_challenge_res = executor.spawn({
                let err = error.clone();
                let control_tx = control_tx.clone();
                let auth = auth.clone();
                Box::pin(async move {
                    while auth_challenge_rx.next().await.is_some() {
                        match Self::prepare_auth_data(auth.clone()).await {
                            Ok(Some(auth_data)) => {
                                let _ = control_tx.send(messages::auth_challenge(auth_data)).await;
                            }
                            Ok(None) => (),
                            Err(e) => {
                                err.set(e);
                                break;
                            }
                        }
                    }
                })
            });
            if auth_challenge_res.is_err() {
                error!("the executor could not spawn the auth_challenge future");
                return Err(ConnectionError::Shutdown);
            }
        }

        let sender = ConnectionSender::new(
            connection_id,
            data_tx,
            control_tx,
            registrations_tx,
            receiver_shutdown_tx,
            SerialId::new(),
            error,
            executor.clone(),
            operation_timeout,
            closed_producers,
        );

        Ok(sender)
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn id(&self) -> Uuid {
        self.id
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn error(&self) -> Option<ConnectionError> {
        self.sender.error.remove()
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn is_valid(&self) -> bool {
        !self.sender.error.is_set()
    }

    /// Marks the connection failed, as a dropped socket does: its users
    /// reconnect the next time they check it.
    #[cfg(test)]
    pub(crate) fn fail_for_test(&self) {
        self.sender.error.set(ConnectionError::Disconnected);
    }

    /// whether the broker closed this producer on this connection
    pub fn is_producer_closed(&self, producer_id: u64) -> bool {
        self.sender
            .closed_producers
            .lock()
            .unwrap()
            .contains(&producer_id)
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn url(&self) -> &Url {
        &self.url
    }

    /// Chain to send a message, e.g. conn.sender().send_ping()
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn sender(&self) -> &ConnectionSender<Exe> {
        &self.sender
    }
}

impl<Exe: Executor> Drop for Connection<Exe> {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn drop(&mut self) {
        debug!("dropping connection {} for {}", self.id, self.url);
        if let Some(shutdown) = self.sender.receiver_shutdown.take() {
            let _ = shutdown.send(());
        }
    }
}

#[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
pub(crate) fn extract_message<T: Debug, F>(
    message: Message,
    extract: F,
) -> Result<T, ConnectionError>
where
    F: FnOnce(Message) -> Option<T>,
{
    if let Some(e) = message.command.error {
        Err(ConnectionError::PulsarError(
            crate::error::server_error(e.error),
            Some(e.message),
        ))
    } else {
        let cmd = message.command.clone();
        if let Some(extracted) = extract(message) {
            trace!("extracted message: {:?}", extracted);
            Ok(extracted)
        } else {
            Err(ConnectionError::UnexpectedResponse(format!(
                "{:?}",
                crate::message::redacted(&cmd)
            )))
        }
    }
}

pub(crate) mod messages {
    use chrono::Utc;
    use proto::MessageIdData;

    use crate::{
        connection::Authentication,
        consumer::ConsumerOptions,
        message::{
            proto::{self, base_command::Type as CommandType, command_subscribe::SubType},
            Message, Payload,
        },
        producer::{self, ProducerOptions},
    };

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn connect(auth: Option<Authentication>, proxy_to_broker_url: Option<String>) -> Message {
        let (auth_method_name, auth_data) = match auth {
            Some(auth) => (Some(auth.name), Some(auth.data)),
            None => (None, None),
        };

        Message {
            command: proto::BaseCommand {
                r#type: CommandType::Connect as i32,
                connect: Some(proto::CommandConnect {
                    auth_method_name,
                    auth_data,
                    proxy_to_broker_url,
                    client_version: proto::client_version(),
                    protocol_version: Some(12),
                    ..Default::default()
                }),
                ..Default::default()
            },
            payload: None,
        }
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn ping() -> Message {
        Message {
            command: proto::BaseCommand {
                r#type: CommandType::Ping as i32,
                ping: Some(proto::CommandPing {}),
                ..Default::default()
            },
            payload: None,
        }
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn pong() -> Message {
        Message {
            command: proto::BaseCommand {
                r#type: CommandType::Pong as i32,
                pong: Some(proto::CommandPong {}),
                ..Default::default()
            },
            payload: None,
        }
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn create_producer(
        topic: String,
        producer_name: Option<String>,
        producer_id: u64,
        request_id: u64,
        options: ProducerOptions,
    ) -> Message {
        Message {
            command: proto::BaseCommand {
                r#type: CommandType::Producer as i32,
                producer: Some(proto::CommandProducer {
                    topic,
                    producer_id,
                    request_id,
                    producer_name,
                    encrypted: options.encrypted,
                    metadata: options
                        .metadata
                        .iter()
                        .map(|(k, v)| proto::KeyValue {
                            key: k.clone(),
                            value: v.clone(),
                        })
                        .collect(),
                    schema: options.schema,
                    producer_access_mode: options.access_mode,
                    ..Default::default()
                }),
                ..Default::default()
            },
            payload: None,
        }
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn get_topics_of_namespace(
        request_id: u64,
        namespace: String,
        mode: proto::command_get_topics_of_namespace::Mode,
    ) -> Message {
        Message {
            command: proto::BaseCommand {
                r#type: CommandType::GetTopicsOfNamespace as i32,
                get_topics_of_namespace: Some(proto::CommandGetTopicsOfNamespace {
                    request_id,
                    namespace,
                    mode: Some(mode as i32),
                    ..Default::default()
                }),
                ..Default::default()
            },
            payload: None,
        }
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn get_schema(request_id: u64, topic: &str, version: Option<Vec<u8>>) -> Message {
        Message {
            command: proto::BaseCommand {
                r#type: CommandType::GetSchema as i32,
                get_schema: Some(proto::CommandGetSchema {
                    request_id,
                    topic: topic.to_string(),
                    schema_version: version,
                }),
                ..Default::default()
            },
            payload: None,
        }
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub(crate) fn send(
        producer_id: u64,
        producer_name: String,
        sequence_id: u64,
        message: producer::ProducerMessage,
    ) -> Message {
        let properties = message
            .properties
            .into_iter()
            .map(|(key, value)| proto::KeyValue { key, value })
            .collect();

        Message {
            command: proto::BaseCommand {
                r#type: CommandType::Send as i32,
                send: Some(proto::CommandSend {
                    producer_id,
                    sequence_id,
                    num_messages: message.num_messages_in_batch,
                    ..Default::default()
                }),
                ..Default::default()
            },
            payload: Some(Payload {
                metadata: proto::MessageMetadata {
                    producer_name,
                    sequence_id,
                    properties,
                    publish_time: Utc::now().timestamp_millis() as u64,
                    replicated_from: None,
                    partition_key: message.partition_key,
                    partition_key_b64_encoded: message.partition_key_b64_encoded,
                    ordering_key: message.ordering_key,
                    replicate_to: message.replicate_to,
                    compression: message.compression,
                    uncompressed_size: message.uncompressed_size,
                    num_messages_in_batch: message.num_messages_in_batch,
                    event_time: message.event_time,
                    encryption_keys: message.encryption_keys,
                    encryption_algo: message.encryption_algo,
                    encryption_param: message.encryption_param,
                    schema_version: message.schema_version,
                    deliver_at_time: message.deliver_at_time,
                    ..Default::default()
                },
                data: message.payload,
            }),
        }
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn lookup_topic(topic: String, authoritative: bool, request_id: u64) -> Message {
        Message {
            command: proto::BaseCommand {
                r#type: CommandType::Lookup as i32,
                lookup_topic: Some(proto::CommandLookupTopic {
                    topic,
                    request_id,
                    authoritative: Some(authoritative),
                    ..Default::default()
                }),
                ..Default::default()
            },
            payload: None,
        }
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn lookup_partitioned_topic(topic: String, request_id: u64) -> Message {
        Message {
            command: proto::BaseCommand {
                r#type: CommandType::PartitionedMetadata as i32,
                partition_metadata: Some(proto::CommandPartitionedTopicMetadata {
                    topic,
                    request_id,
                    ..Default::default()
                }),
                ..Default::default()
            },
            payload: None,
        }
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn consumer_stats(request_id: u64, consumer_id: u64) -> Message {
        Message {
            command: proto::BaseCommand {
                r#type: CommandType::ConsumerStats as i32,
                consumer_stats: Some(proto::CommandConsumerStats {
                    request_id,
                    consumer_id,
                }),
                ..Default::default()
            },
            payload: None,
        }
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn get_last_message_id(consumer_id: u64, request_id: u64) -> Message {
        Message {
            command: proto::BaseCommand {
                r#type: CommandType::GetLastMessageId as i32,
                get_last_message_id: Some(proto::CommandGetLastMessageId {
                    consumer_id,
                    request_id,
                }),
                ..Default::default()
            },
            payload: None,
        }
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn close_producer(producer_id: u64, request_id: u64) -> Message {
        Message {
            command: proto::BaseCommand {
                r#type: CommandType::CloseProducer as i32,
                close_producer: Some(proto::CommandCloseProducer {
                    producer_id,
                    request_id,
                }),
                ..Default::default()
            },
            payload: None,
        }
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn subscribe(
        topic: String,
        subscription: String,
        sub_type: SubType,
        consumer_id: u64,
        request_id: u64,
        consumer_name: Option<String>,
        options: ConsumerOptions,
    ) -> Message {
        Message {
            command: proto::BaseCommand {
                r#type: CommandType::Subscribe as i32,
                subscribe: Some(proto::CommandSubscribe {
                    topic,
                    subscription,
                    sub_type: sub_type as i32,
                    consumer_id,
                    request_id,
                    consumer_name,
                    priority_level: options.priority_level,
                    durable: options.durable,
                    metadata: options
                        .metadata
                        .iter()
                        .map(|(k, v)| proto::KeyValue {
                            key: k.clone(),
                            value: v.clone(),
                        })
                        .collect(),
                    read_compacted: Some(options.read_compacted.unwrap_or(false)),
                    initial_position: Some(options.initial_position.into()),
                    schema: options.schema,
                    start_message_id: options.start_message_id,
                    ..Default::default()
                }),
                ..Default::default()
            },
            payload: None,
        }
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn flow(consumer_id: u64, message_permits: u32) -> Message {
        Message {
            command: proto::BaseCommand {
                r#type: CommandType::Flow as i32,
                flow: Some(proto::CommandFlow {
                    consumer_id,
                    message_permits,
                }),
                ..Default::default()
            },
            payload: None,
        }
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn ack(
        consumer_id: u64,
        message_id: Vec<proto::MessageIdData>,
        cumulative: bool,
    ) -> Message {
        Message {
            command: proto::BaseCommand {
                r#type: CommandType::Ack as i32,
                ack: Some(proto::CommandAck {
                    consumer_id,
                    ack_type: if cumulative {
                        proto::command_ack::AckType::Cumulative as i32
                    } else {
                        proto::command_ack::AckType::Individual as i32
                    },
                    message_id,
                    validation_error: None,
                    properties: Vec::new(),
                    ..Default::default()
                }),
                ..Default::default()
            },
            payload: None,
        }
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn redeliver_unacknowleged_messages(
        consumer_id: u64,
        message_ids: Vec<proto::MessageIdData>,
    ) -> Message {
        Message {
            command: proto::BaseCommand {
                r#type: CommandType::RedeliverUnacknowledgedMessages as i32,
                redeliver_unacknowledged_messages: Some(
                    proto::CommandRedeliverUnacknowledgedMessages {
                        consumer_id,
                        message_ids,
                        ..Default::default()
                    },
                ),
                ..Default::default()
            },
            payload: None,
        }
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn close_consumer(consumer_id: u64, request_id: u64) -> Message {
        Message {
            command: proto::BaseCommand {
                r#type: CommandType::CloseConsumer as i32,
                close_consumer: Some(proto::CommandCloseConsumer {
                    consumer_id,
                    request_id,
                }),
                ..Default::default()
            },
            payload: None,
        }
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn seek(
        consumer_id: u64,
        request_id: u64,
        message_id: Option<MessageIdData>,
        message_publish_time: Option<u64>,
    ) -> Message {
        Message {
            command: proto::BaseCommand {
                r#type: CommandType::Seek as i32,
                seek: Some(proto::CommandSeek {
                    consumer_id,
                    request_id,
                    message_id,
                    message_publish_time,
                }),
                ..Default::default()
            },
            payload: None,
        }
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn unsubscribe(consumer_id: u64, request_id: u64) -> Message {
        Message {
            command: proto::BaseCommand {
                r#type: CommandType::Unsubscribe as i32,
                unsubscribe: Some(proto::CommandUnsubscribe {
                    consumer_id,
                    request_id,
                }),
                ..Default::default()
            },
            payload: None,
        }
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn auth_challenge(auth: Authentication) -> Message {
        Message {
            command: proto::BaseCommand {
                r#type: CommandType::AuthResponse as i32,
                auth_response: Some(proto::CommandAuthResponse {
                    response: Some(proto::AuthData {
                        auth_method_name: Some(auth.name),
                        auth_data: Some(auth.data),
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            },
            payload: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use async_trait::async_trait;
    use futures::{
        channel::{mpsc, oneshot},
        lock::Mutex,
        stream::StreamExt,
        SinkExt,
    };
    use tokio::{net::TcpListener, sync::RwLock};
    use uuid::Uuid;

    use super::{Connection, Receiver, Register, RequestKey};
    #[cfg(any(
        feature = "tokio-runtime",
        feature = "tokio-rustls-runtime-aws-lc-rs",
        feature = "tokio-rustls-runtime-ring"
    ))]
    use crate::TokioExecutor;
    use crate::{
        authentication::Authentication,
        error::{AuthenticationError, SharedError},
        message::{BaseCommand, Codec, Message},
        proto::{AuthData, CommandAuthChallenge, CommandAuthResponse, CommandConnected},
    };

    #[tokio::test]
    #[cfg(any(
        feature = "tokio-runtime",
        feature = "tokio-rustls-runtime-aws-lc-rs",
        feature = "tokio-rustls-runtime-ring"
    ))]
    async fn receiver_auth_challenge_test() {
        let (message_tx, message_rx) = mpsc::unbounded();
        let (control_tx, _control_rx) = async_channel::unbounded();
        let (_registrations_tx, registrations_rx) = mpsc::unbounded();
        let error = SharedError::new();
        let (_receiver_shutdown_tx, receiver_shutdown_rx) = oneshot::channel();
        let (auth_challenge_tx, mut auth_challenge_rx) = mpsc::unbounded();

        let _ = message_tx.unbounded_send(Ok(Message {
            command: BaseCommand {
                r#type: 36,
                auth_challenge: Some(CommandAuthChallenge {
                    ..Default::default()
                }),
                ..Default::default()
            },
            payload: None,
        }));

        tokio::spawn(Box::pin(Receiver::new(
            message_rx,
            control_tx,
            error.clone(),
            registrations_rx,
            receiver_shutdown_rx,
            auth_challenge_tx,
            Default::default(),
        )));

        if error.is_set() {
            panic!("{:?}", error.remove())
        }

        match tokio::time::timeout(Duration::from_secs(1), auth_challenge_rx.next()).await {
            Ok(auth) => assert!(auth.is_some()),
            _ => panic!("operation timeout"),
        };
    }

    #[tokio::test]
    #[cfg(any(
        feature = "tokio-runtime",
        feature = "tokio-rustls-runtime-aws-lc-rs",
        feature = "tokio-rustls-runtime-ring"
    ))]
    async fn receiver_routes_ping_to_control_channel() {
        let (message_tx, message_rx) = mpsc::unbounded();
        let (control_tx, control_rx) = async_channel::unbounded();
        let (_registrations_tx, registrations_rx) = mpsc::unbounded();
        let error = SharedError::new();
        let (_receiver_shutdown_tx, receiver_shutdown_rx) = oneshot::channel();
        let (auth_challenge_tx, _auth_challenge_rx) = mpsc::unbounded();

        message_tx
            .unbounded_send(Ok(super::messages::ping()))
            .unwrap();

        tokio::spawn(Box::pin(Receiver::new(
            message_rx,
            control_tx,
            error.clone(),
            registrations_rx,
            receiver_shutdown_rx,
            auth_challenge_tx,
            Default::default(),
        )));

        let pong = tokio::time::timeout(Duration::from_secs(1), control_rx.recv())
            .await
            .expect("timed out waiting for pong on control channel")
            .expect("control channel closed unexpectedly");

        assert!(
            pong.command.pong.is_some(),
            "expected pong response on control channel, got {:?}",
            pong.command
        );
        assert!(!error.is_set(), "receiver should not set error on ping");
    }

    #[tokio::test]
    #[cfg(any(
        feature = "tokio-runtime",
        feature = "tokio-rustls-runtime-aws-lc-rs",
        feature = "tokio-rustls-runtime-ring"
    ))]
    async fn receiver_fails_sends_of_producer_closed_by_broker() {
        let (message_tx, message_rx) = mpsc::unbounded();
        let (control_tx, _control_rx) = async_channel::unbounded();
        let (registrations_tx, registrations_rx) = mpsc::unbounded();
        let error = SharedError::new();
        let (_receiver_shutdown_tx, receiver_shutdown_rx) = oneshot::channel();
        let (auth_challenge_tx, _auth_challenge_rx) = mpsc::unbounded();
        let closed_producers = super::ClosedProducers::default();

        tokio::spawn(Box::pin(Receiver::new(
            message_rx,
            control_tx,
            error.clone(),
            registrations_rx,
            receiver_shutdown_rx,
            auth_challenge_tx,
            closed_producers.clone(),
        )));

        let send = |producer_id, sequence_id| {
            let (resolver, response) = oneshot::channel();
            let key = RequestKey::ProducerSend {
                producer_id,
                sequence_id,
            };
            registrations_tx
                .unbounded_send(Register::Request { key, resolver })
                .unwrap();
            response
        };
        let wait = Duration::from_millis(200);
        let pending = send(1, 0);
        let other_producer = send(2, 0);

        // the broker sends request_id -1 when it closes a producer
        message_tx
            .unbounded_send(Ok(super::messages::close_producer(1, u64::MAX)))
            .unwrap();

        let msg = tokio::time::timeout(wait, pending).await.unwrap().unwrap();
        assert!(msg.command.close_producer.is_some());
        assert!(closed_producers.lock().unwrap().contains(&1));
        // a send registered after the close does not wait for a receipt either
        tokio::time::timeout(wait, send(1, 1))
            .await
            .unwrap()
            .unwrap();
        assert!(tokio::time::timeout(wait, other_producer).await.is_err());

        // re-creating the producer forgets the close
        registrations_tx
            .unbounded_send(Register::Producer { producer_id: 1 })
            .unwrap();
        assert!(tokio::time::timeout(wait, send(1, 2)).await.is_err());
        assert!(!closed_producers.lock().unwrap().contains(&1));
        assert!(!error.is_set());
    }

    struct TestAuthentication {
        count: Arc<RwLock<usize>>,
    }

    #[async_trait]
    impl Authentication for TestAuthentication {
        fn auth_method_name(&self) -> String {
            "test_auth".to_string()
        }
        async fn initialize(&mut self) -> Result<(), AuthenticationError> {
            Ok(())
        }
        async fn auth_data(&mut self) -> Result<Vec<u8>, AuthenticationError> {
            *self.count.write().await += 1;
            Ok("test_auth_data".as_bytes().to_vec())
        }
    }

    #[tokio::test]
    #[cfg(any(
        feature = "tokio-runtime",
        feature = "tokio-rustls-runtime-aws-lc-rs",
        feature = "tokio-rustls-runtime-ring"
    ))]
    async fn connection_auth_challenge_test() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();

        let stream = tokio::net::TcpStream::connect(listener.local_addr().unwrap())
            .await
            .map(|stream| tokio_util::codec::Framed::new(stream, Codec))
            .unwrap();

        let mut server_stream = listener
            .accept()
            .await
            .map(|(stream, _)| tokio_util::codec::Framed::new(stream, Codec))
            .unwrap();

        let auth_count = Arc::new(RwLock::new(0));

        server_stream
            .send(Message {
                command: BaseCommand {
                    connected: Some(CommandConnected {
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                payload: None,
            })
            .await
            .unwrap();

        server_stream
            .send(Message {
                command: BaseCommand {
                    r#type: 36,
                    auth_challenge: Some(CommandAuthChallenge {
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                payload: None,
            })
            .await
            .unwrap();

        let connection = Connection::connect(
            Uuid::new_v4(),
            stream,
            Some(Arc::new(Mutex::new(Box::new(TestAuthentication {
                count: auth_count.clone(),
            })))),
            None,
            TokioExecutor.into(),
            Duration::from_secs(10),
            100,
        )
        .await;

        let _ = server_stream.next().await;
        let auth_challenge = server_stream.next().await.unwrap();

        assert!(connection.is_ok());
        assert_eq!(*auth_count.read().await, 2);
        match auth_challenge {
            Ok(Message {
                command:
                    BaseCommand {
                        auth_response:
                            Some(CommandAuthResponse {
                                response:
                                    Some(AuthData {
                                        auth_method_name,
                                        auth_data,
                                    }),
                                ..
                            }),
                        ..
                    },
                ..
            }) => {
                assert_eq!(auth_method_name.unwrap(), "test_auth");
                assert_eq!(
                    String::from_utf8(auth_data.unwrap()).unwrap(),
                    "test_auth_data"
                );
            }
            _ => panic!("Unexpected message"),
        };
    }

    /// A receiver fed by the returned senders, polled by hand.
    #[allow(clippy::type_complexity)]
    fn hand_polled_receiver() -> (
        Receiver<mpsc::UnboundedReceiver<Result<Message, crate::error::ConnectionError>>>,
        mpsc::UnboundedSender<Result<Message, crate::error::ConnectionError>>,
        mpsc::UnboundedSender<Register>,
        (
            oneshot::Sender<()>,
            async_channel::Receiver<Message>,
            mpsc::UnboundedReceiver<()>,
        ),
    ) {
        let (message_tx, message_rx) = mpsc::unbounded();
        let (control_tx, control_rx) = async_channel::unbounded();
        let (registrations_tx, registrations_rx) = mpsc::unbounded();
        let (receiver_shutdown_tx, receiver_shutdown_rx) = oneshot::channel();
        let (auth_challenge_tx, auth_challenge_rx) = mpsc::unbounded();
        let receiver = Receiver::new(
            message_rx,
            control_tx,
            SharedError::new(),
            registrations_rx,
            receiver_shutdown_rx,
            auth_challenge_tx,
            Default::default(),
        );
        (
            receiver,
            message_tx,
            registrations_tx,
            (receiver_shutdown_tx, control_rx, auth_challenge_rx),
        )
    }

    fn lookup_reply(request_id: u64) -> Message {
        Message {
            command: BaseCommand {
                r#type: 24,
                lookup_topic_response: Some(crate::proto::CommandLookupTopicResponse {
                    request_id,
                    ..Default::default()
                }),
                ..Default::default()
            },
            payload: None,
        }
    }

    fn send_receipt(producer_id: u64, sequence_id: u64) -> Message {
        Message {
            command: BaseCommand {
                r#type: 7,
                send_receipt: Some(crate::proto::CommandSendReceipt {
                    producer_id,
                    sequence_id,
                    ..Default::default()
                }),
                ..Default::default()
            },
            payload: None,
        }
    }

    fn register(
        registrations_tx: &mpsc::UnboundedSender<Register>,
        key: RequestKey,
    ) -> oneshot::Receiver<Message> {
        let (resolver, response) = oneshot::channel();
        registrations_tx
            .unbounded_send(Register::Request { key, resolver })
            .unwrap();
        response
    }

    /// An inbound stream that, on its first poll, registers `key` (as a
    /// request on another thread does, after the receiver's first drain)
    /// and yields `reply`; then it reads `rest`.
    fn racing_inbound(
        registrations_tx: mpsc::UnboundedSender<Register>,
        key: RequestKey,
        reply: Message,
        mut rest: mpsc::UnboundedReceiver<Result<Message, crate::error::ConnectionError>>,
    ) -> (
        impl futures::Stream<Item = Result<Message, crate::error::ConnectionError>>,
        oneshot::Receiver<Message>,
    ) {
        let (resolver, response) = oneshot::channel();
        let mut resolver = Some(resolver);
        let mut reply = Some(reply);
        let inbound = futures::stream::poll_fn(move |cx| {
            if let Some(resolver) = resolver.take() {
                registrations_tx
                    .unbounded_send(Register::Request {
                        key: key.clone(),
                        resolver,
                    })
                    .unwrap();
                return std::task::Poll::Ready(reply.take().map(Ok));
            }
            rest.poll_next_unpin(cx)
        });
        (inbound, response)
    }

    /// A reply read in the same poll as its request's registration, after
    /// the receiver's first drain of the registrations, is delivered in that
    /// poll.
    #[tokio::test]
    async fn a_reply_ahead_of_its_registration_is_delivered() {
        let (_message_tx, message_rx) = mpsc::unbounded();
        let (control_tx, _control_rx) = async_channel::unbounded();
        let (registrations_tx, registrations_rx) = mpsc::unbounded();
        let (_receiver_shutdown_tx, receiver_shutdown_rx) = oneshot::channel();
        let (auth_challenge_tx, _auth_challenge_rx) = mpsc::unbounded();
        let (inbound, mut response) = racing_inbound(
            registrations_tx.clone(),
            RequestKey::RequestId(7),
            lookup_reply(7),
            message_rx,
        );
        let mut receiver = Receiver::new(
            inbound,
            control_tx,
            SharedError::new(),
            registrations_rx,
            receiver_shutdown_rx,
            auth_challenge_tx,
            Default::default(),
        );
        assert!(futures::poll!(&mut receiver).is_pending());
        let msg = response
            .try_recv()
            .unwrap()
            .expect("the early reply was lost");
        assert!(msg.command.lookup_topic_response.is_some());
        assert!(receiver.received_messages.is_empty());
    }

    /// FORK-PATCH (3rdi): a producer's exclusive-access answer comes
    /// unrequested and its waiter registers only after the first answer:
    /// it is kept for that registration.
    #[tokio::test]
    async fn an_unrequested_producer_success_waits_for_its_registration() {
        let (mut receiver, message_tx, registrations_tx, _keep) = hand_polled_receiver();
        message_tx
            .unbounded_send(Ok(Message {
                command: BaseCommand {
                    r#type: 17,
                    producer_success: Some(crate::proto::CommandProducerSuccess {
                        request_id: 9,
                        producer_ready: Some(true),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                payload: None,
            }))
            .unwrap();
        assert!(futures::poll!(&mut receiver).is_pending());

        let mut response = register(&registrations_tx, RequestKey::RequestId(9));
        assert!(futures::poll!(&mut receiver).is_pending());
        let msg = response
            .try_recv()
            .unwrap()
            .expect("the exclusive-access answer was dropped");
        assert!(msg.command.producer_success.is_some());
    }

    /// FORK-PATCH (3rdi): a cancellation forgets only the cancelled
    /// request's resolver, never a live one registered since under the same
    /// key (a resend).
    #[tokio::test]
    async fn a_cancellation_leaves_a_live_resolver_alone() {
        let (mut receiver, _message_tx, registrations_tx, _keep) = hand_polled_receiver();
        let key = RequestKey::ProducerSend {
            producer_id: 1,
            sequence_id: 5,
        };
        let _resent = register(&registrations_tx, key.clone());
        registrations_tx
            .unbounded_send(Register::CancelRequest { key })
            .unwrap();
        assert!(futures::poll!(&mut receiver).is_pending());
        assert_eq!(
            receiver.pending_requests.len(),
            1,
            "a live resolver was cancelled"
        );
    }

    /// A sender wired to a hand-polled receiver, with a 1 s operation
    /// timeout and a data channel of `data_capacity`.
    #[allow(clippy::type_complexity)]
    fn hand_polled_connection(
        data_capacity: usize,
    ) -> (
        super::ConnectionSender<crate::TokioExecutor>,
        Receiver<mpsc::UnboundedReceiver<Result<Message, crate::error::ConnectionError>>>,
        (
            mpsc::UnboundedSender<Result<Message, crate::error::ConnectionError>>,
            async_channel::Receiver<Message>,
            async_channel::Receiver<Message>,
            mpsc::UnboundedReceiver<()>,
        ),
    ) {
        let (message_tx, message_rx) = mpsc::unbounded();
        let (control_tx, control_rx) = async_channel::unbounded();
        let (data_tx, data_rx) = async_channel::bounded(data_capacity);
        let (registrations_tx, registrations_rx) = mpsc::unbounded();
        let (receiver_shutdown_tx, receiver_shutdown_rx) = oneshot::channel();
        let (auth_challenge_tx, auth_challenge_rx) = mpsc::unbounded();
        let error = SharedError::new();
        let receiver = Receiver::new(
            message_rx,
            control_tx.clone(),
            error.clone(),
            registrations_rx,
            receiver_shutdown_rx,
            auth_challenge_tx,
            Default::default(),
        );
        let sender = super::ConnectionSender::new(
            Uuid::new_v4(),
            data_tx,
            control_tx,
            registrations_tx,
            receiver_shutdown_tx,
            super::SerialId::new(),
            error,
            Arc::new(crate::TokioExecutor),
            Duration::from_secs(1),
            Default::default(),
        );
        (
            sender,
            receiver,
            (message_tx, control_rx, data_rx, auth_challenge_rx),
        )
    }

    /// FORK-PATCH (3rdi): a request that timed out forgets its resolver.
    #[tokio::test(start_paused = true)]
    async fn a_timed_out_request_forgets_its_resolver() {
        let (sender, mut receiver, _keep) = hand_polled_connection(10);
        let lookup = sender.lookup_topic("persistent://t/n/x", false);
        futures::pin_mut!(lookup);
        assert!(futures::poll!(lookup.as_mut()).is_pending());
        assert!(futures::poll!(&mut receiver).is_pending());
        assert_eq!(receiver.pending_requests.len(), 1);

        tokio::time::advance(Duration::from_secs(2)).await;
        assert!(matches!(
            futures::poll!(lookup.as_mut()),
            std::task::Poll::Ready(Err(_))
        ));
        assert!(futures::poll!(&mut receiver).is_pending());
        assert!(receiver.pending_requests.is_empty(), "the resolver leaked");
    }

    /// FORK-PATCH (3rdi): a request future dropped before its answer (an
    /// application timeout, an aborted task) forgets its resolver too.
    #[tokio::test]
    async fn a_dropped_request_forgets_its_resolver() {
        let (sender, mut receiver, _keep) = hand_polled_connection(10);
        {
            let lookup = sender.lookup_topic("persistent://t/n/x", false);
            futures::pin_mut!(lookup);
            assert!(futures::poll!(lookup.as_mut()).is_pending());
        }
        assert!(futures::poll!(&mut receiver).is_pending());
        assert!(receiver.pending_requests.is_empty(), "the resolver leaked");
    }

    /// FORK-PATCH (3rdi): a send dropped while it waits for room on the
    /// data channel (it registered, it was never written) forgets its
    /// resolver.
    #[tokio::test]
    async fn a_send_dropped_while_blocked_forgets_its_resolver() {
        let (sender, mut receiver, keep) = hand_polled_connection(1);
        // the data channel is full
        let _queued = keep.0.clone();
        {
            let first = sender.send(1, "p".to_string(), 0, Default::default(), true);
            futures::pin_mut!(first);
            let _ = futures::poll!(first.as_mut());
        }
        {
            let blocked = sender.send(1, "p".to_string(), 1, Default::default(), true);
            futures::pin_mut!(blocked);
            assert!(
                futures::poll!(blocked.as_mut()).is_pending(),
                "the send waits for room"
            );
        }
        assert!(futures::poll!(&mut receiver).is_pending());
        assert!(receiver.pending_requests.is_empty(), "the resolver leaked");
    }

    /// FORK-PATCH (3rdi): a reply nobody waits for (its request timed out)
    /// is dropped, not kept for the connection's lifetime.
    #[tokio::test]
    async fn a_stale_reply_is_dropped() {
        let (mut receiver, message_tx, _registrations_tx, _keep) = hand_polled_receiver();
        message_tx.unbounded_send(Ok(lookup_reply(7))).unwrap();
        message_tx.unbounded_send(Ok(send_receipt(1, 5))).unwrap();
        assert!(futures::poll!(&mut receiver).is_pending());
        assert!(
            receiver.received_messages.is_empty(),
            "a stale reply was kept"
        );
        assert!(receiver.pending_requests.is_empty());
    }

    /// FORK-PATCH (3rdi): a request dropped but not yet cancelled (its
    /// cancellation still in the channel) leaves a resolver nobody waits
    /// on: a reply for a resend under the same key, read in the poll that
    /// registers the resend, goes to the resend, not to the dead resolver.
    #[tokio::test]
    async fn a_dead_resolver_does_not_take_a_resends_reply() {
        let (_message_tx, message_rx) = mpsc::unbounded();
        let (control_tx, _control_rx) = async_channel::unbounded();
        let (registrations_tx, registrations_rx) = mpsc::unbounded();
        let (_receiver_shutdown_tx, receiver_shutdown_rx) = oneshot::channel();
        let (auth_challenge_tx, _auth_challenge_rx) = mpsc::unbounded();
        let key = RequestKey::ProducerSend {
            producer_id: 1,
            sequence_id: 5,
        };
        let (inbound, mut resent) = racing_inbound(
            registrations_tx.clone(),
            key.clone(),
            send_receipt(1, 5),
            message_rx,
        );
        let mut receiver = Receiver::new(
            inbound,
            control_tx,
            SharedError::new(),
            registrations_rx,
            receiver_shutdown_rx,
            auth_challenge_tx,
            Default::default(),
        );
        // the first send registered, then its future was dropped; its
        // cancellation has not reached the receiver yet
        drop(register(&registrations_tx, key));

        assert!(futures::poll!(&mut receiver).is_pending());
        let msg = resent
            .try_recv()
            .unwrap()
            .expect("the resend's receipt went to the dead resolver");
        assert!(msg.command.send_receipt.is_some());
    }

    /// FORK-PATCH (3rdi): a producer resends under the same sequence id, so
    /// a send receipt key comes back. After PS(1,5) timed out and was
    /// cancelled, the resend registers PS(1,5) again and its receipt is read
    /// in the same poll, after the poll's first drain of the registrations
    /// (another thread registered meanwhile): it is delivered.
    #[tokio::test]
    async fn a_receipt_for_a_resent_key_is_delivered_after_a_cancel() {
        let (_message_tx, message_rx) =
            mpsc::unbounded::<Result<Message, crate::error::ConnectionError>>();
        let (control_tx, _control_rx) = async_channel::unbounded();
        let (registrations_tx, registrations_rx) = mpsc::unbounded();
        let (_receiver_shutdown_tx, receiver_shutdown_rx) = oneshot::channel();
        let (auth_challenge_tx, _auth_challenge_rx) = mpsc::unbounded();
        let key = RequestKey::ProducerSend {
            producer_id: 1,
            sequence_id: 5,
        };

        // the inbound stream registers the resend just before it yields the
        // resend's receipt, as a producer on another thread does
        let (resend_tx, mut resend_rx) = oneshot::channel();
        let mut resend_tx = Some(resend_tx);
        let racing_registrations = registrations_tx.clone();
        let racing_key = key.clone();
        let mut message_rx = message_rx;
        let mut receipt = Some(send_receipt(1, 5));
        let inbound = futures::stream::poll_fn(move |cx| {
            if let Some(resend_tx) = resend_tx.take() {
                racing_registrations
                    .unbounded_send(Register::Request {
                        key: racing_key.clone(),
                        resolver: resend_tx,
                    })
                    .unwrap();
                return std::task::Poll::Ready(receipt.take().map(Ok));
            }
            message_rx.poll_next_unpin(cx)
        });
        let mut receiver = Receiver::new(
            inbound,
            control_tx,
            SharedError::new(),
            registrations_rx,
            receiver_shutdown_rx,
            auth_challenge_tx,
            Default::default(),
        );

        // the first send of PS(1,5) timed out and was cancelled
        // (its future is dropped first, as the request guard does)
        let first = register(&registrations_tx, key.clone());
        drop(first);
        registrations_tx
            .unbounded_send(Register::CancelRequest { key })
            .unwrap();
        assert!(futures::poll!(&mut receiver).is_pending());

        let msg = resend_rx
            .try_recv()
            .unwrap()
            .expect("the resend's receipt was lost");
        assert!(msg.command.send_receipt.is_some());
        assert!(receiver.received_messages.is_empty());
        assert!(receiver.pending_requests.is_empty());
    }

    /// FORK-PATCH (3rdi): a connection whose keepalive ping failed is dead for
    /// good. Reading the error (the consumer engine does, to log it) must not
    /// make it valid again, or the connection manager hands the dead
    /// connection to every later lookup (prophesee-service-rs #284).
    #[tokio::test]
    #[cfg(any(
        feature = "tokio-runtime",
        feature = "tokio-rustls-runtime-aws-lc-rs",
        feature = "tokio-rustls-runtime-ring"
    ))]
    async fn reading_the_error_does_not_revive_a_dead_connection() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let stream = tokio::net::TcpStream::connect(listener.local_addr().unwrap())
            .await
            .map(|stream| tokio_util::codec::Framed::new(stream, Codec))
            .unwrap();
        let mut server_stream = listener
            .accept()
            .await
            .map(|(stream, _)| tokio_util::codec::Framed::new(stream, Codec))
            .unwrap();
        server_stream
            .send(Message {
                command: BaseCommand {
                    connected: Some(CommandConnected {
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                payload: None,
            })
            .await
            .unwrap();

        let sender = Connection::connect(
            Uuid::new_v4(),
            stream,
            None,
            None,
            TokioExecutor.into(),
            Duration::from_millis(200),
            100,
        )
        .await
        .unwrap();
        let connection = Connection {
            id: Uuid::new_v4(),
            url: url::Url::parse("pulsar://127.0.0.1:6650").unwrap(),
            sender,
        };
        assert!(connection.is_valid());

        // the broker never answers the keepalive ping: the connection is dead
        assert!(connection.sender().send_ping().await.is_err());
        assert!(
            !connection.is_valid(),
            "a failed ping retires the connection"
        );

        assert!(connection.error().is_some(), "the failure is reported once");
        assert!(
            !connection.is_valid(),
            "reading the error must not revive the dead connection"
        );
        drop(server_stream);
    }
}
