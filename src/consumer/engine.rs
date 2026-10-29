use std::{
    collections::{HashMap, HashSet},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use futures::{
    channel::{mpsc, mpsc::UnboundedSender},
    SinkExt, StreamExt,
};

use crate::{
    connection::Connection,
    consumer::{
        data::{DeadLetterPolicy, EngineEvent, EngineMessage},
        options::ConsumerOptions,
        queue::QueueSender,
    },
    error::{ConnectionError, ConsumerError, DecodeRefusal},
    message::{
        proto::{command_subscribe::SubType, MessageIdData},
        Message as RawMessage,
    },
    producer::Message,
    proto,
    proto::{BaseCommand, CommandCloseConsumer, CommandMessage},
    retry_op::retry_subscribe_consumer,
    Error, Executor, Payload, Pulsar,
};

/// The permits one broker entry consumed, booked exactly as the broker
/// debits them: the declared `num_messages_in_batch`, unclamped
/// (`AbstractBaseDispatcher` sums it, `Consumer.sendMessages` subtracts
/// it), so the window mirrors the broker's and the next flow re-grants it.
/// Zero or negative is booked as 1. The broker's window then drifts from
/// this one (a single-active-consumer dispatcher credits a negative count,
/// and `i32::MIN` overflows it), which no client booking repairs: on
/// Exclusive/Failover the engine resubscribes (see
/// [`forged_count_corrupts_window`]); on Shared the publisher trust boundary
/// (mesh + publish authorization) is the defence.
fn permits_consumed(num_messages_in_batch: Option<i32>) -> i64 {
    i64::from(num_messages_in_batch.unwrap_or(1)).max(1)
}

/// Whether the permit window has drained far enough to ask for more.
fn needs_flow(batch_size: u32, remaining: i64) -> bool {
    remaining < (batch_size.div_ceil(2) as i64)
}

/// The flow request that tops the window back up to `batch_size`, at most
/// `i32::MAX`: the broker reads the uint32 into a Java `int` and rejects a
/// non-positive one. A deficit past that is granted over further requests.
fn flow_permits(batch_size: u32, remaining: i64) -> u32 {
    let wanted = (i64::from(batch_size) - remaining).clamp(0, i64::from(i32::MAX));
    u32::try_from(wanted).unwrap_or(0)
}

/// The least time between two warnings about entries declaring zero or
/// fewer members, so a hostile publisher cannot flood the log.
const NON_POSITIVE_COUNT_WARN_INTERVAL: Duration = Duration::from_secs(60);

/// Whether a non-positive-count warning is due at `now`, given when the
/// last one was logged.
fn warn_due(last: Option<Instant>, now: Instant) -> bool {
    !matches!(last, Some(last) if now.saturating_duration_since(last) < NON_POSITIVE_COUNT_WARN_INTERVAL)
}

/// FORK-PATCH (3rdi): whether a failed reconnect is worth retrying: the
/// broker or the network was briefly away (I/O, a timeout, a dropped
/// connection, a broker not ready or still holding the old consumer, a
/// metadata hiccup or rate limit on lookup). Anything else (the topic is
/// gone or terminated, the subscriber is not authorized, the schema is
/// incompatible, a TLS failure, a DNS failure) is permanent and reaches the
/// user. A DNS failure ends the engine as upstream: our clients reach the
/// brokers through the stable proxy Service, so a restarted broker pod's
/// headless DNS name never fails for them.
fn reconnect_is_transient(e: &Error) -> bool {
    use crate::{error::ServiceDiscoveryError, message::proto::ServerError};
    fn server(code: &Option<ServerError>) -> bool {
        matches!(
            code,
            Some(
                ServerError::ServiceNotReady
                    | ServerError::ConsumerBusy
                    | ServerError::MetadataError
                    | ServerError::TooManyRequests
            )
        )
    }
    fn connection(e: &ConnectionError) -> bool {
        match e {
            // a TLS failure (bad certificate data, permission denied) is
            // not going away on a retry
            ConnectionError::Io(e) => !matches!(
                e.kind(),
                std::io::ErrorKind::InvalidData | std::io::ErrorKind::PermissionDenied
            ),
            ConnectionError::Disconnected | ConnectionError::Canceled => true,
            ConnectionError::PulsarError(code, _) => server(code),
            e => e.establish_retryable(),
        }
    }
    match e {
        Error::Connection(e) => connection(e),
        Error::Consumer(ConsumerError::Connection(e)) => connection(e),
        Error::ServiceDiscovery(ServiceDiscoveryError::Connection(e)) => connection(e),
        Error::ServiceDiscovery(ServiceDiscoveryError::Query(code, _)) => server(code),
        Error::ServiceDiscovery(ServiceDiscoveryError::Canceled) => true,
        _ => false,
    }
}

/// FORK-PATCH (3rdi): why a resubscribe is pending.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Pending {
    /// The connection failed and a reconnect off it failed too.
    ConnectionRetry,
    /// A non-positive batch count corrupted the broker's permit window.
    CorruptWindow,
}

impl Pending {
    fn reason(self) -> &'static str {
        match self {
            Pending::ConnectionRetry => "the reconnect off a failed connection failed",
            Pending::CorruptWindow => {
                "a non-positive batch count corrupted the broker's permit window"
            }
        }
    }
}

/// FORK-PATCH (3rdi): how long a reconnect waits for the broker to answer
/// the close of the consumer on a failed connection: a broker still behind
/// the socket answers in milliseconds, one that is gone never does.
const DEAD_CONNECTION_CLOSE_TIMEOUT: Duration = Duration::from_secs(1);

/// The first forced-resubscribe delay, and the most it backs off to: the
/// Java client's reconnection backoff (`initialBackoffInterval` 100 ms,
/// `maxBackoffInterval` 60 s). One forged entry on an honest subscription
/// is recovered from within 100 ms; a publisher forging them continuously
/// gets at most one resubscribe a minute.
const FORCED_RESUBSCRIBE_INITIAL_BACKOFF: Duration = Duration::from_millis(100);
const FORCED_RESUBSCRIBE_MAX_BACKOFF: Duration = Duration::from_secs(60);
/// A forced resubscribe this long after the previous one starts the backoff
/// over: past twice the cap, so a steady stream of forged entries (one per
/// capped delay) never resets it.
const FORCED_RESUBSCRIBE_RESET_AFTER: Duration = Duration::from_secs(120);

/// The delay before the forced resubscribe numbered `attempt` (from 0):
/// doubling from `initial`, capped.
fn forced_resubscribe_backoff(attempt: u32, initial: Duration) -> Duration {
    initial
        .saturating_mul(2u32.saturating_pow(attempt))
        .min(FORCED_RESUBSCRIBE_MAX_BACKOFF)
}

/// The forced-resubscribe schedule of one consumer.
#[derive(Debug, Default)]
struct ForcedResubscribe {
    /// When the scheduled resubscribe is due, if one is.
    due: Option<Instant>,
    /// Why the scheduled resubscribe is pending (set with `due`).
    pending: Option<Pending>,
    /// The backoff step of the next one.
    attempt: u32,
    /// When the last one fired (or, once its reconnect is done, completed):
    /// the quiet period that resets the backoff runs from here.
    last: Option<Instant>,
}

impl ForcedResubscribe {
    /// Schedules a resubscribe at `now` plus the backoff and returns that
    /// delay, unless one is already pending (it resets the window for every
    /// entry before it): then nothing changes and this returns `None`. The
    /// backoff starts over when the last one fired (or completed) more than
    /// [`FORCED_RESUBSCRIBE_RESET_AFTER`] before `now`.
    fn schedule(&mut self, now: Instant, initial: Duration) -> Option<Duration> {
        if self.due.is_some() {
            return None;
        }
        if self.last.is_some_and(|last| {
            now.saturating_duration_since(last) > FORCED_RESUBSCRIBE_RESET_AFTER
        }) {
            self.attempt = 0;
        }
        let delay = forced_resubscribe_backoff(self.attempt, initial);
        self.attempt = self.attempt.saturating_add(1);
        self.last = Some(now + delay);
        self.due = Some(now + delay);
        Some(delay)
    }

    /// Records that a resubscribe completed at `now`, so a reconnect slower
    /// than the quiet period does not reset the backoff by itself.
    fn completed(&mut self, now: Instant) {
        self.last = self.last.map(|last| last.max(now));
    }

    /// Reschedules after a resubscribe that failed at `now`: the failed one
    /// counts as the last, so a slow failure does not reset the backoff.
    fn retry(&mut self, now: Instant, initial: Duration) -> Option<Duration> {
        self.completed(now);
        self.schedule(now, initial)
    }
}

/// Whether an acknowledged entry declaring `num_messages_in_batch` left the
/// broker's permit window corrupt for this subscription type, so only a
/// fresh subscription (a new broker-side consumer) restores it. A
/// single-active-consumer dispatcher credits a non-positive count and
/// `i32::MIN` overflows its window; Shared skips the entry.
fn forged_count_corrupts_window(sub_type: SubType, num_messages_in_batch: Option<i32>) -> bool {
    matches!(sub_type, SubType::Exclusive | SubType::Failover)
        && matches!(num_messages_in_batch, Some(..=0))
}

const SYSTEM_PROPERTY_REAL_TOPIC: &str = "REAL_TOPIC";
const SYSTEM_PROPERTY_ORIGIN_MESSAGE_ID: &str = "ORIGIN_MESSAGE_ID";
/// Set on a dead-lettered message the engine refused to decode: the
/// [`DecodeRefusal::as_str`] label.
const SYSTEM_PROPERTY_DECODE_REFUSAL: &str = "DECODE_REFUSAL";
/// Set on a dead-lettered refused message that arrived compressed: the codec
/// name (`ZSTD`, `LZ4`, ...); the payload is the compressed bytes as received.
const SYSTEM_PROPERTY_ORIGINAL_COMPRESSION: &str = "ORIGINAL_COMPRESSION";
/// Set alongside [`SYSTEM_PROPERTY_ORIGINAL_COMPRESSION`] when the message
/// declared one: its `uncompressed_size`.
const SYSTEM_PROPERTY_ORIGINAL_UNCOMPRESSED_SIZE: &str = "ORIGINAL_UNCOMPRESSED_SIZE";

pub struct ConsumerEngine<Exe: Executor> {
    client: Pulsar<Exe>,
    connection: Arc<Connection<Exe>>,
    topic: String,
    subscription: String,
    sub_type: SubType,
    id: u64,
    name: Option<String>,
    tx: QueueSender,
    messages_rx: Option<mpsc::UnboundedReceiver<RawMessage>>,
    engine_rx: Option<mpsc::UnboundedReceiver<EngineMessage<Exe>>>,
    event_rx: mpsc::UnboundedReceiver<EngineEvent<Exe>>,
    event_tx: UnboundedSender<EngineEvent<Exe>>,
    batch_size: u32,
    remaining_messages: i64,
    /// When the last non-positive-count warning was logged.
    non_positive_count_warned: Option<Instant>,
    unacked_message_redelivery_delay: Option<Duration>,
    unacked_messages: HashMap<MessageIdData, Instant>,
    dead_letter_policy: Option<DeadLetterPolicy>,
    options: ConsumerOptions,
    /// How many forced resubscribes have been scheduled.
    forced_resubscribes: Arc<AtomicU64>,
    /// The current subscription session: bumped by every resubscribe, in
    /// step with the receive queue's. A message from an older session is
    /// dropped (the broker redelivers it on this one, which never charged
    /// a permit for it), as the Java client's `clearReceiverQueue`.
    generation: u64,
    /// The forced-resubscribe schedule.
    forced: ForcedResubscribe,
    /// The first forced-resubscribe delay: zero in this crate's tests, so
    /// they wait on nothing.
    forced_initial_backoff: Duration,
    /// Test seam: how many coming `reconnect` calls fail before doing anything.
    #[cfg(test)]
    reconnect_failures: Arc<std::sync::atomic::AtomicU32>,
    /// Test seam: the injected failures are permanent (topic not found).
    #[cfg(test)]
    injected_permanent: bool,
    /// Test seam: a notification per injected reconnect failure.
    #[cfg(test)]
    injected_tx: mpsc::UnboundedSender<()>,
    #[cfg(test)]
    injected_rx: Option<mpsc::UnboundedReceiver<()>>,
    /// Test seam: after every successful resubscribe, the count of messages
    /// still tracked for unacked redelivery.
    #[cfg(test)]
    resubscribed_tx: mpsc::UnboundedSender<usize>,
    #[cfg(test)]
    resubscribed_rx: Option<mpsc::UnboundedReceiver<usize>>,
}

impl<Exe: Executor> ConsumerEngine<Exe> {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn new(
        client: Pulsar<Exe>,
        connection: Arc<Connection<Exe>>,
        topic: String,
        subscription: String,
        sub_type: SubType,
        id: u64,
        name: Option<String>,
        tx: QueueSender,
        messages_rx: mpsc::UnboundedReceiver<RawMessage>,
        engine_rx: mpsc::UnboundedReceiver<EngineMessage<Exe>>,
        batch_size: u32,
        unacked_message_redelivery_delay: Option<Duration>,
        dead_letter_policy: Option<DeadLetterPolicy>,
        options: ConsumerOptions,
    ) -> ConsumerEngine<Exe> {
        let (event_tx, event_rx) = mpsc::unbounded();
        #[cfg(test)]
        let (resubscribed_tx, resubscribed_rx) = mpsc::unbounded();
        #[cfg(test)]
        let (injected_tx, injected_rx) = mpsc::unbounded();
        ConsumerEngine {
            client,
            connection,
            topic,
            subscription,
            sub_type,
            id,
            name,
            tx,
            messages_rx: Some(messages_rx),
            engine_rx: Some(engine_rx),
            event_rx,
            event_tx,
            batch_size,
            remaining_messages: batch_size as i64,
            non_positive_count_warned: None,
            unacked_message_redelivery_delay,
            unacked_messages: HashMap::new(),
            dead_letter_policy,
            options,
            forced_resubscribes: Arc::new(AtomicU64::new(0)),
            generation: 0,
            forced: ForcedResubscribe::default(),
            forced_initial_backoff: if cfg!(test) {
                Duration::ZERO
            } else {
                FORCED_RESUBSCRIBE_INITIAL_BACKOFF
            },
            #[cfg(test)]
            reconnect_failures: Arc::new(std::sync::atomic::AtomicU32::new(0)),
            #[cfg(test)]
            injected_permanent: false,
            #[cfg(test)]
            injected_tx,
            #[cfg(test)]
            injected_rx: Some(injected_rx),
            #[cfg(test)]
            resubscribed_tx,
            #[cfg(test)]
            resubscribed_rx: Some(resubscribed_rx),
        }
    }

    /// The count of forced resubscribes scheduled, shared with the consumer.
    #[cfg(test)]
    pub(crate) fn forced_resubscribes(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.forced_resubscribes)
    }

    /// The reconnect-failure injection counter, shared with the consumer.
    #[cfg(test)]
    pub(crate) fn reconnect_failures(&self) -> Arc<std::sync::atomic::AtomicU32> {
        Arc::clone(&self.reconnect_failures)
    }

    /// The resubscribe notifications, taken once by the consumer.
    #[cfg(test)]
    pub(crate) fn take_resubscribed(&mut self) -> Option<mpsc::UnboundedReceiver<usize>> {
        self.resubscribed_rx.take()
    }

    fn register_source<E, M>(&self, mut rx: mpsc::UnboundedReceiver<E>, mapper: M) -> Result<(), ()>
    where
        E: Send + 'static,
        M: Fn(Option<E>) -> EngineEvent<Exe> + Send + Sync + 'static,
    {
        let mut event_tx = self.event_tx.clone();

        self.client.executor.spawn(Box::pin(async move {
            while let Some(msg) = rx.next().await {
                let r = event_tx.send(mapper(Some(msg))).await;
                if let Err(err) = r {
                    log::error!("Error sending event to channel - {err}");
                    if err.is_disconnected() {
                        break;
                    }
                }
            }

            let send_end_res = event_tx.send(mapper(None)).await;
            if let Err(err) = send_end_res {
                log::debug!("Error sending close event to channel - {err}");
            }

            log::trace!("rx terminated");
        }))
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn engine(&mut self) -> Result<(), Error> {
        debug!("starting the consumer engine for topic {}", self.topic);

        loop {
            // FORK-PATCH (3rdi): a failed reconnect is retried on the
            // forced-resubscribe backoff (the branch below fires it once
            // due), never the end of the engine; upstream returned the
            // error, so one lookup failing during a broker restart killed
            // the consumer.
            // A pending corrupt-window resubscribe does not hold back the
            // reconnect off a failed connection; only a pending connection
            // retry (already on the backoff) does. A permanent failure (see
            // `reconnect_is_transient`) ends the engine, as upstream did.
            if self.forced.pending != Some(Pending::ConnectionRetry)
                && connection_failed(&self.connection)
            {
                if let Err(e) = self.reconnect().await {
                    if !reconnect_is_transient(&e) {
                        error!("{}: reconnect failed for good: {e}", self.debug_format());
                        return Err(e);
                    }
                    warn!("{}: reconnect failed: {e}; retrying", self.debug_format());
                    self.schedule_resubscribe(true, Pending::ConnectionRetry);
                }
            }

            if self.forced.due.is_some_and(|due| Instant::now() >= due) {
                // A failed forced resubscribe is retried with the backoff,
                // never the end of the engine (`reconnect` cleared `due`),
                // except a connection retry that failed for good.
                let pending = self.forced.pending.unwrap_or(Pending::CorruptWindow);
                if let Err(e) = self.reconnect().await {
                    if pending == Pending::ConnectionRetry && !reconnect_is_transient(&e) {
                        error!("{}: reconnect failed for good: {e}", self.debug_format());
                        return Err(e);
                    }
                    warn!(
                        "{}: {} and the resubscribe failed: {e}; retrying",
                        self.debug_format(),
                        pending.reason()
                    );
                    self.schedule_resubscribe(true, pending);
                }
            }

            if let Some(messages_rx) = self.messages_rx.take() {
                let generation = self.generation;
                self.register_source(messages_rx, move |msg| {
                    EngineEvent::Message(generation, msg)
                })
                .map_err(|_| Error::Custom(String::from("Error registering messages_rx source")))?;
            }

            if let Some(engine_rx) = self.engine_rx.take() {
                self.register_source(engine_rx, |msg| EngineEvent::EngineMessage(msg))
                    .map_err(|_| {
                        Error::Custom(String::from("Error registering engine_rx source"))
                    })?;
            }

            // In the casual workflow, we use the `batch_size` as a maximum number that we could
            // send using a send flow command message to ask the broker to send us
            // messages, which is why we have a subtraction below (`batch_size` -
            // `remaining_messages`).
            //
            // In the special case of batch messages (which is defined by clients), the number of
            // messages could be greater than the given batch size and the remaining
            // messages goes negative, which is why we use an `i64` as we want to keep
            // track of the number of negative messages as the next send flow will be
            // the batch_size - minus remaining messages which allow us to retrieve the casual
            // workflow of flow command messages.
            //
            // Here is the example of it works for a batch_size at 1000 and the error case:
            //
            // ```
            // Message (1) -> (batch_size = 1000, remaing_messages = 999, no flow message trigger)
            // ... 499 messages later ...
            // Message (1) -> (batch_size = 1000, remaing_messages = 499, flow message trigger, ask broker to send => batch_size - remaining_messages = 501)
            // ... 200 messages later ...
            // BatchMessage (1024) -> (batch_size = 1000, remaining_messages = 4294967096, no flow message trigger) [underflow on remaining messages - without the patch]
            // BatchMessage (1024) -> (batch_size = 1000, remaining_messages = -1124, flow message trigger, ask broker to send =>  batch_size - remaining_messages = 2124) [no underflow on remaining messages - with the patch]
            // ```
            // FORK-PATCH (3rdi): no flow on a dead connection while its
            // reconnect waits out the backoff (a send failing there loops).
            if self.connection.is_valid() && needs_flow(self.batch_size, self.remaining_messages) {
                let permits = flow_permits(self.batch_size, self.remaining_messages);
                match self.connection.sender().send_flow(self.id, permits).await {
                    Ok(()) => {}
                    Err(ConnectionError::Disconnected) => {
                        debug!("consumer engine: consumer connection disconnected, trying to reconnect");
                        continue;
                    }
                    // we don't need to handle the SlowDown error, since send_flow waits on the
                    // channel to be not full
                    Err(e) => {
                        error!("consumer engine: we got a unrecoverable connection error, {e}");
                        return Err(e.into());
                    }
                }

                // Mirror the broker: it adds exactly what was granted.
                self.remaining_messages += i64::from(permits);
            }

            // Wake for a due forced resubscribe, else at least every second.
            let wait = self.forced.due.map_or(Duration::from_secs(1), |due| {
                due.saturating_duration_since(Instant::now())
                    .min(Duration::from_secs(1))
            });
            // FORK-PATCH (3rdi): on the runtime's own timer (the executor's
            // delay), the one timer source of the engine
            let tick = self.client.executor.delay(wait);
            let next = match futures::future::select(Box::pin(self.event_rx.next()), Box::pin(tick))
                .await
            {
                futures::future::Either::Left((event, _)) => Ok(event),
                futures::future::Either::Right(_) => Err(()),
            };
            match next {
                Err(_) => {
                    // If you are reading this comment, you may have an issue where you have
                    // received a batched message that is greater that the batch
                    // size and then break the way that we send flow command message.
                    //
                    // In that case, you could increase your batch size or patch this driver by
                    // adding the following line, if you are sure that you have
                    // at least 1 incoming message per second.
                    //
                    // ```rust
                    // self.remaining_messages = 0;
                    // ```
                    debug!("consumer engine: no event within {wait:?}");
                }
                Ok(Some(EngineEvent::Message(generation, _))) if generation != self.generation => {
                    // A replaced session's message (or its end): neither
                    // delivered nor debited.
                    trace!("consumer engine: dropped a message from session {generation}");
                }
                Ok(Some(EngineEvent::Message(_, msg))) => {
                    // The command only: the payload is publisher bytes.
                    debug!(
                        "consumer engine: received message, {:?}",
                        msg.as_ref().map(|m| crate::message::redacted(&m.command))
                    );
                    let out = self.handle_message_opt(msg).await;
                    if let Some(res) = out {
                        return res;
                    }
                }
                Ok(Some(EngineEvent::EngineMessage(msg))) => {
                    debug!("consumer engine: received engine message");
                    let continue_loop = self.handle_ack_opt(msg).await;
                    if !continue_loop {
                        return Ok(());
                    }
                }
                Ok(None) => {
                    log::warn!("Event stream is terminated");
                    return Ok(());
                }
            }
        }
    }

    async fn handle_message_opt(
        &mut self,
        message_opt: Option<crate::message::Message>,
    ) -> Option<Result<(), Error>> {
        match message_opt {
            None => {
                debug!("Consumer: old message source terminated");
                None
            }
            Some(message) => {
                self.remaining_messages -=
                    permits_consumed(message.payload.as_ref().and_then(|payload| {
                        debug!(
                            "Consumer: received message payload, num_messages_in_batch = {:?}",
                            payload.metadata.num_messages_in_batch
                        );
                        payload.metadata.num_messages_in_batch
                    }));
                // A count of zero or less is refused (and acknowledged) like
                // any undecodable batch; it is also logged, at most once per
                // interval.
                if let Some(count @ ..=0) = message
                    .payload
                    .as_ref()
                    .and_then(|p| p.metadata.num_messages_in_batch)
                {
                    let now = Instant::now();
                    if warn_due(self.non_positive_count_warned, now) {
                        self.non_positive_count_warned = Some(now);
                        warn!(
                            "{}: entry declares {count} messages; booked as 1 permit (further such entries are not logged for {:?})",
                            self.debug_format(),
                            NON_POSITIVE_COUNT_WARN_INTERVAL
                        );
                    }
                }

                match self.process_message(message).await {
                    // Continue
                    Ok(true) => None,
                    // End of Topic
                    Ok(false) => Some(Ok(())),
                    Err(e) => {
                        if let Err(e) = self.tx.send(Err(e)).await {
                            error!("cannot send a message from the consumer engine to the consumer({}), stopping the engine", self.id);
                            Some(Err(Error::Consumer(e.into())))
                        } else {
                            None
                        }
                    }
                }
            }
        }
    }

    async fn handle_ack_opt(&mut self, ack_opt: Option<EngineMessage<Exe>>) -> bool {
        match ack_opt {
            None => {
                trace!("ack channel was closed");
                false
            }
            Some(EngineMessage::Ack(message_id, cumulative)) => {
                self.ack(message_id, cumulative).await;
                true
            }
            Some(EngineMessage::Nack(message_id)) => {
                if let Err(e) = self
                    .connection
                    .sender()
                    .send_redeliver_unacknowleged_messages(self.id, vec![message_id.clone()])
                    .await
                {
                    error!(
                        "could not ask for redelivery for message {:?}: {:?}",
                        message_id, e
                    );
                }
                true
            }
            Some(EngineMessage::UnackedRedelivery) => {
                let mut h = HashSet::new();
                let now = Instant::now();
                // info!("unacked messages length: {}", self.unacked_messages.len());
                for (id, t) in self.unacked_messages.iter() {
                    if *t < now {
                        h.insert(id.clone());
                    }
                }

                let ids: Vec<_> = h.iter().cloned().collect();
                if !ids.is_empty() {
                    // info!("will unack ids: {:?}", ids);
                    if let Err(e) = self
                        .connection
                        .sender()
                        .send_redeliver_unacknowleged_messages(self.id, ids)
                        .await
                    {
                        error!("could not ask for redelivery: {:?}", e);
                    } else {
                        for i in h.iter() {
                            self.unacked_messages.remove(i);
                        }
                    }
                }
                true
            }
            Some(EngineMessage::GetConnection(sender)) => {
                let _ = sender.send(self.connection.clone()).map_err(|_| {
                    error!(
                        "consumer requested the engine's connection but dropped the \
                                     channel before receiving"
                    );
                });
                true
            }
        }
    }

    /// Acknowledges `message_id`; whether the ack reached the connection.
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    async fn ack(&mut self, message_id: MessageIdData, cumulative: bool) -> bool {
        // FIXME: this does not handle cumulative acks
        self.unacked_messages.remove(&message_id);
        let res = self
            .connection
            .sender()
            .send_ack(self.id, vec![message_id], cumulative)
            .await;
        if res.is_err() {
            error!("ack error: {:?}", res);
        }
        res.is_ok()
    }

    /// Process the message. Returns `true` if there are more messages to process
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    async fn process_message(&mut self, message: RawMessage) -> Result<bool, Error> {
        match message {
            RawMessage {
                command:
                    BaseCommand {
                        reached_end_of_topic: Some(_),
                        ..
                    },
                ..
            } => {
                return Ok(false);
            }
            RawMessage {
                command:
                    BaseCommand {
                        active_consumer_change: Some(active_consumer_change),
                        ..
                    },
                ..
            } => {
                // TODO: Communicate this status to the Consumer and expose it
                debug!(
                    "Active consumer change for {} - Active: {:?}",
                    self.debug_format(),
                    active_consumer_change.is_active
                );
            }
            RawMessage {
                command:
                    BaseCommand {
                        message: Some(message),
                        ..
                    },
                payload: Some(payload),
            } => {
                self.process_payload(message, payload).await?;
            }
            // The broker never sends a CommandMessage without its payload; a
            // frame that parsed without one is logged and dropped (nothing is
            // delivered, so nothing reaches the consumer stream).
            RawMessage {
                command: BaseCommand {
                    message: Some(_), ..
                },
                payload: None,
            } => {
                error!(
                    "Consumer {} received message without payload",
                    self.debug_format()
                );
            }
            RawMessage {
                command:
                    BaseCommand {
                        close_consumer: Some(CommandCloseConsumer { consumer_id, .. }),
                        ..
                    },
                ..
            } => {
                error!(
                    "Broker notification of closed consumer {}: {}",
                    consumer_id,
                    self.debug_format()
                );

                self.reconnect().await?;
            }
            unexpected => {
                let r#type = proto::base_command::Type::try_from(unexpected.command.r#type)
                    .map(|t| format!("{t:?}"))
                    .unwrap_or_else(|_| unexpected.command.r#type.to_string());
                warn!(
                    "Unexpected message type sent to consumer: {}. This is probably a bug!",
                    r#type
                );
            }
        }
        Ok(true)
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    async fn process_payload(
        &mut self,
        message: CommandMessage,
        payload: Payload,
    ) -> Result<(), Error> {
        let limit = self
            .options
            .max_decompressed_size
            .unwrap_or(super::decompress::DEFAULT_MAX_DECOMPRESSED_SIZE);
        let max_members = self
            .options
            .max_batch_members
            .unwrap_or(super::decompress::DEFAULT_MAX_BATCH_MEMBERS);
        let max_envelope = super::decompress::envelope_bound(self.options.max_batch_envelope_bytes);
        let id = message.message_id.clone();
        let payloads = match super::decompress::decode_payload(
            id,
            payload,
            limit,
            max_members,
            max_envelope,
        ) {
            Ok(payloads) => payloads,
            Err(refused) => {
                let (payload, reason) = *refused;
                return Err(self.settle_refused(message, payload, reason).await);
            }
        };
        for (message_id, payload) in payloads {
            match (message.redelivery_count, self.dead_letter_policy.clone()) {
                (Some(redelivery_count), Some(dead_letter_policy))
                    if redelivery_count as usize >= dead_letter_policy.max_redeliver_count =>
                {
                    // Send message to Dead Letter Topic and ack message in original topic
                    let message = self.dead_letter_message(&message_id, payload);
                    self.send_to_dead_letter(&dead_letter_policy.dead_letter_topic, message)
                        .await?;
                    self.ack(message_id, false).await;
                }
                _ => self.send_to_consumer(message_id, payload).await?,
            }
        }
        Ok(())
    }

    /// The dead-letter copy of `payload`: its data, properties and keys,
    /// plus the real topic and the origin message id.
    fn dead_letter_message(&self, message_id: &MessageIdData, payload: Payload) -> Message {
        let Payload { data, metadata } = payload;
        let mut properties = metadata
            .properties
            .into_iter()
            .map(|p| (p.key, p.value))
            .collect::<HashMap<_, _>>();
        properties
            .entry(SYSTEM_PROPERTY_REAL_TOPIC.to_string())
            .or_insert_with(|| self.topic.clone());
        properties
            .entry(SYSTEM_PROPERTY_ORIGIN_MESSAGE_ID.to_string())
            .or_insert_with(|| {
                if let Some(batch_index) = message_id.batch_index {
                    format!(
                        "{}:{}:{}:{}",
                        message_id.ledger_id,
                        message_id.entry_id,
                        message_id.partition.unwrap_or(-1),
                        batch_index
                    )
                } else {
                    format!(
                        "{}:{}:{}",
                        message_id.ledger_id,
                        message_id.entry_id,
                        message_id.partition.unwrap_or(-1)
                    )
                }
            });
        Message {
            payload: data,
            properties,
            partition_key: metadata.partition_key,
            partition_key_b64_encoded: metadata.partition_key_b64_encoded,
            ordering_key: metadata.ordering_key,
            event_time: metadata.event_time,
            ..Default::default()
        }
    }

    /// Publishes `message` to the dead-letter `topic` and waits for the receipt.
    async fn send_to_dead_letter(&self, topic: &str, message: Message) -> Result<(), Error> {
        self.client.send(topic, message).await?.await.map_err(|e| {
            error!("One shot cancelled {:?}", e);
            Error::Custom("DLQ send error".to_string())
        })?;
        Ok(())
    }

    /// Settles a message the engine refused to decode, so the broker never
    /// redelivers it: an unsettled refusal comes back on every reconnect and,
    /// on a Shared subscription, piles up until the consumer stalls.
    ///
    /// With a dead-letter policy the message goes to the dead-letter topic as
    /// it arrived (still compressed), tagged with the refusal; it is then
    /// acknowledged either way. Returns the error the consumer stream yields:
    /// [`ConsumerError::Refused`], or the dead-letter send's own error, in
    /// which case the message is NOT acknowledged and the broker redelivers it.
    /// Nothing logged here carries payload bytes or the message id.
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    async fn settle_refused(
        &mut self,
        message: CommandMessage,
        payload: Payload,
        reason: DecodeRefusal,
    ) -> Error {
        let message_id = message.message_id;
        let declared_count = payload.metadata.num_messages_in_batch;
        warn!(
            "{}: refusing a message ({}): {reason}",
            self.debug_format(),
            reason.as_str()
        );
        let dead_lettered = match self.dead_letter_policy.clone() {
            Some(policy) => {
                let compression = payload
                    .metadata
                    .compression
                    .and_then(|c| proto::CompressionType::try_from(c).ok())
                    .filter(|c| *c != proto::CompressionType::None);
                let uncompressed_size = payload.metadata.uncompressed_size;
                let mut dead_letter = self.dead_letter_message(&message_id, payload);
                dead_letter.properties.insert(
                    SYSTEM_PROPERTY_DECODE_REFUSAL.to_string(),
                    reason.as_str().to_string(),
                );
                if let Some(compression) = compression {
                    dead_letter.properties.insert(
                        SYSTEM_PROPERTY_ORIGINAL_COMPRESSION.to_string(),
                        compression.as_str_name().to_string(),
                    );
                    if let Some(size) = uncompressed_size {
                        dead_letter.properties.insert(
                            SYSTEM_PROPERTY_ORIGINAL_UNCOMPRESSED_SIZE.to_string(),
                            size.to_string(),
                        );
                    }
                }
                if let Err(e) = self
                    .send_to_dead_letter(&policy.dead_letter_topic, dead_letter)
                    .await
                {
                    return e;
                }
                true
            }
            None => false,
        };
        // Only once the entry is settled: resubscribing before (or after a
        // failed dead-letter send, which returned above) would redeliver
        // it, refuse it again and resubscribe again, in a loop.
        if self.ack(message_id, false).await
            && forged_count_corrupts_window(self.sub_type, declared_count)
        {
            self.schedule_forced_resubscribe(false);
        }
        Error::Consumer(ConsumerError::Refused {
            reason,
            dead_lettered,
        })
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    async fn send_to_consumer(
        &mut self,
        message_id: MessageIdData,
        payload: Payload,
    ) -> Result<(), Error> {
        let now = Instant::now();
        self.tx
            .send(Ok((message_id.clone(), payload)))
            .await
            .map_err(|e| {
                error!("tx returned {:?}", e);
                Error::Custom("tx closed".to_string())
            })?;
        if let Some(duration) = self.unacked_message_redelivery_delay {
            self.unacked_messages.insert(message_id, now + duration);
        }
        Ok(())
    }

    /// Schedules a resubscribe, backing off per consumer, unless one is
    /// already pending (it resets the window for every entry before it).
    /// The ack goes out before the resubscribe's `CloseConsumer` on the same
    /// connection, so the broker settles the entry before the new session.
    /// `after_failure` reschedules a forced resubscribe that just failed.
    fn schedule_forced_resubscribe(&mut self, after_failure: bool) {
        self.schedule_resubscribe(after_failure, Pending::CorruptWindow);
    }

    /// Schedules a resubscribe on the forced-resubscribe backoff; `reason`
    /// is logged.
    fn schedule_resubscribe(&mut self, after_failure: bool, pending: Pending) {
        let (now, initial) = (Instant::now(), self.forced_initial_backoff);
        let scheduled = if after_failure {
            self.forced.retry(now, initial)
        } else {
            self.forced.schedule(now, initial)
        };
        let Some(delay) = scheduled else {
            return;
        };
        self.forced.pending = Some(pending);
        let scheduled = self.forced_resubscribes.fetch_add(1, Ordering::SeqCst) + 1;
        warn!(
            "{}: {}; resubscribing in {delay:?} (forced resubscribe #{scheduled})",
            self.debug_format(),
            pending.reason()
        );
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    async fn reconnect(&mut self) -> Result<(), Error> {
        debug!("reconnecting consumer for topic: {}", self.topic);

        // A new session before the old one is closed: whatever it already
        // sent, queued or not, is dropped from here on.
        self.generation = self.tx.new_generation();

        // Any resubscribe gives a fresh window, so a pending forced one is
        // redundant; and the old session's messages are redelivered on the
        // new one, so their redelivery timers go (Java's clearReceiverQueue).
        self.forced.due = None;
        self.forced.pending = None;
        self.unacked_messages.clear();

        // send CloseConsumer to server
        // remove our resolver from the Connection consumers BTreeMap,
        // stopping the current Message source
        // FORK-PATCH (3rdi): on a failed connection the close is best
        // effort, bounded by DEAD_CONNECTION_CLOSE_TIMEOUT. Upstream waited
        // out the whole operation timeout (30 s by default) for an answer a
        // broker that is gone never sends; a broker still behind the socket
        // answers within the bound, so an exclusive subscription is still
        // released before it is taken again.
        let close = self.connection.sender().close_consumer(self.id);
        let closed = if self.connection.is_valid() {
            close.await
        } else {
            // the runtime's own timer, as every other timeout here
            let cap = self.client.executor.delay(DEAD_CONNECTION_CLOSE_TIMEOUT);
            match futures::future::select(Box::pin(close), Box::pin(cap)).await {
                futures::future::Either::Left((closed, _)) => closed,
                futures::future::Either::Right(_) => Err(ConnectionError::Io(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "no answer to the close on a failed connection",
                ))),
            }
        };
        if let Err(e) = closed {
            error!(
                "could not close consumer {:?}({}) for topic {}: {:?}",
                self.name, self.id, self.topic, e
            );
        }
        // should have logged "rx terminated" by now

        // A failed lookup or subscribe from here on, injected in tests.
        #[cfg(test)]
        if self
            .reconnect_failures
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
            .is_ok()
        {
            let _ = self.injected_tx.unbounded_send(());
            return Err(if self.injected_permanent {
                Error::ServiceDiscovery(crate::error::ServiceDiscoveryError::NotFound)
            } else {
                Error::Connection(ConnectionError::Io(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "injected reconnect failure",
                )))
            });
        }

        let broker_address = self.client.lookup_topic(&self.topic).await?;

        let messages = retry_subscribe_consumer(
            &self.client,
            &mut self.connection,
            broker_address,
            &self.topic,
            &self.subscription,
            self.sub_type,
            self.id,
            &self.name,
            &self.options,
            self.batch_size,
        )
        .await?;

        #[cfg(test)]
        let _ = self
            .resubscribed_tx
            .unbounded_send(self.unacked_messages.len());

        self.forced.completed(Instant::now());
        self.remaining_messages = self.batch_size as i64;
        self.messages_rx = Some(messages);

        Ok(())
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn debug_format(&self) -> String {
        format!(
            "[{id} - {subscription}{name}: {topic}]",
            id = self.id,
            subscription = self.subscription,
            name = self
                .name
                .as_ref()
                .map(|s| format!("({s})"))
                .unwrap_or_default(),
            topic = self.topic
        )
    }
}

impl<Exe: Executor> std::ops::Drop for ConsumerEngine<Exe> {
    fn drop(&mut self) {
        let conn = self.connection.clone();
        let id = self.id;
        let name = self.name.clone();
        let topic = self.topic.clone();
        let _ = self.client.executor.spawn(Box::pin(async move {
            if let Err(e) = conn.sender().close_consumer(id).await {
                error!(
                    "could not close consumer {:?}({}) for topic {}: {:?}",
                    name, id, topic, e
                );
            }
        }));
    }
}

/// FORK-PATCH (3rdi): whether an engine on `connection` must reconnect.
/// Every engine sharing a failed connection must, but only the first takes
/// the error to log it; upstream reconnected only when `error()` returned
/// one, so the other engines stalled on the dead connection
/// (prophesee-service-rs #284).
fn connection_failed<Exe: Executor>(connection: &Connection<Exe>) -> bool {
    if connection.is_valid() {
        return false;
    }
    match connection.error() {
        Some(err) => error!(
            "Consumer: connection {} is not valid: {:?}",
            connection.id(),
            err
        ),
        None => debug!("Consumer: connection {} is not valid", connection.id()),
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An engine on a connection that already failed (a ping went
    /// unanswered), against the silent in-process broker.
    #[cfg(any(
        feature = "tokio-runtime",
        feature = "tokio-rustls-runtime-aws-lc-rs",
        feature = "tokio-rustls-runtime-ring"
    ))]
    async fn engine_on_a_failed_connection() -> (
        ConsumerEngine<crate::TokioExecutor>,
        mpsc::UnboundedReceiver<()>,
        crate::consumer::queue::QueueReceiver,
        (
            mpsc::UnboundedSender<RawMessage>,
            mpsc::UnboundedSender<EngineMessage<crate::TokioExecutor>>,
        ),
    ) {
        use crate::{
            connection_manager::{tests::silent_broker, OperationRetryOptions},
            consumer::queue::receive_queue,
            Pulsar, TokioExecutor,
        };

        let (url, _seen) = silent_broker::listen().await;
        let client = Pulsar::builder(url, TokioExecutor)
            .with_operation_retry_options(OperationRetryOptions {
                operation_timeout: Duration::from_millis(200),
                ..Default::default()
            })
            .build()
            .await
            .unwrap();
        let conn = client.manager.get_base_connection().await.unwrap();
        silent_broker::fail(&conn).await;

        let (tx, rx) = receive_queue(10, 1 << 20);
        let (messages_tx, messages_rx) = mpsc::unbounded();
        let (engine_tx, engine_rx) = mpsc::unbounded();
        let mut engine = ConsumerEngine::new(
            client,
            conn,
            "persistent://t/n/x".to_string(),
            "s".to_string(),
            SubType::Exclusive,
            1,
            None,
            tx,
            messages_rx,
            engine_rx,
            10,
            None,
            None,
            ConsumerOptions::default(),
        );
        let injected = engine.injected_rx.take().unwrap();
        (engine, injected, rx, (messages_tx, engine_tx))
    }

    /// Runs `engine` until `n` injected reconnect failures fired; panics if
    /// it ends first or they do not fire within 10 s.
    #[cfg(any(
        feature = "tokio-runtime",
        feature = "tokio-rustls-runtime-aws-lc-rs",
        feature = "tokio-rustls-runtime-ring"
    ))]
    async fn run_until_injected(
        engine: &mut ConsumerEngine<crate::TokioExecutor>,
        injected: &mut mpsc::UnboundedReceiver<()>,
        n: usize,
    ) {
        let fired = async {
            for _ in 0..n {
                injected.next().await.expect("the engine is alive");
            }
        };
        let ran = tokio::time::timeout(
            Duration::from_secs(10),
            futures::future::select(Box::pin(engine.engine()), Box::pin(fired)),
        )
        .await
        .expect("the injected reconnect failures did not fire");
        if let futures::future::Either::Left((res, _)) = ran {
            panic!("the engine ended on a failed reconnect: {res:?}");
        }
    }

    /// FORK-PATCH (3rdi): which reconnect failures are retried. A TLS
    /// failure (bad data, permission denied) and a DNS failure are
    /// permanent; a timeout, a broker not ready or still holding the old
    /// consumer are transient.
    #[test]
    fn only_transient_reconnect_failures_are_retried() {
        use crate::{error::ServiceDiscoveryError as Sd, message::proto::ServerError as Se};
        let io = |kind| Error::Connection(ConnectionError::Io(std::io::Error::new(kind, "x")));
        let pulsar = |code| Error::Connection(ConnectionError::PulsarError(Some(code), None));
        let cases = [
            (io(std::io::ErrorKind::TimedOut), true),
            (io(std::io::ErrorKind::ConnectionReset), true),
            (io(std::io::ErrorKind::InvalidData), false),
            (io(std::io::ErrorKind::PermissionDenied), false),
            (pulsar(Se::ConsumerBusy), true),
            (pulsar(Se::ServiceNotReady), true),
            (pulsar(Se::AuthorizationError), false),
            (pulsar(Se::TopicNotFound), false),
            (
                Error::ServiceDiscovery(Sd::Query(Some(Se::MetadataError), None)),
                true,
            ),
            (
                Error::ServiceDiscovery(Sd::Query(Some(Se::TooManyRequests), None)),
                true,
            ),
            (
                Error::ServiceDiscovery(Sd::Query(Some(Se::TopicNotFound), None)),
                false,
            ),
            (Error::ServiceDiscovery(Sd::NotFound), false),
            (Error::ServiceDiscovery(Sd::DnsLookupError), false),
        ];
        for (err, transient) in cases {
            assert_eq!(reconnect_is_transient(&err), transient, "{err:?}");
        }
    }

    /// FORK-PATCH (3rdi): a transient failure of the reconnect off a dead
    /// connection is retried with the backoff; upstream ended the engine,
    /// so the consumer died on the first lookup that failed during a broker
    /// restart.
    #[tokio::test]
    #[cfg(any(
        feature = "tokio-runtime",
        feature = "tokio-rustls-runtime-aws-lc-rs",
        feature = "tokio-rustls-runtime-ring"
    ))]
    async fn a_failed_reconnect_is_retried() {
        let (mut engine, mut injected, _rx, _keep) = engine_on_a_failed_connection().await;
        engine.reconnect_failures.store(3, Ordering::SeqCst);
        run_until_injected(&mut engine, &mut injected, 3).await;
    }

    /// FORK-PATCH (3rdi): a permanent failure of that reconnect (here the
    /// topic is not found) ends the engine as upstream did, so it reaches
    /// the user instead of retrying forever.
    #[tokio::test]
    #[cfg(any(
        feature = "tokio-runtime",
        feature = "tokio-rustls-runtime-aws-lc-rs",
        feature = "tokio-rustls-runtime-ring"
    ))]
    async fn a_permanent_reconnect_failure_ends_the_engine() {
        let (mut engine, _injected, _rx, _keep) = engine_on_a_failed_connection().await;
        engine.injected_permanent = true;
        engine.reconnect_failures.store(3, Ordering::SeqCst);
        let ended = tokio::time::timeout(Duration::from_secs(10), engine.engine())
            .await
            .expect("a permanent failure was retried");
        assert!(
            matches!(
                ended,
                Err(Error::ServiceDiscovery(
                    crate::error::ServiceDiscoveryError::NotFound
                ))
            ),
            "{ended:?}"
        );
        assert_eq!(engine.reconnect_failures.load(Ordering::SeqCst), 2);
    }

    /// FORK-PATCH (3rdi): a connection that fails while a corrupt-window
    /// resubscribe waits out a long backoff reconnects at once, not when
    /// that resubscribe is due.
    #[tokio::test]
    #[cfg(any(
        feature = "tokio-runtime",
        feature = "tokio-rustls-runtime-aws-lc-rs",
        feature = "tokio-rustls-runtime-ring"
    ))]
    async fn a_failed_connection_does_not_wait_for_a_pending_corrupt_window_resubscribe() {
        let (mut engine, mut injected, _rx, _keep) = engine_on_a_failed_connection().await;
        engine.forced.due = Some(Instant::now() + Duration::from_secs(3600));
        engine.forced.pending = Some(Pending::CorruptWindow);
        engine.reconnect_failures.store(1, Ordering::SeqCst);
        run_until_injected(&mut engine, &mut injected, 1).await;
    }

    /// FORK-PATCH (3rdi): a reconnect off a failed connection still sends
    /// CloseConsumer, but does not wait out the operation timeout for an
    /// answer a broker that is gone never sends (upstream did: 30 s by
    /// default before the consumer could look its topic up again).
    #[tokio::test]
    #[cfg(any(
        feature = "tokio-runtime",
        feature = "tokio-rustls-runtime-aws-lc-rs",
        feature = "tokio-rustls-runtime-ring"
    ))]
    async fn a_reconnect_off_a_failed_connection_does_not_wait_out_the_close() {
        use crate::{
            connection_manager::{tests::silent_broker, OperationRetryOptions},
            consumer::queue::receive_queue,
            Pulsar, TokioExecutor,
        };

        let (url, mut seen) = silent_broker::listen().await;
        let client = Pulsar::builder(url, TokioExecutor)
            .with_operation_retry_options(OperationRetryOptions {
                operation_timeout: Duration::from_secs(60),
                ..Default::default()
            })
            .build()
            .await
            .unwrap();
        let conn = client.manager.get_base_connection().await.unwrap();
        conn.fail_for_test();

        let (tx, _rx) = receive_queue(10, 1 << 20);
        let (_messages_tx, messages_rx) = mpsc::unbounded();
        let (_engine_tx, engine_rx) = mpsc::unbounded();
        let mut engine = ConsumerEngine::new(
            client,
            conn,
            "persistent://t/n/x".to_string(),
            "s".to_string(),
            SubType::Exclusive,
            1,
            None,
            tx,
            messages_rx,
            engine_rx,
            10,
            None,
            None,
            ConsumerOptions::default(),
        );
        // stop the reconnect right after the close, before the lookup
        engine.reconnect_failures.store(1, Ordering::SeqCst);
        let reconnected = tokio::time::timeout(Duration::from_secs(10), engine.reconnect())
            .await
            .expect("the reconnect waited out the close's operation timeout");
        assert!(reconnected.is_err());

        let mut closed = false;
        while let Ok(cmd) = seen.try_recv() {
            closed |= cmd.close_consumer.is_some();
        }
        assert!(closed, "the best-effort close was sent");
    }

    /// FORK-PATCH (3rdi): on a live connection the close still waits for
    /// the broker's answer, however slow (up to the operation timeout); only
    /// a failed connection caps it.
    #[tokio::test]
    #[cfg(any(
        feature = "tokio-runtime",
        feature = "tokio-rustls-runtime-aws-lc-rs",
        feature = "tokio-rustls-runtime-ring"
    ))]
    async fn a_reconnect_off_a_live_connection_waits_for_the_close() {
        use futures::SinkExt;
        use tokio::net::TcpListener;

        use crate::{
            connection_manager::OperationRetryOptions,
            consumer::queue::receive_queue,
            message::{BaseCommand, Codec, Message},
            proto::{CommandConnected, CommandSuccess},
            Pulsar, TokioExecutor,
        };

        // a broker that holds its answer to CloseConsumer until the test
        // releases it
        let (release_tx, release_rx) = futures::channel::oneshot::channel::<()>();
        let release = Arc::new(std::sync::Mutex::new(Some(release_rx)));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let release = Arc::clone(&release);
                tokio::spawn(async move {
                    let mut framed = tokio_util::codec::Framed::new(stream, Codec);
                    while let Some(Ok(msg)) = framed.next().await {
                        let reply = if msg.command.connect.is_some() {
                            BaseCommand {
                                r#type: 3,
                                connected: Some(CommandConnected::default()),
                                ..Default::default()
                            }
                        } else if let Some(close) = msg.command.close_consumer {
                            let held = release.lock().unwrap().take();
                            if let Some(held) = held {
                                let _ = held.await;
                            }
                            BaseCommand {
                                r#type: 13,
                                success: Some(CommandSuccess {
                                    request_id: close.request_id,
                                    ..Default::default()
                                }),
                                ..Default::default()
                            }
                        } else {
                            continue;
                        };
                        let reply = Message {
                            command: reply,
                            payload: None,
                        };
                        if framed.send(reply).await.is_err() {
                            return;
                        }
                    }
                });
            }
        });
        let client = Pulsar::builder(format!("pulsar://127.0.0.1:{port}"), TokioExecutor)
            .with_operation_retry_options(OperationRetryOptions {
                operation_timeout: Duration::from_secs(60),
                ..Default::default()
            })
            .build()
            .await
            .unwrap();
        let conn = client.manager.get_base_connection().await.unwrap();
        assert!(conn.is_valid());

        let (tx, _rx) = receive_queue(10, 1 << 20);
        let (_messages_tx, messages_rx) = mpsc::unbounded();
        let (_engine_tx, engine_rx) = mpsc::unbounded();
        let mut engine = ConsumerEngine::new(
            client,
            conn,
            "persistent://t/n/x".to_string(),
            "s".to_string(),
            SubType::Exclusive,
            1,
            None,
            tx,
            messages_rx,
            engine_rx,
            10,
            None,
            None,
            ConsumerOptions::default(),
        );
        // stop the reconnect right after the close, before the lookup
        engine.reconnect_failures.store(1, Ordering::SeqCst);

        // virtual time from here: past the failed-connection cap, the
        // reconnect still waits for the live connection's answer
        tokio::time::pause();
        let reconnect = engine.reconnect();
        futures::pin_mut!(reconnect);
        assert!(futures::poll!(reconnect.as_mut()).is_pending());
        tokio::time::advance(DEAD_CONNECTION_CLOSE_TIMEOUT * 2).await;
        assert!(
            futures::poll!(reconnect.as_mut()).is_pending(),
            "the reconnect did not wait for the live connection's close"
        );

        // the broker answers: the reconnect goes on (and stops at the
        // injected failure)
        tokio::time::resume();
        release_tx.send(()).unwrap();
        let done = tokio::time::timeout(Duration::from_secs(10), reconnect)
            .await
            .expect("the reconnect did not complete after the answer");
        assert!(done.is_err());
    }

    /// FORK-PATCH (3rdi): two engines sharing one failed connection (a
    /// multi-topic consumer, pool members) must both reconnect, not only the
    /// one that read the error (prophesee-service-rs #284).
    #[tokio::test]
    #[cfg(any(
        feature = "tokio-runtime",
        feature = "tokio-rustls-runtime-aws-lc-rs",
        feature = "tokio-rustls-runtime-ring"
    ))]
    async fn every_engine_on_a_failed_connection_reconnects() {
        use crate::connection_manager::tests::silent_broker;

        let (manager, broker) = silent_broker::manager().await;
        let shared = manager.get_connection(&broker).await.unwrap();
        assert!(!connection_failed(&shared), "a live connection is kept");
        silent_broker::fail(&shared).await;

        assert!(connection_failed(&shared), "the first engine reconnects");
        assert!(connection_failed(&shared), "the second engine reconnects");
    }

    /// Models the broker's permit counter beside the engine's window: the
    /// broker debits the declared count, the engine books the same, and
    /// every flow the engine sends is added to both. Whatever positive count
    /// a publisher declares, the two stay equal, every request is a valid
    /// Java `int` above zero, and the window ends at least half full.
    #[test]
    fn the_window_mirrors_the_brokers_permits() {
        const BATCH: u32 = 1000;
        for count in [1, 1000, 70_000, i32::MAX] {
            let mut broker = i64::from(BATCH) - i64::from(count);
            let mut window = i64::from(BATCH) - permits_consumed(Some(count));
            let mut flows = 0;
            while needs_flow(BATCH, window) {
                let permits = flow_permits(BATCH, window);
                assert!(
                    (1..=i32::MAX as u32).contains(&permits),
                    "{count}: {permits}"
                );
                broker += i64::from(permits);
                window += i64::from(permits);
                flows += 1;
                assert!(flows <= 2, "{count}: {flows} flows");
            }
            assert_eq!(window, broker, "{count}");
            assert!(window >= i64::from(BATCH.div_ceil(2)), "{count}: {window}");
        }
    }

    /// A declared count of zero or less is booked as one permit, so the
    /// window still drains and refills.
    #[test]
    fn a_non_positive_count_is_booked_as_one_permit() {
        for count in [0, -1, i32::MIN] {
            assert_eq!(permits_consumed(Some(count)), 1, "{count}");
        }
        assert_eq!(permits_consumed(None), 1);
    }

    /// The first non-positive-count entry is logged, the next ones only
    /// once the interval has passed: one line per interval, not per entry.
    #[test]
    fn non_positive_count_warnings_are_rate_limited() {
        let t0 = Instant::now();
        assert!(warn_due(None, t0), "the first is logged");
        assert!(!warn_due(Some(t0), t0), "an immediate repeat is not");
        let almost = t0 + NON_POSITIVE_COUNT_WARN_INTERVAL - Duration::from_millis(1);
        assert!(!warn_due(Some(t0), almost), "nor one inside the interval");
        assert!(
            warn_due(Some(t0), t0 + NON_POSITIVE_COUNT_WARN_INTERVAL),
            "one at the interval is"
        );
    }

    /// The forced-resubscribe delay doubles from 100 ms and caps at 60 s,
    /// never overflowing however many attempts.
    #[test]
    fn the_forced_resubscribe_backoff_doubles_and_caps() {
        assert_eq!(
            forced_resubscribe_backoff(0, FORCED_RESUBSCRIBE_INITIAL_BACKOFF),
            Duration::from_millis(100)
        );
        assert_eq!(
            forced_resubscribe_backoff(1, FORCED_RESUBSCRIBE_INITIAL_BACKOFF),
            Duration::from_millis(200)
        );
        assert_eq!(
            forced_resubscribe_backoff(9, FORCED_RESUBSCRIBE_INITIAL_BACKOFF),
            Duration::from_millis(51_200)
        );
        assert_eq!(
            forced_resubscribe_backoff(10, FORCED_RESUBSCRIBE_INITIAL_BACKOFF),
            Duration::from_secs(60)
        );
        assert_eq!(
            forced_resubscribe_backoff(u32::MAX, FORCED_RESUBSCRIBE_INITIAL_BACKOFF),
            Duration::from_secs(60)
        );
        assert!(FORCED_RESUBSCRIBE_RESET_AFTER > FORCED_RESUBSCRIBE_MAX_BACKOFF);
    }

    /// Only a single-active-consumer subscription with a non-positive count
    /// needs the forced resubscribe.
    #[test]
    fn only_a_single_active_consumer_window_needs_a_resubscribe() {
        for sub_type in [SubType::Exclusive, SubType::Failover] {
            for count in [0, -1, i32::MIN] {
                assert!(
                    forged_count_corrupts_window(sub_type, Some(count)),
                    "{sub_type:?} {count}"
                );
            }
            assert!(!forged_count_corrupts_window(sub_type, Some(1)));
            assert!(!forged_count_corrupts_window(sub_type, None));
        }
        for count in [0, -1, i32::MIN] {
            assert!(!forged_count_corrupts_window(SubType::Shared, Some(count)));
            assert!(!forged_count_corrupts_window(
                SubType::KeyShared,
                Some(count)
            ));
        }
    }

    const INITIAL: Duration = FORCED_RESUBSCRIBE_INITIAL_BACKOFF;

    /// Runs `forced` through one resubscribe at `now`: schedules, returns
    /// the delay, and clears `due` as `reconnect` does.
    fn resubscribe_at(forced: &mut ForcedResubscribe, now: Instant) -> Duration {
        let delay = forced.schedule(now, INITIAL).expect("nothing pending");
        forced.due = None;
        delay
    }

    /// A second schedule while one is pending changes nothing.
    #[test]
    fn a_pending_forced_resubscribe_coalesces() {
        let t0 = Instant::now();
        let mut forced = ForcedResubscribe::default();
        assert_eq!(forced.schedule(t0, INITIAL), Some(INITIAL));
        let due = forced.due;
        assert_eq!(
            forced.schedule(t0 + Duration::from_millis(50), INITIAL),
            None
        );
        assert_eq!(forced.due, due, "the pending one stands");
        assert_eq!(forced.attempt, 1, "and the backoff did not advance");
    }

    /// Back-to-back resubscribes back off 100 ms, 200 ms, ... up to 60 s;
    /// 119 s of silence keeps the cap, 121 s starts over at 100 ms.
    #[test]
    fn the_forced_resubscribe_schedule_backs_off_and_resets() {
        let mut now = Instant::now();
        let mut forced = ForcedResubscribe::default();
        let mut delays = Vec::new();
        for _ in 0..12 {
            let delay = resubscribe_at(&mut forced, now);
            delays.push(delay.as_millis());
            now += delay;
        }
        assert_eq!(
            delays,
            [100, 200, 400, 800, 1600, 3200, 6400, 12800, 25600, 51200, 60000, 60000]
        );
        // `now` is when the last one fired; each later one fires `max` on.
        let max = FORCED_RESUBSCRIBE_MAX_BACKOFF;
        now += Duration::from_secs(119);
        assert_eq!(resubscribe_at(&mut forced, now), max);
        now += max + Duration::from_secs(120);
        assert_eq!(resubscribe_at(&mut forced, now), max, "exactly 120 s");
        now += max + Duration::from_secs(121);
        assert_eq!(resubscribe_at(&mut forced, now), INITIAL);
        assert_eq!(resubscribe_at(&mut forced, now), 2 * INITIAL);
    }

    /// A reconnect slower than the quiet period keeps the backoff.
    #[test]
    fn a_slow_forced_resubscribe_keeps_the_backoff() {
        let (max, mut now) = (FORCED_RESUBSCRIBE_MAX_BACKOFF, Instant::now());
        let mut forced = ForcedResubscribe {
            attempt: 20,
            ..Default::default()
        };
        assert_eq!(resubscribe_at(&mut forced, now), max);
        now += max + Duration::from_secs(90); // fired, then reconnected 90 s
        forced.completed(now);
        now += Duration::from_secs(119);
        assert_eq!(resubscribe_at(&mut forced, now), max);
    }

    /// One slower than the quiet period that then fails keeps it too.
    #[test]
    fn a_slow_failed_forced_resubscribe_keeps_the_backoff() {
        let (max, mut now) = (FORCED_RESUBSCRIBE_MAX_BACKOFF, Instant::now());
        let mut forced = ForcedResubscribe {
            attempt: 20,
            ..Default::default()
        };
        assert_eq!(resubscribe_at(&mut forced, now), max);
        now += max + Duration::from_secs(121); // fired, failed 121 s later
        assert_eq!(forced.retry(now, INITIAL), Some(max));
    }
}
