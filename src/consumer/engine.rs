use std::{
    collections::{HashMap, HashSet},
    future::Future,
    sync::Arc,
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
/// Zero or negative (the broker credits instead) is booked as 1: the broker
/// then holds that many more permits than this window until a reconnect.
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
    tx: mpsc::Sender<Result<(MessageIdData, Payload), Error>>,
    messages_rx: Option<mpsc::UnboundedReceiver<RawMessage>>,
    engine_rx: Option<mpsc::UnboundedReceiver<EngineMessage<Exe>>>,
    event_rx: mpsc::UnboundedReceiver<EngineEvent<Exe>>,
    event_tx: UnboundedSender<EngineEvent<Exe>>,
    batch_size: u32,
    remaining_messages: i64,
    unacked_message_redelivery_delay: Option<Duration>,
    unacked_messages: HashMap<MessageIdData, Instant>,
    dead_letter_policy: Option<DeadLetterPolicy>,
    options: ConsumerOptions,
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
        tx: mpsc::Sender<Result<(MessageIdData, Payload), Error>>,
        messages_rx: mpsc::UnboundedReceiver<RawMessage>,
        engine_rx: mpsc::UnboundedReceiver<EngineMessage<Exe>>,
        batch_size: u32,
        unacked_message_redelivery_delay: Option<Duration>,
        dead_letter_policy: Option<DeadLetterPolicy>,
        options: ConsumerOptions,
    ) -> ConsumerEngine<Exe> {
        let (event_tx, event_rx) = mpsc::unbounded();
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
            unacked_message_redelivery_delay,
            unacked_messages: HashMap::new(),
            dead_letter_policy,
            options,
        }
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
            if !self.connection.is_valid() {
                if let Some(err) = self.connection.error() {
                    error!(
                        "Consumer: connection {} is not valid: {:?}",
                        self.connection.id(),
                        err
                    );
                    self.reconnect().await?;
                }
            }

            if let Some(messages_rx) = self.messages_rx.take() {
                self.register_source(messages_rx, |msg| EngineEvent::Message(msg))
                    .map_err(|_| {
                        Error::Custom(String::from("Error registering messages_rx source"))
                    })?;
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
            if needs_flow(self.batch_size, self.remaining_messages) {
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

            match Self::timeout(self.event_rx.next(), Duration::from_secs(1)).await {
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
                    debug!("consumer engine: timeout (1s)");
                }
                Ok(Some(EngineEvent::Message(msg))) => {
                    // The command only: the payload is publisher bytes.
                    debug!(
                        "consumer engine: received message, {:?}",
                        msg.as_ref().map(|m| &m.command)
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
                if let Some(count @ ..=0) = message
                    .payload
                    .as_ref()
                    .and_then(|p| p.metadata.num_messages_in_batch)
                {
                    warn!(
                        "{}: entry declares {count} messages; booked as 1 permit until reconnect",
                        self.debug_format()
                    );
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

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    async fn ack(&mut self, message_id: MessageIdData, cumulative: bool) {
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
        let id = message.message_id.clone();
        let payloads = match super::decompress::decode_payload(id, payload, limit, max_members) {
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
        self.ack(message_id, false).await;
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

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    async fn reconnect(&mut self) -> Result<(), Error> {
        debug!("reconnecting consumer for topic: {}", self.topic);

        // send CloseConsumer to server
        // remove our resolver from the Connection consumers BTreeMap,
        // stopping the current Message source
        if let Err(e) = self.connection.sender().close_consumer(self.id).await {
            error!(
                "could not close consumer {:?}({}) for topic {}: {:?}",
                self.name, self.id, self.topic, e
            );
        }
        // should have logged "rx terminated" by now

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

    #[cfg(feature = "async-std")]
    async fn timeout<F: Future<Output = O>, O>(
        fut: F,
        dur: Duration,
    ) -> Result<O, async_std::future::TimeoutError> {
        use async_std::prelude::FutureExt;
        fut.timeout(dur).await
    }

    #[cfg(all(not(feature = "async-std"), feature = "tokio"))]
    async fn timeout<F: Future<Output = O>, O>(
        fut: F,
        dur: Duration,
    ) -> Result<O, tokio::time::error::Elapsed> {
        tokio::time::timeout_at(tokio::time::Instant::now() + dur, fut).await
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

#[cfg(test)]
mod tests {
    use super::*;

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
}
