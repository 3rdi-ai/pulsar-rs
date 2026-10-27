use std::collections::BTreeMap;

use crate::{
    consumer::initial_position::InitialPosition,
    message::proto::{MessageIdData, Schema},
};

/// Configuration options for consumers
#[derive(Clone, Default, Debug)]
pub struct ConsumerOptions {
    pub priority_level: Option<i32>,
    /// Signal whether the subscription should be backed by a
    /// durable cursor or not
    pub durable: Option<bool>,
    /// If specified, the subscription will position the cursor
    /// marked-delete position on the particular message id and
    /// will send messages from that point
    pub start_message_id: Option<MessageIdData>,
    /// Add optional metadata key=value to this consumer
    pub metadata: BTreeMap<String, String>,
    pub read_compacted: Option<bool>,
    pub schema: Option<Schema>,
    /// size of the receiver queue
    pub receiver_queue_size: Option<u32>,
    /// Signal whether the subscription will initialize on latest
    /// or earliest message (default on latest)
    ///
    /// an enum can be used to initialize it:
    ///
    /// ```rust,ignore
    /// ConsumerOptions {
    ///     initial_position: InitialPosition::Earliest,
    /// }
    /// ```
    pub initial_position: InitialPosition,
    /// The most bytes one message (a whole batch envelope) may decompress
    /// to; `None` means [`crate::consumer::DEFAULT_MAX_DECOMPRESSED_SIZE`]
    /// (5 MiB, the broker's default `maxMessageSize`). A message past it is
    /// refused and settled (dead-lettered or acknowledged), never decoded.
    /// Raise it on purpose for a topic with large legitimate messages.
    pub max_decompressed_size: Option<usize>,
    /// The most members one batch may declare; `None` means
    /// [`crate::consumer::DEFAULT_MAX_BATCH_MEMBERS`] (5_000, five times
    /// the producers' default batch). A batch past it is refused and
    /// settled like any undecodable message. A second bound applies with
    /// it: the members' copies of the envelope (about 500 bytes each plus
    /// the envelope's copied fields) must fit `max_decompressed_size`, so
    /// raising this cap past `max_decompressed_size / 1 KiB` or so needs
    /// that cap raised too.
    pub max_batch_members: Option<u32>,
}

impl ConsumerOptions {
    /// within options, sets the most bytes one message may decompress to
    /// (see [`ConsumerOptions::max_decompressed_size`])
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn with_max_decompressed_size(mut self, bytes: usize) -> Self {
        self.max_decompressed_size = Some(bytes);
        self
    }

    /// within options, sets the most members one batch may declare
    /// (see [`ConsumerOptions::max_batch_members`])
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn with_max_batch_members(mut self, members: u32) -> Self {
        self.max_batch_members = Some(members);
        self
    }

    /// within options, sets the priority level
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn with_priority_level(mut self, priority_level: i32) -> Self {
        self.priority_level = Some(priority_level);
        self
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn durable(mut self, durable: bool) -> Self {
        self.durable = Some(durable);
        self
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn starting_on_message(mut self, message_id_data: MessageIdData) -> Self {
        self.start_message_id = Some(message_id_data);
        self
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn with_metadata(mut self, metadata: BTreeMap<String, String>) -> Self {
        self.metadata = metadata;
        self
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn read_compacted(mut self, read_compacted: bool) -> Self {
        self.read_compacted = Some(read_compacted);
        self
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn with_schema(mut self, schema: Schema) -> Self {
        self.schema = Some(schema);
        self
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn with_initial_position(mut self, initial_position: InitialPosition) -> Self {
        self.initial_position = initial_position;
        self
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn with_receiver_queue_size(mut self, size: u32) -> Self {
        // todo: support zero_queue_size consumer
        self.receiver_queue_size = Some(if size == 0 { 1000 } else { size });
        self
    }
}
