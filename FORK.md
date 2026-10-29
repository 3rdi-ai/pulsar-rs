# 3rdi-ai fork of pulsar-rs

This is a 3rdi AI fork of [streamnative/pulsar-rs](https://github.com/streamnative/pulsar-rs),
licensed as upstream (MIT OR Apache-2.0; see `LICENSE-APACHE`, `LICENSE-MIT`, `COPYRIGHT`).

## Why it exists

Upstream `pulsar` 6.9.1 (the latest release) requires `zstd ^0.13.2` under its
`compression` feature. zstd 0.13 / zstd-safe 7 / zstd-sys < 2.1.2 carry
RUSTSEC-2026-0391, -0398, -0403 and -0410 (among others), fixed only on
zstd >= 0.14.3. The fork began as that one change (`zstd = "^0.14.3"`, plus
a zstd round-trip test) and has since gained consume-path hardening, with
additive API and changed behaviour, listed per release below.

Since `v6.9.1-3rdi.2` the fork also **bounds decompression on the consume
path** (`src/consumer/decompress.rs`). Upstream decodes zlib/zstd/snappy to
completion and trusts the producer-supplied `uncompressed_size` for lz4, so
one small compressed message can expand to gigabytes (a decompression bomb).
Every codec now decodes through a `take(limit + 1)` reader capped at
`DEFAULT_MAX_DECOMPRESSED_SIZE` (5 MiB, the broker's default
`maxMessageSize`), a declared `uncompressed_size` over the cap is refused
before decoding, and the decoded length must equal the declared size. A refused
message surfaces as `ConsumerError::Io(InvalidData)` on the consumer stream,
never a panic or an allocation past the cap (the Java client likewise discards
a message whose uncompressed size exceeds the max).

Since `v6.9.1-3rdi.3` a refused message is **settled**, never left unacked
(an unacked refusal is redelivered on every reconnect and, on a Shared
subscription, piles up until `maxUnackedMessagesPerConsumer` stalls the
consumer). With a `DeadLetterPolicy` the message goes to the dead-letter topic
as it arrived (still compressed), tagged `DECODE_REFUSAL` (`over_cap`,
`size_mismatch` or `undecodable`), `ORIGINAL_COMPRESSION` and
`ORIGINAL_UNCOMPRESSED_SIZE`; then it is acknowledged either way (a failed
dead-letter send leaves it unacked for redelivery). The consumer stream yields
the typed `ConsumerError::Refused { reason: DecodeRefusal, dead_lettered }`.
The cap is `ConsumerOptions::max_decompressed_size` (default
`DEFAULT_MAX_DECOMPRESSED_SIZE`, 5 MiB), so a topic with large legitimate
messages can raise it on purpose.

## Unreleased

- **A failed connection stays failed** (FORK-PATCH in `src/error.rs`
  `SharedError::remove` and `src/consumer/engine.rs` `connection_failed`):
  upstream `Connection::error()` cleared the connection's error flag, so
  after a request timed out (a broker restart) the consumer engine that
  read the error made the dead connection `is_valid()` again. The
  connection manager then kept handing it to every lookup (minutes of
  `timedout sending message` until a ping timeout), and every other engine
  on that connection stopped reconnecting. Reading the error now leaves the
  flag set, the manager replaces the connection, and every engine on an
  invalid connection reconnects, whether or not it took the error
  (prophesee-service-rs #284). Drop when upstream stops clearing the flag.
- **Only a failed keepalive ping retires a connection** (FORK-PATCH in
  `src/connection.rs` `send_message`): upstream also set the
  connection-wide error when a single request timed out, so one slow
  lookup retired the connection for every producer and consumer on it.
  The slow request now fails alone; the keepalive ping
  (`ConnectionRetryOptions::keep_alive`, 60 s by default, timing out after
  the operation timeout) declares a connection dead, as the Java client
  does. Safe now that a dead connection can no longer be revived.
- **One replacement connection per broker** (FORK-PATCH in
  `src/connection_manager.rs` `get_connection`): a caller that finds no
  connection, or an invalid one, marks the broker `Connecting` under the
  pool lock before dialing, so concurrent callers queue for its
  connection; upstream dialed one connection per caller. The dial runs as
  a detached task that every caller, the claimer included, waits on, so
  cancelling any caller (an application timeout, an aborted task, a lost
  `select!`) strands no one; an error, a panic or a failed spawn resolves
  every waiter and clears the entry. Waiters get a copy of the dial's own
  error (same variant, same I/O kind) instead of upstream's `Canceled`, so
  they classify the retry the same way.
- **A failed reconnect is retried, never the end of the consumer**
  (FORK-PATCH in `src/consumer/engine.rs` `engine`): upstream returned the
  error when the reconnect off a dead connection failed (a lookup during a
  broker restart), so the consumer died. A transient failure (I/O or a
  timeout, a dropped connection, ServiceNotReady, ConsumerBusy, a lookup
  MetadataError or TooManyRequests) is now retried on the
  forced-resubscribe backoff (100 ms doubling to 60 s, the Java client's),
  and no flow is sent on the dead connection meanwhile. A permanent one
  (topic not found or terminated, not authorized, incompatible schema, a
  TLS failure such as InvalidData or PermissionDenied, a DNS failure)
  still ends the engine, so it reaches the user instead of retrying
  forever. DNS failure ends it as upstream does: our clients reach the
  brokers through the stable proxy Service, so a restarted broker pod's
  headless DNS name never fails for them. Deferred: a bounded DNS retry
  for deployments that reach brokers directly. A pending corrupt-window resubscribe never delays the reconnect
  off a failed connection, and each retry logs its own reason
  (prophesee-service-rs #284).

- **A reconnect off a failed connection does not wait out the close**
  (FORK-PATCH in `src/consumer/engine.rs` `reconnect`): upstream sent
  CloseConsumer on the dead connection and waited out the whole operation
  timeout (30 s by default) before looking the topic up again, the slow
  reattach measured after broker restarts (prophesee-service-rs #284). On a
  failed connection the close is now best effort, bounded by 1 s
  (`DEAD_CONNECTION_CLOSE_TIMEOUT`): a broker still behind the socket
  answers within it, so an exclusive subscription is released before it is
  taken again.

- **A forced resubscribe that fails for good ends the consumer**
  (FORK-PATCH in `src/consumer/engine.rs` `engine`): a corrupt-window
  resubscribe whose reconnect failed permanently (topic gone or
  terminated, not authorized, ...) was retried on the backoff forever,
  the consumer silent. It now ends the engine, as a connection retry that
  fails for good does; a transient failure is still retried. In both
  cases the error is queued for the consumer (kept aside when the queue
  is full, and yielded after it drains): a single-topic consumer's
  stream yields it and then ends (upstream's stream just ended, the error
  only logged by the engine task). A multi-topic consumer still logs the
  error and drops that topic (`consumer/multi.rs`, upstream behaviour for
  any error item, refused messages included); passing it to the user
  there is a separate, larger change.
- **A reply nobody waits for is dropped; a request that ends unanswered
  forgets its resolver** (FORK-PATCH in `src/connection.rs` `Receiver`,
  `PendingRequest`): now that a timeout no longer retires the connection,
  an unanswered request would keep its resolver for the connection's
  lifetime, and upstream kept every unmatched reply forever too. A request
  that ends without its answer (a timeout, its future dropped, even while
  it waits for room on the data channel) drops its receiver end and then
  cancels its registration (`Register::CancelRequest`); the receiver
  forgets that resolver only (one whose receiver is gone), never a live
  one a resend registered since under the same key. A request registers
  before it is written, so a reply can only be read ahead of a
  registration still in the channel: before routing an unmatched reply
  the receiver drains the registrations once more, and a reply still
  unmatched after that is stale and dropped. This also settles upstream's
  CloseConsumer FIXME and keeps a resend's receipt deliverable. One
  exception is kept: a producer's exclusive-access answer comes
  unrequested and its waiter registers only after the first answer, so an
  unmatched `producer_success` is kept for it, as upstream did.
  `SerialId` is an `AtomicU64`, which needs 64-bit atomics on the target
  (every target we build for has them; one without would not compile).
- **Key_Shared recovers from a non-positive batch count on a 4.0.x
  broker** (FORK-PATCH in `src/consumer/engine.rs`
  `forged_count_corrupts_window`): a 4.0.x broker (4.0.8 is what the Helm
  chart 4.4.0 that Scania runs ships) delivers an entry declaring a
  non-positive count on Key_Shared and debits it, which wraps its permit
  window (measured: `availablePermits` -2147482648 after `i32::MIN`) and
  stalls dispatch. The client refuses the entry, as before, and now also
  recovers with one forced resubscribe, as on Exclusive/Failover.
  Measured broker matrix (`pulsar standalone`, forged `i32::MIN` and `-1`
  on Key_Shared):

  | broker | the entry | outcome with this change |
  |---|---|---|
  | 3.3.0 | never delivered | dispatch stalls (no client-side repair) |
  | 4.0.5 | delivered, window wrapped | refused, one resubscribe, recovers |
  | 4.0.8 | delivered, window wrapped | refused, one resubscribe, recovers |
  | 5.0.0 | never delivered | dispatch stalls (no client-side repair) |

  The Key_Shared test asserts the measured outcome per version and takes
  an explicit, logged "unmeasured broker version" skip on any other.
  **The cost:** each forced resubscribe closes this consumer, so the
  broker moves its hash ranges to the other consumers and back, redelivers
  its unacknowledged messages to them, and while it drains them can
  briefly hold dispatch to the others. A publisher that keeps forging such
  entries therefore causes churn across the whole subscription (bounded by
  the forced-resubscribe backoff, at most one a minute per consumer once
  it backs off). That still beats the alternative on these brokers: a
  permanent stall of the subscription that only a restart repairs. The
  publisher trust boundary remains the defence (the producer-trust
  statement under `.8`).
- **Known on a 3.3.0 broker (not one we run the Rust client against):** on
  Shared, an entry declaring a count above the 5000-member cap (70000,
  `i32::MAX`) leaves the consumer `blockedConsumerOnUnackedMsgs` with 0
  unacked after the client refuses and acknowledges it, so dispatch stops;
  4.0.5, 4.0.8 and 5.0.0 do not. The two Shared tests fail on 3.3.0.
- **An abandoned dial gives up** (FORK-PATCH in `src/connection_manager.rs`
  `give_up_if_nobody_waits`): a detached dial whose callers all stopped
  waiting (every one cancelled) stops at its next retry instead of
  retrying for nobody. The check and the removal of its `Connecting` entry
  are one step under the pool lock, so a caller arriving after it finds
  no entry and starts a fresh dial (one arriving before keeps this dial
  going), and the dial that gave up never touches an entry it no longer
  owns. The check runs after each backoff, so an abandoned dial still makes
  the one full `Connection::new` attempt it is in (up to the connection
  timeout) and its backoff before it gives up.
- **OAuth2 and Basic credentials never reach formatted output**: the
  `Debug` and `Display` of `OAuth2Params` show the credentials URL as its
  scheme only (a `data:` URL embeds the client secret; a `file:` URL keeps
  its path), the private OAuth2 params and `BasicAuthentication` print
  `<redacted>` for the secret, and credentials-read and token-endpoint
  errors name the failure kind (parse position, an RFC 6749 error code by
  name or an extension code by length, a plain Content-Type, the transport
  kind such as connect/timeout/tls/dns, body length), never the URL or the
  token URL, a response body, a server-chosen code or an
  `error_description`. The issuer URL prints as scheme, host, port and
  path only, and discovery errors name their kind the same way. A source-scan test (`tests/debug_redaction_guard.rs`)
  fails if a type under `src/` that holds a credential derives `Debug`.

## v6.9.1-3rdi.9

Supersedes `v6.9.1-3rdi.8` (left in place, not deleted). No new public
items.

- **Session generations** (fixes a pre-existing upstream defect): every
  resubscribe (a connection error, a broker `CloseConsumer`, or the forced
  resubscribe below) starts a new session. Messages the old session already
  dispatched, still in the engine's channel or already in the receive queue,
  are dropped: not delivered and not debited against the new session's
  permit window; the broker redelivers them on the new session (as the Java
  client's `clearReceiverQueue`). Before this, each reconnect over-granted
  the broker up to `batch_size` permits and delivered those messages twice.
  A message the application had already taken before the reconnect and not
  acknowledged is still redelivered (at-least-once, as before). An error
  item already queued (a refusal notice) is still yielded.
- **Exclusive/Failover recovery from a non-positive batch count**: once the
  refused entry is acknowledged, the consumer resubscribes, which gives the
  broker a fresh permit window (`i32::MIN` no longer stalls the
  subscription). The resubscribe backs off per consumer: 100 ms, doubling,
  capped at 60 s (the Java client's reconnection backoff), and starts over
  only after a quiet period of more than 120 s measured from when the last
  resubscribe fired (or, if its reconnect was slow, completed or failed).
  A publisher forging entries continuously rides the cap: about one
  resubscribe a minute. The worst case is a burst cycle: k resubscribes
  back to back fire over 0.1 * (2^k - 1) s, then just over 120 s of quiet
  restarts the backoff, so the cycle repeats every 0.1 * (2^k - 1) + 120 s.
  That peaks at k = 8 (100 ms up to 12.8 s, 25.5 s of firing): at most
  about 3.3 resubscribes a minute (k = 7: 3.17, k = 9: 3.16, k = 10:
  2.70). Every resubscribe redelivers
  the consumer's whole unacknowledged set. A failed dead-letter send leaves
  the entry unacknowledged and schedules no resubscribe (which would
  redeliver, refuse and resubscribe in a loop); a failed ack schedules none
  either. A forced resubscribe that itself fails is retried on the backoff
  instead of ending the consumer. Any resubscribe (a connection error too)
  cancels a pending forced one and clears the unacked-redelivery timers of
  the old session (as `clearReceiverQueue`). The ack and the resubscribe's
  `CloseConsumer` go out in that order on the same connection. Like any
  reconnect, the resubscribe redelivers what is unacknowledged at that
  moment, including messages whose ack the engine had not sent yet
  (at-least-once). **Shared and Key_Shared** are unchanged: against a 5.0.0
  standalone broker, an entry declaring `-1` or `i32::MIN` on Key_Shared is
  never delivered and nor is any ordinary message after it (measured: 0 of
  20 arrived), the same as Shared. The client never sees the entry, so no
  client-side change can repair it (the producer-trust statement under .8
  stands).
- **Receive queue**: each item is measured once, on send, and the receiver
  releases exactly that (the measure travels with the item).
- **Redaction**: an `UnexpectedResponse` / `Unexpected` connection error, the
  "unexpected message" warning and the engine's per-message debug line print
  the command through the same redaction as `Message`'s `Debug`, so a
  broker's `AuthChallenge` data never reaches an error string or a log.
- **Planned follow-ups**: send the acks still queued in the engine before a
  forced resubscribe's close (fewer redeliveries); move the test suite's
  `Pulsar::builder` constructors onto `test_utils::new_pulsar` (one fixture
  constructor); a source-scan guard on log content, if a raw-command log
  recurs.

## v6.9.1-3rdi.8

Supersedes `v6.9.1-3rdi.7` (left in place, not deleted). Fixes from the
security review of .7:

- **New public items** (additive): `ConsumerOptions::max_batch_envelope_bytes`
  / `with_max_batch_envelope_bytes`, default `DEFAULT_MAX_BATCH_ENVELOPE_BYTES`
  (64 MiB, the same value as the queue budget but its own knob); a setting
  below `MIN_MAX_BATCH_ENVELOPE_BYTES` (64 KiB; 0 would refuse every batch)
  is raised to it. The members' envelope copies are bounded by THIS, no
  longer by `max_decompressed_size`, so the two caps no longer interact: an
  honest end-to-end-encrypted batch of 5000 members carrying RSA-4096-wrapped
  data keys is accepted at the defaults (under .7 it was refused). Measured
  peak memory per message: the received bytes, plus `max_decompressed_size`
  (the decompressed table), plus about 8x `max_decompressed_size` (the
  members' own metadata and payloads cut from it, as allocated), plus
  `max_batch_envelope_bytes` of envelope copies.
  `max_queued_bytes = 0` does not deadlock (an empty queue admits one
  payload), so it is documented, not clamped.
- **Structural measure**: a metadata's cost is its encoded length plus the
  in-memory struct of every repeated element (`properties`, `replicate_to`,
  `encryption_keys` and each key's own `metadata`). `encoded_len` alone
  under-measured many EMPTY entries about 35x (20k empty `encryption_keys`:
  about 40 KB encoded, 1.44 MB per clone). The same measure bounds the
  envelope copies and the receive queue.
- **Receive queue**: a payload's budget cost is its data PLUS its metadata,
  so zero-data payloads with large properties are no longer free. Dropping
  the consumer while the engine waits on the budget now wakes the engine,
  whose send fails, so the engine stops and sends `CloseConsumer` (before
  this it parked forever and the broker kept a zombie consumer).
- **Non-positive batch counts**: an entry declaring zero or fewer members is
  refused and acknowledged (as in .7), booked as one permit, and logged at
  most once a minute per consumer (no per-message flood).
  **Producer-trust limit**: a forged count still corrupts the BROKER's permit
  window, which no client booking repairs. On Exclusive/Failover the
  single-active-consumer dispatcher credits a negative count (over-granting)
  and `i32::MIN` overflows its window, stalling the subscription. On
  **Shared** (the default for `prophesee-service-rs` consumers) the entry is
  never delivered and no later message was either, against a 5.0.0
  standalone broker; that the dispatcher skips an entry whose message count
  is not positive is INFERRED from that behaviour, not read from the broker
  source. The defence is the publisher trust boundary: the mesh plus Pulsar
  publish authorization.
  (Exclusive/Failover recovery: see `v6.9.1-3rdi.9`.)

## v6.9.1-3rdi.7

Supersedes `v6.9.1-3rdi.6` (left in place, not deleted).

**New public items** (all additive):

- `ConsumerOptions::max_batch_members` / `with_max_batch_members`, and
  `consumer::DEFAULT_MAX_BATCH_MEMBERS` (5_000).
- `ConsumerOptions::max_queued_bytes` / `with_max_queued_bytes`, and
  `consumer::DEFAULT_MAX_QUEUED_BYTES` (64 MiB).

**Three new batch refusals**, all checked before decompression and all
settled like any refusal (`DECODE_REFUSAL=undecodable`, dead-lettered when a
`DeadLetterPolicy` is set, then acknowledged):

- a batch declaring more members than `max_batch_members` (or fewer than one);
- a batch whose members would copy past `max_decompressed_size`: every member
  gets its own copy of the envelope metadata, so the bound is
  `members x (encoded envelope without its member-owned properties,
  partition_key and ordering_key + size_of MessageMetadata + size_of
  MessageIdData)`. The two caps interact: raising `max_batch_members` past
  roughly `max_decompressed_size / 1 KiB` needs `max_decompressed_size`
  raised too. Peak memory per message is the received bytes plus about three
  times `max_decompressed_size`;
- a batch with bytes left over after its declared members. A conforming
  producer never writes these; a **non-conforming producer's batches are
  dead-lettered**, not partly delivered.

**Permit accounting** mirrors the broker (`Consumer.java` debits a batch's
raw `num_messages_in_batch`): the client debits the declared count exactly,
with no upper clamp, and caps each flow request at `i32::MAX` (the broker
rejects a non-positive grant). A declared count of zero or less is booked as
one permit with a `warn!`; the broker debited nothing for it, so the client's
window runs one short until the next reconnect re-grants it. Before this, a
batch declaring more than 65535 members stalled the consumer for good
(broker tests at 70000 and `i32::MAX` members cover it).

**Redaction**: the `Debug` output of `Message` and `Payload` carries no message
bodies (only `data_len` and `properties_len`) and no credentials
(`auth_data` and `original_auth_data` on connect, auth challenge/response,
lookup and partition-metadata commands print as `<redacted>`). Decoder errors
report the remaining input's length, never its bytes.

**Receive queue budget**: the decoded payloads queued for one consumer are
bounded by bytes as well as by `receiver_queue_size`. The engine waits
(without polling) before queueing a payload past `max_queued_bytes` and the
consumer releases the bytes as it takes each payload; an empty queue always
admits one payload, so a payload larger than the budget is still delivered.
Without it, 1000 queued 5 MiB bombs pinned about 5 GiB for a stalled consumer.

**CI note (for when the fork's CI is enabled)**: it needs a job with
`prophesee-service-rs`'s exact feature set (`default-features = false`,
`compression`, `tokio-rustls-runtime-aws-lc-rs`). RUSTSEC-2023-0071 (rsa)
comes in through a default feature only, so a default-features audit reports
an advisory our consumers never link.

Releases are tagged `v<upstream>-3rdi.<n>`, e.g. `v6.9.1-3rdi.1`.

## When to drop it

As soon as upstream publishes a `pulsar` release on zstd >= 0.14.3 **and** a
bounded consume-path decompression: point
consumers (`prophesee-service-rs`) back at crates.io and archive this fork.
