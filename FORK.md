# 3rdi-ai fork of pulsar-rs

This is a 3rdi AI fork of [streamnative/pulsar-rs](https://github.com/streamnative/pulsar-rs),
licensed as upstream (MIT OR Apache-2.0; see `LICENSE-APACHE`, `LICENSE-MIT`, `COPYRIGHT`).

## Why it exists

Upstream `pulsar` 6.9.1 (the latest release) requires `zstd ^0.13.2` under its
`compression` feature. zstd 0.13 / zstd-safe 7 / zstd-sys < 2.1.2 carry
RUSTSEC-2026-0391, -0398, -0403 and -0410 (among others), fixed only on
zstd >= 0.14.3. This fork changes exactly one thing: `zstd = "^0.14.3"`
(plus a zstd round-trip test). No API or behaviour change.

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

## v6.9.1-3rdi.8

Supersedes `v6.9.1-3rdi.7` (left in place, not deleted). Fixes from the
security review of .7:

- **New public item** (additive): `ConsumerOptions::max_batch_envelope_bytes`
  / `with_max_batch_envelope_bytes`, default `DEFAULT_MAX_QUEUED_BYTES`
  (64 MiB). The members' envelope copies are bounded by THIS, no longer by
  `max_decompressed_size`, so the two caps no longer interact: an honest
  end-to-end-encrypted batch of 5000 members carrying RSA-4096-wrapped data
  keys is accepted at the defaults (under .7 it was refused). Peak memory per
  message is the received bytes plus about twice `max_decompressed_size` plus
  `max_batch_envelope_bytes`.
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
  still refused and acknowledged, and the consumer then **resubscribes**
  (CloseConsumer + subscribe), which resets the broker's permits: the
  single-active-consumer dispatcher (Exclusive/Failover) credits permits on
  a negative count, and `i32::MIN` overflowed its window and stalled the
  subscription for good. One `warn!` per resubscribe. A **Shared**
  subscription's dispatcher never sends such an entry at all (it computes
  a non-positive message count and skips it), and the subscription stalls
  for good BROKER-side: against a 5.0.0 standalone broker, no later honest
  message was delivered. No client change reaches that.

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
