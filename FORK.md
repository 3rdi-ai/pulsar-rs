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

Releases are tagged `v<upstream>-3rdi.<n>`, e.g. `v6.9.1-3rdi.1`.

## When to drop it

As soon as upstream publishes a `pulsar` release on zstd >= 0.14.3 **and** a
bounded consume-path decompression: point
consumers (`prophesee-service-rs`) back at crates.io and archive this fork.
