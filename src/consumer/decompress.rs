//! Payload decompression for the consumer engine, bounded so that a small
//! compressed frame from any publisher cannot expand into an OOM
//! (a decompression bomb). A message that would decompress past `limit`, or
//! whose output does not match its declared `uncompressed_size`, is refused
//! with a [`DecodeRefusal`] — a per-message error, never a panic and never
//! an unbounded allocation.
//!
//! A batch is bounded before it is decompressed or split: past
//! `max_members` members, or when its members' copies of the envelope would
//! cost more than `max_envelope_bytes` (each member copies the envelope
//! minus the fields it replaces, measured structurally by
//! [`metadata_footprint`], plus a `Metadata` and a `MessageIdData`), it is
//! refused. So one message peaks at about its received bytes plus twice
//! `limit` (the decompressed table, and the members' own metadata and
//! payloads cut from it) plus `max_envelope_bytes` of envelope copies.

use crate::{
    consumer::batched_message_iterator::BatchedMessageIterator,
    error::DecodeRefusal,
    message::{parse_batched_message, proto},
    Payload,
};

/// Pulsar's default broker `maxMessageSize` (5 MiB).
pub const DEFAULT_MAX_DECOMPRESSED_SIZE: usize = 5 * 1024 * 1024;

/// The default most members one batch may declare: five times the member
/// cap Pulsar producers default to (`batchingMaxMessages`, 1000). A full
/// batch of this many members fits the default envelope-copy bound
/// ([`DEFAULT_MAX_BATCH_ENVELOPE_BYTES`], 64 MiB) with up to about
/// 12 KiB of copied envelope per member, so an end-to-end-encrypted batch
/// carrying a few RSA-4096-wrapped data keys is accepted.
pub const DEFAULT_MAX_BATCH_MEMBERS: u32 = 5_000;

/// The default most bytes one batch's members may spend copying the
/// envelope: 64 MiB, the same value as the receive-queue budget but its
/// own knob.
pub const DEFAULT_MAX_BATCH_ENVELOPE_BYTES: usize = 64 * 1024 * 1024;

/// The least envelope-copy bound honoured: a smaller setting (0 would
/// refuse every batch) is raised to this.
pub const MIN_MAX_BATCH_ENVELOPE_BYTES: usize = 64 * 1024;

/// The envelope-copy bound for a configured `max_batch_envelope_bytes`:
/// the default when unset, never below [`MIN_MAX_BATCH_ENVELOPE_BYTES`].
pub(crate) fn envelope_bound(configured: Option<usize>) -> usize {
    configured
        .unwrap_or(DEFAULT_MAX_BATCH_ENVELOPE_BYTES)
        .max(MIN_MAX_BATCH_ENVELOPE_BYTES)
}

/// The fixed in-memory cost of one batch member beyond its copied envelope.
const MEMBER_OVERHEAD: usize =
    std::mem::size_of::<crate::message::Metadata>() + std::mem::size_of::<proto::MessageIdData>();

/// The envelope fields every batch member replaces with its own: held
/// aside so they are neither measured nor copied per member, and put back
/// when the message is refused (it is settled as it arrived).
struct MemberOwned {
    properties: Vec<proto::KeyValue>,
    partition_key: Option<String>,
    ordering_key: Option<Vec<u8>>,
}

impl MemberOwned {
    fn take(metadata: &mut crate::message::Metadata) -> Self {
        Self {
            properties: std::mem::take(&mut metadata.properties),
            partition_key: metadata.partition_key.take(),
            ordering_key: metadata.ordering_key.take(),
        }
    }

    fn restore(self, mut metadata: crate::message::Metadata) -> crate::message::Metadata {
        metadata.properties = self.properties;
        metadata.partition_key = self.partition_key;
        metadata.ordering_key = self.ordering_key;
        metadata
    }
}

/// The bytes one in-memory copy of `metadata` holds, beyond its own struct:
/// its encoded length (which bounds the heap its strings and bytes take)
/// plus the struct of every repeated element. An empty entry encodes in two
/// bytes but a clone allocates its whole struct, so `encoded_len` alone
/// under-measures a metadata of many empty entries by about 35 times.
pub(crate) fn metadata_footprint(metadata: &proto::MessageMetadata) -> usize {
    use std::mem::size_of;
    let keys: usize = metadata
        .encryption_keys
        .iter()
        .map(|key| {
            size_of::<proto::EncryptionKeys>() + key.metadata.len() * size_of::<proto::KeyValue>()
        })
        .sum();
    prost::Message::encoded_len(metadata)
        + metadata.properties.len() * size_of::<proto::KeyValue>()
        + metadata.replicate_to.len() * size_of::<String>()
        + keys
}

fn undecodable(e: impl std::fmt::Display) -> DecodeRefusal {
    DecodeRefusal::Undecodable(e.to_string())
}

/// Reads at most `limit + 1` bytes from `reader` (saturating, so a
/// `usize::MAX` cap cannot wrap to a zero-byte read); more than `limit` is
/// refused.
#[cfg(any(feature = "flate2", feature = "zstd", feature = "snap"))]
fn read_bounded(reader: impl std::io::Read, limit: usize) -> Result<Vec<u8>, DecodeRefusal> {
    use std::io::Read;
    let mut out = Vec::new();
    reader
        .take((limit as u64).saturating_add(1))
        .read_to_end(&mut out)
        .map_err(undecodable)?;
    if out.len() > limit {
        return Err(DecodeRefusal::OverCap { limit });
    }
    Ok(out)
}

/// One payload a message carries, with its (batch-indexed) id.
pub(crate) type Decoded = (proto::MessageIdData, Payload);

/// Decodes one broker message into the payloads it carries: decompressed
/// under `limit` and, for a batch envelope, split into at most
/// `max_members` members whose envelope copies total at most
/// `max_envelope_bytes` (measured by [`metadata_footprint`]).
///
/// Every way the message can fail to decode (an unknown codec, a refused
/// decompression, a member table that does not parse) is a [`DecodeRefusal`]
/// returned WITH the payload exactly as it arrived (still compressed), so the
/// engine settles it like any refusal instead of failing the consumer.
/// The refusal is boxed: it carries the whole payload.
pub(crate) fn decode_payload(
    message_id: proto::MessageIdData,
    payload: Payload,
    limit: usize,
    max_members: u32,
    max_envelope_bytes: usize,
) -> Result<Vec<Decoded>, Box<(Payload, DecodeRefusal)>> {
    let compression = match payload.metadata.compression {
        None => proto::CompressionType::None,
        Some(compression) => match proto::CompressionType::try_from(compression) {
            Ok(compression) => compression,
            Err(err) => {
                let reason = undecodable(format!("unknown compression type {compression}: {err}"));
                return Err(Box::new((payload, reason)));
            }
        },
    };
    let Some(count) = payload.metadata.num_messages_in_batch else {
        if compression == proto::CompressionType::None {
            return Ok(vec![(message_id, payload)]);
        }
        let declared = payload.metadata.uncompressed_size;
        return match decompress(compression, &payload.data, declared, limit) {
            Ok(data) => Ok(vec![(
                message_id,
                Payload {
                    data,
                    metadata: payload.metadata,
                },
            )]),
            Err(reason) => Err(Box::new((payload, reason))),
        };
    };
    // A batch of no members would deliver nothing, so the message would be
    // neither acknowledged nor refused and hold a permit forever.
    let Ok(count @ 1..) = u32::try_from(count) else {
        let reason = undecodable(format!("batch declares {count} messages"));
        return Err(Box::new((payload, reason)));
    };
    // The batch bounds need only the metadata, so they run before anything
    // is decompressed.
    if count > max_members {
        let reason = undecodable(format!(
            "batch declares {count} messages, past the {max_members}-member cap"
        ));
        return Err(Box::new((payload, reason)));
    }
    let Payload { data, mut metadata } = payload;
    let owned = MemberOwned::take(&mut metadata);
    // Every member copies what is left of the envelope, which the frame
    // alone bounds only to megabytes: the copies, not the member table, are
    // what a small batch would amplify into an OOM.
    let per_member = metadata_footprint(&metadata) + MEMBER_OVERHEAD;
    if (count as usize).saturating_mul(per_member) > max_envelope_bytes {
        let reason = undecodable(format!(
            "batch of {count} messages would copy {per_member} bytes each past the {max_envelope_bytes}-byte envelope bound"
        ));
        let metadata = owned.restore(metadata);
        return Err(Box::new((Payload { data, metadata }, reason)));
    }
    let decompressed = if compression == proto::CompressionType::None {
        None
    } else {
        match decompress(compression, &data, metadata.uncompressed_size, limit) {
            Ok(table) => Some(table),
            Err(reason) => {
                let metadata = owned.restore(metadata);
                return Err(Box::new((Payload { data, metadata }, reason)));
            }
        }
    };
    let table = decompressed.as_deref().unwrap_or(&data);
    match parse_batched_message(count, table) {
        Ok(members) => Ok(BatchedMessageIterator::from_members(
            message_id, metadata, count, members,
        )
        .collect()),
        Err(err) => {
            let metadata = owned.restore(metadata);
            Err(Box::new((Payload { data, metadata }, undecodable(err))))
        }
    }
}

/// Decompresses `data` per `compression`. `uncompressed_size` is the
/// message metadata's declared size; `limit` is the most bytes a message
/// may decompress to.
pub(crate) fn decompress(
    compression: proto::CompressionType,
    data: &[u8],
    uncompressed_size: Option<u32>,
    limit: usize,
) -> Result<Vec<u8>, DecodeRefusal> {
    if compression != proto::CompressionType::None {
        if let Some(declared) = uncompressed_size {
            if declared as usize > limit {
                return Err(DecodeRefusal::OverCap { limit });
            }
        }
    }

    let out: Result<Vec<u8>, DecodeRefusal> = match compression {
        proto::CompressionType::None => return Ok(data.to_vec()),
        proto::CompressionType::Lz4 => {
            #[cfg(not(feature = "lz4"))]
            {
                Err(undecodable(
                    "got a LZ4 compressed message but 'lz4' cargo feature is deactivated",
                ))
            }

            #[cfg(feature = "lz4")]
            {
                let declared = uncompressed_size.ok_or_else(|| {
                    undecodable("lz4 message without a declared uncompressed size")
                })?;
                // The decoder takes an `i32` size hint: past `i32::MAX` the
                // cast goes negative, so such a declaration is refused here.
                let hint = i32::try_from(declared).map_err(|_| {
                    undecodable(format!(
                        "lz4 declared uncompressed size {declared} exceeds i32::MAX"
                    ))
                })?;
                lz4::block::decompress(data, Some(hint)).map_err(undecodable)
            }
        }
        proto::CompressionType::Zlib => {
            #[cfg(not(feature = "flate2"))]
            {
                Err(undecodable(
                    "got a zlib compressed message but 'flate2' cargo feature is deactivated",
                ))
            }

            #[cfg(feature = "flate2")]
            {
                read_bounded(flate2::read::ZlibDecoder::new(data), limit)
            }
        }
        proto::CompressionType::Zstd => {
            #[cfg(not(feature = "zstd"))]
            {
                Err(undecodable(
                    "got a zstd compressed message but 'zstd' cargo feature is deactivated",
                ))
            }

            #[cfg(feature = "zstd")]
            {
                zstd::Decoder::new(data)
                    .map_err(undecodable)
                    .and_then(|d| read_bounded(d, limit))
            }
        }
        proto::CompressionType::Snappy => {
            #[cfg(not(feature = "snap"))]
            {
                Err(undecodable(
                    "got a Snappy compressed message but 'snap' cargo feature is deactivated",
                ))
            }

            #[cfg(feature = "snap")]
            {
                read_bounded(snap::read::FrameDecoder::new(data), limit)
            }
        }
    };
    let out = out?;

    if let Some(declared) = uncompressed_size {
        if out.len() != declared as usize {
            return Err(DecodeRefusal::SizeMismatch {
                declared,
                actual: out.len(),
            });
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    const LIMIT: usize = 1024 * 1024;
    const MEMBERS: u32 = DEFAULT_MAX_BATCH_MEMBERS;
    const ENVELOPE: usize = DEFAULT_MAX_BATCH_ENVELOPE_BYTES;

    fn bomb() -> Vec<u8> {
        vec![0u8; LIMIT * 8]
    }

    #[cfg(feature = "zstd")]
    #[test]
    fn zstd_bomb_over_the_limit_is_refused() {
        let c = zstd::encode_all(&bomb()[..], 3).unwrap();
        assert!(c.len() < 64 * 1024);
        let err = decompress(proto::CompressionType::Zstd, &c, None, LIMIT).unwrap_err();
        assert_eq!(err, DecodeRefusal::OverCap { limit: LIMIT });
    }

    #[cfg(feature = "zstd")]
    #[test]
    fn zstd_bomb_with_a_lying_declared_size_is_refused() {
        let c = zstd::encode_all(&bomb()[..], 3).unwrap();
        assert!(c.len() < 64 * 1024);
        let err = decompress(proto::CompressionType::Zstd, &c, Some(100), LIMIT).unwrap_err();
        assert_eq!(err, DecodeRefusal::OverCap { limit: LIMIT });
    }

    #[cfg(feature = "flate2")]
    #[test]
    fn zlib_bomb_over_the_limit_is_refused() {
        use flate2::write::ZlibEncoder;
        use flate2::Compression;

        let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(&bomb()).unwrap();
        let c = encoder.finish().unwrap();

        let err = decompress(proto::CompressionType::Zlib, &c, None, LIMIT).unwrap_err();
        assert_eq!(err, DecodeRefusal::OverCap { limit: LIMIT });
    }

    #[cfg(feature = "snap")]
    #[test]
    fn snappy_bomb_over_the_limit_is_refused() {
        use snap::write::FrameEncoder;

        let mut encoder = FrameEncoder::new(Vec::new());
        encoder.write_all(&bomb()).unwrap();
        let c = encoder.into_inner().unwrap();

        let err = decompress(proto::CompressionType::Snappy, &c, None, LIMIT).unwrap_err();
        assert_eq!(err, DecodeRefusal::OverCap { limit: LIMIT });
    }

    #[cfg(feature = "lz4")]
    #[test]
    fn lz4_bomb_over_the_limit_is_refused() {
        let c = lz4::block::compress(&bomb()[..], None, false).unwrap();
        let err = decompress(
            proto::CompressionType::Lz4,
            &c,
            Some((LIMIT * 8) as u32),
            LIMIT,
        )
        .unwrap_err();
        assert_eq!(err, DecodeRefusal::OverCap { limit: LIMIT });
    }

    #[cfg(feature = "lz4")]
    #[test]
    fn lz4_without_a_declared_size_is_refused() {
        let c = lz4::block::compress(&bomb()[..], None, false).unwrap();
        let err = decompress(proto::CompressionType::Lz4, &c, None, LIMIT).unwrap_err();
        assert!(matches!(err, DecodeRefusal::Undecodable(_)));
    }

    #[cfg(feature = "zstd")]
    #[test]
    fn a_declared_size_that_does_not_match_is_refused() {
        let payload = b"hello world".repeat(10);
        let c = zstd::encode_all(&payload[..], 3).unwrap();
        let err = decompress(proto::CompressionType::Zstd, &c, Some(5), LIMIT).unwrap_err();
        assert!(matches!(err, DecodeRefusal::SizeMismatch { .. }));
    }

    #[cfg(feature = "zstd")]
    #[test]
    fn zstd_under_the_limit_round_trips() {
        let payload = b"pulsar payload ".repeat(1000);
        let c = zstd::encode_all(&payload[..], 3).unwrap();
        let res = decompress(
            proto::CompressionType::Zstd,
            &c,
            Some(payload.len() as u32),
            LIMIT,
        )
        .unwrap();
        assert_eq!(res, payload);
    }

    #[cfg(feature = "flate2")]
    #[test]
    fn zlib_under_the_limit_round_trips() {
        use flate2::write::ZlibEncoder;
        use flate2::Compression;

        let payload = b"pulsar payload ".repeat(1000);
        let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(&payload).unwrap();
        let c = encoder.finish().unwrap();

        let res = decompress(
            proto::CompressionType::Zlib,
            &c,
            Some(payload.len() as u32),
            LIMIT,
        )
        .unwrap();
        assert_eq!(res, payload);
    }

    #[cfg(feature = "snap")]
    #[test]
    fn snappy_under_the_limit_round_trips() {
        use snap::write::FrameEncoder;

        let payload = b"pulsar payload ".repeat(1000);
        let mut encoder = FrameEncoder::new(Vec::new());
        encoder.write_all(&payload).unwrap();
        let c = encoder.into_inner().unwrap();

        let res = decompress(
            proto::CompressionType::Snappy,
            &c,
            Some(payload.len() as u32),
            LIMIT,
        )
        .unwrap();
        assert_eq!(res, payload);
    }

    #[cfg(feature = "lz4")]
    #[test]
    fn lz4_under_the_limit_round_trips() {
        let payload = b"pulsar payload ".repeat(1000);
        let c = lz4::block::compress(&payload[..], None, false).unwrap();

        let res = decompress(
            proto::CompressionType::Lz4,
            &c,
            Some(payload.len() as u32),
            LIMIT,
        )
        .unwrap();
        assert_eq!(res, payload);
    }

    #[test]
    fn uncompressed_passes_through() {
        let res = decompress(proto::CompressionType::None, b"abc", None, LIMIT).unwrap();
        assert_eq!(res, b"abc".to_vec());
    }

    #[cfg(feature = "zstd")]
    #[test]
    fn exactly_the_limit_decodes() {
        let payload = vec![7u8; LIMIT];
        let c = zstd::encode_all(&payload[..], 3).unwrap();
        let res = decompress(proto::CompressionType::Zstd, &c, Some(LIMIT as u32), LIMIT).unwrap();
        assert_eq!(res.len(), LIMIT);
    }

    #[cfg(feature = "zstd")]
    #[test]
    fn one_byte_past_the_limit_is_refused() {
        let payload = vec![7u8; LIMIT + 1];
        let c = zstd::encode_all(&payload[..], 3).unwrap();
        assert_eq!(
            decompress(proto::CompressionType::Zstd, &c, None, LIMIT).unwrap_err(),
            DecodeRefusal::OverCap { limit: LIMIT }
        );
        assert_eq!(
            decompress(
                proto::CompressionType::Zstd,
                &c,
                Some(LIMIT as u32 + 1),
                LIMIT
            )
            .unwrap_err(),
            DecodeRefusal::OverCap { limit: LIMIT }
        );
    }

    /// A batch envelope is decoded as one unit, so the cap bounds the whole
    /// batch: members that are small one by one but over the cap together
    /// are refused before the batch iterator ever sees them; under the cap the
    /// iterator yields every member.
    #[cfg(feature = "zstd")]
    #[test]
    fn a_batched_envelope_is_bounded_as_a_whole() {
        use crate::{
            message::{proto::SingleMessageMetadata, BatchedMessage, Metadata},
            Payload,
        };

        fn envelope(members: usize, member_len: usize) -> (Vec<u8>, u32) {
            let mut data = Vec::new();
            for _ in 0..members {
                BatchedMessage {
                    metadata: SingleMessageMetadata {
                        payload_size: member_len as i32,
                        ..Default::default()
                    },
                    payload: vec![1u8; member_len],
                }
                .serialize(&mut data);
            }
            let len = data.len() as u32;
            (zstd::encode_all(&data[..], 3).unwrap(), len)
        }

        // 16 members of LIMIT/8 bytes: each under the cap, twice it together.
        let (over, over_len) = envelope(16, LIMIT / 8);
        assert!(decompress(proto::CompressionType::Zstd, &over, None, LIMIT).is_err());
        assert!(decompress(proto::CompressionType::Zstd, &over, Some(over_len), LIMIT).is_err());

        let (under, under_len) = envelope(4, LIMIT / 8);
        let payload = Payload {
            metadata: Metadata {
                producer_name: "test".into(),
                num_messages_in_batch: Some(4),
                compression: Some(proto::CompressionType::Zstd as i32),
                uncompressed_size: Some(under_len),
                ..Default::default()
            },
            data: under,
        };
        let members = decode_payload(
            Default::default(),
            payload,
            LIMIT,
            DEFAULT_MAX_BATCH_MEMBERS,
            ENVELOPE,
        )
        .unwrap();
        assert_eq!(members.len(), 4);
        assert!(members.iter().all(|(_, p)| p.data.len() == LIMIT / 8));
    }

    /// The largest cap a caller can pass must not overflow the `limit + 1`
    /// read bound (a debug panic; a release wrap to a zero-byte read).
    #[cfg(all(feature = "zstd", feature = "flate2", feature = "snap"))]
    #[test]
    fn a_usize_max_cap_round_trips() {
        use flate2::{write::ZlibEncoder, Compression};
        let payload = b"pulsar payload ".repeat(100);
        let declared = Some(payload.len() as u32);

        let zstd = zstd::encode_all(&payload[..], 3).unwrap();
        let mut zlib = ZlibEncoder::new(Vec::new(), Compression::default());
        zlib.write_all(&payload).unwrap();
        let zlib = zlib.finish().unwrap();
        let mut snappy = snap::write::FrameEncoder::new(Vec::new());
        snappy.write_all(&payload).unwrap();
        let snappy = snappy.into_inner().unwrap();

        for (compression, data) in [
            (proto::CompressionType::Zstd, zstd),
            (proto::CompressionType::Zlib, zlib),
            (proto::CompressionType::Snappy, snappy),
        ] {
            let res = decompress(compression, &data, declared, usize::MAX).unwrap();
            assert_eq!(res, payload, "{compression:?}");
        }
    }

    /// An lz4 size hint past `i32::MAX` would go negative in the decoder's
    /// `i32` hint: refused before the cast, even under the largest cap.
    #[cfg(feature = "lz4")]
    #[test]
    fn an_lz4_declared_size_past_i32_max_is_refused() {
        let c = lz4::block::compress(b"tiny", None, false).unwrap();
        let declared = i32::MAX as u32 + 1;
        let err =
            decompress(proto::CompressionType::Lz4, &c, Some(declared), usize::MAX).unwrap_err();
        match err {
            DecodeRefusal::Undecodable(why) => {
                assert!(why.contains("i32"), "refused before the cast: {why}")
            }
            other => panic!("expected Undecodable, got {other:?}"),
        }
    }

    fn batch_payload(
        data: Vec<u8>,
        compression: Option<proto::CompressionType>,
        uncompressed_size: Option<u32>,
        members: i32,
    ) -> crate::Payload {
        crate::Payload {
            metadata: crate::message::Metadata {
                producer_name: "test".into(),
                num_messages_in_batch: Some(members),
                compression: compression.map(|c| c as i32),
                uncompressed_size,
                ..Default::default()
            },
            data,
        }
    }

    /// A batch whose member table does not parse is refused as undecodable
    /// (so the engine settles it) and hands back the payload AS RECEIVED:
    /// the still-compressed bytes go to the dead-letter topic.
    #[cfg(feature = "zstd")]
    #[test]
    fn a_compressed_batch_with_a_corrupt_member_table_is_refused_with_the_original_bytes() {
        let table = [0xffu8; 64];
        let compressed = zstd::encode_all(&table[..], 3).unwrap();
        let payload = batch_payload(
            compressed.clone(),
            Some(proto::CompressionType::Zstd),
            Some(table.len() as u32),
            2,
        );
        let (back, reason) = *decode_payload(
            Default::default(),
            payload,
            LIMIT,
            DEFAULT_MAX_BATCH_MEMBERS,
            ENVELOPE,
        )
        .unwrap_err();
        assert!(
            matches!(reason, DecodeRefusal::Undecodable(_)),
            "{reason:?}"
        );
        assert_eq!(
            back.data, compressed,
            "the original compressed bytes come back"
        );
        assert_eq!(
            back.metadata.compression,
            Some(proto::CompressionType::Zstd as i32)
        );
    }

    /// The bytes a corrupt member table would leak: a distinctive marker.
    const MARKER: &[u8] = b"TENANT-SECRET-MARKER";

    /// True when `reason` carries `MARKER` raw or as its decimal rendering
    /// (how a `&[u8]` Debug prints), or is long enough to carry a body.
    fn leaks(reason: &str) -> bool {
        let decimal = format!("{:?}", &MARKER[..4]);
        let decimal = decimal.trim_matches(|c| c == '[' || c == ']');
        reason.len() >= 256
            || reason.contains(std::str::from_utf8(MARKER).unwrap())
            || reason.contains(decimal)
    }

    /// One valid member, then a member whose metadata (marker bytes) is not
    /// a protobuf, so the parser fails WITH the remaining table as its
    /// input: the refusal names the parse failure, never those bytes.
    #[test]
    fn a_corrupt_member_table_refusal_carries_no_payload_bytes() {
        use crate::message::{proto::SingleMessageMetadata, BatchedMessage};
        let mut table = Vec::new();
        BatchedMessage {
            metadata: SingleMessageMetadata {
                payload_size: 3,
                ..Default::default()
            },
            payload: b"one".to_vec(),
        }
        .serialize(&mut table);
        table.extend_from_slice(&(MARKER.len() as u32).to_be_bytes());
        table.extend_from_slice(&MARKER.repeat(64));
        let payload = batch_payload(table, None, None, 2);
        let (_, reason) = *decode_payload(
            Default::default(),
            payload,
            LIMIT,
            DEFAULT_MAX_BATCH_MEMBERS,
            ENVELOPE,
        )
        .unwrap_err();
        let reason = reason.to_string();
        assert!(!leaks(&reason), "{} bytes: {reason}", reason.len());
    }

    /// An uncompressed batch of `count` empty members (4 bytes each: a zero
    /// `metadata_size`) under a plain envelope.
    fn empty_members(count: usize) -> crate::Payload {
        batch_payload(vec![0u8; 4 * count], None, None, count as i32)
    }

    /// A 64 KiB `replicate_to` and 64 KiB of `encryption_keys` (fields every
    /// member copies) over 2000 empty members would clone 256 MiB of
    /// metadata out of an 8000-byte table: refused, with the bytes.
    #[test]
    fn an_envelope_amplified_by_many_members_is_refused() {
        let mut payload = empty_members(2000);
        payload.metadata.replicate_to.push("r".repeat(64 * 1024));
        payload
            .metadata
            .encryption_keys
            .push(proto::EncryptionKeys {
                key: "k".into(),
                value: vec![7u8; 64 * 1024],
                ..Default::default()
            });
        let data = payload.data.clone();
        let (back, reason) =
            *decode_payload(Default::default(), payload, LIMIT, MEMBERS, ENVELOPE).unwrap_err();
        assert!(
            matches!(reason, DecodeRefusal::Undecodable(_)),
            "{reason:?}"
        );
        assert_eq!(back.data, data);
        assert_eq!(back.metadata.replicate_to.len(), 1, "refused as it arrived");
    }

    /// 20_000 EMPTY `encryption_keys` encode to about 40 KB, but every
    /// member's clone allocates one `EncryptionKeys` struct per entry
    /// (about 1.4 MB): over 100 members that is past the envelope bound, so
    /// the batch is refused, as it arrived.
    #[test]
    fn many_empty_repeated_entries_are_measured_by_what_a_clone_allocates() {
        const KEYS: usize = 20_000;
        let mut payload = empty_members(100);
        payload.metadata.encryption_keys = vec![proto::EncryptionKeys::default(); KEYS];
        assert!(
            metadata_footprint(&payload.metadata)
                >= KEYS * std::mem::size_of::<proto::EncryptionKeys>(),
            "the footprint counts each entry's struct"
        );
        let data = payload.data.clone();
        let (back, reason) =
            *decode_payload(Default::default(), payload, LIMIT, MEMBERS, ENVELOPE).unwrap_err();
        assert!(
            matches!(reason, DecodeRefusal::Undecodable(_)),
            "{reason:?}"
        );
        assert_eq!(back.data, data);
        assert_eq!(
            back.metadata.encryption_keys.len(),
            KEYS,
            "refused as it arrived"
        );
    }

    /// A data key's own `metadata` entries are cloned with it, so they are
    /// measured too: 1000 empty entries under one key cost their structs.
    #[test]
    fn nested_encryption_key_metadata_is_measured() {
        let metadata = crate::message::Metadata {
            encryption_keys: vec![proto::EncryptionKeys {
                key: "k".into(),
                metadata: vec![proto::KeyValue::default(); 1000],
                ..Default::default()
            }],
            ..Default::default()
        };
        assert!(
            metadata_footprint(&metadata)
                >= std::mem::size_of::<proto::EncryptionKeys>()
                    + 1000 * std::mem::size_of::<proto::KeyValue>()
        );
    }

    /// Unset is the default; a configured bound is honoured, but never
    /// below the minimum (0 would refuse every batch).
    #[test]
    fn the_envelope_bound_defaults_and_is_clamped() {
        assert_eq!(envelope_bound(None), DEFAULT_MAX_BATCH_ENVELOPE_BYTES);
        assert_eq!(envelope_bound(Some(0)), MIN_MAX_BATCH_ENVELOPE_BYTES);
        assert_eq!(envelope_bound(Some(1 << 30)), 1 << 30);
    }

    /// An honest end-to-end-encrypted batch at the member cap: 5000 members
    /// under an envelope carrying two RSA-4096-wrapped data keys (512-byte
    /// values) is accepted at the DEFAULT decompressed-size cap, since the
    /// envelope copies have their own bound.
    #[test]
    fn a_full_encrypted_batch_with_two_rsa_4096_keys_is_accepted() {
        let mut payload = empty_members(DEFAULT_MAX_BATCH_MEMBERS as usize);
        payload.metadata.encryption_algo = Some("RSA-OAEP".into());
        payload.metadata.encryption_param = Some(vec![9u8; 12]);
        payload.metadata.encryption_keys = (0..2)
            .map(|i| proto::EncryptionKeys {
                key: format!("tenant-data-key-{i}.pem"),
                value: vec![7u8; 512],
                ..Default::default()
            })
            .collect();
        let members = decode_payload(
            Default::default(),
            payload,
            DEFAULT_MAX_DECOMPRESSED_SIZE,
            DEFAULT_MAX_BATCH_MEMBERS,
            DEFAULT_MAX_BATCH_ENVELOPE_BYTES,
        )
        .unwrap();
        assert_eq!(members.len(), DEFAULT_MAX_BATCH_MEMBERS as usize);
        assert_eq!(members[0].1.metadata.encryption_keys.len(), 2);
    }

    /// Envelope properties are not copied to members (each member carries
    /// its own), so a 64 KiB-properties envelope over 2000 members decodes
    /// at the default cap.
    #[test]
    fn a_large_properties_envelope_over_many_members_decodes() {
        let mut payload = empty_members(2000);
        payload.metadata.properties.push(proto::KeyValue {
            key: "k".into(),
            value: "v".repeat(64 * 1024),
        });
        let members = decode_payload(
            Default::default(),
            payload,
            DEFAULT_MAX_DECOMPRESSED_SIZE,
            MEMBERS,
            ENVELOPE,
        )
        .unwrap();
        assert_eq!(members.len(), 2000);
        assert!(members
            .iter()
            .all(|(_, p)| p.metadata.properties.is_empty()));
    }

    /// More members than the default cap, however small the envelope:
    /// refused, with the bytes.
    #[test]
    fn a_batch_past_the_member_cap_is_refused() {
        let payload = empty_members(MEMBERS as usize + 1);
        let data = payload.data.clone();
        let (back, reason) = *decode_payload(
            Default::default(),
            payload,
            DEFAULT_MAX_DECOMPRESSED_SIZE,
            MEMBERS,
            ENVELOPE,
        )
        .unwrap_err();
        assert!(
            matches!(reason, DecodeRefusal::Undecodable(_)),
            "{reason:?}"
        );
        assert_eq!(back.data, data);
    }

    /// The bounds need only the metadata, so they refuse BEFORE anything is
    /// decompressed: a zstd batch past the cap whose bytes are not zstd at
    /// all is refused for its member count, not for its codec.
    #[test]
    fn the_member_cap_is_checked_before_decompression() {
        let payload = batch_payload(
            vec![0xffu8; 64],
            Some(proto::CompressionType::Zstd),
            Some(64),
            MEMBERS as i32 + 1,
        );
        let (_, reason) =
            *decode_payload(Default::default(), payload, LIMIT, MEMBERS, ENVELOPE).unwrap_err();
        assert!(reason.to_string().contains("member cap"), "{reason}");
    }

    /// At the defaults, a full batch of the default member cap decodes.
    #[test]
    fn a_batch_at_the_member_cap_decodes() {
        let payload = empty_members(MEMBERS as usize);
        let members = decode_payload(
            Default::default(),
            payload,
            DEFAULT_MAX_DECOMPRESSED_SIZE,
            MEMBERS,
            ENVELOPE,
        )
        .unwrap();
        assert_eq!(members.len(), MEMBERS as usize);
    }

    /// Members keep their own properties and keys, never the envelope's,
    /// and an empty member stays empty.
    #[test]
    fn members_keep_their_own_properties_and_keys() {
        use crate::message::{proto::SingleMessageMetadata, BatchedMessage};
        let mut table = Vec::new();
        BatchedMessage {
            metadata: SingleMessageMetadata {
                properties: vec![proto::KeyValue {
                    key: "own".into(),
                    value: "1".into(),
                }],
                partition_key: Some("k1".into()),
                ordering_key: Some(b"o1".to_vec()),
                payload_size: 3,
                ..Default::default()
            },
            payload: b"one".to_vec(),
        }
        .serialize(&mut table);
        BatchedMessage {
            metadata: SingleMessageMetadata::default(),
            payload: Vec::new(),
        }
        .serialize(&mut table);
        let mut payload = batch_payload(table, None, None, 2);
        payload.metadata.properties.push(proto::KeyValue {
            key: "envelope".into(),
            value: "x".into(),
        });
        payload.metadata.partition_key = Some("envelope-key".into());
        payload.metadata.ordering_key = Some(b"envelope-order".to_vec());
        let members =
            decode_payload(Default::default(), payload, LIMIT, MEMBERS, ENVELOPE).unwrap();
        assert_eq!(members.len(), 2);
        let first = &members[0].1.metadata;
        assert_eq!(first.properties.len(), 1);
        assert_eq!(first.properties[0].key, "own");
        assert_eq!(first.partition_key.as_deref(), Some("k1"));
        assert_eq!(first.ordering_key.as_deref(), Some(&b"o1"[..]));
        let empty = &members[1].1;
        assert!(empty.metadata.properties.is_empty());
        assert_eq!(empty.metadata.partition_key, None);
        assert_eq!(empty.metadata.ordering_key, None);
        assert!(empty.data.is_empty());
    }

    /// Bytes left after the declared members mean the table is not what
    /// it declares: refused, with the bytes.
    #[test]
    fn a_batch_with_bytes_past_its_members_is_refused() {
        use crate::message::{proto::SingleMessageMetadata, BatchedMessage};
        let mut table = Vec::new();
        BatchedMessage {
            metadata: SingleMessageMetadata {
                payload_size: 3,
                ..Default::default()
            },
            payload: b"one".to_vec(),
        }
        .serialize(&mut table);
        table.extend_from_slice(b"xyz");
        let payload = batch_payload(table.clone(), None, None, 1);
        let (back, reason) =
            *decode_payload(Default::default(), payload, LIMIT, MEMBERS, ENVELOPE).unwrap_err();
        assert!(
            matches!(reason, DecodeRefusal::Undecodable(_)),
            "{reason:?}"
        );
        assert_eq!(back.data, table);
    }

    #[test]
    fn an_uncompressed_batch_with_a_corrupt_member_table_is_refused() {
        let table = vec![0xffu8; 64];
        let payload = batch_payload(table.clone(), None, None, 2);
        let (back, reason) = *decode_payload(
            Default::default(),
            payload,
            LIMIT,
            DEFAULT_MAX_BATCH_MEMBERS,
            ENVELOPE,
        )
        .unwrap_err();
        assert!(
            matches!(reason, DecodeRefusal::Undecodable(_)),
            "{reason:?}"
        );
        assert_eq!(back.data, table);
    }

    #[cfg(feature = "zstd")]
    #[test]
    fn a_well_formed_compressed_batch_yields_every_member() {
        use crate::message::{proto::SingleMessageMetadata, BatchedMessage};
        let mut table = Vec::new();
        for member in [b"one".to_vec(), b"two".to_vec()] {
            BatchedMessage {
                metadata: SingleMessageMetadata {
                    payload_size: member.len() as i32,
                    ..Default::default()
                },
                payload: member,
            }
            .serialize(&mut table);
        }
        let compressed = zstd::encode_all(&table[..], 3).unwrap();
        let payload = batch_payload(
            compressed,
            Some(proto::CompressionType::Zstd),
            Some(table.len() as u32),
            2,
        );
        let members = decode_payload(
            Default::default(),
            payload,
            LIMIT,
            DEFAULT_MAX_BATCH_MEMBERS,
            ENVELOPE,
        )
        .unwrap();
        let data: Vec<_> = members.iter().map(|(_, p)| p.data.clone()).collect();
        assert_eq!(data, vec![b"one".to_vec(), b"two".to_vec()]);
        assert_eq!(members[1].0.batch_index, Some(1));
    }

    #[test]
    fn an_unknown_compression_type_is_refused_with_the_original_bytes() {
        let mut payload = batch_payload(b"abc".to_vec(), None, None, 1);
        payload.metadata.num_messages_in_batch = None;
        payload.metadata.compression = Some(99);
        let (back, reason) = *decode_payload(
            Default::default(),
            payload,
            LIMIT,
            DEFAULT_MAX_BATCH_MEMBERS,
            ENVELOPE,
        )
        .unwrap_err();
        assert!(
            matches!(reason, DecodeRefusal::Undecodable(_)),
            "{reason:?}"
        );
        assert_eq!(back.data, b"abc".to_vec());
    }

    #[test]
    fn an_uncompressed_single_message_passes_through() {
        let mut payload = batch_payload(b"abc".to_vec(), None, None, 1);
        payload.metadata.num_messages_in_batch = None;
        let members = decode_payload(
            Default::default(),
            payload,
            LIMIT,
            DEFAULT_MAX_BATCH_MEMBERS,
            ENVELOPE,
        )
        .unwrap();
        assert_eq!(members.len(), 1);
        assert_eq!(members[0].1.data, b"abc".to_vec());
    }

    /// A batch that declares no members (or a negative count) would decode
    /// to NOTHING: no payload is delivered, so the message is never acked or
    /// refused and holds a permit forever. Refused as undecodable instead,
    /// with the bytes as received, so the engine settles it.
    #[cfg(feature = "zstd")]
    #[test]
    fn a_batch_declaring_no_members_is_refused_with_the_original_bytes() {
        use crate::message::{proto::SingleMessageMetadata, BatchedMessage};
        let mut table = Vec::new();
        BatchedMessage {
            metadata: SingleMessageMetadata {
                payload_size: 3,
                ..Default::default()
            },
            payload: b"one".to_vec(),
        }
        .serialize(&mut table);
        let compressed = zstd::encode_all(&table[..], 3).unwrap();
        for count in [0, -1, i32::MIN] {
            for (data, compression, size) in [
                (table.clone(), None, None),
                (
                    compressed.clone(),
                    Some(proto::CompressionType::Zstd),
                    Some(table.len() as u32),
                ),
            ] {
                let payload = batch_payload(data.clone(), compression, size, count);
                let (back, reason) = *decode_payload(
                    Default::default(),
                    payload,
                    LIMIT,
                    DEFAULT_MAX_BATCH_MEMBERS,
                    ENVELOPE,
                )
                .unwrap_err();
                assert!(
                    matches!(reason, DecodeRefusal::Undecodable(_)),
                    "{count}: {reason:?}"
                );
                assert_eq!(back.data, data, "{count}: the bytes as received come back");
            }
        }
    }

    /// The envelope-copy bound the proptest decodes under, small enough that
    /// the generated repeated-field counts reach both arms.
    const PROP_ENVELOPE: usize = 1024 * 1024;

    proptest::proptest! {
        /// Whatever a publisher puts in the codec, declared size, batch count
        /// and body, decoding never panics, a refusal hands back the bytes
        /// exactly as received and names no body bytes, and success yields
        /// exactly the declared member count (an empty success would leave
        /// the message unsettled).
        #[test]
        fn decode_payload_never_panics_and_refusals_carry_the_original_bytes(
            codec in proptest::option::of(-2i32..8),
            declared in proptest::option::of(proptest::prelude::any::<u32>()),
            count in proptest::option::of(proptest::prop_oneof![
                -3i32..6,
                proptest::prelude::Just(i32::MAX),
                proptest::prelude::Just(i32::MIN),
            ]),
            data in proptest::collection::vec(proptest::prelude::any::<u8>(), 0..256),
            keys in 0usize..30_000,
            replicas in 0usize..30_000,
            properties in 0usize..30_000,
            empty_table in proptest::prelude::any::<bool>(),
        ) {
            // Random bytes rarely parse as a member table, so half the cases
            // carry a valid table of empty members (uncompressed) to reach
            // the envelope-copy bound.
            let (codec, data) = match count {
                Some(n @ 1..=5) if empty_table => (None, vec![0u8; 4 * n as usize]),
                _ => (codec, data),
            };
            let payload = crate::Payload {
                metadata: crate::message::Metadata {
                    producer_name: "prop".into(),
                    num_messages_in_batch: count,
                    compression: codec,
                    uncompressed_size: declared,
                    encryption_keys: vec![proto::EncryptionKeys::default(); keys],
                    replicate_to: vec![String::new(); replicas],
                    properties: vec![proto::KeyValue::default(); properties],
                    ..Default::default()
                },
                data: data.clone(),
            };
            match decode_payload(
                Default::default(),
                payload,
                LIMIT,
                DEFAULT_MAX_BATCH_MEMBERS,
                PROP_ENVELOPE,
            ) {
                Ok(members) => {
                    proptest::prop_assert_eq!(members.len(), count.map_or(1, |c| c as usize));
                    if count.is_some() {
                        // What each member's clone allocates for the copied
                        // repeated fields, measured independently of the
                        // code under test.
                        let copied = keys * std::mem::size_of::<proto::EncryptionKeys>()
                            + replicas * std::mem::size_of::<String>()
                            + MEMBER_OVERHEAD;
                        proptest::prop_assert!(
                            members.len().saturating_mul(copied) <= PROP_ENVELOPE,
                            "{} members x {} bytes accepted",
                            members.len(),
                            copied
                        );
                    }
                }
                Err(refused) => {
                    let (back, reason) = *refused;
                    proptest::prop_assert_eq!(&back.data, &data);
                    proptest::prop_assert_eq!(back.metadata.encryption_keys.len(), keys);
                    proptest::prop_assert_eq!(back.metadata.replicate_to.len(), replicas);
                    proptest::prop_assert_eq!(back.metadata.properties.len(), properties);
                    let reason = reason.to_string();
                    proptest::prop_assert!(reason.len() < 256, "{} bytes: {}", reason.len(), reason);
                    if data.len() >= 8 {
                        let body = format!("{:?}", &data[..8]);
                        let body = body.trim_matches(|c| c == '[' || c == ']');
                        proptest::prop_assert!(!reason.contains(body), "{}", reason);
                    }
                }
            }
        }
    }
}
