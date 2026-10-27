//! Payload decompression for the consumer engine, bounded so that a small
//! compressed frame from any publisher cannot expand into an OOM
//! (a decompression bomb). A message that would decompress past `limit`, or
//! whose output does not match its declared `uncompressed_size`, is refused
//! with a [`DecodeRefusal`] — a per-message error, never a panic and never
//! an unbounded allocation.

use crate::{
    consumer::batched_message_iterator::BatchedMessageIterator,
    error::DecodeRefusal,
    message::{parse_batched_message, proto},
    Payload,
};

/// Pulsar's default broker `maxMessageSize` (5 MiB).
pub const DEFAULT_MAX_DECOMPRESSED_SIZE: usize = 5 * 1024 * 1024;

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
/// under `limit` and, for a batch envelope, split into its members.
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
    let decompressed = if compression == proto::CompressionType::None {
        None
    } else {
        match decompress(
            compression,
            &payload.data,
            payload.metadata.uncompressed_size,
            limit,
        ) {
            Ok(data) => Some(data),
            Err(reason) => return Err(Box::new((payload, reason))),
        }
    };
    let Some(count) = payload.metadata.num_messages_in_batch else {
        let payload = match decompressed {
            Some(data) => Payload {
                data,
                metadata: payload.metadata,
            },
            None => payload,
        };
        return Ok(vec![(message_id, payload)]);
    };
    // A batch of no members would deliver nothing, so the message would be
    // neither acknowledged nor refused and hold a permit forever.
    let Ok(count @ 1..) = u32::try_from(count) else {
        let reason = undecodable(format!("batch declares {count} messages"));
        return Err(Box::new((payload, reason)));
    };
    let table = decompressed.as_deref().unwrap_or(&payload.data);
    match parse_batched_message(count, table) {
        Ok(members) => {
            Ok(
                BatchedMessageIterator::from_members(message_id, payload.metadata, count, members)
                    .collect(),
            )
        }
        Err(err) => Err(Box::new((payload, undecodable(err)))),
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
        let members = decode_payload(Default::default(), payload, LIMIT).unwrap();
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
        let (back, reason) = *decode_payload(Default::default(), payload, LIMIT).unwrap_err();
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
        let (_, reason) = *decode_payload(Default::default(), payload, LIMIT).unwrap_err();
        let reason = reason.to_string();
        assert!(!leaks(&reason), "{} bytes: {reason}", reason.len());
    }

    #[test]
    fn an_uncompressed_batch_with_a_corrupt_member_table_is_refused() {
        let table = vec![0xffu8; 64];
        let payload = batch_payload(table.clone(), None, None, 2);
        let (back, reason) = *decode_payload(Default::default(), payload, LIMIT).unwrap_err();
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
        let members = decode_payload(Default::default(), payload, LIMIT).unwrap();
        let data: Vec<_> = members.iter().map(|(_, p)| p.data.clone()).collect();
        assert_eq!(data, vec![b"one".to_vec(), b"two".to_vec()]);
        assert_eq!(members[1].0.batch_index, Some(1));
    }

    #[test]
    fn an_unknown_compression_type_is_refused_with_the_original_bytes() {
        let mut payload = batch_payload(b"abc".to_vec(), None, None, 1);
        payload.metadata.num_messages_in_batch = None;
        payload.metadata.compression = Some(99);
        let (back, reason) = *decode_payload(Default::default(), payload, LIMIT).unwrap_err();
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
        let members = decode_payload(Default::default(), payload, LIMIT).unwrap();
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
                let (back, reason) =
                    *decode_payload(Default::default(), payload, LIMIT).unwrap_err();
                assert!(
                    matches!(reason, DecodeRefusal::Undecodable(_)),
                    "{count}: {reason:?}"
                );
                assert_eq!(back.data, data, "{count}: the bytes as received come back");
            }
        }
    }

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
        ) {
            let payload = crate::Payload {
                metadata: crate::message::Metadata {
                    producer_name: "prop".into(),
                    num_messages_in_batch: count,
                    compression: codec,
                    uncompressed_size: declared,
                    ..Default::default()
                },
                data: data.clone(),
            };
            match decode_payload(Default::default(), payload, LIMIT) {
                Ok(members) => proptest::prop_assert_eq!(
                    members.len(),
                    count.map_or(1, |c| c as usize)
                ),
                Err(refused) => {
                    let (back, reason) = *refused;
                    proptest::prop_assert_eq!(&back.data, &data);
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
