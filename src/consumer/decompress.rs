//! Payload decompression for the consumer engine, bounded so that a small
//! compressed frame from any publisher cannot expand into an OOM
//! (a decompression bomb). A message that would decompress past `limit`, or
//! whose output does not match its declared `uncompressed_size`, is refused
//! with a [`DecodeRefusal`] — a per-message error, never a panic and never
//! an unbounded allocation.

use crate::{error::DecodeRefusal, message::proto};

/// Pulsar's default broker `maxMessageSize` (5 MiB).
pub const DEFAULT_MAX_DECOMPRESSED_SIZE: usize = 5 * 1024 * 1024;

fn undecodable(e: impl std::fmt::Display) -> DecodeRefusal {
    DecodeRefusal::Undecodable(e.to_string())
}

/// Reads at most `limit + 1` bytes from `reader`; more than `limit` is refused.
#[cfg(any(feature = "flate2", feature = "zstd", feature = "snap"))]
fn read_bounded(reader: impl std::io::Read, limit: usize) -> Result<Vec<u8>, DecodeRefusal> {
    use std::io::Read;
    let mut out = Vec::new();
    reader
        .take(limit as u64 + 1)
        .read_to_end(&mut out)
        .map_err(undecodable)?;
    if out.len() > limit {
        return Err(DecodeRefusal::OverCap { limit });
    }
    Ok(out)
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
                lz4::block::decompress(data, Some(declared as i32)).map_err(undecodable)
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
            consumer::batched_message_iterator::BatchedMessageIterator,
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
        let data =
            decompress(proto::CompressionType::Zstd, &under, Some(under_len), LIMIT).unwrap();
        let payload = Payload {
            metadata: Metadata {
                producer_name: "test".into(),
                num_messages_in_batch: Some(4),
                ..Default::default()
            },
            data,
        };
        let members: Vec<_> = BatchedMessageIterator::new(Default::default(), payload)
            .unwrap()
            .collect();
        assert_eq!(members.len(), 4);
        assert!(members.iter().all(|(_, p)| p.data.len() == LIMIT / 8));
    }
}
