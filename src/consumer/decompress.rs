//! Payload decompression for the consumer engine, bounded so that a small
//! compressed frame from any publisher cannot expand into an OOM
//! (a decompression bomb). A message that would decompress past `limit`, or
//! whose output does not match its declared `uncompressed_size`, is refused
//! with [`ConsumerError::Io`] (`InvalidData`) — a per-message error, never a
//! panic and never an unbounded allocation.

use crate::{error::ConsumerError, message::proto};

/// Pulsar's default broker `maxMessageSize` (5 MiB).
pub(crate) const DEFAULT_MAX_DECOMPRESSED_SIZE: usize = 5 * 1024 * 1024;

fn too_large(limit: usize) -> ConsumerError {
    ConsumerError::Io(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        format!("compressed message exceeds the {limit}-byte decompressed size limit"),
    ))
}

fn size_mismatch(declared: u32, actual: usize) -> ConsumerError {
    ConsumerError::Io(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        format!("decompressed {actual} bytes but the message declared {declared}"),
    ))
}

/// Reads at most `limit + 1` bytes from `reader`; more than `limit` is refused.
#[cfg(any(feature = "flate2", feature = "zstd", feature = "snap"))]
fn read_bounded(reader: impl std::io::Read, limit: usize) -> Result<Vec<u8>, ConsumerError> {
    use std::io::Read;
    let mut out = Vec::new();
    reader
        .take(limit as u64 + 1)
        .read_to_end(&mut out)
        .map_err(ConsumerError::Io)?;
    if out.len() > limit {
        return Err(too_large(limit));
    }
    Ok(out)
}

/// Decompresses `data` per `compression`. `uncompressed_size` is the
/// message metadata's declared size; `limit` is the most bytes a message
/// may decompress to.
pub(crate) fn decompress(
    compression: proto::CompressionType,
    data: Vec<u8>,
    uncompressed_size: Option<u32>,
    limit: usize,
) -> Result<Vec<u8>, ConsumerError> {
    if compression != proto::CompressionType::None {
        if let Some(declared) = uncompressed_size {
            if declared as usize > limit {
                return Err(too_large(limit));
            }
        }
    }

    let out: Result<Vec<u8>, ConsumerError> = match compression {
        proto::CompressionType::None => return Ok(data),
        proto::CompressionType::Lz4 => {
            #[cfg(not(feature = "lz4"))]
            {
                Err(ConsumerError::Io(std::io::Error::other(
                    "got a LZ4 compressed message but 'lz4' cargo feature is deactivated",
                )))
            }

            #[cfg(feature = "lz4")]
            {
                let declared = uncompressed_size.ok_or_else(|| {
                    ConsumerError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "lz4 message without a declared uncompressed size",
                    ))
                })?;
                lz4::block::decompress(&data[..], Some(declared as i32)).map_err(ConsumerError::Io)
            }
        }
        proto::CompressionType::Zlib => {
            #[cfg(not(feature = "flate2"))]
            {
                Err(ConsumerError::Io(std::io::Error::other(
                    "got a zlib compressed message but 'flate2' cargo feature is deactivated",
                )))
            }

            #[cfg(feature = "flate2")]
            {
                read_bounded(flate2::read::ZlibDecoder::new(&data[..]), limit)
            }
        }
        proto::CompressionType::Zstd => {
            #[cfg(not(feature = "zstd"))]
            {
                Err(ConsumerError::Io(std::io::Error::other(
                    "got a zstd compressed message but 'zstd' cargo feature is deactivated",
                )))
            }

            #[cfg(feature = "zstd")]
            {
                zstd::Decoder::new(&data[..])
                    .map_err(ConsumerError::Io)
                    .and_then(|d| read_bounded(d, limit))
            }
        }
        proto::CompressionType::Snappy => {
            #[cfg(not(feature = "snap"))]
            {
                Err(ConsumerError::Io(std::io::Error::other(
                    "got a Snappy compressed message but 'snap' cargo feature is deactivated",
                )))
            }

            #[cfg(feature = "snap")]
            {
                read_bounded(snap::read::FrameDecoder::new(&data[..]), limit)
            }
        }
    };
    let out = out?;

    if let Some(declared) = uncompressed_size {
        if out.len() != declared as usize {
            return Err(size_mismatch(declared, out.len()));
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
        let err = decompress(proto::CompressionType::Zstd, c, None, LIMIT).unwrap_err();
        assert!(matches!(err, ConsumerError::Io(_)));
    }

    #[cfg(feature = "zstd")]
    #[test]
    fn zstd_bomb_with_a_lying_declared_size_is_refused() {
        let c = zstd::encode_all(&bomb()[..], 3).unwrap();
        assert!(c.len() < 64 * 1024);
        let err = decompress(proto::CompressionType::Zstd, c, Some(100), LIMIT).unwrap_err();
        assert!(matches!(err, ConsumerError::Io(_)));
    }

    #[cfg(feature = "flate2")]
    #[test]
    fn zlib_bomb_over_the_limit_is_refused() {
        use flate2::write::ZlibEncoder;
        use flate2::Compression;

        let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(&bomb()).unwrap();
        let c = encoder.finish().unwrap();

        let err = decompress(proto::CompressionType::Zlib, c, None, LIMIT).unwrap_err();
        assert!(matches!(err, ConsumerError::Io(_)));
    }

    #[cfg(feature = "snap")]
    #[test]
    fn snappy_bomb_over_the_limit_is_refused() {
        use snap::write::FrameEncoder;

        let mut encoder = FrameEncoder::new(Vec::new());
        encoder.write_all(&bomb()).unwrap();
        let c = encoder.into_inner().unwrap();

        let err = decompress(proto::CompressionType::Snappy, c, None, LIMIT).unwrap_err();
        assert!(matches!(err, ConsumerError::Io(_)));
    }

    #[cfg(feature = "lz4")]
    #[test]
    fn lz4_bomb_over_the_limit_is_refused() {
        let c = lz4::block::compress(&bomb()[..], None, false).unwrap();
        let err = decompress(
            proto::CompressionType::Lz4,
            c,
            Some((LIMIT * 8) as u32),
            LIMIT,
        )
        .unwrap_err();
        assert!(matches!(err, ConsumerError::Io(_)));
    }

    #[cfg(feature = "lz4")]
    #[test]
    fn lz4_without_a_declared_size_is_refused() {
        let c = lz4::block::compress(&bomb()[..], None, false).unwrap();
        let err = decompress(proto::CompressionType::Lz4, c, None, LIMIT).unwrap_err();
        assert!(matches!(err, ConsumerError::Io(_)));
    }

    #[cfg(feature = "zstd")]
    #[test]
    fn a_declared_size_that_does_not_match_is_refused() {
        let payload = b"hello world".repeat(10);
        let c = zstd::encode_all(&payload[..], 3).unwrap();
        let err = decompress(proto::CompressionType::Zstd, c, Some(5), LIMIT).unwrap_err();
        assert!(matches!(err, ConsumerError::Io(_)));
    }

    #[cfg(feature = "zstd")]
    #[test]
    fn zstd_under_the_limit_round_trips() {
        let payload = b"pulsar payload ".repeat(1000);
        let c = zstd::encode_all(&payload[..], 3).unwrap();
        let res = decompress(
            proto::CompressionType::Zstd,
            c,
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
            c,
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
            c,
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
            c,
            Some(payload.len() as u32),
            LIMIT,
        )
        .unwrap();
        assert_eq!(res, payload);
    }

    #[test]
    fn uncompressed_passes_through() {
        let res = decompress(proto::CompressionType::None, b"abc".to_vec(), None, LIMIT).unwrap();
        assert_eq!(res, b"abc".to_vec());
    }
}
