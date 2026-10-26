# 3rdi-ai fork of pulsar-rs

This is a 3rdi AI fork of [streamnative/pulsar-rs](https://github.com/streamnative/pulsar-rs),
licensed as upstream (MIT OR Apache-2.0; see `LICENSE-APACHE`, `LICENSE-MIT`, `COPYRIGHT`).

## Why it exists

Upstream `pulsar` 6.9.1 (the latest release) requires `zstd ^0.13.2` under its
`compression` feature. zstd 0.13 / zstd-safe 7 / zstd-sys < 2.1.2 carry
RUSTSEC-2026-0391, -0398, -0403 and -0410 (among others), fixed only on
zstd >= 0.14.3. This fork changes exactly one thing: `zstd = "^0.14.3"`
(plus a zstd round-trip test). No API or behaviour change.

Releases are tagged `v<upstream>-3rdi.<n>`, e.g. `v6.9.1-3rdi.1`.

## When to drop it

As soon as upstream publishes a `pulsar` release on zstd >= 0.14.3: point
consumers (`prophesee-service-rs`) back at crates.io and archive this fork.
