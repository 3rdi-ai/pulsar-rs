//! The queue of decoded payloads between a consumer's engine and the
//! consumer, bounded by count (the receiver queue size) AND by bytes.
//!
//! A count bound alone lets `receiver_queue_size` messages, each up to the
//! per-message decompressed cap, sit in memory for a consumer that is not
//! reading: 1000 tiny zstd bombs, each legitimately 5 MiB decoded, pin about
//! 5 GiB. The engine therefore waits before it queues a payload that would
//! take the queued bytes past the budget, and the consumer releases the
//! bytes as it takes each payload. An empty queue always admits one
//! payload, so a payload larger than the budget delays nothing but itself.

use std::{
    pin::Pin,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
    task::{Context, Poll, Waker},
};

use futures::{channel::mpsc, Stream};

use super::decompress::metadata_footprint;
use crate::{message::proto::MessageIdData, Error, Payload};

/// The default most decoded payload bytes queued for one consumer.
pub const DEFAULT_MAX_QUEUED_BYTES: usize = 64 * 1024 * 1024;

type Item = Result<(MessageIdData, Payload), Error>;

/// What a queued item holds: its data plus its metadata, measured
/// structurally (a payload of no data but large properties is not free).
fn bytes_of(item: &Item) -> usize {
    item.as_ref().map_or(0, |(_, payload)| {
        payload.data.len() + metadata_footprint(&payload.metadata)
    })
}

#[derive(Default)]
struct State {
    queued: usize,
    /// The engine is the queue's only sender, so one waiter suffices.
    waiter: Option<Waker>,
    /// The consumer dropped its end: a waiting send stops waiting and
    /// fails on the closed channel.
    closed: bool,
}

struct Budget {
    max: usize,
    state: Mutex<State>,
}

impl Budget {
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn poll_reserve(&self, bytes: usize, cx: &mut Context<'_>) -> Poll<()> {
        let mut state = self.lock();
        if state.closed || state.queued == 0 || state.queued.saturating_add(bytes) <= self.max {
            state.queued = state.queued.saturating_add(bytes);
            Poll::Ready(())
        } else {
            state.waiter = Some(cx.waker().clone());
            Poll::Pending
        }
    }

    fn release(&self, bytes: usize) {
        let waiter = {
            let mut state = self.lock();
            state.queued = state.queued.saturating_sub(bytes);
            state.waiter.take()
        };
        if let Some(waiter) = waiter {
            waiter.wake();
        }
    }

    /// Marks the queue closed and wakes a waiting send.
    fn close(&self) {
        let waiter = {
            let mut state = self.lock();
            state.closed = true;
            state.waiter.take()
        };
        if let Some(waiter) = waiter {
            waiter.wake();
        }
    }
}

/// Bytes reserved for one payload, given back unless it was queued (a
/// failed or cancelled send leaks nothing).
struct Reservation<'a> {
    budget: &'a Budget,
    bytes: usize,
}

impl Drop for Reservation<'_> {
    fn drop(&mut self) {
        self.budget.release(self.bytes);
    }
}

/// The engine's end of the queue.
pub(crate) struct QueueSender {
    tx: mpsc::Sender<Item>,
    budget: Arc<Budget>,
}

impl QueueSender {
    /// Queues `item`, first waiting until its bytes fit the budget.
    pub(crate) async fn send(&mut self, item: Item) -> Result<(), mpsc::SendError> {
        let bytes = bytes_of(&item);
        let budget = Arc::clone(&self.budget);
        futures::future::poll_fn(|cx| budget.poll_reserve(bytes, cx)).await;
        let reservation = Reservation {
            budget: &budget,
            bytes,
        };
        // `poll_ready` + `start_send`, not `SinkExt::send`: its trailing
        // flush can stay pending AFTER the item is queued, and a send
        // cancelled there would give back bytes the receiver releases too.
        futures::future::poll_fn(|cx| self.tx.poll_ready(cx)).await?;
        self.tx.start_send(item)?;
        // Queued: the receiver releases the bytes when it takes the item.
        std::mem::forget(reservation);
        Ok(())
    }
}

/// The consumer's end of the queue.
pub(crate) struct QueueReceiver {
    rx: mpsc::Receiver<Item>,
    budget: Arc<Budget>,
}

impl QueueReceiver {
    /// The decoded payload bytes queued and not yet taken.
    #[cfg(test)]
    pub(crate) fn queued_bytes(&self) -> usize {
        self.budget.lock().queued
    }

    /// The byte budget this queue was built with.
    #[cfg(test)]
    pub(crate) fn max_bytes(&self) -> usize {
        self.budget.max
    }
}

impl Drop for QueueReceiver {
    /// Closes the channel first, so the woken send's `poll_ready` sees the
    /// disconnect instead of queueing into a receiver about to go away.
    fn drop(&mut self) {
        self.rx.close();
        self.budget.close();
    }
}

impl Stream for QueueReceiver {
    type Item = Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Item>> {
        let polled = Pin::new(&mut self.rx).poll_next(cx);
        if let Poll::Ready(Some(item)) = &polled {
            self.budget.release(bytes_of(item));
        }
        polled
    }
}

/// A queue of at most `capacity` items (as `mpsc::channel`) and, past the
/// first, at most `max_bytes` of decoded payload. A `max_bytes` of 0 does
/// not deadlock: an empty queue always admits one item, so it holds one
/// item at a time.
pub(crate) fn receive_queue(capacity: usize, max_bytes: usize) -> (QueueSender, QueueReceiver) {
    let (tx, rx) = mpsc::channel(capacity);
    let budget = Arc::new(Budget {
        max: max_bytes,
        state: Mutex::new(State::default()),
    });
    (
        QueueSender {
            tx,
            budget: Arc::clone(&budget),
        },
        QueueReceiver { rx, budget },
    )
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use futures::{task::ArcWake, Future, FutureExt, StreamExt};

    use super::*;

    /// Counts wake-ups, so a test observes the engine being woken instead
    /// of waiting on a clock.
    #[derive(Default)]
    struct Wakes(AtomicUsize);

    impl ArcWake for Wakes {
        fn wake_by_ref(arc_self: &Arc<Self>) {
            arc_self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn payload(bytes: usize) -> Item {
        Ok((
            MessageIdData::default(),
            Payload {
                metadata: Default::default(),
                data: vec![0u8; bytes],
            },
        ))
    }

    /// A consumer that never reads holds at most the budget, the engine's
    /// next send waits, and taking one payload wakes it and lets it through.
    #[test]
    fn a_stalled_consumer_holds_at_most_the_budget() {
        const EACH: usize = 8 * 1024;
        // Eight payloads' bytes, metadata included.
        let budget = 8 * bytes_of(&payload(EACH));
        let (mut tx, mut rx) = receive_queue(1000, budget);
        let wakes = Arc::new(Wakes::default());
        let waker = futures::task::waker(Arc::clone(&wakes));
        let mut cx = Context::from_waker(&waker);
        for i in 0..8 {
            let sent = tx.send(payload(EACH)).now_or_never();
            assert!(matches!(sent, Some(Ok(()))), "payload {i} fits the budget");
        }
        assert_eq!(rx.queued_bytes(), budget);
        let mut pending = Box::pin(tx.send(payload(EACH)));
        assert!(
            pending.as_mut().poll(&mut cx).is_pending(),
            "the send past the budget waits"
        );
        assert_eq!(rx.queued_bytes(), budget, "a waiting send queues nothing");

        assert!(rx.next().now_or_never().flatten().is_some());
        assert_eq!(
            wakes.0.load(Ordering::SeqCst),
            1,
            "taking one wakes the engine"
        );
        assert!(pending.as_mut().poll(&mut cx).is_ready());
        assert_eq!(rx.queued_bytes(), budget);
    }

    /// One payload larger than the whole budget still goes through an empty
    /// queue: it cannot wait for room that will never exist.
    #[test]
    fn a_payload_past_the_budget_passes_an_empty_queue() {
        let (mut tx, mut rx) = receive_queue(10, 1024);
        tx.send(payload(4096)).now_or_never().unwrap().unwrap();
        assert_eq!(rx.queued_bytes(), bytes_of(&payload(4096)));
        assert!(rx.next().now_or_never().flatten().is_some());
        assert_eq!(rx.queued_bytes(), 0);
    }

    /// A send cancelled while it waits for room in the channel gives its
    /// reserved bytes back.
    #[test]
    fn a_cancelled_send_releases_its_bytes() {
        // `mpsc::channel(0)` holds one item per sender.
        let (mut tx, rx) = receive_queue(0, 1024 * 1024);
        tx.send(payload(100)).now_or_never().unwrap().unwrap();
        assert!(
            tx.send(payload(200)).now_or_never().is_none(),
            "the channel is full"
        );
        assert_eq!(rx.queued_bytes(), bytes_of(&payload(100)));
        let mut rx = rx;
        assert!(rx.next().now_or_never().flatten().is_some());
        assert_eq!(rx.queued_bytes(), 0, "the queued item's bytes, once");
    }

    /// A payload with no data still costs its metadata: 1000 properties of
    /// 1 KiB values take a whole 64 KiB budget, so the next such payload
    /// waits instead of queueing for free.
    #[test]
    fn metadata_counts_against_the_budget() {
        fn heavy() -> Item {
            let mut item = payload(0);
            if let Ok((_, payload)) = &mut item {
                payload.metadata.properties = (0..1000)
                    .map(|i| crate::message::proto::KeyValue {
                        key: format!("k{i}"),
                        value: "v".repeat(1024),
                    })
                    .collect();
            }
            item
        }
        let (mut tx, rx) = receive_queue(1000, 64 * 1024);
        tx.send(heavy()).now_or_never().unwrap().unwrap();
        assert!(
            rx.queued_bytes() > 1000 * 1024,
            "queued {} bytes for 1 MiB of properties",
            rx.queued_bytes()
        );
        assert!(
            tx.send(heavy()).now_or_never().is_none(),
            "the second payload waits for the budget"
        );
    }

    /// The consumer dropped while the engine waits on the budget: the wait
    /// is woken and the send fails (the engine then stops), instead of
    /// parking forever. Polled by hand, so nothing waits on a clock.
    #[test]
    fn dropping_the_receiver_fails_a_send_waiting_on_the_budget() {
        let (mut tx, rx) = receive_queue(1000, 1024);
        tx.send(payload(1024)).now_or_never().unwrap().unwrap();
        let wakes = Arc::new(Wakes::default());
        let waker = futures::task::waker(Arc::clone(&wakes));
        let mut cx = Context::from_waker(&waker);
        let mut pending = Box::pin(tx.send(payload(1024)));
        assert!(
            pending.as_mut().poll(&mut cx).is_pending(),
            "over the budget"
        );

        drop(rx);
        assert_eq!(
            wakes.0.load(Ordering::SeqCst),
            1,
            "the drop wakes the engine"
        );
        match pending.as_mut().poll(&mut cx) {
            Poll::Ready(Err(_)) => {}
            Poll::Ready(Ok(())) => panic!("a send to a dropped consumer succeeded"),
            Poll::Pending => panic!("the send still waits on a dropped consumer"),
        }
    }
}
