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
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex, MutexGuard, PoisonError,
    },
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

/// One queued item with the bytes reserved for it and the subscription
/// session (generation) that received it: the receiver releases exactly the
/// reserved bytes, measured once on send, and drops an item from a session
/// the engine has since replaced (the broker redelivers it on the new one).
struct Queued {
    generation: u64,
    bytes: usize,
    item: Item,
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
    /// The engine's current subscription session; items from an older one are stale.
    generation: AtomicU64,
    state: Mutex<State>,
    /// FORK-PATCH (3rdi): the error that ended the engine when the channel
    /// was full; yielded once the channel is drained and closed.
    terminal: Mutex<Option<Error>>,
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
    tx: mpsc::Sender<Queued>,
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
        let queued = Queued {
            generation: budget.generation.load(Ordering::Acquire),
            bytes,
            item,
        };
        self.tx.start_send(queued)?;
        // Queued: the receiver releases the bytes when it takes the item.
        std::mem::forget(reservation);
        Ok(())
    }

    /// FORK-PATCH (3rdi): queues the error that ends the consumer, without
    /// waiting (an error is free of the byte budget). When the channel is
    /// full it is kept aside and yielded after the channel drains. False
    /// only when the consumer is gone.
    pub(crate) fn try_send_error(&mut self, e: Error) -> bool {
        let generation = self.budget.generation.load(Ordering::Acquire);
        match self.tx.try_send(Queued {
            generation,
            bytes: 0,
            item: Err(e),
        }) {
            Ok(()) => true,
            Err(full) if full.is_full() => {
                let item = full.into_inner().item;
                if let Err(e) = item {
                    *self
                        .budget
                        .terminal
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner) = Some(e);
                }
                true
            }
            Err(_) => false,
        }
    }

    /// Starts a new subscription session: every item queued so far, and
    /// every one sent before the next call, is from the old session and the
    /// receiver drops it (releasing its bytes) instead of yielding it. An
    /// error item is still yielded: it carries no message the broker
    /// redelivers.
    pub(crate) fn new_generation(&self) -> u64 {
        self.budget.generation.fetch_add(1, Ordering::AcqRel) + 1
    }
}

/// The consumer's end of the queue.
pub(crate) struct QueueReceiver {
    rx: mpsc::Receiver<Queued>,
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
        loop {
            match Pin::new(&mut self.rx).poll_next(cx) {
                Poll::Ready(Some(Queued {
                    generation,
                    bytes,
                    item,
                })) => {
                    self.budget.release(bytes);
                    let current = self.budget.generation.load(Ordering::Acquire);
                    if generation == current || item.is_err() {
                        return Poll::Ready(Some(item));
                    }
                }
                Poll::Ready(None) => {
                    let terminal = self
                        .budget
                        .terminal
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .take();
                    return Poll::Ready(terminal.map(Err));
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

/// A queue of at most `capacity` items (as `mpsc::channel`) and, past the
/// first, at most `max_bytes` of decoded payload. A `max_bytes` of 0 does
/// not deadlock: an empty queue always admits one item, so it holds one
/// item at a time.
pub(crate) fn receive_queue(capacity: usize, max_bytes: usize) -> (QueueSender, QueueReceiver) {
    let (tx, rx) = mpsc::channel::<Queued>(capacity);
    let budget = Arc::new(Budget {
        generation: AtomicU64::new(0),
        max: max_bytes,
        state: Mutex::new(State::default()),
        terminal: Mutex::new(None),
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

    /// FORK-PATCH (3rdi): the error that ends the consumer is never lost to
    /// a full queue: the receiver yields it after everything queued before
    /// it, then ends.
    #[test]
    fn the_ending_error_survives_a_full_queue() {
        // `mpsc::channel(0)` holds one item per sender.
        let (mut tx, mut rx) = receive_queue(0, 1024 * 1024);
        tx.send(payload(100)).now_or_never().unwrap().unwrap();
        assert!(tx.try_send_error(Error::Custom("gone".to_string())));
        drop(tx);

        assert!(matches!(rx.next().now_or_never(), Some(Some(Ok(_)))));
        assert!(
            matches!(rx.next().now_or_never(), Some(Some(Err(Error::Custom(m)))) if m == "gone"),
            "the ending error was lost"
        );
        assert!(matches!(rx.next().now_or_never(), Some(None)));
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

    /// Items queued before `new_generation` are from a replaced session:
    /// the receiver drops them, releasing their bytes, and yields only the
    /// new session's items. An error item is yielded whatever its session.
    #[test]
    fn a_replaced_sessions_items_are_dropped_and_released() {
        let (mut tx, mut rx) = receive_queue(1000, 1024 * 1024);
        tx.send(payload(100)).now_or_never().unwrap().unwrap();
        tx.send(Err(Error::Custom("old session".to_string())))
            .now_or_never()
            .unwrap()
            .unwrap();
        tx.send(payload(200)).now_or_never().unwrap().unwrap();
        assert_eq!(tx.new_generation(), 1);
        tx.send(payload(300)).now_or_never().unwrap().unwrap();

        let first = rx.next().now_or_never().flatten().expect("an item");
        assert!(first.is_err(), "the error item is yielded");
        let second = rx.next().now_or_never().flatten().expect("an item");
        assert_eq!(
            second.unwrap().1.data.len(),
            300,
            "only the new session's payload"
        );
        assert_eq!(
            rx.queued_bytes(),
            0,
            "the dropped items' bytes are released"
        );
        assert!(rx.next().now_or_never().is_none(), "nothing else is queued");
    }

    /// Every way an item leaves the queue gives back exactly the bytes it
    /// reserved, measured once on send: taken, dropped as stale, a send
    /// cancelled before queueing, and a send failing on a closed channel.
    #[test]
    fn release_equals_reserve_on_every_path() {
        // Taken.
        let (mut tx, mut rx) = receive_queue(1000, 1024 * 1024);
        tx.send(payload(123)).now_or_never().unwrap().unwrap();
        assert_eq!(rx.queued_bytes(), bytes_of(&payload(123)));
        assert!(rx.next().now_or_never().flatten().is_some());
        assert_eq!(rx.queued_bytes(), 0, "taken");

        // Dropped as stale: the poll that skips it returns Pending.
        tx.send(payload(456)).now_or_never().unwrap().unwrap();
        tx.new_generation();
        assert!(
            rx.next().now_or_never().is_none(),
            "the stale item is not yielded"
        );
        assert_eq!(rx.queued_bytes(), 0, "stale");

        // Cancelled: `mpsc::channel(0)` holds one item per sender.
        let (mut tx, mut rx) = receive_queue(0, 1024 * 1024);
        tx.send(payload(10)).now_or_never().unwrap().unwrap();
        assert!(
            tx.send(payload(20)).now_or_never().is_none(),
            "the channel is full"
        );
        assert_eq!(rx.queued_bytes(), bytes_of(&payload(10)), "cancelled");
        assert!(rx.next().now_or_never().flatten().is_some());
        assert_eq!(rx.queued_bytes(), 0);

        // Failed: the channel closed under a live receiver.
        rx.rx.close();
        assert!(matches!(tx.send(payload(30)).now_or_never(), Some(Err(_))));
        assert_eq!(rx.queued_bytes(), 0, "failed");
    }
}
