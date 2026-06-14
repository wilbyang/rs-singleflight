#![doc = include_str!("../README.md")]

use std::{
    collections::{HashMap, hash_map::RandomState},
    fmt,
    future::Future,
    hash::{BuildHasher, Hash},
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use tokio::sync::broadcast;

type SharedOutcome<T, E> = Arc<Outcome<T, E>>;
type Calls<K, T, E, S> = HashMap<K, Weak<Call<K, T, E, S>>, S>;

/// Result published by the single in-flight computation.
#[derive(Debug)]
pub enum Outcome<T, E> {
    /// The leader completed the computation.
    Complete { result: Result<T, E>, shared: bool },
    /// The leader future was dropped before it completed.
    Canceled,
}

impl<T, E> Outcome<T, E> {
    pub fn is_shared(&self) -> bool {
        matches!(self, Self::Complete { shared: true, .. })
    }

    pub fn result(&self) -> Option<&Result<T, E>> {
        match self {
            Self::Complete { result, .. } => Some(result),
            Self::Canceled => None,
        }
    }
}

/// Error returned when a subscriber cannot receive a leader result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WaitError {
    /// The broadcast channel closed before an outcome was available.
    Closed,
    /// The subscriber lagged behind the broadcast channel.
    Lagged(u64),
}

impl fmt::Display for WaitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Closed => f.write_str("singleflight result channel closed"),
            Self::Lagged(n) => write!(f, "singleflight subscriber lagged by {n} messages"),
        }
    }
}

impl std::error::Error for WaitError {}

/// Namespace for duplicate suppression.
///
/// For a given key, only the leader computes. Duplicate callers subscribe to
/// the leader's broadcast and receive the same [`Outcome`].
pub struct Group<K, T, E, S = RandomState> {
    inner: Arc<Inner<K, T, E, S>>,
}

impl<K, T, E> Group<K, T, E, RandomState> {
    pub fn new() -> Self {
        Self::with_hasher(RandomState::new())
    }
}

impl<K, T, E, S> Group<K, T, E, S> {
    pub fn with_hasher(hasher: S) -> Self {
        Self {
            inner: Arc::new(Inner {
                calls: Mutex::new(HashMap::with_hasher(hasher)),
            }),
        }
    }
}

impl<K, T, E, S> Clone for Group<K, T, E, S> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<K, T, E> Default for Group<K, T, E, RandomState> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K, T, E, S> Group<K, T, E, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    /// Returns a leader for a new key, or a subscriber for an in-flight key.
    pub fn entry(&self, key: K) -> Entry<K, T, E, S> {
        let mut calls = self
            .inner
            .calls
            .lock()
            .expect("singleflight mutex poisoned");

        if let Some(call) = calls.get(&key).and_then(Weak::upgrade) {
            return Entry::Subscriber(call.subscribe());
        }

        let call = Arc::new(Call::new(Arc::downgrade(&self.inner)));
        calls.insert(key, Arc::downgrade(&call));
        Entry::Leader(Leader { call: Some(call) })
    }

    /// Executes `f` once per key while an earlier call is in flight.
    pub async fn run<F, Fut>(&self, key: K, f: F) -> SharedOutcome<T, E>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, E>>,
    {
        match self.entry(key) {
            Entry::Leader(leader) => {
                let result = f().await;
                leader.complete(result)
            }
            Entry::Subscriber(subscriber) => subscriber
                .recv()
                .await
                .unwrap_or_else(|_| Arc::new(Outcome::Canceled)),
        }
    }

    /// Forgets a key so the next [`entry`](Self::entry) or [`run`](Self::run)
    /// starts a fresh leader instead of joining the current call.
    pub fn forget<Q>(&self, key: &Q)
    where
        K: std::borrow::Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.inner
            .calls
            .lock()
            .expect("singleflight mutex poisoned")
            .remove(key);
    }

    pub fn in_flight(&self) -> usize {
        self.inner
            .calls
            .lock()
            .expect("singleflight mutex poisoned")
            .len()
    }
}

/// Returned by [`Group::entry`].
pub enum Entry<K, T, E, S = RandomState> {
    Leader(Leader<K, T, E, S>),
    Subscriber(Subscriber<T, E>),
}

/// Owner of the single computation for a key.
///
/// Dropping a leader before calling [`complete`](Self::complete) publishes
/// [`Outcome::Canceled`] to subscribers and removes the key from the group.
pub struct Leader<K, T, E, S = RandomState> {
    call: Option<Arc<Call<K, T, E, S>>>,
}

impl<K, T, E, S> Leader<K, T, E, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    pub fn complete(mut self, result: Result<T, E>) -> SharedOutcome<T, E> {
        let call = self.call.take().expect("leader completed twice");
        call.cleanup();
        let shared = call.waiters.load(Ordering::SeqCst) > 0;
        let outcome = Arc::new(Outcome::Complete { result, shared });
        call.publish(Arc::clone(&outcome));
        outcome
    }

    pub fn subscribe(&self) -> Subscriber<T, E> {
        self.call
            .as_ref()
            .expect("leader already completed")
            .subscribe()
    }

    pub fn duplicate_count(&self) -> usize {
        self.call
            .as_ref()
            .map(|call| call.waiters.load(Ordering::SeqCst))
            .unwrap_or(0)
    }
}

impl<K, T, E, S> Drop for Leader<K, T, E, S> {
    fn drop(&mut self) {
        if let Some(call) = self.call.take() {
            call.cancel();
        }
    }
}

/// Receiver for a duplicate caller.
pub struct Subscriber<T, E> {
    rx: broadcast::Receiver<SharedOutcome<T, E>>,
}

impl<T, E> Subscriber<T, E> {
    pub async fn recv(mut self) -> Result<SharedOutcome<T, E>, WaitError> {
        match self.rx.recv().await {
            Ok(outcome) => Ok(outcome),
            Err(broadcast::error::RecvError::Closed) => Err(WaitError::Closed),
            Err(broadcast::error::RecvError::Lagged(n)) => Err(WaitError::Lagged(n)),
        }
    }
}

struct Inner<K, T, E, S> {
    calls: Mutex<Calls<K, T, E, S>>,
}

struct Call<K, T, E, S> {
    group: Weak<Inner<K, T, E, S>>,
    tx: broadcast::Sender<SharedOutcome<T, E>>,
    waiters: AtomicUsize,
    finished: AtomicBool,
}

impl<K, T, E, S> Call<K, T, E, S> {
    fn new(group: Weak<Inner<K, T, E, S>>) -> Self {
        let (tx, _) = broadcast::channel(1);
        Self {
            group,
            tx,
            waiters: AtomicUsize::new(0),
            finished: AtomicBool::new(false),
        }
    }

    fn subscribe(&self) -> Subscriber<T, E> {
        self.waiters.fetch_add(1, Ordering::SeqCst);
        Subscriber {
            rx: self.tx.subscribe(),
        }
    }

    fn publish(&self, outcome: SharedOutcome<T, E>) {
        if !self.finished.swap(true, Ordering::SeqCst) {
            let _ = self.tx.send(outcome);
        }
    }

    fn cancel(&self) {
        self.cleanup();
        self.publish(Arc::new(Outcome::Canceled));
    }

    fn cleanup(&self) {
        let Some(group) = self.group.upgrade() else {
            return;
        };

        let mut calls = group.calls.lock().expect("singleflight mutex poisoned");
        calls.retain(|_, existing| {
            existing
                .upgrade()
                .is_some_and(|call| !std::ptr::eq(call.as_ref(), self))
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use tokio::{
        sync::{Barrier, oneshot},
        time::{Duration, sleep, timeout},
    };

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn suppresses_duplicate_calls() {
        let group = Arc::new(Group::<String, String, ()>::new());
        let calls = Arc::new(AtomicUsize::new(0));
        let barrier = Arc::new(Barrier::new(12));
        let mut tasks = Vec::new();

        for _ in 0..12 {
            let group = Arc::clone(&group);
            let calls = Arc::clone(&calls);
            let barrier = Arc::clone(&barrier);
            tasks.push(tokio::spawn(async move {
                barrier.wait().await;
                group
                    .run("key".to_owned(), || async {
                        calls.fetch_add(1, Ordering::SeqCst);
                        sleep(Duration::from_millis(20)).await;
                        Ok("value".to_owned())
                    })
                    .await
            }));
        }

        let mut shared = false;
        for task in tasks {
            let outcome = task.await.expect("task panicked");
            match outcome.as_ref() {
                Outcome::Complete { result, shared: s } => {
                    assert_eq!(result.as_ref().unwrap(), "value");
                    shared |= *s;
                }
                Outcome::Canceled => panic!("leader should complete"),
            }
        }

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(shared);
        assert_eq!(group.in_flight(), 0);
    }

    #[tokio::test]
    async fn subscribers_receive_cancellation_when_leader_is_dropped() {
        let group = Group::<&'static str, usize, ()>::new();
        let leader = match group.entry("key") {
            Entry::Leader(leader) => leader,
            Entry::Subscriber(_) => panic!("first entry must lead"),
        };
        let subscriber = match group.entry("key") {
            Entry::Subscriber(subscriber) => subscriber,
            Entry::Leader(_) => panic!("duplicate entry must subscribe"),
        };

        drop(leader);

        let outcome = timeout(Duration::from_secs(1), subscriber.recv())
            .await
            .expect("subscriber hung")
            .expect("subscriber closed");
        assert!(matches!(outcome.as_ref(), Outcome::Canceled));
        assert_eq!(group.in_flight(), 0);
    }

    #[tokio::test]
    async fn forget_starts_a_new_leader_without_breaking_old_one() {
        let group = Group::<&'static str, usize, ()>::new();
        let first = match group.entry("key") {
            Entry::Leader(leader) => leader,
            Entry::Subscriber(_) => panic!("first entry must lead"),
        };

        group.forget("key");

        let second = match group.entry("key") {
            Entry::Leader(leader) => leader,
            Entry::Subscriber(_) => panic!("forgotten key should create a new leader"),
        };
        let third = match group.entry("key") {
            Entry::Subscriber(subscriber) => subscriber,
            Entry::Leader(_) => panic!("third entry should subscribe to second leader"),
        };

        first.complete(Ok(1));
        let published = second.complete(Ok(2));
        assert!(matches!(
            published.as_ref(),
            Outcome::Complete {
                result: Ok(2),
                shared: true
            }
        ));

        let received = third.recv().await.expect("third subscriber closed");
        assert!(matches!(
            received.as_ref(),
            Outcome::Complete {
                result: Ok(2),
                shared: true
            }
        ));
        assert_eq!(group.in_flight(), 0);
    }

    #[tokio::test]
    async fn custom_entry_api_allows_external_compute_placement() {
        let group = Group::<&'static str, usize, ()>::new();
        let (release_tx, release_rx) = oneshot::channel();

        let leader = match group.entry("key") {
            Entry::Leader(leader) => leader,
            Entry::Subscriber(_) => panic!("first entry must lead"),
        };
        let duplicate = match group.entry("key") {
            Entry::Subscriber(subscriber) => subscriber,
            Entry::Leader(_) => panic!("duplicate entry must subscribe"),
        };

        let task = tokio::spawn(async move {
            release_rx.await.expect("release dropped");
            leader.complete(Ok(42))
        });

        release_tx.send(()).expect("leader task dropped");
        assert!(matches!(
            duplicate.recv().await.unwrap().as_ref(),
            Outcome::Complete {
                result: Ok(42),
                shared: true
            }
        ));
        assert!(task.await.unwrap().is_shared());
    }
}
