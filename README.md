# rs-singleflight

`rs-singleflight` provides async single-flight request coalescing for Rust.

For each key, exactly one leader computes an expensive resource while duplicate
callers subscribe to the same in-flight result. If the leader is dropped before
completion, subscribers receive `Outcome::Canceled` instead of hanging.

## Example

```rust,no_run
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use rs_singleflight::{Group, Outcome};

#[tokio::main]
async fn main() {
    let calls = Arc::new(AtomicUsize::new(0));
    let calls_for_op = Arc::clone(&calls);
    let group = Group::new(move |key: String| {
        let calls = Arc::clone(&calls_for_op);
        async move {
            assert_eq!(key, "resource");
            calls.fetch_add(1, Ordering::SeqCst);
            Ok::<String, ()>("computed value".to_owned())
        }
    });

    let mut tasks = Vec::new();
    for _ in 0..8 {
        let group = group.clone();
        tasks.push(tokio::spawn(async move {
            group.run("resource".to_owned()).await
        }));
    }

    for task in tasks {
        match task.await.unwrap().as_ref() {
            Outcome::Complete { result, shared } => {
                assert_eq!(result.as_ref().unwrap(), "computed value");
                assert!(*shared);
            }
            Outcome::Canceled => panic!("leader was canceled"),
        }
    }

    assert_eq!(calls.load(Ordering::SeqCst), 1);
}
```

## Custom scheduling

Use `Group::entry` when you want to decide where the leader computation runs.
It returns either a `Leader`, which owns the single computation, or a
`Subscriber`, which waits on the leader's broadcast result.

```rust,no_run
use rs_singleflight::{Entry, Group};

#[tokio::main]
async fn main() {
    let group = Group::new(|key: &'static str| async move {
        assert_eq!(key, "key");
        Ok::<usize, ()>(42)
    });

    match group.entry("key") {
        Entry::Leader(leader) => {
            tokio::spawn(async move {
                leader.complete(Ok(42));
            });
        }
        Entry::Subscriber(subscriber) => {
            let outcome = subscriber.recv().await.unwrap();
            assert!(outcome.is_shared());
        }
    }
}
```

## License

BSD-3-Clause.
