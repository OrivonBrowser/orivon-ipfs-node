//! WASI (preview 2) backend.
//!
//! The native backend's global timer runs on a dedicated helper OS
//! thread, spawned lazily on first use (`native/global.rs`'s
//! `HelperThread::new`). `std::thread::Builder::spawn` has no
//! thread-spawn host import to satisfy under `wasm32-wasip2`, so that
//! spawn fails (`Err`, not a panic — the immediate call site handles it
//! fine); but every `Delay` created afterward silently becomes
//! permanently "inert" (its `Weak` handle never upgrades), and only
//! panics ("timer has gone away") the first time anything actually
//! polls or resets one. Confirmed at runtime under wasmtime: a node
//! started, opened its HTTP listener and resolved its bootnodes'
//! `/dnsaddr/` TXT records fine, then panicked a few hundred
//! milliseconds later — the first redial-backoff/connection-idle timer
//! actually polled.
//!
//! Built on `tokio::time::sleep` instead: it needs no background OS
//! thread at all — tokio's own timer wheel runs entirely inside the
//! single-threaded reactor's own poll loop, which was already proven
//! working here (the DNS resolution above times out through the same
//! reactor, and succeeded before this wall was ever reached).

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::time::Sleep;

/// A future representing the notification that an elapsed duration has
/// occurred. See the module docs for how this differs from the native
/// (non-wasi) `Delay`.
pub struct Delay {
    inner: Pin<Box<Sleep>>,
}

impl Delay {
    /// Creates a new future which will fire at `dur` time into the future.
    #[inline]
    pub fn new(dur: Duration) -> Delay {
        Delay {
            inner: Box::pin(tokio::time::sleep(dur)),
        }
    }

    /// Resets this timeout to a new timeout which will fire `dur` from now.
    #[inline]
    pub fn reset(&mut self, dur: Duration) {
        let deadline = tokio::time::Instant::now() + dur;
        self.inner.as_mut().reset(deadline);
    }
}

impl Future for Delay {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.inner.as_mut().poll(cx)
    }
}

impl fmt::Debug for Delay {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Delay").finish()
    }
}
