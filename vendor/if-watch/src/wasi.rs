//! WASI preview 2 backend.
//!
//! There is no interface-enumeration syscall under WASI (no netlink, no
//! `getifaddrs`, no Windows IP Helper API — see `if-addrs`'s own gap for
//! the same reason), so this backend never observes any interface and
//! never emits an event. Callers already treat "no watcher output" as a
//! legitimate state (`libp2p-tcp`'s `if_watcher` field is `Option`-typed
//! precisely because not every listen address needs one), so an
//! always-empty, always-pending watcher degrades a wildcard
//! (`0.0.0.0`/`::`) listen address to "only the literal address the
//! operator configured is reported", never to a compile or runtime error.

use crate::IfEvent;
use futures::stream::Stream;
use ipnet::IpNet;
use std::io::Result;
use std::pin::Pin;
use std::task::{Context, Poll};

#[cfg(feature = "tokio")]
pub mod tokio {
    //! An interface watcher.
    //! **On this platform there is no difference between `tokio` and `smol` features,**
    //! **this was done to maintain the api compatible with other platforms**.

    /// Watches for interface changes.
    pub type IfWatcher = super::IfWatcher;
}

#[cfg(feature = "smol")]
pub mod smol {
    //! An interface watcher.
    //! **On this platform there is no difference between `tokio` and `smol` features,**
    //! **this was done to maintain the api compatible with other platforms**.

    /// Watches for interface changes.
    pub type IfWatcher = super::IfWatcher;
}

/// An address set/watcher. Under WASI this set is always empty and never
/// changes — see the module docs.
#[derive(Debug)]
pub struct IfWatcher;

impl IfWatcher {
    /// Create a watcher. Always succeeds; there is nothing to set up.
    pub fn new() -> Result<Self> {
        Ok(Self)
    }

    /// Iterate over current networks — always empty under WASI.
    pub fn iter(&self) -> impl Iterator<Item = &IpNet> {
        std::iter::empty()
    }

    /// Poll for an address change event. Never resolves: no interface
    /// change can ever be observed under WASI.
    pub fn poll_if_event(&mut self, _cx: &mut Context) -> Poll<Result<IfEvent>> {
        Poll::Pending
    }
}

impl Stream for IfWatcher {
    type Item = Result<IfEvent>;
    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Pending
    }
}
