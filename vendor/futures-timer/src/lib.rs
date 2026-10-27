//! A general purpose crate for working with timeouts and delays with futures.
//!
//! # Examples
//!
//! ```no_run
//! # #[async_std::main]
//! # async fn main() {
//! use std::time::Duration;
//! use futures_timer::Delay;
//!
//! let now = Delay::new(Duration::from_secs(3)).await;
//! println!("waited for 3 secs");
//! # }
//! ```

#![deny(missing_docs)]
#![warn(missing_debug_implementations)]

#[cfg(all(
    not(all(target_arch = "wasm32", feature = "wasm-bindgen")),
    not(target_os = "wasi")
))]
mod native;
#[cfg(all(target_arch = "wasm32", feature = "wasm-bindgen"))]
mod wasm;
// See wasi.rs's module docs: the native backend's global timer needs a
// background OS thread that cannot exist on this target.
#[cfg(target_os = "wasi")]
mod wasi;

#[cfg(all(
    not(all(target_arch = "wasm32", feature = "wasm-bindgen")),
    not(target_os = "wasi")
))]
pub use self::native::Delay;
#[cfg(all(target_arch = "wasm32", feature = "wasm-bindgen"))]
pub use self::wasm::Delay;
#[cfg(target_os = "wasi")]
pub use self::wasi::Delay;
