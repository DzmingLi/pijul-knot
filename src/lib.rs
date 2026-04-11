//! pijul-knot: Pijul repository hosting as a library.
//!
//! Contains `PijulStore` for direct pijul-core operations and an Axum HTTP
//! router to expose them as a REST API. Embed in your app or run standalone.
//!
//! ```rust,ignore
//! use std::sync::Arc;
//!
//! let state = pijul_knot::KnotState {
//!     pijul: Arc::new(pijul_knot::PijulStore::new("./data")),
//! };
//! let app = axum::Router::new()
//!     .nest("/knot", pijul_knot::router(state));
//! ```

pub mod store;
mod routes;

pub use store::{PijulStore, ChannelDiffResult, DiffHunk, DiffResult, TrackedFile};
pub use routes::router;

/// Shared state for the knot router.
#[derive(Clone)]
pub struct KnotState {
    pub pijul: std::sync::Arc<PijulStore>,
}
