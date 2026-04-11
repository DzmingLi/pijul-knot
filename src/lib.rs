//! pijul-knot: Pijul repository hosting as a library.
//!
//! - `PijulStore`: direct local pijul-core operations
//! - `KnotClient`: HTTP client for a remote pijul-knot server
//! - `router()`: Axum HTTP router exposing PijulStore as REST API
//!
//! ```rust,ignore
//! // Embed locally:
//! let state = pijul_knot::KnotState {
//!     pijul: Arc::new(pijul_knot::PijulStore::new("./data")),
//! };
//! let app = axum::Router::new().nest("/knot", pijul_knot::router(state));
//!
//! // Or connect to remote knot:
//! let client = pijul_knot::KnotClient::new("https://knot.example.com");
//! let files = client.list_files("my-repo").await?;
//! ```

pub mod store;
pub mod client;
mod routes;

pub use store::{PijulStore, ChangeInfo, ChannelDiffResult, DiffHunk, DiffResult, TrackedFile};
pub use client::KnotClient;
pub use routes::router;

/// Shared state for the knot router.
#[derive(Clone)]
pub struct KnotState {
    pub pijul: std::sync::Arc<PijulStore>,
}
