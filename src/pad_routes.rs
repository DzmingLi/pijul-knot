//! Generic Axum routes for pijul-backed project editors (pijul-pad, fedi-xanadu series, etc).
//!
//! Apps implement `PadProjectResolver` to map their project IDs to pijul node IDs,
//! then mount `pad_router::<S>()` at a path like `/api/docs/{id}`.
//!
//! The router handles files, channels, history, record, unrecord, push, pull, and diffs.

use std::sync::Arc;

use axum::{
    Json,
    extract::{FromRef, Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Router,
};

use crate::PijulStore;

// ── Traits ─────────────────────────────────────────────────────────────────

/// Implement this to connect the pad router to your app's project/document model.
#[async_trait::async_trait]
pub trait PadProjectResolver: Send + Sync + 'static {
    /// Resolve a project/document ID to a pijul node_id.
    async fn resolve_node_id(&self, project_id: &str) -> Result<String, PadError>;
    /// Get the knot URL for a project (if configured).
    async fn get_knot_url(&self, project_id: &str) -> Option<String>;
    /// Get the owner DID for a project (used for pull).
    async fn get_owner_did(&self, project_id: &str) -> Result<String, PadError>;
    /// Called after a successful write+record to update timestamps etc.
    async fn on_record(&self, project_id: &str) { let _ = project_id; }
    /// Get a Bearer token for pushing to the remote knot.
    /// `session_token` is the user's session cookie/token for looking up OAuth credentials.
    async fn get_push_token(&self, project_id: &str, session_token: &str) -> Option<String> {
        let _ = (project_id, session_token);
        None
    }
}

/// User identity. The consuming app extracts this from its own auth system.
pub trait PadUser: Send + Sync {
    fn did(&self) -> &str;
    /// Session token for obtaining service auth tokens (e.g. for pushing to knot).
    fn session_token(&self) -> Option<&str> { None }
}

// ── Error ──────────────────────────────────────────────────────────────────

#[derive(Debug)]
pub enum PadError {
    NotFound(String),
    BadRequest(String),
    Internal(String),
}

impl std::fmt::Display for PadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PadError::NotFound(m) => write!(f, "{m}"),
            PadError::BadRequest(m) => write!(f, "{m}"),
            PadError::Internal(m) => write!(f, "{m}"),
        }
    }
}

impl IntoResponse for PadError {
    fn into_response(self) -> Response {
        let (status, msg) = match &self {
            PadError::NotFound(m) => (StatusCode::NOT_FOUND, m.clone()),
            PadError::BadRequest(m) => (StatusCode::BAD_REQUEST, m.clone()),
            PadError::Internal(m) => (StatusCode::INTERNAL_SERVER_ERROR, m.clone()),
        };
        (status, Json(serde_json::json!({ "error": msg }))).into_response()
    }
}

impl From<anyhow::Error> for PadError {
    fn from(e: anyhow::Error) -> Self {
        PadError::Internal(e.to_string())
    }
}

// ── Router ─────────────────────────────────────────────────────────────────

/// Create a pad router. Mount at a path with `{id}` parameter, e.g.:
///
/// ```rust,ignore
/// app.nest("/api/projects/{id}", pijul_knot::pad_router::<MyState, MyUser>())
/// ```
///
/// State `S` must provide:
/// - `Arc<PijulStore>` via FromRef
/// - `Arc<dyn PadProjectResolver>` via FromRef
///
/// User `U` must implement `PadUser` + axum `FromRequestParts<S>`.
pub fn pad_router<S, U>() -> Router<S>
where
    S: Clone + Send + Sync + 'static,
    Arc<PijulStore>: FromRef<S>,
    Arc<dyn PadProjectResolver>: FromRef<S>,
    U: PadUser + axum::extract::FromRequestParts<S> + 'static,
    <U as axum::extract::FromRequestParts<S>>::Rejection: IntoResponse,
{
    Router::new()
        .route("/files", get(list_files::<S>))
        .route("/file", get(read_file::<S>).put(write_file::<S, U>).delete(delete_file::<S, U>))
        .route("/push", post(push_doc::<S, U>))
        .route("/pull", post(pull_doc::<S, U>))
        .route("/channels", get(list_channels::<S>))
        .route("/channel/{ch}/file", get(read_channel_file::<S>).put(write_channel_file::<S, U>))
        .route("/channel/{ch}/log", get(channel_log::<S>))
        .route("/channel/{ch}/log-details", get(channel_log_details::<S>))
        .route("/change/{hash}", get(get_change_detail::<S>))
        .route("/change/{hash}/unrecord", post(unrecord_change::<S, U>))
        .route("/channel/{ch}/apply", post(apply_channel_change::<S, U>))
        .route("/channel-diff", get(channel_diff::<S>))
}

// ── Query/Input types ──────────────────────────────────────────────────────

#[derive(serde::Deserialize)]
struct FileQuery { path: String }

#[derive(serde::Deserialize)]
struct WriteFileInput { path: String, content: String, message: Option<String> }

#[derive(serde::Deserialize)]
struct ApplyInput { change_hash: String }

#[derive(serde::Deserialize)]
struct DiffQuery { a: String, b: String }

// ── Handlers ───────────────────────────────────────────────────────────────

async fn list_files<S>(
    State(pijul): State<Arc<PijulStore>>,
    State(resolver): State<Arc<dyn PadProjectResolver>>,
    Path(id): Path<String>,
) -> Result<Json<Vec<serde_json::Value>>, PadError>
where
    S: Clone + Send + Sync + 'static,
    Arc<PijulStore>: FromRef<S>,
    Arc<dyn PadProjectResolver>: FromRef<S>,
{
    let node = resolver.resolve_node_id(&id).await?;
    let tracked = pijul.list_files(&node).map_err(|e| PadError::Internal(e.to_string()))?;

    fn is_hidden(name: &str) -> bool {
        name.starts_with('.') || matches!(name, "content.html" | "cache")
    }

    let mut paths: std::collections::HashSet<String> = tracked.iter()
        .filter(|f| !is_hidden(f.path.rsplit('/').next().unwrap_or(&f.path)))
        .map(|f| f.path.clone()).collect();
    let mut out: Vec<_> = tracked.iter()
        .filter(|f| !is_hidden(f.path.rsplit('/').next().unwrap_or(&f.path)))
        .map(|f| serde_json::json!({ "path": f.path, "is_dir": f.is_dir })).collect();

    let repo = pijul.repo_path(&node);
    fn scan_dir(base: &std::path::Path, prefix: &str, paths: &mut std::collections::HashSet<String>, out: &mut Vec<serde_json::Value>) {
        let Ok(entries) = std::fs::read_dir(base) else { return };
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().to_string();
            if is_hidden(&name) { continue; }
            let rel = if prefix.is_empty() { name.clone() } else { format!("{prefix}/{name}") };
            let is_dir = entry.file_type().map(|t| t.is_dir()).unwrap_or(false);
            if !paths.contains(&rel) {
                paths.insert(rel.clone());
                out.push(serde_json::json!({ "path": rel, "is_dir": is_dir }));
            }
            if is_dir { scan_dir(&entry.path(), &rel, paths, out); }
        }
    }
    scan_dir(&repo, "", &mut paths, &mut out);
    Ok(Json(out))
}

async fn read_file<S>(
    State(pijul): State<Arc<PijulStore>>,
    State(resolver): State<Arc<dyn PadProjectResolver>>,
    Path(id): Path<String>,
    Query(q): Query<FileQuery>,
) -> Result<Json<serde_json::Value>, PadError>
where
    S: Clone + Send + Sync + 'static,
    Arc<PijulStore>: FromRef<S>,
    Arc<dyn PadProjectResolver>: FromRef<S>,
{
    let node = resolver.resolve_node_id(&id).await?;
    let content = match pijul.get_file_content(&node, &q.path) {
        Ok(c) => c,
        Err(_) => {
            let path = pijul.repo_path(&node).join(&q.path);
            std::fs::read(&path).map_err(|e| PadError::Internal(e.to_string()))?
        }
    };
    Ok(Json(serde_json::json!({ "content": String::from_utf8_lossy(&content) })))
}

async fn write_file<S, U>(
    State(pijul): State<Arc<PijulStore>>,
    State(resolver): State<Arc<dyn PadProjectResolver>>,
    Path(id): Path<String>,
    user: U,
    Json(input): Json<WriteFileInput>,
) -> Result<Json<serde_json::Value>, PadError>
where
    S: Clone + Send + Sync + 'static,
    Arc<PijulStore>: FromRef<S>,
    Arc<dyn PadProjectResolver>: FromRef<S>,
    U: PadUser + axum::extract::FromRequestParts<S>,
    <U as axum::extract::FromRequestParts<S>>::Rejection: IntoResponse,
{
    let node = resolver.resolve_node_id(&id).await?;
    let repo = pijul.repo_path(&node);
    let full = repo.join(&input.path);
    if full.is_dir() {
        return Err(PadError::BadRequest(format!("path is a directory: {}", input.path)));
    }
    if let Some(p) = full.parent() { let _ = std::fs::create_dir_all(p); }
    std::fs::write(&full, &input.content).map_err(|e| PadError::Internal(e.to_string()))?;

    if let Some(msg) = &input.message {
        let result = pijul.record(&node, msg, Some(user.did())).map_err(|e| PadError::Internal(e.to_string()))?;
        resolver.on_record(&id).await;

        if let Some(knot) = resolver.get_knot_url(&id).await {
            let remote = format!("{}/{}/{}", knot, user.did(), node);
            let token = match user.session_token() {
                Some(st) => resolver.get_push_token(&id, st).await,
                None => None,
            };
            if let Err(e) = pijul.push(&node, &remote, None, token.as_deref()) {
                tracing::warn!("auto-push failed: {e}");
            }
        }

        let (hash, merkle) = result.unwrap_or_default();
        return Ok(Json(serde_json::json!({ "change_hash": hash, "merkle": merkle })));
    }

    Ok(Json(serde_json::json!({ "saved": true })))
}

async fn delete_file<S, U>(
    State(pijul): State<Arc<PijulStore>>,
    State(resolver): State<Arc<dyn PadProjectResolver>>,
    Path(id): Path<String>,
    _user: U,
    Query(q): Query<FileQuery>,
) -> Result<StatusCode, PadError>
where
    S: Clone + Send + Sync + 'static,
    Arc<PijulStore>: FromRef<S>,
    Arc<dyn PadProjectResolver>: FromRef<S>,
    U: PadUser + axum::extract::FromRequestParts<S>,
    <U as axum::extract::FromRequestParts<S>>::Rejection: IntoResponse,
{
    let node = resolver.resolve_node_id(&id).await?;
    let full = pijul.repo_path(&node).join(&q.path);
    if full.exists() {
        std::fs::remove_file(&full).map_err(|e| PadError::Internal(e.to_string()))?;
    }
    pijul.remove_file(&node, &q.path).map_err(|e| PadError::Internal(e.to_string()))?;
    Ok(StatusCode::NO_CONTENT)
}

async fn push_doc<S, U>(
    State(pijul): State<Arc<PijulStore>>,
    State(resolver): State<Arc<dyn PadProjectResolver>>,
    Path(id): Path<String>,
    user: U,
) -> Result<Json<serde_json::Value>, PadError>
where
    S: Clone + Send + Sync + 'static,
    Arc<PijulStore>: FromRef<S>,
    Arc<dyn PadProjectResolver>: FromRef<S>,
    U: PadUser + axum::extract::FromRequestParts<S>,
    <U as axum::extract::FromRequestParts<S>>::Rejection: IntoResponse,
{
    let node = resolver.resolve_node_id(&id).await?;
    let knot = resolver.get_knot_url(&id).await
        .ok_or(PadError::BadRequest("no knot configured".into()))?;
    let remote = format!("{}/{}/{}", knot, user.did(), node);
    let token = match user.session_token() {
        Some(st) => resolver.get_push_token(&id, st).await,
        None => None,
    };
    let pushed = pijul.push(&node, &remote, None, token.as_deref())
        .map_err(|e| PadError::Internal(format!("push: {e}")))?;
    Ok(Json(serde_json::json!({ "pushed": pushed.len() })))
}

async fn pull_doc<S, U>(
    State(pijul): State<Arc<PijulStore>>,
    State(resolver): State<Arc<dyn PadProjectResolver>>,
    Path(id): Path<String>,
    _user: U,
) -> Result<Json<serde_json::Value>, PadError>
where
    S: Clone + Send + Sync + 'static,
    Arc<PijulStore>: FromRef<S>,
    Arc<dyn PadProjectResolver>: FromRef<S>,
    U: PadUser + axum::extract::FromRequestParts<S>,
    <U as axum::extract::FromRequestParts<S>>::Rejection: IntoResponse,
{
    let node = resolver.resolve_node_id(&id).await?;
    let knot = resolver.get_knot_url(&id).await
        .ok_or(PadError::BadRequest("no knot configured".into()))?;
    let owner = resolver.get_owner_did(&id).await?;
    let remote = format!("{}/{}/{}", knot, owner, node);
    let pulled = pijul.pull(&node, &remote, None)
        .map_err(|e| PadError::Internal(format!("pull: {e}")))?;
    Ok(Json(serde_json::json!({ "pulled": pulled.len() })))
}

async fn list_channels<S>(
    State(pijul): State<Arc<PijulStore>>,
    State(resolver): State<Arc<dyn PadProjectResolver>>,
    Path(id): Path<String>,
) -> Result<Json<Vec<String>>, PadError>
where
    S: Clone + Send + Sync + 'static,
    Arc<PijulStore>: FromRef<S>,
    Arc<dyn PadProjectResolver>: FromRef<S>,
{
    let node = resolver.resolve_node_id(&id).await?;
    Ok(Json(pijul.list_channels(&node).unwrap_or_else(|_| vec!["main".into()])))
}

async fn read_channel_file<S>(
    State(pijul): State<Arc<PijulStore>>,
    State(resolver): State<Arc<dyn PadProjectResolver>>,
    Path((id, ch)): Path<(String, String)>,
    Query(q): Query<FileQuery>,
) -> Result<Json<serde_json::Value>, PadError>
where
    S: Clone + Send + Sync + 'static,
    Arc<PijulStore>: FromRef<S>,
    Arc<dyn PadProjectResolver>: FromRef<S>,
{
    let node = resolver.resolve_node_id(&id).await?;
    let content = pijul.read_file_from_channel(&node, &ch, &q.path).map_err(|e| PadError::Internal(e.to_string()))?;
    Ok(Json(serde_json::json!({ "content": String::from_utf8_lossy(&content) })))
}

async fn write_channel_file<S, U>(
    State(pijul): State<Arc<PijulStore>>,
    State(resolver): State<Arc<dyn PadProjectResolver>>,
    Path((id, ch)): Path<(String, String)>,
    user: U,
    Json(input): Json<WriteFileInput>,
) -> Result<Json<serde_json::Value>, PadError>
where
    S: Clone + Send + Sync + 'static,
    Arc<PijulStore>: FromRef<S>,
    Arc<dyn PadProjectResolver>: FromRef<S>,
    U: PadUser + axum::extract::FromRequestParts<S>,
    <U as axum::extract::FromRequestParts<S>>::Rejection: IntoResponse,
{
    let node = resolver.resolve_node_id(&id).await?;
    let msg = input.message.as_deref().unwrap_or("update");
    let result = pijul.write_and_record_on_channel(&node, &ch, &input.path, input.content.as_bytes(), msg, Some(user.did()))
        .map_err(|e| PadError::Internal(e.to_string()))?;
    resolver.on_record(&id).await;
    let (hash, merkle) = result.unwrap_or_default();
    Ok(Json(serde_json::json!({ "change_hash": hash, "merkle": merkle })))
}

async fn channel_log<S>(
    State(pijul): State<Arc<PijulStore>>,
    State(resolver): State<Arc<dyn PadProjectResolver>>,
    Path((id, ch)): Path<(String, String)>,
) -> Result<Json<Vec<String>>, PadError>
where
    S: Clone + Send + Sync + 'static,
    Arc<PijulStore>: FromRef<S>,
    Arc<dyn PadProjectResolver>: FromRef<S>,
{
    let node = resolver.resolve_node_id(&id).await?;
    Ok(Json(pijul.log_channel(&node, &ch).map_err(|e| PadError::Internal(e.to_string()))?))
}

async fn channel_log_details<S>(
    State(pijul): State<Arc<PijulStore>>,
    State(resolver): State<Arc<dyn PadProjectResolver>>,
    Path((id, ch)): Path<(String, String)>,
) -> Result<Json<Vec<crate::ChangeInfo>>, PadError>
where
    S: Clone + Send + Sync + 'static,
    Arc<PijulStore>: FromRef<S>,
    Arc<dyn PadProjectResolver>: FromRef<S>,
{
    let node = resolver.resolve_node_id(&id).await?;
    Ok(Json(pijul.log_with_details(&node, &ch).map_err(|e| PadError::Internal(e.to_string()))?))
}

async fn get_change_detail<S>(
    State(pijul): State<Arc<PijulStore>>,
    State(resolver): State<Arc<dyn PadProjectResolver>>,
    Path((id, hash)): Path<(String, String)>,
) -> Result<Json<crate::ChangeDetail>, PadError>
where
    S: Clone + Send + Sync + 'static,
    Arc<PijulStore>: FromRef<S>,
    Arc<dyn PadProjectResolver>: FromRef<S>,
{
    let node = resolver.resolve_node_id(&id).await?;
    Ok(Json(pijul.get_change_detail(&node, &hash).map_err(|e| PadError::Internal(e.to_string()))?))
}

async fn unrecord_change<S, U>(
    State(pijul): State<Arc<PijulStore>>,
    State(resolver): State<Arc<dyn PadProjectResolver>>,
    Path((id, hash)): Path<(String, String)>,
    _user: U,
) -> Result<StatusCode, PadError>
where
    S: Clone + Send + Sync + 'static,
    Arc<PijulStore>: FromRef<S>,
    Arc<dyn PadProjectResolver>: FromRef<S>,
    U: PadUser + axum::extract::FromRequestParts<S>,
    <U as axum::extract::FromRequestParts<S>>::Rejection: IntoResponse,
{
    let node = resolver.resolve_node_id(&id).await?;
    pijul.unrecord_change(&node, &hash).map_err(|e| PadError::Internal(e.to_string()))?;
    Ok(StatusCode::NO_CONTENT)
}

async fn apply_channel_change<S, U>(
    State(pijul): State<Arc<PijulStore>>,
    State(resolver): State<Arc<dyn PadProjectResolver>>,
    Path((id, ch)): Path<(String, String)>,
    _user: U,
    Json(input): Json<ApplyInput>,
) -> Result<StatusCode, PadError>
where
    S: Clone + Send + Sync + 'static,
    Arc<PijulStore>: FromRef<S>,
    Arc<dyn PadProjectResolver>: FromRef<S>,
    U: PadUser + axum::extract::FromRequestParts<S>,
    <U as axum::extract::FromRequestParts<S>>::Rejection: IntoResponse,
{
    let node = resolver.resolve_node_id(&id).await?;
    pijul.apply_change_to_channel(&node, &input.change_hash, &ch).map_err(|e| PadError::Internal(e.to_string()))?;
    Ok(StatusCode::NO_CONTENT)
}

async fn channel_diff<S>(
    State(pijul): State<Arc<PijulStore>>,
    State(resolver): State<Arc<dyn PadProjectResolver>>,
    Path(id): Path<String>,
    Query(q): Query<DiffQuery>,
) -> Result<Json<crate::ChannelDiffResult>, PadError>
where
    S: Clone + Send + Sync + 'static,
    Arc<PijulStore>: FromRef<S>,
    Arc<dyn PadProjectResolver>: FromRef<S>,
{
    let node = resolver.resolve_node_id(&id).await?;
    Ok(Json(pijul.diff_channels(&node, &q.a, &q.b).map_err(|e| PadError::Internal(e.to_string()))?))
}
