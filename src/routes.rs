use axum::{
    Router,
    routing::{get, post, put, delete},
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
    response::IntoResponse,
};
use crate::KnotState;

pub fn router(state: KnotState) -> Router {
    Router::new()
        // Repo lifecycle
        .route("/repos", post(init_repo))
        .route("/repos/{node_id}/files", get(list_files))
        // File operations (working copy / main channel)
        .route("/repos/{node_id}/file", get(read_file).put(write_file))
        // Versioning
        .route("/repos/{node_id}/record", post(record))
        .route("/repos/{node_id}/log", get(log))
        .route("/repos/{node_id}/log-details", get(log_details))
        .route("/repos/{node_id}/change/{hash}", get(change_detail))
        .route("/repos/{node_id}/unrecord", post(unrecord))
        .route("/repos/{node_id}/revert", post(revert))
        .route("/repos/{node_id}/diff", get(diff))
        // Cross-repo
        .route("/repos/{node_id}/fork", post(fork_repo))
        .route("/repos/{node_id}/apply", post(apply_cross_repo))
        .route("/repos/{node_id}/diff-repos", get(diff_repos))
        // Channels
        .route("/repos/{node_id}/channels", get(list_channels).post(create_channel))
        .route("/repos/{node_id}/channels/{name}", delete(delete_channel))
        .route("/repos/{node_id}/channel/{ch}/file", get(read_channel_file).put(write_channel_file))
        .route("/repos/{node_id}/channel/{ch}/log", get(channel_log))
        .route("/repos/{node_id}/channel/{ch}/apply", post(apply_to_channel))
        .route("/repos/{node_id}/channel-diff", get(channel_diff))
        // Series-specific
        .route("/repos/{node_id}/init-series", post(init_series_repo))
        .with_state(state)
}

type R<T> = Result<T, AppError>;

struct AppError(anyhow::Error);
impl From<anyhow::Error> for AppError {
    fn from(e: anyhow::Error) -> Self { AppError(e) }
}
impl IntoResponse for AppError {
    fn into_response(self) -> axum::response::Response {
        (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": self.0.to_string() }))).into_response()
    }
}

// ---- Repo lifecycle ----

#[derive(serde::Deserialize)]
struct InitRepoInput { node_id: String, series: Option<bool> }

async fn init_repo(State(state): State<KnotState>, Json(input): Json<InitRepoInput>) -> R<(StatusCode, Json<serde_json::Value>)> {
    let pijul = state.pijul.clone();
    let node_id = input.node_id.clone();
    let is_series = input.series.unwrap_or(false);
    tokio::task::spawn_blocking(move || {
        if is_series { pijul.init_series_repo(&node_id) } else { pijul.init_repo(&node_id) }
    }).await.map_err(|e| anyhow::anyhow!(e))??;
    Ok((StatusCode::CREATED, Json(serde_json::json!({ "node_id": input.node_id }))))
}

async fn init_series_repo(State(state): State<KnotState>, Path(node_id): Path<String>) -> R<StatusCode> {
    let pijul = state.pijul.clone();
    tokio::task::spawn_blocking(move || pijul.init_series_repo(&node_id)).await.map_err(|e| anyhow::anyhow!(e))??;
    Ok(StatusCode::CREATED)
}

async fn list_files(State(state): State<KnotState>, Path(node_id): Path<String>) -> R<Json<Vec<serde_json::Value>>> {
    let pijul = state.pijul.clone();
    let files = tokio::task::spawn_blocking(move || pijul.list_files(&node_id)).await.map_err(|e| anyhow::anyhow!(e))??;
    let out: Vec<_> = files.iter().map(|f| serde_json::json!({ "path": f.path, "is_dir": f.is_dir })).collect();
    Ok(Json(out))
}

// ---- File operations ----

#[derive(serde::Deserialize)]
struct FileQuery { path: String }

async fn read_file(State(state): State<KnotState>, Path(node_id): Path<String>, Query(q): Query<FileQuery>) -> R<Json<serde_json::Value>> {
    let pijul = state.pijul.clone();
    let path = q.path;
    let content = tokio::task::spawn_blocking(move || pijul.get_file_content(&node_id, &path)).await.map_err(|e| anyhow::anyhow!(e))??;
    let text = String::from_utf8_lossy(&content).into_owned();
    Ok(Json(serde_json::json!({ "content": text })))
}

#[derive(serde::Deserialize)]
struct WriteFileInput { path: String, content: String }

async fn write_file(State(state): State<KnotState>, Path(node_id): Path<String>, Json(input): Json<WriteFileInput>) -> R<StatusCode> {
    let pijul = state.pijul.clone();
    tokio::task::spawn_blocking(move || {
        let repo = pijul.repo_path(&node_id);
        let full = repo.join(&input.path);
        if let Some(p) = full.parent() { std::fs::create_dir_all(p)?; }
        std::fs::write(&full, &input.content)?;
        Ok::<_, anyhow::Error>(())
    }).await.map_err(|e| anyhow::anyhow!(e))??;
    Ok(StatusCode::NO_CONTENT)
}

// ---- Versioning ----

#[derive(serde::Deserialize)]
struct RecordInput { message: String, author_did: Option<String> }

async fn record(State(state): State<KnotState>, Path(node_id): Path<String>, Json(input): Json<RecordInput>) -> R<Json<serde_json::Value>> {
    let pijul = state.pijul.clone();
    let result = tokio::task::spawn_blocking(move || {
        pijul.record(&node_id, &input.message, input.author_did.as_deref())
    }).await.map_err(|e| anyhow::anyhow!(e))??;
    let (hash, merkle) = result.unwrap_or_default();
    Ok(Json(serde_json::json!({ "change_hash": hash, "merkle": merkle })))
}

#[derive(serde::Deserialize)]
struct ChannelQuery { channel: Option<String> }

async fn log(State(state): State<KnotState>, Path(node_id): Path<String>, Query(q): Query<ChannelQuery>) -> R<Json<Vec<String>>> {
    let pijul = state.pijul.clone();
    let ch = q.channel;
    let hashes = tokio::task::spawn_blocking(move || {
        match ch.as_deref() {
            Some(c) => pijul.log_channel(&node_id, c),
            None => pijul.log(&node_id),
        }
    }).await.map_err(|e| anyhow::anyhow!(e))??;
    Ok(Json(hashes))
}

async fn log_details(State(state): State<KnotState>, Path(node_id): Path<String>, Query(q): Query<ChannelQuery>) -> R<Json<Vec<crate::ChangeInfo>>> {
    let pijul = state.pijul.clone();
    let ch = q.channel.unwrap_or_else(|| "main".to_string());
    let infos = tokio::task::spawn_blocking(move || pijul.log_with_details(&node_id, &ch)).await.map_err(|e| anyhow::anyhow!(e))??;
    Ok(Json(infos))
}

async fn change_detail(State(state): State<KnotState>, Path((node_id, hash)): Path<(String, String)>) -> R<Json<crate::ChangeDetail>> {
    let pijul = state.pijul.clone();
    let detail = tokio::task::spawn_blocking(move || pijul.get_change_detail(&node_id, &hash)).await.map_err(|e| anyhow::anyhow!(e))??;
    Ok(Json(detail))
}

#[derive(serde::Deserialize)]
struct UnrecordInput { change_hash: String }

async fn unrecord(State(state): State<KnotState>, Path(node_id): Path<String>, Json(input): Json<UnrecordInput>) -> R<StatusCode> {
    let pijul = state.pijul.clone();
    tokio::task::spawn_blocking(move || pijul.unrecord_change(&node_id, &input.change_hash)).await.map_err(|e| anyhow::anyhow!(e))??;
    Ok(StatusCode::NO_CONTENT)
}

async fn revert(State(state): State<KnotState>, Path(node_id): Path<String>) -> R<StatusCode> {
    let pijul = state.pijul.clone();
    tokio::task::spawn_blocking(move || pijul.revert(&node_id)).await.map_err(|e| anyhow::anyhow!(e))??;
    Ok(StatusCode::NO_CONTENT)
}

async fn diff(State(state): State<KnotState>, Path(node_id): Path<String>) -> R<Json<crate::DiffResult>> {
    let pijul = state.pijul.clone();
    let result = tokio::task::spawn_blocking(move || pijul.diff(&node_id)).await.map_err(|e| anyhow::anyhow!(e))??;
    Ok(Json(result))
}

// ---- Cross-repo ----

#[derive(serde::Deserialize)]
struct ForkInput { fork_node_id: String }

async fn fork_repo(State(state): State<KnotState>, Path(node_id): Path<String>, Json(input): Json<ForkInput>) -> R<(StatusCode, Json<serde_json::Value>)> {
    let pijul = state.pijul.clone();
    let fork_id = input.fork_node_id.clone();
    tokio::task::spawn_blocking(move || pijul.fork(&node_id, &input.fork_node_id)).await.map_err(|e| anyhow::anyhow!(e))??;
    Ok((StatusCode::CREATED, Json(serde_json::json!({ "fork_node_id": fork_id }))))
}

#[derive(serde::Deserialize)]
struct ApplyInput { source_node_id: String, change_hash: String }

async fn apply_cross_repo(State(state): State<KnotState>, Path(node_id): Path<String>, Json(input): Json<ApplyInput>) -> R<StatusCode> {
    let pijul = state.pijul.clone();
    tokio::task::spawn_blocking(move || pijul.apply(&input.source_node_id, &node_id, &input.change_hash)).await.map_err(|e| anyhow::anyhow!(e))??;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(serde::Deserialize)]
struct DiffReposQuery { target: String }

async fn diff_repos(State(state): State<KnotState>, Path(node_id): Path<String>, Query(q): Query<DiffReposQuery>) -> R<Json<Vec<String>>> {
    let pijul = state.pijul.clone();
    let ahead = tokio::task::spawn_blocking(move || pijul.diff_repos(&node_id, &q.target)).await.map_err(|e| anyhow::anyhow!(e))??;
    Ok(Json(ahead))
}

// ---- Channels ----

async fn list_channels(State(state): State<KnotState>, Path(node_id): Path<String>) -> R<Json<Vec<String>>> {
    let pijul = state.pijul.clone();
    let chs = tokio::task::spawn_blocking(move || pijul.list_channels(&node_id)).await.map_err(|e| anyhow::anyhow!(e))??;
    Ok(Json(chs))
}

#[derive(serde::Deserialize)]
struct CreateChannelInput { name: String, fork_from: Option<String> }

async fn create_channel(State(state): State<KnotState>, Path(node_id): Path<String>, Json(input): Json<CreateChannelInput>) -> R<StatusCode> {
    let pijul = state.pijul.clone();
    tokio::task::spawn_blocking(move || pijul.create_channel(&node_id, &input.name, input.fork_from.as_deref())).await.map_err(|e| anyhow::anyhow!(e))??;
    Ok(StatusCode::CREATED)
}

async fn delete_channel(State(state): State<KnotState>, Path((node_id, name)): Path<(String, String)>) -> R<StatusCode> {
    let pijul = state.pijul.clone();
    tokio::task::spawn_blocking(move || pijul.delete_channel(&node_id, &name)).await.map_err(|e| anyhow::anyhow!(e))??;
    Ok(StatusCode::NO_CONTENT)
}

async fn read_channel_file(State(state): State<KnotState>, Path((node_id, ch)): Path<(String, String)>, Query(q): Query<FileQuery>) -> R<Json<serde_json::Value>> {
    let pijul = state.pijul.clone();
    let path = q.path;
    let content = tokio::task::spawn_blocking(move || pijul.read_file_from_channel(&node_id, &ch, &path)).await.map_err(|e| anyhow::anyhow!(e))??;
    let text = String::from_utf8_lossy(&content).into_owned();
    Ok(Json(serde_json::json!({ "content": text })))
}

#[derive(serde::Deserialize)]
struct WriteChannelFileInput { path: String, content: String, message: Option<String>, author_did: Option<String> }

async fn write_channel_file(
    State(state): State<KnotState>, Path((node_id, ch)): Path<(String, String)>, Json(input): Json<WriteChannelFileInput>,
) -> R<Json<serde_json::Value>> {
    let pijul = state.pijul.clone();
    let result = tokio::task::spawn_blocking(move || {
        let msg = input.message.as_deref().unwrap_or("update");
        pijul.write_and_record_on_channel(&node_id, &ch, &input.path, input.content.as_bytes(), msg, input.author_did.as_deref())
    }).await.map_err(|e| anyhow::anyhow!(e))??;
    let (hash, merkle) = result.unwrap_or_default();
    Ok(Json(serde_json::json!({ "change_hash": hash, "merkle": merkle })))
}

async fn channel_log(State(state): State<KnotState>, Path((node_id, ch)): Path<(String, String)>) -> R<Json<Vec<String>>> {
    let pijul = state.pijul.clone();
    let hashes = tokio::task::spawn_blocking(move || pijul.log_channel(&node_id, &ch)).await.map_err(|e| anyhow::anyhow!(e))??;
    Ok(Json(hashes))
}

#[derive(serde::Deserialize)]
struct ApplyToChannelInput { change_hash: String }

async fn apply_to_channel(State(state): State<KnotState>, Path((node_id, ch)): Path<(String, String)>, Json(input): Json<ApplyToChannelInput>) -> R<StatusCode> {
    let pijul = state.pijul.clone();
    tokio::task::spawn_blocking(move || pijul.apply_change_to_channel(&node_id, &input.change_hash, &ch)).await.map_err(|e| anyhow::anyhow!(e))??;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(serde::Deserialize)]
struct ChannelDiffQuery { a: String, b: String }

async fn channel_diff(State(state): State<KnotState>, Path(node_id): Path<String>, Query(q): Query<ChannelDiffQuery>) -> R<Json<crate::ChannelDiffResult>> {
    let pijul = state.pijul.clone();
    let result = tokio::task::spawn_blocking(move || pijul.diff_channels(&node_id, &q.a, &q.b)).await.map_err(|e| anyhow::anyhow!(e))??;
    Ok(Json(result))
}
