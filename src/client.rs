//! HTTP client for a remote pijul-knot server.

use crate::store::{ChannelDiffResult, DiffResult, TrackedFile};

/// HTTP client that mirrors PijulStore's API against a remote knot server.
pub struct KnotClient {
    base_url: String,
    http: reqwest::Client,
}

impl KnotClient {
    pub fn new(base_url: &str) -> Self {
        Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            http: reqwest::Client::new(),
        }
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.base_url, path)
    }

    pub async fn init_repo(&self, node_id: &str) -> anyhow::Result<()> {
        self.http.post(self.url("/repos"))
            .json(&serde_json::json!({ "node_id": node_id }))
            .send().await?.error_for_status()?;
        Ok(())
    }

    pub async fn init_series_repo(&self, node_id: &str) -> anyhow::Result<()> {
        self.http.post(self.url("/repos"))
            .json(&serde_json::json!({ "node_id": node_id, "series": true }))
            .send().await?.error_for_status()?;
        Ok(())
    }

    pub async fn list_files(&self, node_id: &str) -> anyhow::Result<Vec<TrackedFile>> {
        let resp: Vec<serde_json::Value> = self.http
            .get(self.url(&format!("/repos/{node_id}/files")))
            .send().await?.error_for_status()?
            .json().await?;
        Ok(resp.iter().map(|v| TrackedFile {
            path: v["path"].as_str().unwrap_or("").to_string(),
            is_dir: v["is_dir"].as_bool().unwrap_or(false),
        }).collect())
    }

    pub async fn get_file_content(&self, node_id: &str, path: &str) -> anyhow::Result<Vec<u8>> {
        let resp: serde_json::Value = self.http
            .get(self.url(&format!("/repos/{node_id}/file?path={}", urlencoding::encode(path))))
            .send().await?.error_for_status()?
            .json().await?;
        Ok(resp["content"].as_str().unwrap_or("").as_bytes().to_vec())
    }

    pub async fn write_file(&self, node_id: &str, path: &str, content: &[u8]) -> anyhow::Result<()> {
        let text = String::from_utf8_lossy(content);
        self.http.put(self.url(&format!("/repos/{node_id}/file")))
            .json(&serde_json::json!({ "path": path, "content": text }))
            .send().await?.error_for_status()?;
        Ok(())
    }

    pub async fn record(&self, node_id: &str, message: &str, author_did: Option<&str>) -> anyhow::Result<Option<(String, String)>> {
        let resp: serde_json::Value = self.http
            .post(self.url(&format!("/repos/{node_id}/record")))
            .json(&serde_json::json!({ "message": message, "author_did": author_did }))
            .send().await?.error_for_status()?
            .json().await?;
        let hash = resp["change_hash"].as_str().unwrap_or("").to_string();
        let merkle = resp["merkle"].as_str().unwrap_or("").to_string();
        if hash.is_empty() { Ok(None) } else { Ok(Some((hash, merkle))) }
    }

    pub async fn log(&self, node_id: &str) -> anyhow::Result<Vec<String>> {
        Ok(self.http.get(self.url(&format!("/repos/{node_id}/log")))
            .send().await?.error_for_status()?
            .json().await?)
    }

    pub async fn log_channel(&self, node_id: &str, channel: &str) -> anyhow::Result<Vec<String>> {
        Ok(self.http.get(self.url(&format!("/repos/{node_id}/channel/{channel}/log")))
            .send().await?.error_for_status()?
            .json().await?)
    }

    pub async fn list_channels(&self, node_id: &str) -> anyhow::Result<Vec<String>> {
        Ok(self.http.get(self.url(&format!("/repos/{node_id}/channels")))
            .send().await?.error_for_status()?
            .json().await?)
    }

    pub async fn create_channel(&self, node_id: &str, name: &str, fork_from: Option<&str>) -> anyhow::Result<()> {
        self.http.post(self.url(&format!("/repos/{node_id}/channels")))
            .json(&serde_json::json!({ "name": name, "fork_from": fork_from }))
            .send().await?.error_for_status()?;
        Ok(())
    }

    pub async fn delete_channel(&self, node_id: &str, name: &str) -> anyhow::Result<()> {
        self.http.delete(self.url(&format!("/repos/{node_id}/channels/{name}")))
            .send().await?.error_for_status()?;
        Ok(())
    }

    pub async fn read_file_from_channel(&self, node_id: &str, channel: &str, path: &str) -> anyhow::Result<Vec<u8>> {
        let resp: serde_json::Value = self.http
            .get(self.url(&format!("/repos/{node_id}/channel/{channel}/file?path={}", urlencoding::encode(path))))
            .send().await?.error_for_status()?
            .json().await?;
        Ok(resp["content"].as_str().unwrap_or("").as_bytes().to_vec())
    }

    pub async fn write_and_record_on_channel(
        &self, node_id: &str, channel: &str, path: &str, content: &[u8], message: &str, author_did: Option<&str>,
    ) -> anyhow::Result<Option<(String, String)>> {
        let text = String::from_utf8_lossy(content);
        let resp: serde_json::Value = self.http
            .put(self.url(&format!("/repos/{node_id}/channel/{channel}/file")))
            .json(&serde_json::json!({ "path": path, "content": text, "message": message, "author_did": author_did }))
            .send().await?.error_for_status()?
            .json().await?;
        let hash = resp["change_hash"].as_str().unwrap_or("").to_string();
        let merkle = resp["merkle"].as_str().unwrap_or("").to_string();
        if hash.is_empty() { Ok(None) } else { Ok(Some((hash, merkle))) }
    }

    pub async fn apply_change_to_channel(&self, node_id: &str, change_hash: &str, channel: &str) -> anyhow::Result<()> {
        self.http.post(self.url(&format!("/repos/{node_id}/channel/{channel}/apply")))
            .json(&serde_json::json!({ "change_hash": change_hash }))
            .send().await?.error_for_status()?;
        Ok(())
    }

    pub async fn diff_channels(&self, node_id: &str, a: &str, b: &str) -> anyhow::Result<ChannelDiffResult> {
        Ok(self.http.get(self.url(&format!("/repos/{node_id}/channel-diff?a={a}&b={b}")))
            .send().await?.error_for_status()?
            .json().await?)
    }

    pub async fn fork(&self, source: &str, target: &str) -> anyhow::Result<()> {
        self.http.post(self.url(&format!("/repos/{source}/fork")))
            .json(&serde_json::json!({ "fork_node_id": target }))
            .send().await?.error_for_status()?;
        Ok(())
    }

    pub async fn diff_repos(&self, source: &str, target: &str) -> anyhow::Result<Vec<String>> {
        Ok(self.http.get(self.url(&format!("/repos/{source}/diff-repos?target={target}")))
            .send().await?.error_for_status()?
            .json().await?)
    }

    pub async fn diff(&self, node_id: &str) -> anyhow::Result<DiffResult> {
        Ok(self.http.get(self.url(&format!("/repos/{node_id}/diff")))
            .send().await?.error_for_status()?
            .json().await?)
    }

    pub async fn revert(&self, node_id: &str) -> anyhow::Result<()> {
        self.http.post(self.url(&format!("/repos/{node_id}/revert")))
            .send().await?.error_for_status()?;
        Ok(())
    }

    pub async fn unrecord_change(&self, node_id: &str, change_hash: &str) -> anyhow::Result<()> {
        self.http.post(self.url(&format!("/repos/{node_id}/unrecord")))
            .json(&serde_json::json!({ "change_hash": change_hash }))
            .send().await?.error_for_status()?;
        Ok(())
    }
}
