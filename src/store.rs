use std::path::{Path, PathBuf};
use std::sync::Mutex;

use dashmap::DashMap;
use pijul_core::{
    Base32, MutTxnT, MutTxnTExt, TxnT, TxnTExt, TreeTxnT,
    RecordBuilder, Algorithm,
    changestore::ChangeStore,
    working_copy::WorkingCopyRead,
};

/// A single line-level difference between the working copy and the last recorded state.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DiffHunk {
    /// The file path relative to the repo root.
    pub path: String,
    /// Lines removed from the recorded state (prefixed with context of where).
    pub removed: Vec<String>,
    /// Lines added in the working copy.
    pub added: Vec<String>,
}

/// Summary of differences between the working copy and the last recorded state.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct DiffResult {
    /// Per-file hunks showing what changed.
    pub hunks: Vec<DiffHunk>,
    /// Files that exist in the working copy but are not yet tracked.
    pub untracked: Vec<String>,
}

/// Metadata about a tracked file in a repository.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrackedFile {
    /// The file path relative to the repo root.
    pub path: String,
    /// Whether this entry is a directory.
    pub is_dir: bool,
}

/// Result of comparing two channels' change sets.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct ChannelDiffResult {
    /// Changes present in channel A but not in channel B.
    pub only_in_a: Vec<String>,
    /// Changes present in channel B but not in channel A.
    pub only_in_b: Vec<String>,
}

/// Info about a single pijul change.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ChangeInfo {
    pub hash: String,
    pub message: String,
    pub author: Option<String>,
}

/// A content line within a change (addition or deletion).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ChangeLine {
    pub kind: String, // "add" or "del"
    pub content: String,
}

/// A group of change lines belonging to the same file.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct FileHunk {
    pub path: String,
    pub lines: Vec<ChangeLine>,
}

/// Full details of a change including content hunks grouped by file.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ChangeDetail {
    pub hash: String,
    pub message: String,
    pub author: Option<String>,
    pub files: Vec<FileHunk>,
}

/// Wrapper around pijul-core for managing article repositories.
///
/// Each article gets its own pijul repo under `base_path/node_id/`.
pub struct PijulStore {
    base_path: PathBuf,
    /// Per-repo write locks. Read operations (from pristine graph) don't need this.
    /// Write operations that touch the working copy must acquire the lock.
    repo_locks: DashMap<String, Mutex<()>>,
}

impl PijulStore {
    pub fn new(base_path: impl AsRef<Path>) -> Self {
        Self {
            base_path: base_path.as_ref().to_path_buf(),
            repo_locks: DashMap::new(),
        }
    }

    /// Acquire a per-repo write lock. Returns a guard that releases on drop.
    fn lock_repo(&self, node_id: &str) -> dashmap::mapref::one::Ref<'_, String, Mutex<()>> {
        self.repo_locks.entry(node_id.to_string()).or_insert_with(|| Mutex::new(()));
        self.repo_locks.get(node_id).unwrap()
    }

    pub fn repo_path(&self, node_id: &str) -> PathBuf {
        self.base_path.join(node_id)
    }

    /// Initialize a new pijul repo for an article.
    pub fn init_repo(&self, node_id: &str) -> anyhow::Result<PathBuf> {
        let path = self.repo_path(node_id);
        std::fs::create_dir_all(&path)?;

        let dot_dir = path.join(pijul_core::DOT_DIR);
        if dot_dir.exists() {
            tracing::debug!("pijul repo already exists at {}", path.display());
            return Ok(path);
        }

        // Create .pijul directory structure
        let pristine_dir = dot_dir.join("pristine");
        let changes_dir = dot_dir.join("changes");
        std::fs::create_dir_all(&pristine_dir)?;
        std::fs::create_dir_all(&changes_dir)?;

        // Write a minimal .ignore file
        std::fs::write(path.join(".ignore"), "*.html\n")?;

        // Initialize the pristine database and create main channel
        let pristine = pijul_core::pristine::sanakirja::Pristine::new(&pristine_dir.join("db"))?;
        let mut txn = pristine.mut_txn_begin()?;
        txn.open_or_create_channel(pijul_core::DEFAULT_CHANNEL)?;
        txn.commit()?;

        tracing::info!("initialized pijul repo at {}", path.display());
        Ok(path)
    }

    /// Record all working copy changes as a new pijul change.
    /// Returns `Some((change_hash, new_merkle_state))` or `None` if nothing changed.
    pub fn record(&self, node_id: &str, message: &str, author_did: Option<&str>) -> anyhow::Result<Option<(String, String)>> {
        let path = self.repo_path(node_id);
        self.record_at_path(&path, node_id, message, author_did)
    }

    /// Record all working copy changes in a series repo.
    /// Returns `Some((change_hash, new_merkle_state))` or `None` if nothing changed.
    pub fn record_series(&self, series_node_id: &str, message: &str, author_did: Option<&str>) -> anyhow::Result<Option<(String, String)>> {
        let path = self.series_repo_path(series_node_id);
        self.record_at_path(&path, series_node_id, message, author_did)
    }

    fn record_at_path(&self, path: &std::path::Path, node_id: &str, message: &str, author_did: Option<&str>) -> anyhow::Result<Option<(String, String)>> {
        self.record_at_path_on_channel(path, node_id, pijul_core::DEFAULT_CHANNEL, message, author_did)
    }

    fn record_at_path_on_channel(&self, path: &std::path::Path, node_id: &str, channel_name: &str, message: &str, author_did: Option<&str>) -> anyhow::Result<Option<(String, String)>> {
        let repo = self.open_repo(path)?;

        let txn = repo.pristine.arc_txn_begin()?;

        // Load channel
        let channel = {
            let t = txn.read();
            t.load_channel(channel_name)?
                .ok_or_else(|| anyhow::anyhow!("Channel {channel_name} not found"))?
        };

        // Apply root change if needed (first record)
        txn.write().apply_root_change_if_needed(&repo.changes, &channel, rand::rng())?;

        // Add all untracked files in working copy (recursive)
        {
            let mut t = txn.write();
            let mut dirs = vec![("".to_string(), path.to_path_buf())];
            while let Some((prefix, dir)) = dirs.pop() {
                for entry in std::fs::read_dir(&dir)? {
                    let entry = entry?;
                    let name = entry.file_name();
                    let name_str = name.to_string_lossy();
                    if name_str.starts_with('.') || name_str == "content.html" || name_str == "cache" {
                        continue;
                    }
                    let rel_path = if prefix.is_empty() { name_str.to_string() } else { format!("{prefix}/{name_str}") };
                    let is_dir = entry.file_type()?.is_dir();
                    if !t.is_tracked(&rel_path).unwrap_or(false) {
                        t.add(&rel_path, is_dir, 0)?;
                    }
                    if is_dir {
                        dirs.push((rel_path, entry.path()));
                    }
                }
            }
        }

        // Record changes
        let mut builder = RecordBuilder::new();
        builder.record(
            txn.clone(),
            Algorithm::default(),
            false,
            &pijul_core::DEFAULT_SEPARATOR,
            channel.clone(),
            &repo.working_copy,
            &repo.changes,
            "",
            1,
        )?;
        let rec = builder.finish();

        if rec.actions.is_empty() {
            txn.commit()?;
            return Ok(None);
        }

        // Build and save the change
        let actions: Vec<_> = rec.actions.into_iter().map(|a| {
            let t = txn.read();
            a.globalize(&*t).unwrap()
        }).collect();

        let contents = std::sync::Arc::try_unwrap(rec.contents).unwrap().into_inner();
        let contents_hash = {
            let mut hasher = pijul_core::pristine::Hasher::default();
            hasher.update(&contents[..]);
            hasher.finish()
        };

        let unhashed = author_did.map(|did| {
            serde_json::json!({ "identity": { "did": did } })
        });

        let mut change = pijul_core::change::LocalChange {
            offsets: pijul_core::change::Offsets::default(),
            hashed: pijul_core::change::Hashed {
                version: pijul_core::change::VERSION,
                contents_hash,
                changes: actions,
                metadata: Vec::new(),
                dependencies: Vec::new(),
                extra_known: Vec::new(),
                header: pijul_core::change::ChangeHeader {
                    message: message.to_string(),
                    ..Default::default()
                },
            },
            unhashed,
            contents,
        };

        let hash = repo.changes.save_change(&mut change, |_, _| Ok::<_, anyhow::Error>(()))?;

        // Apply the change to the channel
        {
            let mut t = txn.write();
            t.apply_local_change(&channel, &change, &hash, &rec.updatables)?;
        }

        // Capture Merkle state after applying the change
        let new_state = {
            let t = txn.read();
            let ch = channel.read();
            t.reverse_log(&*ch, None)
                .ok()
                .and_then(|mut iter| iter.next())
                .and_then(|r| r.ok())
                .map(|(_, (_, mrk))| pijul_core::Merkle::from(mrk).to_base32())
                .unwrap_or_default()
        };

        txn.commit()?;

        let hash_str = hash.to_base32();
        tracing::info!("recorded change {} for {}: {message}", hash_str, node_id);
        Ok(Some((hash_str, new_state)))
    }

    /// Unrecord a change by its base32 hash from the main channel.
    ///
    /// Fails if any other change in the channel depends on this one
    /// (pijul returns `ChangeIsDependedUpon`).
    pub fn unrecord_change(&self, node_id: &str, change_hash_b32: &str) -> anyhow::Result<()> {
        let path = self.repo_path(node_id);
        let hash = pijul_core::Hash::from_base32(change_hash_b32.as_bytes())
            .ok_or_else(|| anyhow::anyhow!("Invalid change hash: {change_hash_b32}"))?;

        let repo = self.open_repo(&path)?;
        let txn = repo.pristine.arc_txn_begin()?;

        let channel = {
            let t = txn.read();
            t.load_channel(pijul_core::DEFAULT_CHANNEL)?
                .ok_or_else(|| anyhow::anyhow!("No main channel"))?
        };

        // Check hash is actually in the channel
        let change_id = {
            let t = txn.read();
            t.has_change(&channel, &hash)?
                .ok_or_else(|| anyhow::anyhow!("Change {} not in channel", change_hash_b32))?
        };
        let _ = change_id;

        {
            let mut t = txn.write();
            t.unrecord(&repo.changes, &channel, &hash, 0, &repo.working_copy)
                .map_err(|e| anyhow::anyhow!("{}", e))?;
        }

        // Output restored working copy
        pijul_core::output::output_repository_no_pending(
            &repo.working_copy,
            &repo.changes,
            &txn,
            &channel,
            "",
            true,
            None,
            1,
            0,
        ).map_err(|e| anyhow::anyhow!("Failed to output working copy: {:?}", e))?;

        txn.commit()?;
        tracing::info!("unrecorded change {} from {}", change_hash_b32, node_id);
        Ok(())
    }

    /// Check whether a change can be unrecorded (no other change in the channel depends on it).
    pub fn is_unrecordable(&self, node_id: &str, change_hash_b32: &str) -> bool {
        let path = self.repo_path(node_id);
        let hash = match pijul_core::Hash::from_base32(change_hash_b32.as_bytes()) {
            Some(h) => h,
            None => return false,
        };
        let repo = match self.open_repo(&path) {
            Ok(r) => r,
            Err(_) => return false,
        };
        let txn = match repo.pristine.txn_begin() {
            Ok(t) => t,
            Err(_) => return false,
        };
        let channel = match txn.load_channel(pijul_core::DEFAULT_CHANNEL) {
            Ok(Some(ch)) => ch,
            _ => return false,
        };

        // A change is unrecordable if no change in the channel lists it as a dependency
        let ch = channel.read();
        let log_iter = match txn.log(&*ch, 0) {
            Ok(i) => i,
            Err(_) => return false,
        };
        for entry in log_iter {
            let (_, (candidate_hash, _)) = match entry {
                Ok(e) => e,
                Err(_) => return false,
            };
            let candidate: pijul_core::Hash = candidate_hash.into();
            if candidate == hash {
                continue;
            }
            if let Ok(change) = repo.changes.get_change(&candidate) {
                if change.hashed.dependencies.contains(&hash) {
                    return false;
                }
            }
        }
        true
    }

    /// Fork an existing repo by copying the entire directory.
    pub fn fork(&self, source_node_id: &str, fork_node_id: &str) -> anyhow::Result<PathBuf> {
        let source = self.repo_path(source_node_id);
        let fork_path = self.repo_path(fork_node_id);

        if !source.exists() {
            anyhow::bail!("source repo does not exist: {}", source.display());
        }

        copy_dir_recursive(&source, &fork_path)?;
        tracing::info!("forked {} -> {}", source.display(), fork_path.display());
        Ok(fork_path)
    }

    /// Get the list of change hashes in a repo's main channel.
    pub fn log(&self, node_id: &str) -> anyhow::Result<Vec<String>> {
        self.log_channel(node_id, pijul_core::DEFAULT_CHANNEL)
    }

    /// Get the list of change hashes in a specific channel.
    pub fn log_channel(&self, node_id: &str, channel_name: &str) -> anyhow::Result<Vec<String>> {
        let path = self.repo_path(node_id);
        let repo = self.open_repo(&path)?;
        let txn = repo.pristine.txn_begin()?;
        let channel = txn.load_channel(channel_name)?
            .ok_or_else(|| anyhow::anyhow!("Channel {channel_name} not found"))?;

        let mut hashes = Vec::new();
        let ch = channel.read();
        for entry in txn.log(&*ch, 0)? {
            let (_, (hash, _)) = entry?;
            let h: pijul_core::Hash = hash.into();
            hashes.push(h.to_base32());
        }
        Ok(hashes)
    }

    /// Extract author from a change's unhashed metadata.
    fn extract_author(unhashed: Option<&serde_json::Value>) -> Option<String> {
        tracing::debug!("extract_author unhashed={:?}", unhashed);
        let u = unhashed?;
        if let Some(arr) = u.get("authors").and_then(|a| a.as_array()) {
            if let Some(first) = arr.first() {
                if let Some(did) = first.get("did").and_then(|v| v.as_str()) {
                    return Some(did.to_string());
                }
                if let Some(key) = first.get("key").and_then(|v| v.as_str()) {
                    return Some(format!("key:{}", &key[..key.len().min(12)]));
                }
                if let Some(s) = first.as_str() {
                    return Some(s.to_string());
                }
            }
        }
        u.get("identity")
            .and_then(|i| i.get("did").or_else(|| i.get("login")))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
    }

    /// Get details of a specific change (message, author).
    pub fn get_change_info(&self, node_id: &str, change_hash: &str) -> anyhow::Result<ChangeInfo> {
        let hash = pijul_core::Hash::from_base32(change_hash.as_bytes())
            .ok_or_else(|| anyhow::anyhow!("Invalid hash: {change_hash}"))?;
        let path = self.repo_path(node_id);
        let repo = self.open_repo(&path)?;
        let change = repo.changes.get_change(&hash)?;

        let message = change.hashed.header.message.clone();
        let author = Self::extract_author(change.unhashed.as_ref());

        Ok(ChangeInfo { hash: change_hash.to_string(), message, author })
    }

    /// Get the log with change details (message, author) for each entry.
    pub fn log_with_details(&self, node_id: &str, channel_name: &str) -> anyhow::Result<Vec<ChangeInfo>> {
        let hashes = self.log_channel(node_id, channel_name)?;
        let mut infos = Vec::new();
        for h in &hashes {
            match self.get_change_info(node_id, h) {
                Ok(info) => infos.push(info),
                Err(_) => infos.push(ChangeInfo { hash: h.clone(), message: String::new(), author: None }),
            }
        }
        Ok(infos)
    }

    /// Get full details of a change including its content lines (additions/deletions).
    pub fn get_change_detail(&self, node_id: &str, change_hash: &str) -> anyhow::Result<ChangeDetail> {
        let hash = pijul_core::Hash::from_base32(change_hash.as_bytes())
            .ok_or_else(|| anyhow::anyhow!("Invalid hash: {change_hash}"))?;
        let path = self.repo_path(node_id);
        let repo = self.open_repo(&path)?;
        let change = repo.changes.get_change(&hash)?;

        let message = change.hashed.header.message.clone();
        let author = Self::extract_author(change.unhashed.as_ref());

        let contents = &change.contents;
        let mut file_map: std::collections::BTreeMap<String, Vec<ChangeLine>> = std::collections::BTreeMap::new();

        for hunk in &change.hashed.changes {
            self.extract_hunk_lines_grouped(hunk, contents, &mut file_map);
        }

        let files = file_map.into_iter().map(|(path, lines)| FileHunk { path, lines }).collect();
        Ok(ChangeDetail { hash: change_hash.to_string(), message, author, files })
    }

    /// Extract added/deleted lines from a hunk, grouped by file path.
    fn extract_hunk_lines_grouped(
        &self,
        hunk: &pijul_core::change::Hunk<Option<pijul_core::Hash>, pijul_core::change::Local>,
        contents: &[u8],
        file_map: &mut std::collections::BTreeMap<String, Vec<ChangeLine>>,
    ) {
        use pijul_core::change::{Hunk, Atom};

        fn extract_lines(atom: &Atom<Option<pijul_core::Hash>>, contents: &[u8], kind: &str) -> Vec<ChangeLine> {
            let mut lines = Vec::new();
            if let Atom::NewVertex(nv) = atom {
                let start: usize = nv.start.0.into();
                let end: usize = nv.end.0.into();
                if end > start && end <= contents.len() {
                    let text = String::from_utf8_lossy(&contents[start..end]);
                    for line in text.lines() {
                        if !line.is_empty() {
                            lines.push(ChangeLine { kind: kind.into(), content: line.to_string() });
                        }
                    }
                }
            }
            lines
        }

        match hunk {
            Hunk::FileAdd { contents: Some(atom), path, .. } => {
                let lines = extract_lines(atom, contents, "add");
                file_map.entry(path.clone()).or_default().extend(lines);
            }
            Hunk::Edit { change: atom, local, .. } => {
                let lines = extract_lines(atom, contents, "add");
                file_map.entry(local.path.clone()).or_default().extend(lines);
            }
            Hunk::FileUndel { contents: Some(atom), path, .. } => {
                let lines = extract_lines(atom, contents, "add");
                file_map.entry(path.clone()).or_default().extend(lines);
            }
            Hunk::Replacement { replacement, local, .. } => {
                let lines = extract_lines(replacement, contents, "add");
                file_map.entry(local.path.clone()).or_default().extend(lines);
            }
            Hunk::FileDel { path, .. } => {
                file_map.entry(path.clone()).or_default().push(
                    ChangeLine { kind: "del".into(), content: format!("(file deleted)") }
                );
            }
            Hunk::FileMove { path, .. } => {
                file_map.entry(path.clone()).or_default().push(
                    ChangeLine { kind: "add".into(), content: format!("(file moved)") }
                );
            }
            _ => {}
        }
    }

    /// Show what changed between the working copy and the last recorded state.
    ///
    /// For each tracked file, compares the working copy content against the pristine
    /// (last-recorded) content and returns structured diff hunks. Also reports untracked
    /// files that exist in the working directory but have not been added.
    pub fn diff(&self, node_id: &str) -> anyhow::Result<DiffResult> {
        let path = self.repo_path(node_id);
        let repo = self.open_repo(&path)?;
        let txn = repo.pristine.arc_txn_begin()?;
        let channel_name = pijul_core::DEFAULT_CHANNEL;

        let channel = {
            let t = txn.read();
            t.load_channel(channel_name)?
                .ok_or_else(|| anyhow::anyhow!("Channel {channel_name} not found"))?
        };

        let mut result = DiffResult::default();

        // Collect tracked files from the pristine
        let tracked: Vec<(String, bool)> = {
            let t = txn.read();
            let mut files = Vec::new();
            for entry in t.iter_working_copy() {
                let (_inode, name, is_dir) = entry?;
                files.push((name, is_dir));
            }
            files
        };

        // For each tracked file, get its pristine content and compare with working copy
        for (file_path, is_dir) in &tracked {
            if *is_dir {
                continue;
            }

            // Read working copy content
            let wc_content = match repo.working_copy.read_file(file_path, &mut Vec::new()) {
                Ok(()) => {
                    let mut buf = Vec::new();
                    repo.working_copy.read_file(file_path, &mut buf)?;
                    buf
                }
                Err(_) => {
                    // File was deleted from working copy
                    continue;
                }
            };

            // Get pristine content by outputting from the graph
            let pristine_content = {
                let t = txn.read();
                let ch = channel.read();
                // Find the file's position in the graph
                match pijul_core::fs::find_inode(&*t, file_path) {
                    Ok(inode) => {
                        match t.get_inodes(&inode, None) {
                            Ok(Some(&pos)) => {
                                drop(ch);
                                drop(t);
                                let mut buf = Vec::new();
                                let mut out = pijul_core::vertex_buffer::Writer::new(&mut buf);
                                match pijul_core::output::output_file(
                                    &repo.changes, &txn, &channel, pos, &mut out,
                                ) {
                                    Ok(()) => Some(buf),
                                    Err(_) => None,
                                }
                            }
                            _ => None,
                        }
                    }
                    Err(_) => None,
                }
            };

            let pristine_str = pristine_content
                .as_ref()
                .map(|b| String::from_utf8_lossy(b).into_owned())
                .unwrap_or_default();
            let wc_str = String::from_utf8_lossy(&wc_content).into_owned();

            if pristine_str != wc_str {
                let old_lines: Vec<&str> = pristine_str.lines().collect();
                let new_lines: Vec<&str> = wc_str.lines().collect();

                let removed: Vec<String> = old_lines
                    .iter()
                    .filter(|l| !new_lines.contains(l))
                    .map(|l| l.to_string())
                    .collect();
                let added: Vec<String> = new_lines
                    .iter()
                    .filter(|l| !old_lines.contains(l))
                    .map(|l| l.to_string())
                    .collect();

                if !removed.is_empty() || !added.is_empty() {
                    result.hunks.push(DiffHunk {
                        path: file_path.clone(),
                        removed,
                        added,
                    });
                }
            }
        }

        // Find untracked files
        let tracked_names: std::collections::HashSet<&str> =
            tracked.iter().map(|(n, _)| n.as_str()).collect();
        for entry in std::fs::read_dir(&path)? {
            let entry = entry?;
            let name = entry.file_name();
            let name_str = name.to_string_lossy().to_string();
            if name_str.starts_with('.') || name_str == "content.html" {
                continue;
            }
            if !tracked_names.contains(name_str.as_str()) {
                result.untracked.push(name_str);
            }
        }

        txn.commit()?;
        Ok(result)
    }

    /// Compare two repos' main channel logs. Returns changes in source but not in target.
    /// Useful for showing what a fork has that the original doesn't.
    pub fn diff_repos(&self, source_node_id: &str, target_node_id: &str) -> anyhow::Result<Vec<String>> {
        let source_log = self.log(source_node_id)?;
        let target_log = self.log(target_node_id)?;
        let target_set: std::collections::HashSet<&str> = target_log.iter().map(|s| s.as_str()).collect();
        Ok(source_log.into_iter().filter(|h| !target_set.contains(h.as_str())).collect())
    }

    /// Apply a recorded change (by hash) from a source repo into a target repo.
    ///
    /// This copies the change file from the source repo's change store into the
    /// target repo's change store, then applies it to the target's main channel.
    /// After applying, the working copy is updated via `output_repository_no_pending`.
    ///
    /// This is a simplified form of collaborative merging: it works well for
    /// non-conflicting changes. For conflicting changes, pijul will insert
    /// conflict markers.
    pub fn apply(
        &self,
        source_node_id: &str,
        target_node_id: &str,
        change_hash: &str,
    ) -> anyhow::Result<()> {
        let hash = pijul_core::Hash::from_base32(change_hash.as_bytes())
            .ok_or_else(|| anyhow::anyhow!("Invalid change hash: {change_hash}"))?;

        let source_path = self.repo_path(source_node_id);
        let target_path = self.repo_path(target_node_id);

        if !source_path.exists() {
            anyhow::bail!("source repo does not exist: {}", source_path.display());
        }
        if !target_path.exists() {
            anyhow::bail!("target repo does not exist: {}", target_path.display());
        }

        // Copy the change file from source to target change store.
        // pijul stores changes at `.pijul/changes/XX/REST.change` where XX is
        // the first two base32 chars of the hash.
        let source_repo = self.open_repo(&source_path)?;
        let source_change_file = source_repo.changes.filename(&hash);
        let target_repo_tmp = self.open_repo(&target_path)?;
        let target_change_file = target_repo_tmp.changes.filename(&hash);

        if !source_change_file.exists() {
            anyhow::bail!(
                "change file not found in source: {}",
                source_change_file.display()
            );
        }

        if let Some(parent) = target_change_file.parent() {
            std::fs::create_dir_all(parent)?;
        }
        if !target_change_file.exists() {
            std::fs::copy(&source_change_file, &target_change_file)?;
        }

        // Also copy any dependency change files that aren't in the target yet
        let change = source_repo.changes.get_change(&hash)?;
        for dep_hash in &change.hashed.dependencies {
            let dep_src = source_repo.changes.filename(dep_hash);
            let dep_dst = target_repo_tmp.changes.filename(dep_hash);
            if dep_src.exists() && !dep_dst.exists() {
                if let Some(parent) = dep_dst.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                std::fs::copy(&dep_src, &dep_dst)?;
            }
        }

        // Drop temporary handles before reopening
        drop(source_repo);
        drop(target_repo_tmp);

        // Open target repo and apply the change
        let repo = self.open_repo(&target_path)?;
        let txn = repo.pristine.arc_txn_begin()?;

        let channel = {
            let t = txn.read();
            t.load_channel(pijul_core::DEFAULT_CHANNEL)?
                .ok_or_else(|| anyhow::anyhow!("No main channel in target repo"))?
        };

        // Check if this change is already applied
        let already_applied = {
            let t = txn.read();
            t.has_change(&channel, &hash)?.is_some()
        };
        if already_applied {
            txn.commit()?;
            tracing::debug!("change {} already applied to {}", change_hash, target_node_id);
            return Ok(());
        }

        // Apply the change (and its dependencies recursively)
        {
            let mut t = txn.write();
            let mut ch = channel.write();
            t.apply_change_rec(&repo.changes, &mut *ch, &hash)?;
        }

        // Output the updated pristine to the working copy
        pijul_core::output::output_repository_no_pending(
            &repo.working_copy,
            &repo.changes,
            &txn,
            &channel,
            "",
            true,
            None,
            1,
            0,
        ).map_err(|e| anyhow::anyhow!("Failed to output working copy: {:?}", e))?;

        txn.commit()?;

        tracing::info!(
            "applied change {} from {} to {}",
            change_hash,
            source_node_id,
            target_node_id
        );
        Ok(())
    }

    /// Revert the working copy to the last recorded state.
    ///
    /// This overwrites all files in the working copy with their pristine
    /// (last-recorded) versions, discarding any unrecorded modifications.
    /// Untracked files are left untouched.
    pub fn revert(&self, node_id: &str) -> anyhow::Result<()> {
        let path = self.repo_path(node_id);
        let repo = self.open_repo(&path)?;
        let txn = repo.pristine.arc_txn_begin()?;

        let channel = {
            let t = txn.read();
            t.load_channel(pijul_core::DEFAULT_CHANNEL)?
                .ok_or_else(|| anyhow::anyhow!("No main channel"))?
        };

        // Output the pristine state to the working copy, overwriting modifications
        pijul_core::output::output_repository_no_pending(
            &repo.working_copy,
            &repo.changes,
            &txn,
            &channel,
            "",
            true,
            None,
            1,
            0,
        ).map_err(|e| anyhow::anyhow!("Failed to revert working copy: {:?}", e))?;

        txn.commit()?;
        tracing::info!("reverted working copy for {}", node_id);
        Ok(())
    }

    /// Read the current content of a file from the working copy.
    ///
    /// The `file_name` is relative to the repo root (e.g. `"content.typ"`).
    /// Returns the raw bytes of the file.
    pub fn get_file_content(&self, node_id: &str, file_name: &str) -> anyhow::Result<Vec<u8>> {
        let path = self.repo_path(node_id);
        let file_path = path.join(file_name);

        if !file_path.exists() {
            anyhow::bail!(
                "file not found: {} in repo {}",
                file_name,
                node_id
            );
        }

        // Ensure the resolved path is still inside the repo (prevent path traversal)
        let canonical = file_path.canonicalize()?;
        let repo_canonical = path.canonicalize()?;
        if !canonical.starts_with(&repo_canonical) {
            anyhow::bail!("path traversal detected: {}", file_name);
        }

        let content = std::fs::read(&file_path)?;
        Ok(content)
    }

    /// List all tracked files in a repository.
    ///
    /// Returns a list of `TrackedFile` entries with their relative paths
    /// and whether each entry is a directory.
    pub fn list_files(&self, node_id: &str) -> anyhow::Result<Vec<TrackedFile>> {
        let path = self.repo_path(node_id);
        let repo = self.open_repo(&path)?;
        let txn = repo.pristine.txn_begin()?;

        let mut files = Vec::new();
        for entry in txn.iter_working_copy() {
            let (_inode, name, is_dir) = entry?;
            files.push(TrackedFile {
                path: name,
                is_dir,
            });
        }

        Ok(files)
    }

    // --- Channel methods ---

    /// Create a new channel, forked from an existing one (CoW, nearly instant).
    /// If `fork_from` is None, forks from DEFAULT_CHANNEL ("main").
    pub fn create_channel(
        &self,
        node_id: &str,
        channel_name: &str,
        fork_from: Option<&str>,
    ) -> anyhow::Result<()> {
        let path = self.repo_path(node_id);
        let repo = self.open_repo(&path)?;
        let mut txn = repo.pristine.mut_txn_begin()?;

        let source_name = fork_from.unwrap_or(pijul_core::DEFAULT_CHANNEL);
        let source = txn.load_channel(source_name)?
            .ok_or_else(|| anyhow::anyhow!("Source channel {source_name} not found"))?;

        txn.fork(&source, channel_name)
            .map_err(|e| anyhow::anyhow!("Failed to create channel {channel_name}: {e}"))?;
        txn.commit()?;

        tracing::info!("created channel {channel_name} (forked from {source_name}) in {node_id}");
        Ok(())
    }

    /// List all channel names in a repo.
    pub fn list_channels(&self, node_id: &str) -> anyhow::Result<Vec<String>> {
        let path = self.repo_path(node_id);
        let repo = self.open_repo(&path)?;
        let txn = repo.pristine.txn_begin()?;

        use pijul_core::ChannelTxnT;
        let channels = txn.channels("")?;
        let names: Vec<String> = channels
            .iter()
            .map(|ch| txn.name(&*ch.read()).to_string())
            .collect();
        Ok(names)
    }

    /// Delete a channel. Cannot delete "main".
    pub fn delete_channel(&self, node_id: &str, channel_name: &str) -> anyhow::Result<()> {
        if channel_name == pijul_core::DEFAULT_CHANNEL {
            anyhow::bail!("Cannot delete the main channel");
        }
        let path = self.repo_path(node_id);
        let repo = self.open_repo(&path)?;
        let mut txn = repo.pristine.mut_txn_begin()?;

        let deleted = txn.drop_channel(channel_name)?;
        if !deleted {
            anyhow::bail!("Channel {channel_name} not found");
        }
        txn.commit()?;

        tracing::info!("deleted channel {channel_name} from {node_id}");
        Ok(())
    }

    /// Read a file's content directly from a channel's pristine graph,
    /// without touching the working copy. Safe for concurrent reads.
    pub fn read_file_from_channel(
        &self,
        node_id: &str,
        channel_name: &str,
        file_path: &str,
    ) -> anyhow::Result<Vec<u8>> {
        let path = self.repo_path(node_id);
        let repo = self.open_repo(&path)?;
        let txn = repo.pristine.arc_txn_begin()?;

        let channel = {
            let t = txn.read();
            t.load_channel(channel_name)?
                .ok_or_else(|| anyhow::anyhow!("Channel {channel_name} not found"))?
        };

        let content = {
            let t = txn.read();
            match pijul_core::fs::find_inode(&*t, file_path) {
                Ok(inode) => {
                    match t.get_inodes(&inode, None) {
                        Ok(Some(&pos)) => {
                            drop(t);
                            let mut buf = Vec::new();
                            let mut out = pijul_core::vertex_buffer::Writer::new(&mut buf);
                            pijul_core::output::output_file(
                                &repo.changes, &txn, &channel, pos, &mut out,
                            ).map_err(|e| anyhow::anyhow!("Failed to output file {file_path}: {e:?}"))?;
                            buf
                        }
                        _ => anyhow::bail!("File {file_path} not found in channel {channel_name}"),
                    }
                }
                Err(_) => anyhow::bail!("File {file_path} not tracked in channel {channel_name}"),
            }
        };

        txn.commit()?;
        Ok(content)
    }

    /// Write a file and record the change on a specific channel.
    /// Acquires per-repo lock since it touches the working copy.
    /// Returns `Some((change_hash, merkle))` or `None` if no effective change.
    pub fn write_and_record_on_channel(
        &self,
        node_id: &str,
        channel_name: &str,
        file_path: &str,
        content: &[u8],
        message: &str,
        author_did: Option<&str>,
    ) -> anyhow::Result<Option<(String, String)>> {
        let lock_ref = self.lock_repo(node_id);
        let _guard = lock_ref.lock().map_err(|e| anyhow::anyhow!("Lock poisoned: {e}"))?;

        let path = self.repo_path(node_id);

        // Output the target channel's state to working copy
        {
            let repo = self.open_repo(&path)?;
            let txn = repo.pristine.arc_txn_begin()?;
            let channel = {
                let t = txn.read();
                t.load_channel(channel_name)?
                    .ok_or_else(|| anyhow::anyhow!("Channel {channel_name} not found"))?
            };
            pijul_core::output::output_repository_no_pending(
                &repo.working_copy, &repo.changes, &txn, &channel,
                "", true, None, 1, 0,
            ).map_err(|e| anyhow::anyhow!("Failed to output channel state: {e:?}"))?;
            txn.commit()?;
        }

        // Write the file to the working copy
        let full_path = path.join(file_path);
        if let Some(parent) = full_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&full_path, content)?;

        // Record the change on this channel
        self.record_at_path_on_channel(&path, node_id, channel_name, message, author_did)
    }

    /// Apply an existing change (already in the change store) to a specific channel.
    /// The change can come from any channel in the same repo.
    pub fn apply_change_to_channel(
        &self,
        node_id: &str,
        change_hash: &str,
        target_channel: &str,
    ) -> anyhow::Result<()> {
        let hash = pijul_core::Hash::from_base32(change_hash.as_bytes())
            .ok_or_else(|| anyhow::anyhow!("Invalid change hash: {change_hash}"))?;

        let lock_ref = self.lock_repo(node_id);
        let _guard = lock_ref.lock().map_err(|e| anyhow::anyhow!("Lock poisoned: {e}"))?;

        let path = self.repo_path(node_id);
        let repo = self.open_repo(&path)?;
        let txn = repo.pristine.arc_txn_begin()?;

        let channel = {
            let t = txn.read();
            t.load_channel(target_channel)?
                .ok_or_else(|| anyhow::anyhow!("Channel {target_channel} not found"))?
        };

        // Check if already applied
        let already = {
            let t = txn.read();
            t.has_change(&channel, &hash)?.is_some()
        };
        if already {
            txn.commit()?;
            return Ok(());
        }

        // Apply the change (and its dependencies recursively)
        {
            let mut t = txn.write();
            let mut ch = channel.write();
            t.apply_change_rec(&repo.changes, &mut *ch, &hash)?;
        }

        // Output updated state to working copy if this is the main channel
        if target_channel == pijul_core::DEFAULT_CHANNEL {
            pijul_core::output::output_repository_no_pending(
                &repo.working_copy, &repo.changes, &txn, &channel,
                "", true, None, 1, 0,
            ).map_err(|e| anyhow::anyhow!("Failed to output working copy: {e:?}"))?;
        }

        txn.commit()?;
        tracing::info!("applied change {change_hash} to channel {target_channel} in {node_id}");
        Ok(())
    }

    /// Compare two channels: list changes unique to each.
    pub fn diff_channels(
        &self,
        node_id: &str,
        channel_a: &str,
        channel_b: &str,
    ) -> anyhow::Result<ChannelDiffResult> {
        let path = self.repo_path(node_id);
        let repo = self.open_repo(&path)?;
        let txn = repo.pristine.txn_begin()?;

        let ch_a = txn.load_channel(channel_a)?
            .ok_or_else(|| anyhow::anyhow!("Channel {channel_a} not found"))?;
        let ch_b = txn.load_channel(channel_b)?
            .ok_or_else(|| anyhow::anyhow!("Channel {channel_b} not found"))?;

        let hashes_a: std::collections::HashSet<String> = {
            let ch = ch_a.read();
            let mut set = std::collections::HashSet::new();
            for entry in txn.log(&*ch, 0)? {
                let (_, (hash, _)) = entry?;
                let h: pijul_core::Hash = hash.into();
                set.insert(h.to_base32());
            }
            set
        };
        let hashes_b: std::collections::HashSet<String> = {
            let ch = ch_b.read();
            let mut set = std::collections::HashSet::new();
            for entry in txn.log(&*ch, 0)? {
                let (_, (hash, _)) = entry?;
                let h: pijul_core::Hash = hash.into();
                set.insert(h.to_base32());
            }
            set
        };

        Ok(ChannelDiffResult {
            only_in_a: hashes_a.difference(&hashes_b).cloned().collect(),
            only_in_b: hashes_b.difference(&hashes_a).cloned().collect(),
        })
    }

    /// Record changes on a specific channel of a series repo.
    pub fn record_series_on_channel(
        &self,
        series_node_id: &str,
        channel_name: &str,
        message: &str,
        author_did: Option<&str>,
    ) -> anyhow::Result<Option<(String, String)>> {
        let path = self.series_repo_path(series_node_id);
        self.record_at_path_on_channel(&path, series_node_id, channel_name, message, author_did)
    }

    // --- Series repo methods ---

    /// Initialize a series repo with a cache/ directory.
    pub fn init_series_repo(&self, node_id: &str) -> anyhow::Result<PathBuf> {
        let path = self.repo_path(node_id);
        std::fs::create_dir_all(path.join("cache"))?;

        let dot_dir = path.join(pijul_core::DOT_DIR);
        if dot_dir.exists() {
            tracing::debug!("series pijul repo already exists at {}", path.display());
            return Ok(path);
        }

        let pristine_dir = dot_dir.join("pristine");
        let changes_dir = dot_dir.join("changes");
        std::fs::create_dir_all(&pristine_dir)?;
        std::fs::create_dir_all(&changes_dir)?;

        // Ignore HTML caches and the cache directory
        std::fs::write(path.join(".ignore"), "*.html\ncache/\n")?;

        let pristine = pijul_core::pristine::sanakirja::Pristine::new(&pristine_dir.join("db"))?;
        let mut txn = pristine.mut_txn_begin()?;
        txn.open_or_create_channel(pijul_core::DEFAULT_CHANNEL)?;
        txn.commit()?;

        tracing::info!("initialized series pijul repo at {}", path.display());
        Ok(path)
    }

    /// Write a chapter file into a series repo (flat, at repo root).
    pub fn write_chapter(
        &self,
        series_node_id: &str,
        chapter_id: &str,
        content: &str,
        ext: &str,
    ) -> anyhow::Result<()> {
        let repo = self.repo_path(series_node_id);
        let filename = format!("{chapter_id}.{ext}");
        std::fs::write(repo.join(&filename), content)?;
        Ok(())
    }

    /// Read a chapter source from a series repo (flat, at repo root).
    pub fn read_chapter(
        &self,
        series_node_id: &str,
        chapter_id: &str,
        ext: &str,
    ) -> anyhow::Result<String> {
        let filename = format!("{chapter_id}.{ext}");
        let path = self.repo_path(series_node_id).join(&filename);
        Ok(std::fs::read_to_string(&path)?)
    }

    /// Write a shared resource file (bib, macros, etc.) to a series repo root.
    pub fn write_resource(
        &self,
        series_node_id: &str,
        filename: &str,
        content: &[u8],
    ) -> anyhow::Result<()> {
        let path = self.repo_path(series_node_id).join(filename);
        std::fs::write(&path, content)?;
        Ok(())
    }

    /// Fork an entire series repo (including all chapters and shared resources).
    pub fn fork_series(
        &self,
        source_node_id: &str,
        fork_node_id: &str,
    ) -> anyhow::Result<PathBuf> {
        // Reuse the existing fork mechanism (deep directory copy)
        self.fork(source_node_id, fork_node_id)
    }

    /// Get the series repo path for a series node.
    pub fn series_repo_path(&self, series_node_id: &str) -> PathBuf {
        self.repo_path(series_node_id)
    }

    // --- Push / Pull (Tangled knot HTTP protocol) ---

    /// Push local changes to a remote knot.
    ///
    /// `remote_url` is the pijul remote URL, e.g.
    /// `https://knot.example.com/did:plc:xxx/my-repo`
    ///
    /// Protocol: get remote changelist, find local changes not on remote,
    /// POST each change file to `{remote_url}/.pijul?apply={hash}&to_channel={channel}`.
    pub fn push(
        &self,
        node_id: &str,
        remote_url: &str,
        channel: Option<&str>,
        auth_token: Option<&str>,
    ) -> anyhow::Result<Vec<String>> {
        let ch = channel.unwrap_or("main");
        let pijul_url = format!("{}/.pijul", remote_url.trim_end_matches('/'));

        // Get remote changelist
        let remote_hashes = self.fetch_remote_changelist(&pijul_url, ch)?;
        let remote_set: std::collections::HashSet<&str> = remote_hashes.iter().map(|s| s.as_str()).collect();

        // Get local log
        let local_hashes = self.log_channel(node_id, ch)?;

        // Find changes to push (local but not remote)
        let to_push: Vec<&str> = local_hashes.iter()
            .filter(|h| !remote_set.contains(h.as_str()))
            .map(|s| s.as_str())
            .collect();

        if to_push.is_empty() {
            return Ok(vec![]);
        }

        // Push each change
        let path = self.repo_path(node_id);
        let repo = self.open_repo(&path)?;
        let mut pushed = Vec::new();

        for hash_str in &to_push {
            let hash = pijul_core::Hash::from_base32(hash_str.as_bytes())
                .ok_or_else(|| anyhow::anyhow!("Invalid hash: {hash_str}"))?;
            let change_path = repo.changes.filename(&hash);

            if !change_path.exists() {
                tracing::warn!("change file not found: {}", change_path.display());
                continue;
            }

            let change_bytes = std::fs::read(&change_path)?;
            let apply_url = encode_url_path_colons(&format!("{pijul_url}?apply={hash_str}&to_channel={ch}"));

            let mut req = ureq::post(&apply_url)
                .header("Content-Type", "application/octet-stream");
            if let Some(token) = auth_token {
                req = req.header("Authorization", &format!("Bearer {token}"));
            }

            req.send(&change_bytes[..])
                .map_err(|e| anyhow::anyhow!("push change {hash_str}: {e}"))?;

            pushed.push(hash_str.to_string());
        }

        tracing::info!("pushed {} changes to {remote_url}", pushed.len());
        Ok(pushed)
    }

    /// Pull remote changes from a knot into local repo.
    ///
    /// Fetches changelist, downloads missing changes, applies them locally.
    pub fn pull(
        &self,
        node_id: &str,
        remote_url: &str,
        channel: Option<&str>,
    ) -> anyhow::Result<Vec<String>> {
        let ch = channel.unwrap_or("main");
        let pijul_url = format!("{}/.pijul", remote_url.trim_end_matches('/'));

        // Get remote changelist
        let remote_hashes = self.fetch_remote_changelist(&pijul_url, ch)?;

        // Get local log
        let local_hashes = self.log_channel(node_id, ch)?;
        let local_set: std::collections::HashSet<&str> = local_hashes.iter().map(|s| s.as_str()).collect();

        // Find changes to pull (remote but not local)
        let to_pull: Vec<&str> = remote_hashes.iter()
            .filter(|h| !local_set.contains(h.as_str()))
            .map(|s| s.as_str())
            .collect();

        if to_pull.is_empty() {
            return Ok(vec![]);
        }

        let path = self.repo_path(node_id);
        let mut pulled = Vec::new();

        for hash_str in &to_pull {
            // Download change file
            let change_url = encode_url_path_colons(&format!("{pijul_url}?change={hash_str}"));
            let resp = ureq::get(&change_url).call()
                .map_err(|e| anyhow::anyhow!("fetch change {hash_str}: {e}"))?;

            let change_bytes = resp.into_body().read_to_vec()
                .map_err(|e| anyhow::anyhow!("read change body: {e}"))?;

            // Save to local change store
            let hash = pijul_core::Hash::from_base32(hash_str.as_bytes())
                .ok_or_else(|| anyhow::anyhow!("Invalid hash: {hash_str}"))?;
            let repo = self.open_repo(&path)?;
            let change_file = repo.changes.filename(&hash);
            if let Some(parent) = change_file.parent() {
                std::fs::create_dir_all(parent)?;
            }
            if !change_file.exists() {
                std::fs::write(&change_file, &change_bytes)?;
            }
            drop(repo);

            // Apply to channel
            self.apply_change_to_channel(node_id, hash_str, ch)?;
            pulled.push(hash_str.to_string());
        }

        tracing::info!("pulled {} changes from {remote_url}", pulled.len());
        Ok(pulled)
    }

    /// Fetch the changelist from a remote knot (Tangled HTTP protocol).
    fn fetch_remote_changelist(&self, pijul_url: &str, channel: &str) -> anyhow::Result<Vec<String>> {
        let url = format!("{pijul_url}?changelist=0&channel={channel}");
        // Encode colons in the path (e.g. did:web:) for ureq URL parser
        let url = encode_url_path_colons(&url);
        let resp = ureq::get(&url).call()
            .map_err(|e| anyhow::anyhow!("fetch changelist: {e}"))?;
        let body = resp.into_body().read_to_string()
            .map_err(|e| anyhow::anyhow!("read changelist: {}", e))?;

        // Format: "{pos}.{hash}.{state}\n" per line, terminated by empty line
        let hashes: Vec<String> = body.lines()
            .filter(|line| !line.is_empty())
            .filter_map(|line| {
                let parts: Vec<&str> = line.splitn(3, '.').collect();
                if parts.len() >= 2 { Some(parts[1].to_string()) } else { None }
            })
            .collect();

        Ok(hashes)
    }

}

/// Encode colons in the URL path segment (but not in scheme, host, or query).
/// ureq 3.x is strict about colons in paths.
fn encode_url_path_colons(url: &str) -> String {
    let (scheme, rest) = if let Some(r) = url.strip_prefix("https://") {
        ("https://", r)
    } else if let Some(r) = url.strip_prefix("http://") {
        ("http://", r)
    } else {
        return url.to_string();
    };

    // Split off query string
    let (host_and_path, query) = match rest.find('?') {
        Some(pos) => (&rest[..pos], &rest[pos..]),
        None => (rest, ""),
    };

    // Split host from path at first '/'
    let (host, path) = match host_and_path.find('/') {
        Some(pos) => (&host_and_path[..pos], &host_and_path[pos..]),
        None => (host_and_path, ""),
    };

    format!("{}{}{}{}", scheme, host, path.replace(':', "%3A"), query)
}

impl PijulStore {
    fn open_repo(&self, path: &Path) -> anyhow::Result<RepoHandle> {
        let dot_dir = path.join(pijul_core::DOT_DIR);
        let pristine_dir = dot_dir.join("pristine");

        Ok(RepoHandle {
            pristine: pijul_core::pristine::sanakirja::Pristine::new(&pristine_dir.join("db"))?,
            changes: pijul_core::changestore::filesystem::FileSystem::from_root(
                path,
                pijul_repository::max_files(),
            ),
            working_copy: pijul_core::working_copy::filesystem::FileSystem::from_root(path),
        })
    }
}

struct RepoHandle {
    pristine: pijul_core::pristine::sanakirja::Pristine,
    changes: pijul_core::changestore::filesystem::FileSystem,
    working_copy: pijul_core::working_copy::filesystem::FileSystem,
}

fn copy_dir_recursive(src: &Path, dst: &Path) -> anyhow::Result<()> {
    std::fs::create_dir_all(dst)?;
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let ty = entry.file_type()?;
        let dst_path = dst.join(entry.file_name());
        if ty.is_dir() {
            copy_dir_recursive(&entry.path(), &dst_path)?;
        } else {
            std::fs::copy(entry.path(), &dst_path)?;
        }
    }
    Ok(())
}
