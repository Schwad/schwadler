//! Full RubyGems index download and management
//!
//! This module implements the "apt update" model for RubyGems:
//! - Download entire gem index once (all gems + their dependencies)
//! - Store in a compact rkyv format for fast mmap access
//! - Incremental updates using HTTP conditional requests
//! - Enable fully offline dependency resolution
//!
//! The index is stored at ~/.schwadler/full_index.rkyv

use anyhow::{anyhow, Context, Result};
use indicatif::{ProgressBar, ProgressStyle};
use memmap2::Mmap;
use reqwest::header::{HeaderMap, HeaderValue, ACCEPT_ENCODING, ETAG, IF_MODIFIED_SINCE, IF_NONE_MATCH, LAST_MODIFIED, USER_AGENT};
use reqwest::StatusCode;
use rkyv::{Archive, Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::Semaphore;

use crate::index::{ArchivedGemIndex, GemIndex, IndexedDep, IndexedGem, IndexedVersion};

/// Path to the full index file: ~/.schwadler/full_index.rkyv
pub fn full_index_path() -> PathBuf {
    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".schwadler")
        .join("full_index.rkyv")
}

/// Path to the index metadata file: ~/.schwadler/full_index.meta.json
fn metadata_path() -> PathBuf {
    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".schwadler")
        .join("full_index.meta.json")
}

/// Metadata for the full index (for incremental updates)
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct FullIndexMetadata {
    /// ETag of the /versions file
    pub versions_etag: Option<String>,
    /// Last-Modified of the /versions file
    pub versions_last_modified: Option<String>,
    /// When this index was last updated (Unix timestamp)
    pub updated_at: u64,
    /// Total number of gems in the index
    pub gem_count: usize,
    /// Total number of versions across all gems
    pub version_count: usize,
    /// Size of the index file in bytes
    pub index_size_bytes: u64,
    /// Hash of the /versions content for change detection
    pub versions_hash: String,
}

impl FullIndexMetadata {
    fn load() -> Option<Self> {
        let path = metadata_path();
        if !path.exists() {
            return None;
        }
        let content = fs::read_to_string(path).ok()?;
        serde_json::from_str(&content).ok()
    }

    fn save(&self) -> Result<()> {
        let path = metadata_path();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let content = serde_json::to_string_pretty(self)?;
        fs::write(path, content)?;
        Ok(())
    }
}

const COMPACT_INDEX: &str = "https://index.rubygems.org";

/// Statistics from downloading the index
#[derive(Debug)]
pub struct DownloadStats {
    pub gem_count: usize,
    pub version_count: usize,
    pub dependency_count: usize,
    pub index_size_bytes: usize,
    pub download_time_secs: f64,
    pub network_requests: usize,
}

impl std::fmt::Display for DownloadStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} gems, {} versions, {} dependencies ({:.1} MB index, {} requests in {:.1}s)",
            self.gem_count,
            self.version_count,
            self.dependency_count,
            self.index_size_bytes as f64 / 1024.0 / 1024.0,
            self.network_requests,
            self.download_time_secs
        )
    }
}

/// Download the complete RubyGems index
///
/// 1. Fetch /versions to get list of all gems and their version info
/// 2. For each gem, fetch /info/{gem} to get dependencies
/// 3. Build a complete GemIndex and serialize with rkyv
pub async fn download_full_index(parallelism: usize) -> Result<DownloadStats> {
    let start = std::time::Instant::now();
    
    // Create HTTP client
    let mut headers = HeaderMap::new();
    headers.insert(USER_AGENT, HeaderValue::from_static("schwadl/0.1.0"));
    headers.insert(ACCEPT_ENCODING, HeaderValue::from_static("gzip, deflate"));
    
    let http = reqwest::Client::builder()
        .default_headers(headers)
        .pool_max_idle_per_host(parallelism)
        .pool_idle_timeout(std::time::Duration::from_secs(30))
        .timeout(std::time::Duration::from_secs(60))
        .gzip(true)
        .build()?;
    
    println!("📥 Fetching gem list from RubyGems compact index...");
    
    // 1. Fetch /versions
    let versions_url = format!("{}/versions", COMPACT_INDEX);
    let response = http.get(&versions_url).send().await?;
    
    if !response.status().is_success() {
        return Err(anyhow!("Failed to fetch versions: {}", response.status()));
    }
    
    let versions_etag = response.headers()
        .get(ETAG)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    let versions_last_modified = response.headers()
        .get(LAST_MODIFIED)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    
    let versions_content = response.text().await?;
    let versions_hash = format!("{:x}", md5_hash(&versions_content));
    
    // 2. Parse versions file to get gem list
    let gem_list = parse_versions_file(&versions_content)?;
    let total_gems = gem_list.len();
    
    println!("   Found {} gems in index", total_gems);
    println!("📡 Downloading gem metadata ({} parallel)...", parallelism);
    
    // 3. Download all gem info in parallel with progress
    let progress = ProgressBar::new(total_gems as u64);
    progress.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} gems ({eta})")
            .unwrap()
            .progress_chars("█▓░")
    );
    
    let semaphore = Arc::new(Semaphore::new(parallelism));
    let completed = Arc::new(AtomicUsize::new(0));
    let network_requests = Arc::new(AtomicUsize::new(1)); // Start at 1 for /versions
    
    let mut handles = Vec::with_capacity(total_gems);
    
    for (gem_name, versions_info) in gem_list {
        let http = http.clone();
        let sem = semaphore.clone();
        let completed = completed.clone();
        let progress = progress.clone();
        let network_requests = network_requests.clone();
        
        handles.push(tokio::spawn(async move {
            let _permit = sem.acquire().await.unwrap();
            
            let url = format!("{}/info/{}", COMPACT_INDEX, gem_name);
            let result = http.get(&url).send().await;
            
            network_requests.fetch_add(1, Ordering::Relaxed);
            completed.fetch_add(1, Ordering::Relaxed);
            progress.inc(1);
            
            match result {
                Ok(response) if response.status().is_success() => {
                    let text = response.text().await.unwrap_or_default();
                    Some((gem_name, text, versions_info))
                }
                Ok(response) => {
                    // Gem might have been yanked or is unavailable
                    None
                }
                Err(_) => None,
            }
        }));
    }
    
    // Collect results
    let mut gems: Vec<IndexedGem> = Vec::with_capacity(total_gems);
    let mut name_to_idx: HashMap<String, u32> = HashMap::with_capacity(total_gems);
    let mut total_versions = 0usize;
    let mut total_deps = 0usize;
    
    for handle in handles {
        if let Ok(Some((gem_name, content, _versions_info))) = handle.await {
            let versions = parse_compact_index_info(&content);
            total_versions += versions.len();
            total_deps += versions.iter().map(|v| v.dependencies.len()).sum::<usize>();
            
            if !versions.is_empty() {
                let idx = gems.len() as u32;
                name_to_idx.insert(gem_name.clone(), idx);
                gems.push(IndexedGem {
                    name: gem_name,
                    versions,
                });
            }
        }
    }
    
    progress.finish_with_message("Download complete!");
    
    println!("💾 Building index...");
    
    // 4. Build and serialize the index
    let index = GemIndex {
        gems,
        name_to_idx,
        version: 1,
        built_at: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs(),
    };
    
    let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&index)
        .map_err(|e| anyhow!("Serialization failed: {}", e))?;
    
    // Write index file
    let path = full_index_path();
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    
    let mut file = File::create(&path)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    
    let index_size = bytes.len();
    
    // 5. Save metadata for incremental updates
    let metadata = FullIndexMetadata {
        versions_etag,
        versions_last_modified,
        updated_at: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs(),
        gem_count: index.gems.len(),
        version_count: total_versions,
        index_size_bytes: index_size as u64,
        versions_hash,
    };
    metadata.save()?;
    
    let elapsed = start.elapsed();
    
    Ok(DownloadStats {
        gem_count: index.gems.len(),
        version_count: total_versions,
        dependency_count: total_deps,
        index_size_bytes: index_size,
        download_time_secs: elapsed.as_secs_f64(),
        network_requests: network_requests.load(Ordering::Relaxed),
    })
}

/// Perform an incremental update of the index
///
/// Uses If-Modified-Since on /versions to detect changes, then only
/// fetches gems that have been added or updated.
pub async fn update_index(parallelism: usize) -> Result<UpdateStats> {
    let start = std::time::Instant::now();
    
    // Load existing metadata
    let old_metadata = FullIndexMetadata::load()
        .ok_or_else(|| anyhow!("No existing index found. Run `schwadl index download` first."))?;
    
    // Load existing index
    let old_index = load_full_index()?;
    
    // Create HTTP client
    let mut headers = HeaderMap::new();
    headers.insert(USER_AGENT, HeaderValue::from_static("schwadl/0.1.0"));
    headers.insert(ACCEPT_ENCODING, HeaderValue::from_static("gzip, deflate"));
    
    let http = reqwest::Client::builder()
        .default_headers(headers)
        .pool_max_idle_per_host(parallelism)
        .timeout(std::time::Duration::from_secs(60))
        .gzip(true)
        .build()?;
    
    println!("🔄 Checking for updates...");
    
    // Conditional request to /versions
    let versions_url = format!("{}/versions", COMPACT_INDEX);
    let mut request = http.get(&versions_url);
    
    if let Some(ref etag) = old_metadata.versions_etag {
        request = request.header(IF_NONE_MATCH, etag.as_str());
    }
    if let Some(ref lm) = old_metadata.versions_last_modified {
        request = request.header(IF_MODIFIED_SINCE, lm.as_str());
    }
    
    let response = request.send().await?;
    
    // Handle 304 Not Modified
    if response.status() == StatusCode::NOT_MODIFIED {
        println!("✅ Index is up to date (no changes on server)");
        return Ok(UpdateStats {
            was_modified: false,
            gems_added: 0,
            gems_updated: 0,
            gems_removed: 0,
            download_time_secs: start.elapsed().as_secs_f64(),
            network_requests: 1,
        });
    }
    
    if !response.status().is_success() {
        return Err(anyhow!("Failed to fetch versions: {}", response.status()));
    }
    
    let versions_etag = response.headers()
        .get(ETAG)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    let versions_last_modified = response.headers()
        .get(LAST_MODIFIED)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    
    let versions_content = response.text().await?;
    let versions_hash = format!("{:x}", md5_hash(&versions_content));
    
    // Quick hash check - if same, no updates needed
    if versions_hash == old_metadata.versions_hash {
        println!("✅ Index is up to date (content unchanged)");
        return Ok(UpdateStats {
            was_modified: false,
            gems_added: 0,
            gems_updated: 0,
            gems_removed: 0,
            download_time_secs: start.elapsed().as_secs_f64(),
            network_requests: 1,
        });
    }
    
    // Parse and compare versions
    let new_gem_list = parse_versions_file(&versions_content)?;
    let old_gem_set: HashMap<String, String> = old_index.index()
        .gems.iter()
        .map(|g| {
            // Create a hash of versions for comparison
            let version_hash: String = g.versions.iter()
                .map(|v| v.version.as_str())
                .collect::<Vec<_>>()
                .join(",");
            (g.name.to_string(), version_hash)
        })
        .collect();
    
    // Find new/updated gems
    let mut gems_to_fetch: Vec<(String, String)> = Vec::new();
    
    for (gem_name, version_info) in &new_gem_list {
        match old_gem_set.get(gem_name) {
            None => {
                // New gem
                gems_to_fetch.push((gem_name.clone(), version_info.clone()));
            }
            Some(old_versions) => {
                // Check if versions changed
                if old_versions != version_info {
                    gems_to_fetch.push((gem_name.clone(), version_info.clone()));
                }
            }
        }
    }
    
    // Find removed gems
    let new_gem_set: std::collections::HashSet<&String> = new_gem_list.iter()
        .map(|(name, _)| name)
        .collect();
    let gems_removed: usize = old_gem_set.keys()
        .filter(|name| !new_gem_set.contains(name))
        .count();
    
    let gems_added = gems_to_fetch.iter()
        .filter(|(name, _)| !old_gem_set.contains_key(name))
        .count();
    let gems_updated = gems_to_fetch.len() - gems_added;
    
    println!("   {} new gems, {} updated, {} removed", gems_added, gems_updated, gems_removed);
    
    if gems_to_fetch.is_empty() && gems_removed == 0 {
        println!("✅ No gem changes detected");
        return Ok(UpdateStats {
            was_modified: false,
            gems_added: 0,
            gems_updated: 0,
            gems_removed: 0,
            download_time_secs: start.elapsed().as_secs_f64(),
            network_requests: 1,
        });
    }
    
    println!("📡 Fetching {} gem updates...", gems_to_fetch.len());
    
    // Fetch changed gems
    let progress = ProgressBar::new(gems_to_fetch.len() as u64);
    progress.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta})")
            .unwrap()
    );
    
    let semaphore = Arc::new(Semaphore::new(parallelism));
    let network_requests = Arc::new(AtomicUsize::new(1)); // Start at 1 for /versions
    
    let mut handles = Vec::new();
    
    for (gem_name, _) in gems_to_fetch {
        let http = http.clone();
        let sem = semaphore.clone();
        let progress = progress.clone();
        let network_requests = network_requests.clone();
        
        handles.push(tokio::spawn(async move {
            let _permit = sem.acquire().await.unwrap();
            
            let url = format!("{}/info/{}", COMPACT_INDEX, gem_name);
            let result = http.get(&url).send().await;
            
            network_requests.fetch_add(1, Ordering::Relaxed);
            progress.inc(1);
            
            match result {
                Ok(response) if response.status().is_success() => {
                    let text = response.text().await.unwrap_or_default();
                    Some((gem_name, text))
                }
                _ => None,
            }
        }));
    }
    
    // Collect updated gem data
    let mut updated_gems: HashMap<String, Vec<IndexedVersion>> = HashMap::new();
    
    for handle in handles {
        if let Ok(Some((gem_name, content))) = handle.await {
            let versions = parse_compact_index_info(&content);
            if !versions.is_empty() {
                updated_gems.insert(gem_name, versions);
            }
        }
    }
    
    progress.finish_with_message("Updates fetched!");
    
    // Build new index
    println!("💾 Rebuilding index...");
    
    let mut new_gems: Vec<IndexedGem> = Vec::new();
    let mut new_name_to_idx: HashMap<String, u32> = HashMap::new();
    
    // Copy existing gems (except removed ones)
    for archived_gem in old_index.index().gems.iter() {
        let name = archived_gem.name.to_string();
        
        if !new_gem_set.contains(&name) {
            // Gem was removed, skip it
            continue;
        }
        
        if let Some(new_versions) = updated_gems.remove(&name) {
            // Use updated data
            let idx = new_gems.len() as u32;
            new_name_to_idx.insert(name.clone(), idx);
            new_gems.push(IndexedGem {
                name,
                versions: new_versions,
            });
        } else {
            // Copy existing data
            let idx = new_gems.len() as u32;
            new_name_to_idx.insert(name.clone(), idx);
            new_gems.push(IndexedGem {
                name,
                versions: archived_gem.versions.iter()
                    .map(|v| IndexedVersion {
                        version: v.version.to_string(),
                        dependencies: v.dependencies.iter()
                            .map(|d| IndexedDep {
                                name: d.name.to_string(),
                                requirements: d.requirements.to_string(),
                            })
                            .collect(),
                        sha256: v.sha256.as_ref().map(|s| s.to_string()),
                    })
                    .collect(),
            });
        }
    }
    
    // Add any remaining new gems
    for (name, versions) in updated_gems {
        let idx = new_gems.len() as u32;
        new_name_to_idx.insert(name.clone(), idx);
        new_gems.push(IndexedGem { name, versions });
    }
    
    let index = GemIndex {
        gems: new_gems,
        name_to_idx: new_name_to_idx,
        version: 1,
        built_at: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs(),
    };
    
    // Serialize and write
    let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&index)
        .map_err(|e| anyhow!("Serialization failed: {}", e))?;
    
    let path = full_index_path();
    let mut file = File::create(&path)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    
    // Update metadata
    let mut total_versions = 0;
    for gem in &index.gems {
        total_versions += gem.versions.len();
    }
    
    let metadata = FullIndexMetadata {
        versions_etag,
        versions_last_modified,
        updated_at: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs(),
        gem_count: index.gems.len(),
        version_count: total_versions,
        index_size_bytes: bytes.len() as u64,
        versions_hash,
    };
    metadata.save()?;
    
    Ok(UpdateStats {
        was_modified: true,
        gems_added,
        gems_updated,
        gems_removed,
        download_time_secs: start.elapsed().as_secs_f64(),
        network_requests: network_requests.load(Ordering::Relaxed),
    })
}

/// Statistics from an update operation
#[derive(Debug)]
pub struct UpdateStats {
    pub was_modified: bool,
    pub gems_added: usize,
    pub gems_updated: usize,
    pub gems_removed: usize,
    pub download_time_secs: f64,
    pub network_requests: usize,
}

impl std::fmt::Display for UpdateStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.was_modified {
            write!(
                f,
                "+{} new, ~{} updated, -{} removed ({} requests in {:.1}s)",
                self.gems_added,
                self.gems_updated,
                self.gems_removed,
                self.network_requests,
                self.download_time_secs
            )
        } else {
            write!(f, "No changes")
        }
    }
}

/// Get statistics about the full index
pub fn get_stats() -> Result<FullIndexStats> {
    let metadata = FullIndexMetadata::load()
        .ok_or_else(|| anyhow!("No index found. Run `schwadl index download` first."))?;
    
    let path = full_index_path();
    let file_size = fs::metadata(&path)?.len();
    
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let age_secs = now.saturating_sub(metadata.updated_at);
    
    Ok(FullIndexStats {
        gem_count: metadata.gem_count,
        version_count: metadata.version_count,
        index_size_bytes: file_size,
        updated_at: metadata.updated_at,
        age_secs,
    })
}

/// Statistics about the full index
#[derive(Debug)]
pub struct FullIndexStats {
    pub gem_count: usize,
    pub version_count: usize,
    pub index_size_bytes: u64,
    pub updated_at: u64,
    pub age_secs: u64,
}

impl std::fmt::Display for FullIndexStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let days = self.age_secs / 86400;
        let hours = (self.age_secs % 86400) / 3600;
        let mins = (self.age_secs % 3600) / 60;
        
        write!(
            f,
            "{} gems, {} versions ({:.1} MB)\nLast updated: ",
            self.gem_count,
            self.version_count,
            self.index_size_bytes as f64 / 1024.0 / 1024.0
        )?;
        
        if days > 0 {
            write!(f, "{}d {}h ago", days, hours)?;
        } else if hours > 0 {
            write!(f, "{}h {}m ago", hours, mins)?;
        } else {
            write!(f, "{}m ago", mins)?;
        }
        
        Ok(())
    }
}

// ============================================================================
// FULL INDEX LOADING (MMAP)
// ============================================================================

/// Memory-mapped full index handle
pub struct FullIndex {
    _mmap: Mmap,
    archived: *const ArchivedGemIndex,
}

// Safety: The archived data is read-only and the mmap is immutable
unsafe impl Send for FullIndex {}
unsafe impl Sync for FullIndex {}

impl FullIndex {
    /// Check if the full index exists
    pub fn exists() -> bool {
        full_index_path().exists()
    }

    /// Load the full index via mmap
    pub fn load() -> Result<Self> {
        let path = full_index_path();
        if !path.exists() {
            return Err(anyhow!(
                "Full index not found. Run `schwadl index download` first."
            ));
        }

        let file = File::open(&path)?;
        let mmap = unsafe { Mmap::map(&file)? };
        
        let data_ptr = mmap.as_ptr();
        let data_len = mmap.len();

        let mut result = FullIndex {
            _mmap: mmap,
            archived: std::ptr::null(),
        };

        let archived = unsafe {
            let data = std::slice::from_raw_parts(data_ptr, data_len);
            rkyv::access::<ArchivedGemIndex, rkyv::rancor::Error>(data)
                .map_err(|e| anyhow!("Index validation failed: {}", e))?
        };

        result.archived = archived as *const ArchivedGemIndex;
        Ok(result)
    }

    /// Get the archived index
    pub fn index(&self) -> &ArchivedGemIndex {
        unsafe { &*self.archived }
    }

    /// Look up a gem by name
    pub fn get_gem(&self, name: &str) -> Option<&crate::index::ArchivedIndexedGem> {
        let idx = self.index().name_to_idx.get(name)?;
        let idx_val: u32 = (*idx).into();
        self.index().gems.get(idx_val as usize)
    }

    /// Get all version strings for a gem (sorted descending)
    pub fn get_versions(&self, gem_name: &str) -> Option<Vec<String>> {
        let gem = self.get_gem(gem_name)?;
        let mut versions: Vec<String> = gem.versions.iter()
            .map(|v| v.version.to_string())
            .collect();
        
        // Sort descending by semver
        versions.sort_by(|a, b| {
            let av = semver::Version::parse(a).ok();
            let bv = semver::Version::parse(b).ok();
            bv.cmp(&av)
        });
        
        Some(versions)
    }

    /// Get dependencies for a specific gem version
    pub fn get_dependencies(&self, gem_name: &str, version: &str) -> Option<Vec<(String, String)>> {
        let gem = self.get_gem(gem_name)?;
        let ver = gem.versions.iter().find(|v| v.version == version)?;
        Some(
            ver.dependencies.iter()
                .map(|d| (d.name.to_string(), d.requirements.to_string()))
                .collect()
        )
    }
}

/// Load the full index
pub fn load_full_index() -> Result<FullIndex> {
    FullIndex::load()
}

// ============================================================================
// PARSING HELPERS
// ============================================================================

/// Parse the /versions file
/// Format: gem_name version1,version2,version3 checksum
fn parse_versions_file(content: &str) -> Result<Vec<(String, String)>> {
    let mut gems = Vec::new();
    
    for line in content.lines() {
        // Skip header lines (start with ---)
        if line.starts_with("---") || line.is_empty() {
            continue;
        }
        
        // Format: gem_name version1,version2,...
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.is_empty() {
            continue;
        }
        
        let gem_name = parts[0].to_string();
        let versions_str = parts.get(1).unwrap_or(&"").to_string();
        
        gems.push((gem_name, versions_str));
    }
    
    Ok(gems)
}

/// Parse compact index /info/{gem} format
fn parse_compact_index_info(text: &str) -> Vec<IndexedVersion> {
    let mut versions = Vec::new();

    for line in text.lines() {
        if line.starts_with("---") || line.is_empty() {
            continue;
        }

        // Split version from rest
        let mut parts = line.splitn(2, ' ');
        let version = match parts.next() {
            Some(v) if !v.is_empty() => v.to_string(),
            _ => continue,
        };

        let rest = parts.next().unwrap_or("");
        let deps_part = rest.split('|').next().unwrap_or("");

        let mut dependencies = Vec::new();
        let mut sha256 = None;

        for dep in deps_part.split(',') {
            let dep = dep.trim();
            if dep.is_empty() {
                continue;
            }

            let mut dep_parts = dep.splitn(2, ':');
            let name = dep_parts.next().unwrap_or("").to_string();
            let requirements = dep_parts
                .next()
                .map(|s| s.replace('&', ", "))
                .unwrap_or_else(|| ">= 0".to_string());

            // Skip metadata entries
            if name == "checksum" {
                sha256 = Some(requirements);
                continue;
            }
            if name == "ruby" || name == "rubygems" {
                continue;
            }

            dependencies.push(IndexedDep { name, requirements });
        }

        versions.push(IndexedVersion {
            version,
            dependencies,
            sha256,
        });
    }

    versions
}

/// Simple MD5 hash for change detection (not security-critical)
fn md5_hash(data: &str) -> u128 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    
    let mut hasher = DefaultHasher::new();
    data.hash(&mut hasher);
    hasher.finish() as u128
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_versions_file() {
        let content = r#"---
created_at: 2024-01-01
---
rack 1.0.0,2.0.0,3.0.0
rails 7.0.0,7.1.0
"#;
        let gems = parse_versions_file(content).unwrap();
        assert_eq!(gems.len(), 2);
        assert_eq!(gems[0].0, "rack");
        assert!(gems[0].1.contains("1.0.0"));
    }

    #[test]
    fn test_parse_compact_index_info() {
        let content = r#"---
3.0.0 rack-test:>= 1.0,other:>= 0&< 2|checksum:abc123
2.0.0 |checksum:def456
"#;
        let versions = parse_compact_index_info(content);
        
        assert_eq!(versions.len(), 2);
        assert_eq!(versions[0].version, "3.0.0");
        assert_eq!(versions[0].dependencies.len(), 2);
        assert_eq!(versions[0].dependencies[0].name, "rack-test");
    }
}
