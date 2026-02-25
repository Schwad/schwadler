//! Persistent caching layer for Schwadler
//!
//! Caches RubyGems compact index responses locally with:
//! - ETag/Last-Modified conditional requests
//! - TTL-based expiration for gem info
//! - Range requests for incremental updates
//! - ~/.schwadler/cache/ directory structure

use anyhow::{anyhow, Result};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Cache entry metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheEntry {
    /// ETag from response (for conditional requests)
    pub etag: Option<String>,
    /// Last-Modified header value
    pub last_modified: Option<String>,
    /// When this entry was cached (Unix timestamp)
    pub cached_at: u64,
    /// Content length (for Range requests)
    pub content_length: u64,
}

/// Cache metadata index
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CacheIndex {
    /// Map from cache key to entry metadata
    pub entries: HashMap<String, CacheEntry>,
    /// Index version for future migrations
    pub version: u32,
}

/// The persistent cache manager
pub struct PersistentCache {
    /// Base cache directory (~/.schwadler/cache/)
    cache_dir: PathBuf,
    /// Subdirectory for compact index data
    compact_dir: PathBuf,
    /// Subdirectory for gem info
    gems_dir: PathBuf,
    /// Cache index (loaded lazily, saved on write)
    index: CacheIndex,
    /// TTL for gem info (default: 1 hour)
    gem_ttl: Duration,
    /// TTL for versions list (default: 5 minutes)
    versions_ttl: Duration,
}

impl PersistentCache {
    /// Default cache location: ~/.schwadler/cache/
    pub fn default_cache_dir() -> PathBuf {
        dirs::home_dir()
            .unwrap_or_else(|| PathBuf::from("."))
            .join(".schwadler")
            .join("cache")
    }

    /// Create a new persistent cache
    pub fn new() -> Result<Self> {
        Self::with_path(Self::default_cache_dir())
    }

    /// Create a cache at a specific path
    pub fn with_path(cache_dir: PathBuf) -> Result<Self> {
        let compact_dir = cache_dir.join("compact");
        let gems_dir = cache_dir.join("gems");

        // Create directories if they don't exist
        fs::create_dir_all(&compact_dir)?;
        fs::create_dir_all(&gems_dir)?;

        // Load or create index
        let index_path = cache_dir.join("index.json");
        let index = if index_path.exists() {
            let mut file = File::open(&index_path)?;
            let mut contents = String::new();
            file.read_to_string(&mut contents)?;
            serde_json::from_str(&contents).unwrap_or_default()
        } else {
            CacheIndex::default()
        };

        Ok(Self {
            cache_dir,
            compact_dir,
            gems_dir,
            index,
            gem_ttl: Duration::from_secs(3600),       // 1 hour
            versions_ttl: Duration::from_secs(300),   // 5 minutes
        })
    }

    /// Get current Unix timestamp
    fn now_timestamp() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }

    /// Save the cache index
    pub fn save_index(&self) -> Result<()> {
        let index_path = self.cache_dir.join("index.json");
        let contents = serde_json::to_string_pretty(&self.index)?;
        let mut file = File::create(index_path)?;
        file.write_all(contents.as_bytes())?;
        Ok(())
    }

    // ========================================================================
    // COMPACT INDEX: versions file
    // ========================================================================

    /// Cache key for the versions list
    const VERSIONS_KEY: &'static str = "versions";

    /// Path to cached versions file
    fn versions_path(&self) -> PathBuf {
        self.compact_dir.join("versions")
    }

    /// Get cached versions file if fresh
    pub fn get_versions(&self) -> Option<(String, &CacheEntry)> {
        let entry = self.index.entries.get(Self::VERSIONS_KEY)?;
        
        // Check TTL
        let age = Self::now_timestamp().saturating_sub(entry.cached_at);
        if age > self.versions_ttl.as_secs() {
            return None;  // Expired, but return metadata for conditional request
        }

        // Read cached content
        let path = self.versions_path();
        if !path.exists() {
            return None;
        }

        let mut file = File::open(path).ok()?;
        let mut contents = String::new();
        file.read_to_string(&mut contents).ok()?;

        Some((contents, entry))
    }

    /// Get versions entry metadata (even if expired) for conditional request
    pub fn get_versions_metadata(&self) -> Option<&CacheEntry> {
        self.index.entries.get(Self::VERSIONS_KEY)
    }

    /// Cache the versions file
    pub fn put_versions(
        &mut self,
        content: &str,
        etag: Option<String>,
        last_modified: Option<String>,
    ) -> Result<()> {
        let path = self.versions_path();
        let mut file = File::create(path)?;
        file.write_all(content.as_bytes())?;

        let entry = CacheEntry {
            etag,
            last_modified,
            cached_at: Self::now_timestamp(),
            content_length: content.len() as u64,
        };

        self.index.entries.insert(Self::VERSIONS_KEY.to_string(), entry);
        self.save_index()?;

        Ok(())
    }

    /// Append to versions file (for Range request updates)
    pub fn append_versions(&mut self, additional_content: &str) -> Result<()> {
        let path = self.versions_path();
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;
        file.write_all(additional_content.as_bytes())?;

        // Update content length in index
        if let Some(entry) = self.index.entries.get_mut(Self::VERSIONS_KEY) {
            entry.content_length += additional_content.len() as u64;
            entry.cached_at = Self::now_timestamp();
        }
        self.save_index()?;

        Ok(())
    }

    // ========================================================================
    // GEM INFO: individual gem dependency data
    // ========================================================================

    /// Path to cached gem info
    fn gem_info_path(&self, gem_name: &str) -> PathBuf {
        // Use first 2 chars as subdirectory for better filesystem performance
        let prefix = if gem_name.len() >= 2 {
            &gem_name[..2]
        } else {
            gem_name
        };
        let subdir = self.gems_dir.join(prefix);
        subdir.join(format!("{}.txt", gem_name))
    }

    /// Cache key for a gem
    fn gem_key(gem_name: &str) -> String {
        format!("gem:{}", gem_name)
    }

    /// Get cached gem info if fresh
    pub fn get_gem_info(&self, gem_name: &str) -> Option<(String, &CacheEntry)> {
        let key = Self::gem_key(gem_name);
        let entry = self.index.entries.get(&key)?;

        // Check TTL
        let age = Self::now_timestamp().saturating_sub(entry.cached_at);
        if age > self.gem_ttl.as_secs() {
            return None;  // Expired
        }

        // Read cached content
        let path = self.gem_info_path(gem_name);
        if !path.exists() {
            return None;
        }

        let mut file = File::open(path).ok()?;
        let mut contents = String::new();
        file.read_to_string(&mut contents).ok()?;

        Some((contents, entry))
    }

    /// Get gem info metadata for conditional request
    pub fn get_gem_info_metadata(&self, gem_name: &str) -> Option<&CacheEntry> {
        let key = Self::gem_key(gem_name);
        self.index.entries.get(&key)
    }

    /// Cache gem info from compact index
    pub fn put_gem_info(
        &mut self,
        gem_name: &str,
        content: &str,
        etag: Option<String>,
        last_modified: Option<String>,
    ) -> Result<()> {
        let path = self.gem_info_path(gem_name);
        
        // Create subdirectory if needed
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        let mut file = File::create(path)?;
        file.write_all(content.as_bytes())?;

        let entry = CacheEntry {
            etag,
            last_modified,
            cached_at: Self::now_timestamp(),
            content_length: content.len() as u64,
        };

        let key = Self::gem_key(gem_name);
        self.index.entries.insert(key, entry);
        self.save_index()?;

        Ok(())
    }

    // ========================================================================
    // CACHE MANAGEMENT
    // ========================================================================

    /// Clear all cached data
    pub fn clear(&mut self) -> Result<()> {
        // Remove all files
        if self.compact_dir.exists() {
            fs::remove_dir_all(&self.compact_dir)?;
            fs::create_dir_all(&self.compact_dir)?;
        }
        if self.gems_dir.exists() {
            fs::remove_dir_all(&self.gems_dir)?;
            fs::create_dir_all(&self.gems_dir)?;
        }

        // Clear index
        self.index.entries.clear();
        self.save_index()?;

        Ok(())
    }

    /// Get cache statistics
    pub fn stats(&self) -> CacheStats {
        let mut gem_count = 0;
        let mut total_size: u64 = 0;
        let mut oldest_timestamp = u64::MAX;
        let mut newest_timestamp = 0u64;

        for (key, entry) in &self.index.entries {
            if key.starts_with("gem:") {
                gem_count += 1;
            }
            total_size += entry.content_length;
            oldest_timestamp = oldest_timestamp.min(entry.cached_at);
            newest_timestamp = newest_timestamp.max(entry.cached_at);
        }

        CacheStats {
            gem_count,
            total_entries: self.index.entries.len(),
            total_size_bytes: total_size,
            oldest_entry_age_secs: if oldest_timestamp == u64::MAX {
                0
            } else {
                Self::now_timestamp().saturating_sub(oldest_timestamp)
            },
            newest_entry_age_secs: Self::now_timestamp().saturating_sub(newest_timestamp),
        }
    }

    /// Prune expired entries (uses Rayon for parallel file deletion)
    pub fn prune_expired(&mut self) -> Result<usize> {
        let now = Self::now_timestamp();
        let gem_ttl_secs = self.gem_ttl.as_secs();
        let versions_ttl_secs = self.versions_ttl.as_secs();

        // Collect expired keys in parallel
        let expired_keys: Vec<String> = self.index.entries
            .par_iter()
            .filter(|(key, entry)| {
                let ttl = if key.starts_with("gem:") {
                    gem_ttl_secs
                } else {
                    versions_ttl_secs
                };
                now.saturating_sub(entry.cached_at) > ttl * 2  // Prune if 2x TTL
            })
            .map(|(key, _)| key.clone())
            .collect();

        let removed = expired_keys.len();

        // Build paths for parallel file deletion
        let paths_to_delete: Vec<PathBuf> = expired_keys.iter()
            .filter_map(|key| {
                if key.starts_with("gem:") {
                    let gem_name = &key[4..];
                    Some(self.gem_info_path(gem_name))
                } else {
                    None
                }
            })
            .collect();

        // Delete files in parallel
        paths_to_delete.par_iter().for_each(|path| {
            let _ = fs::remove_file(path);
        });

        // Remove from index (sequential, since HashMap isn't concurrent)
        for key in expired_keys {
            self.index.entries.remove(&key);
        }

        if removed > 0 {
            self.save_index()?;
        }

        Ok(removed)
    }
}

/// Cache statistics
#[derive(Debug, Clone)]
pub struct CacheStats {
    pub gem_count: usize,
    pub total_entries: usize,
    pub total_size_bytes: u64,
    pub oldest_entry_age_secs: u64,
    pub newest_entry_age_secs: u64,
}

impl std::fmt::Display for CacheStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} gems cached ({} total entries, {:.1} KB)",
            self.gem_count,
            self.total_entries,
            self.total_size_bytes as f64 / 1024.0
        )
    }
}

/// Result of a conditional fetch
#[derive(Debug)]
pub enum FetchResult {
    /// Cache hit, content is fresh
    Fresh(String),
    /// Cache hit with 304 Not Modified
    NotModified,
    /// Content was fetched (new or updated)
    Fetched(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    fn test_cache() -> PersistentCache {
        let test_dir = env::temp_dir().join("schwadler_cache_test");
        let _ = fs::remove_dir_all(&test_dir);
        PersistentCache::with_path(test_dir).unwrap()
    }

    #[test]
    fn test_gem_info_caching() {
        let mut cache = test_cache();
        
        // Initially empty
        assert!(cache.get_gem_info("rack").is_none());
        
        // Put gem info
        cache.put_gem_info("rack", "3.0.0 rack-test:>=1.0", Some("abc123".into()), None).unwrap();
        
        // Now should be available
        let (content, entry) = cache.get_gem_info("rack").unwrap();
        assert!(content.contains("3.0.0"));
        assert_eq!(entry.etag, Some("abc123".to_string()));
    }

    #[test]
    fn test_versions_caching() {
        let mut cache = test_cache();
        
        // Put versions
        cache.put_versions("rack 3.0.0\nrails 7.0.0", Some("etag123".into()), None).unwrap();
        
        // Get versions
        let (content, entry) = cache.get_versions().unwrap();
        assert!(content.contains("rack"));
        assert!(content.contains("rails"));
        assert_eq!(entry.etag, Some("etag123".to_string()));
    }

    #[test]
    fn test_cache_stats() {
        let mut cache = test_cache();
        
        cache.put_gem_info("rack", "content1", None, None).unwrap();
        cache.put_gem_info("rails", "content2", None, None).unwrap();
        cache.put_versions("versions", None, None).unwrap();
        
        let stats = cache.stats();
        assert_eq!(stats.gem_count, 2);
        assert_eq!(stats.total_entries, 3);
    }
}
