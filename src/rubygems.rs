//! RubyGems API client with async fetching, connection pooling, and persistent caching
//! 
//! Uses the compact index for fast dependency resolution.
//! Implements conditional requests (ETag/Last-Modified) and Range requests.

use anyhow::{anyhow, Result};
use dashmap::DashMap;
use rayon::prelude::*;
use reqwest::header::{
    HeaderMap, HeaderValue, ACCEPT, ACCEPT_ENCODING, ETAG, IF_MODIFIED_SINCE,
    IF_NONE_MATCH, LAST_MODIFIED, RANGE, USER_AGENT,
};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::{Mutex, Semaphore};

use crate::cache::{CacheEntry, FetchResult, PersistentCache};

const RUBYGEMS_API: &str = "https://rubygems.org";
const COMPACT_INDEX: &str = "https://index.rubygems.org";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GemSpec {
    pub name: String,
    pub version: String,
    pub platform: String,
    pub dependencies: Vec<GemDependency>,
    pub sha256: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GemDependency {
    pub name: String,
    pub requirements: String,
    pub dep_type: DependencyType,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum DependencyType {
    Runtime,
    Development,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GemInfo {
    pub name: String,
    pub versions: Vec<VersionInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VersionInfo {
    pub number: String,
    pub platform: String,
    pub sha: String,
    pub dependencies: VersionDependencies,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct VersionDependencies {
    #[serde(default)]
    pub runtime: Vec<ApiDependency>,
    #[serde(default)]
    pub development: Vec<ApiDependency>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiDependency {
    pub name: String,
    pub requirements: String,
}

/// Statistics for cache usage
#[derive(Debug, Default)]
pub struct FetchStats {
    pub cache_hits: usize,
    pub cache_misses: usize,
    pub conditional_not_modified: usize,
    pub network_fetches: usize,
    /// Prefetch tasks spawned
    pub prefetch_spawned: usize,
    /// Times data was ready from prefetch when needed
    pub prefetch_hits: usize,
}

pub struct Client {
    http: reqwest::Client,
    semaphore: Arc<Semaphore>,
    cache: Arc<DashMap<String, GemInfo>>,
    versions_cache: Arc<DashMap<String, Vec<String>>>,
    /// Persistent disk cache
    persistent_cache: Arc<Mutex<PersistentCache>>,
    /// Fetch statistics for the session
    stats: Arc<Mutex<FetchStats>>,
}

impl Client {
    pub fn new(max_concurrent: usize) -> Self {
        let mut headers = HeaderMap::new();
        headers.insert(USER_AGENT, HeaderValue::from_static("schwadl/0.1.0"));
        headers.insert(ACCEPT, HeaderValue::from_static("application/json"));
        headers.insert(ACCEPT_ENCODING, HeaderValue::from_static("gzip, deflate"));
        
        let http = reqwest::Client::builder()
            .default_headers(headers)
            .pool_max_idle_per_host(max_concurrent)
            .pool_idle_timeout(std::time::Duration::from_secs(30))
            .timeout(std::time::Duration::from_secs(30))
            .gzip(true)
            .build()
            .expect("Failed to create HTTP client");
        
        // Try to create persistent cache, fall back to disabled if it fails
        let persistent_cache = PersistentCache::new()
            .unwrap_or_else(|e| {
                eprintln!("Warning: Could not initialize persistent cache: {}", e);
                // Return a disabled cache that won't fail
                PersistentCache::with_path(std::env::temp_dir().join(".schwadler_fallback"))
                    .expect("Fallback cache failed")
            });
        
        Self {
            http,
            semaphore: Arc::new(Semaphore::new(max_concurrent)),
            cache: Arc::new(DashMap::new()),
            versions_cache: Arc::new(DashMap::new()),
            persistent_cache: Arc::new(Mutex::new(persistent_cache)),
            stats: Arc::new(Mutex::new(FetchStats::default())),
        }
    }

    /// Get fetch statistics
    pub async fn get_stats(&self) -> FetchStats {
        let stats = self.stats.lock().await;
        FetchStats {
            cache_hits: stats.cache_hits,
            cache_misses: stats.cache_misses,
            conditional_not_modified: stats.conditional_not_modified,
            network_fetches: stats.network_fetches,
            prefetch_spawned: stats.prefetch_spawned,
            prefetch_hits: stats.prefetch_hits,
        }
    }

    /// Print cache statistics
    pub async fn print_cache_stats(&self) {
        let cache = self.persistent_cache.lock().await;
        let stats = cache.stats();
        eprintln!("📦 Cache: {}", stats);
        
        let fetch_stats = self.stats.lock().await;
        eprintln!(
            "🌐 Network: {} fetches, {} cache hits, {} 304s",
            fetch_stats.network_fetches,
            fetch_stats.cache_hits,
            fetch_stats.conditional_not_modified
        );
        if fetch_stats.prefetch_spawned > 0 {
            let hit_rate = if fetch_stats.prefetch_spawned > 0 {
                (fetch_stats.prefetch_hits as f64 / fetch_stats.prefetch_spawned as f64) * 100.0
            } else {
                0.0
            };
            eprintln!(
                "🚀 Prefetch: {} spawned, {} hits ({:.1}% effective)",
                fetch_stats.prefetch_spawned,
                fetch_stats.prefetch_hits,
                hit_rate
            );
        }
    }
    
    /// Fetch all versions of a gem
    pub async fn fetch_versions(&self, gem_name: &str) -> Result<Vec<String>> {
        // Check memory cache first
        if let Some(versions) = self.versions_cache.get(gem_name) {
            return Ok(versions.clone());
        }
        
        let _permit = self.semaphore.acquire().await?;
        
        // Use versions API endpoint
        let url = format!("{}/api/v1/versions/{}.json", RUBYGEMS_API, gem_name);
        
        let response = self.http.get(&url).send().await?;
        
        if !response.status().is_success() {
            return Err(anyhow!("Failed to fetch versions for {}: {}", gem_name, response.status()));
        }
        
        #[derive(Deserialize)]
        struct VersionEntry {
            number: String,
            platform: Option<String>,
        }
        
        let entries: Vec<VersionEntry> = response.json().await?;
        let versions: Vec<String> = entries
            .into_iter()
            .filter(|e| e.platform.as_deref() == Some("ruby") || e.platform.is_none())
            .map(|e| e.number)
            .collect();
        
        // Cache it
        self.versions_cache.insert(gem_name.to_string(), versions.clone());
        
        Ok(versions)
    }
    
    /// Fetch gem info including dependencies for a specific version
    pub async fn fetch_spec(&self, gem_name: &str, version: &str) -> Result<GemSpec> {
        // Check memory cache
        let cache_key = format!("{}:{}", gem_name, version);
        if let Some(info) = self.cache.get(gem_name) {
            if let Some(ver_info) = info.versions.iter().find(|v| v.number == version) {
                return Ok(self.version_info_to_spec(gem_name, ver_info));
            }
        }
        
        let _permit = self.semaphore.acquire().await?;
        
        // Use the gems API for specific version info
        let url = format!("{}/api/v2/rubygems/{}/versions/{}.json", RUBYGEMS_API, gem_name, version);
        
        let response = self.http.get(&url).send().await?;
        
        if !response.status().is_success() {
            // Fallback to dependencies API
            return self.fetch_spec_via_deps(gem_name, version).await;
        }
        
        #[derive(Deserialize)]
        struct GemVersionInfo {
            version: String,
            platform: Option<String>,
            sha: Option<String>,
            #[serde(default)]
            dependencies: GemVersionDeps,
        }
        
        #[derive(Deserialize, Default)]
        struct GemVersionDeps {
            #[serde(default)]
            runtime: Vec<DepInfo>,
            #[serde(default)]
            development: Vec<DepInfo>,
        }
        
        #[derive(Deserialize)]
        struct DepInfo {
            name: String,
            requirements: String,
        }
        
        let info: GemVersionInfo = response.json().await?;
        
        let mut dependencies = Vec::new();
        for dep in info.dependencies.runtime {
            dependencies.push(GemDependency {
                name: dep.name,
                requirements: dep.requirements,
                dep_type: DependencyType::Runtime,
            });
        }
        
        Ok(GemSpec {
            name: gem_name.to_string(),
            version: info.version,
            platform: info.platform.unwrap_or_else(|| "ruby".to_string()),
            dependencies,
            sha256: info.sha,
        })
    }
    
    /// Fallback: fetch spec via dependencies API
    async fn fetch_spec_via_deps(&self, gem_name: &str, version: &str) -> Result<GemSpec> {
        let url = format!("{}/api/v1/dependencies.json?gems={}", RUBYGEMS_API, gem_name);
        
        let response = self.http.get(&url).send().await?;
        
        if !response.status().is_success() {
            return Err(anyhow!("Failed to fetch dependencies for {}", gem_name));
        }
        
        #[derive(Deserialize)]
        struct DepEntry {
            name: String,
            number: String,
            platform: String,
            dependencies: Vec<(String, String)>,
        }
        
        let entries: Vec<DepEntry> = response.json().await?;
        
        let entry = entries.iter()
            .find(|e| e.number == version && (e.platform == "ruby" || e.platform.is_empty()))
            .ok_or_else(|| anyhow!("Version {} not found for {}", version, gem_name))?;
        
        let dependencies = entry.dependencies.iter()
            .map(|(name, req)| GemDependency {
                name: name.clone(),
                requirements: req.clone(),
                dep_type: DependencyType::Runtime,
            })
            .collect();
        
        Ok(GemSpec {
            name: entry.name.clone(),
            version: entry.number.clone(),
            platform: entry.platform.clone(),
            dependencies,
            sha256: None,
        })
    }
    
    /// Fetch multiple gem specs in parallel
    pub async fn fetch_specs_parallel(&self, gems: &[(String, String)]) -> Result<Vec<GemSpec>> {
        let mut handles = Vec::with_capacity(gems.len());
        
        for (name, version) in gems {
            let client = self.clone();
            let name = name.clone();
            let version = version.clone();
            
            handles.push(tokio::spawn(async move {
                client.fetch_spec(&name, &version).await
            }));
        }
        
        let mut specs = Vec::with_capacity(handles.len());
        for handle in handles {
            specs.push(handle.await??);
        }
        
        Ok(specs)
    }
    
    fn version_info_to_spec(&self, gem_name: &str, info: &VersionInfo) -> GemSpec {
        let mut dependencies = Vec::new();
        
        for dep in &info.dependencies.runtime {
            dependencies.push(GemDependency {
                name: dep.name.clone(),
                requirements: dep.requirements.clone(),
                dep_type: DependencyType::Runtime,
            });
        }
        
        GemSpec {
            name: gem_name.to_string(),
            version: info.number.clone(),
            platform: info.platform.clone(),
            dependencies,
            sha256: Some(info.sha.clone()),
        }
    }
    
    /// Fetch all specs for multiple gems in parallel using compact index
    /// Much faster than sequential fetches - WITH PERSISTENT CACHING
    pub async fn fetch_deps_batch(&self, gem_names: &[&str]) -> Result<Vec<GemSpec>> {
        if gem_names.is_empty() {
            return Ok(Vec::new());
        }
        
        // Fetch all gems in parallel using compact index
        let mut handles = Vec::with_capacity(gem_names.len());
        
        for name in gem_names {
            let client = self.clone();
            let name = name.to_string();
            
            handles.push(tokio::spawn(async move {
                client.fetch_compact_index_cached(&name).await
            }));
        }
        
        let mut all_specs = Vec::new();
        for handle in handles {
            match handle.await? {
                Ok(specs) => all_specs.extend(specs),
                Err(e) => eprintln!("Warning: Failed to fetch: {}", e),
            }
        }
        
        Ok(all_specs)
    }

    /// Speculatively prefetch metadata for multiple gems in parallel.
    /// Spawns fire-and-forget tasks that populate the cache.
    /// Returns immediately after spawning - does NOT await results.
    pub fn prefetch_batch(&self, gem_names: &[String]) {
        for name in gem_names {
            let client = self.clone();
            let name = name.clone();
            tokio::spawn(async move {
                // Fire and forget - we don't care about the result
                let _ = client.fetch_compact_index_cached(&name).await;
            });
        }
        
        // Track spawned prefetches (best effort, don't block)
        let stats = self.stats.clone();
        let count = gem_names.len();
        tokio::spawn(async move {
            let mut s = stats.lock().await;
            s.prefetch_spawned += count;
        });
    }

    /// Record a prefetch hit (data was ready when needed due to speculative fetch)
    pub async fn record_prefetch_hit(&self) {
        let mut stats = self.stats.lock().await;
        stats.prefetch_hits += 1;
    }

    /// Fetch gem info from compact index WITH PERSISTENT CACHING
    pub async fn fetch_compact_index_cached(&self, gem_name: &str) -> Result<Vec<GemSpec>> {
        // 1. Check persistent cache first
        {
            let cache = self.persistent_cache.lock().await;
            if let Some((content, _entry)) = cache.get_gem_info(gem_name) {
                // Skip corrupted cache entries (empty content)
                if !content.is_empty() {
                    // Cache hit! Parse and return
                    let mut stats = self.stats.lock().await;
                    stats.cache_hits += 1;
                    return self.parse_compact_index(gem_name, &content);
                }
                // Fall through to refetch if cache is empty/corrupted
            }
        }

        // 2. Get metadata for conditional request
        let metadata: Option<CacheEntry> = {
            let cache = self.persistent_cache.lock().await;
            cache.get_gem_info_metadata(gem_name).cloned()
        };

        // 3. Fetch with conditional headers
        let _permit = self.semaphore.acquire().await?;
        
        let url = format!("{}/info/{}", COMPACT_INDEX, gem_name);
        let mut request = self.http.get(&url).header("Accept", "text/plain");
        
        // Add conditional headers if we have cached metadata
        if let Some(ref meta) = metadata {
            if let Some(ref etag) = meta.etag {
                request = request.header(IF_NONE_MATCH, etag.as_str());
            }
            if let Some(ref lm) = meta.last_modified {
                request = request.header(IF_MODIFIED_SINCE, lm.as_str());
            }
        }
        
        let response = request.send().await?;
        let status = response.status();
        
        // 4. Handle 304 Not Modified
        if status == StatusCode::NOT_MODIFIED {
            let mut stats = self.stats.lock().await;
            stats.conditional_not_modified += 1;
            drop(stats);  // Release lock before cache access
            
            // Read from cache (we know it exists since we have metadata)
            let cache = self.persistent_cache.lock().await;
            if let Some((content, _)) = cache.get_gem_info(gem_name) {
                return self.parse_compact_index(gem_name, &content);
            }
            drop(cache);
            
            // BUG FIX: Cache read failed but we got 304 - need to refetch WITHOUT conditional headers
            // A 304 response has no body, so falling through would cache empty content!
            let url = format!("{}/info/{}", COMPACT_INDEX, gem_name);
            let response = self.http.get(&url).header("Accept", "text/plain").send().await?;
            let status = response.status();
            if !status.is_success() {
                return Err(anyhow!("Failed to fetch compact index for {}: {}", gem_name, status));
            }
            let etag = response.headers().get(ETAG).and_then(|v| v.to_str().ok()).map(|s| s.to_string());
            let last_modified = response.headers().get(LAST_MODIFIED).and_then(|v| v.to_str().ok()).map(|s| s.to_string());
            let text = response.text().await?;
            
            // Update cache
            {
                let mut cache = self.persistent_cache.lock().await;
                let _ = cache.put_gem_info(gem_name, &text, etag, last_modified);
            }
            
            return self.parse_compact_index(gem_name, &text);
        }
        
        if !status.is_success() {
            return Err(anyhow!("Failed to fetch compact index for {}: {}", gem_name, status));
        }
        
        // 5. Extract cache headers
        let etag = response.headers()
            .get(ETAG)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        let last_modified = response.headers()
            .get(LAST_MODIFIED)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        
        // 6. Get response body
        let text = response.text().await?;
        
        // 7. Update stats
        {
            let mut stats = self.stats.lock().await;
            stats.network_fetches += 1;
            stats.cache_misses += 1;
        }
        
        // 8. Cache the response
        {
            let mut cache = self.persistent_cache.lock().await;
            if let Err(e) = cache.put_gem_info(gem_name, &text, etag, last_modified) {
                eprintln!("Warning: Failed to cache {}: {}", gem_name, e);
            }
        }
        
        // 9. Parse and return
        self.parse_compact_index(gem_name, &text)
    }
    
    /// Parse compact index format into GemSpecs
    /// Uses Rayon for parallel parsing - significant speedup for gems with many versions
    fn parse_compact_index(&self, gem_name: &str, text: &str) -> Result<Vec<GemSpec>> {
        // Format: version deps|checksum:sha
        // deps is: dep1:constraint,dep2:constraint,checksum:sha,ruby:>= 2.0
        
        // Collect lines first for parallel processing
        let lines: Vec<&str> = text.lines()
            .filter(|line| !line.starts_with("---") && !line.is_empty())
            .collect();
        
        // Parse lines in parallel using Rayon
        let specs: Vec<GemSpec> = lines.par_iter()
            .filter_map(|line| {
                // Split version from rest
                let mut parts = line.splitn(2, ' ');
                let version = match parts.next() {
                    Some(v) if !v.is_empty() => v.to_string(),
                    _ => return None,
                };
                
                let rest = parts.next().unwrap_or("");
                
                // The rest is: dep1:req,dep2:req|checksum:... OR dep1:req,dep2:req,checksum:...
                // Split by | first to separate checksum section
                let deps_part = rest.split('|').next().unwrap_or("");
                
                // Parse dependencies (skip checksum and ruby entries)
                let mut dependencies = Vec::new();
                let mut sha256 = None;
                
                for dep in deps_part.split(',') {
                    let dep = dep.trim();
                    if dep.is_empty() {
                        continue;
                    }
                    
                    // Split name:requirement
                    let mut dep_parts = dep.splitn(2, ':');
                    let name = dep_parts.next().unwrap_or("").to_string();
                    let requirements = dep_parts.next()
                        .map(|s| s.replace('&', ", "))  // Convert & to ,
                        .unwrap_or_else(|| ">= 0".to_string());
                    
                    // Skip metadata entries
                    if name == "checksum" {
                        sha256 = Some(requirements);
                        continue;
                    }
                    if name == "ruby" || name == "rubygems" {
                        continue;  // Skip ruby version requirements
                    }
                    
                    dependencies.push(GemDependency {
                        name,
                        requirements,
                        dep_type: DependencyType::Runtime,
                    });
                }
                
                Some(GemSpec {
                    name: gem_name.to_string(),
                    version,
                    platform: "ruby".to_string(),
                    dependencies,
                    sha256,
                })
            })
            .collect();
        
        Ok(specs)
    }
    
    /// Download a gem file
    pub async fn download_gem(&self, gem_name: &str, version: &str) -> Result<Vec<u8>> {
        let _permit = self.semaphore.acquire().await?;
        
        let url = format!("{}/gems/{}-{}.gem", RUBYGEMS_API, gem_name, version);
        
        let response = self.http.get(&url).send().await?;
        
        if !response.status().is_success() {
            return Err(anyhow!("Failed to download {}-{}: {}", gem_name, version, response.status()));
        }
        
        Ok(response.bytes().await?.to_vec())
    }

    /// Clear the persistent cache
    pub async fn clear_cache(&self) -> Result<()> {
        let mut cache = self.persistent_cache.lock().await;
        cache.clear()
    }

    /// Prune expired cache entries
    pub async fn prune_cache(&self) -> Result<usize> {
        let mut cache = self.persistent_cache.lock().await;
        cache.prune_expired()
    }
}

impl Clone for Client {
    fn clone(&self) -> Self {
        Self {
            http: self.http.clone(),
            semaphore: Arc::clone(&self.semaphore),
            cache: Arc::clone(&self.cache),
            versions_cache: Arc::clone(&self.versions_cache),
            persistent_cache: Arc::clone(&self.persistent_cache),
            stats: Arc::clone(&self.stats),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_fetch_versions() {
        let client = Client::new(4);
        let versions = client.fetch_versions("rack").await.unwrap();
        assert!(!versions.is_empty());
        assert!(versions.iter().any(|v| v.starts_with("3.")));
    }
    
    #[tokio::test]
    async fn test_fetch_spec() {
        let client = Client::new(4);
        let spec = client.fetch_spec("rack", "3.0.0").await.unwrap();
        assert_eq!(spec.name, "rack");
        assert_eq!(spec.version, "3.0.0");
    }

    #[tokio::test]
    async fn test_caching_works() {
        let client = Client::new(4);
        
        // First fetch - should go to network
        let specs1 = client.fetch_compact_index_cached("rack").await.unwrap();
        let stats1 = client.get_stats().await;
        assert_eq!(stats1.network_fetches, 1);
        
        // Second fetch - should use cache
        let specs2 = client.fetch_compact_index_cached("rack").await.unwrap();
        let stats2 = client.get_stats().await;
        assert_eq!(stats2.cache_hits, 1);
        
        assert_eq!(specs1.len(), specs2.len());
    }
}
