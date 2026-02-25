//! Memory-mapped zero-copy gem index using rkyv
//!
//! This module provides blazingly fast index loading by:
//! - Pre-serializing all gem data to a binary format (rkyv)
//! - Memory-mapping the index file at startup
//! - Zero parsing, zero deserialization - just pointer arithmetic
//!
//! Build the index once with `schwadl index build`, then enjoy microsecond startup.

use anyhow::{anyhow, Context, Result};
use memmap2::Mmap;
use rkyv::{Archive, Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::Write;
use std::path::PathBuf;

use crate::cache::PersistentCache;

/// Path to the index file: ~/.schwadler/index.rkyv
pub fn index_path() -> PathBuf {
    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".schwadler")
        .join("index.rkyv")
}

// ============================================================================
// RKYV-SERIALIZABLE STRUCTS
// ============================================================================

/// The complete gem index - serialized to disk
#[derive(Archive, Serialize, Deserialize, Debug, Clone)]
#[rkyv(derive(Debug))]
pub struct GemIndex {
    /// All indexed gems
    pub gems: Vec<IndexedGem>,
    /// Quick lookup: gem name -> index in gems vec
    pub name_to_idx: HashMap<String, u32>,
    /// Index version for future migrations
    pub version: u32,
    /// When this index was built (Unix timestamp)
    pub built_at: u64,
}

/// A single gem with all its versions
#[derive(Archive, Serialize, Deserialize, Debug, Clone, PartialEq)]
#[rkyv(compare(PartialEq), derive(Debug))]
pub struct IndexedGem {
    /// Gem name (e.g., "rack")
    pub name: String,
    /// All known versions of this gem
    pub versions: Vec<IndexedVersion>,
}

/// A specific version of a gem
#[derive(Archive, Serialize, Deserialize, Debug, Clone, PartialEq)]
#[rkyv(compare(PartialEq), derive(Debug))]
pub struct IndexedVersion {
    /// Version string (e.g., "3.0.0")
    pub version: String,
    /// Runtime dependencies
    pub dependencies: Vec<IndexedDep>,
    /// SHA256 checksum if known
    pub sha256: Option<String>,
}

/// A dependency requirement
#[derive(Archive, Serialize, Deserialize, Debug, Clone, PartialEq)]
#[rkyv(compare(PartialEq), derive(Debug))]
pub struct IndexedDep {
    /// Dependency gem name
    pub name: String,
    /// Version requirements (e.g., ">= 1.0, < 3.0")
    pub requirements: String,
}

// ============================================================================
// INDEX BUILDING
// ============================================================================

/// Build the index from the persistent cache
///
/// This reads all cached gem info from ~/.schwadler/cache/gems/
/// and serializes it into a single mmap-able binary file.
pub fn build_index() -> Result<BuildStats> {
    let cache = PersistentCache::new().context("Failed to open cache")?;
    let cache_dir = PersistentCache::default_cache_dir();
    let gems_dir = cache_dir.join("gems");

    if !gems_dir.exists() {
        return Err(anyhow!(
            "No cached gems found. Run `schwadl lock` first to populate the cache."
        ));
    }

    let mut gems = Vec::new();
    let mut name_to_idx = HashMap::new();
    let mut total_versions = 0usize;
    let mut total_deps = 0usize;

    // Walk through all cached gem files
    for entry in walkdir(&gems_dir)? {
        let path = entry?;
        if !path.is_file() || !path.extension().map_or(false, |e| e == "txt") {
            continue;
        }

        // Extract gem name from filename
        let gem_name = path
            .file_stem()
            .and_then(|s| s.to_str())
            .ok_or_else(|| anyhow!("Invalid gem filename: {:?}", path))?
            .to_string();

        // Read and parse the compact index format
        let content = fs::read_to_string(&path)
            .with_context(|| format!("Failed to read {:?}", path))?;

        let versions = parse_compact_index(&gem_name, &content)?;
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

    if gems.is_empty() {
        return Err(anyhow!(
            "No gems found in cache. Run `schwadl lock` first to populate the cache."
        ));
    }

    let index = GemIndex {
        gems,
        name_to_idx,
        version: 1,
        built_at: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs(),
    };

    // Serialize with rkyv
    let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&index)
        .map_err(|e| anyhow!("Serialization failed: {}", e))?;

    // Write to disk
    let path = index_path();
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let mut file = File::create(&path).with_context(|| format!("Failed to create {:?}", path))?;
    file.write_all(&bytes)?;
    file.sync_all()?;

    Ok(BuildStats {
        gem_count: index.gems.len(),
        version_count: total_versions,
        dependency_count: total_deps,
        index_size_bytes: bytes.len(),
    })
}

/// Statistics from building the index
#[derive(Debug)]
pub struct BuildStats {
    pub gem_count: usize,
    pub version_count: usize,
    pub dependency_count: usize,
    pub index_size_bytes: usize,
}

impl std::fmt::Display for BuildStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} gems, {} versions, {} dependencies ({:.1} KB)",
            self.gem_count,
            self.version_count,
            self.dependency_count,
            self.index_size_bytes as f64 / 1024.0
        )
    }
}

// ============================================================================
// INDEX LOADING (MMAP)
// ============================================================================

/// Memory-mapped index handle
///
/// The mmap must be kept alive for the lifetime of the archived reference.
pub struct MappedIndex {
    /// The memory-mapped file (must stay alive)
    _mmap: Mmap,
    /// Pointer to the archived data (valid as long as _mmap lives)
    archived: *const ArchivedGemIndex,
}

// Safety: The archived data is read-only and the mmap is immutable
unsafe impl Send for MappedIndex {}
unsafe impl Sync for MappedIndex {}

impl MappedIndex {
    /// Load the index via mmap
    ///
    /// This is extremely fast - just maps the file into memory, no parsing.
    pub fn load() -> Result<Self> {
        let path = index_path();
        if !path.exists() {
            return Err(anyhow!(
                "Index not found at {:?}. Run `schwadl index build` first.",
                path
            ));
        }

        let file = File::open(&path).with_context(|| format!("Failed to open {:?}", path))?;

        // Safety: The file is read-only and we keep the mmap alive
        let mmap = unsafe { Mmap::map(&file)? };

        // Get pointer to the data before moving mmap
        let data_ptr = mmap.as_ptr();
        let data_len = mmap.len();

        // Create the struct first, then validate
        let mut result = MappedIndex {
            _mmap: mmap,
            archived: std::ptr::null(),
        };

        // Now validate using the data from our owned mmap
        // Safety: _mmap is now owned by result and won't move
        let archived = unsafe {
            let data = std::slice::from_raw_parts(data_ptr, data_len);
            rkyv::access::<ArchivedGemIndex, rkyv::rancor::Error>(data)
                .map_err(|e| anyhow!("Index validation failed: {}", e))?
        };

        result.archived = archived as *const ArchivedGemIndex;
        Ok(result)
    }

    /// Get the archived index (zero-copy access)
    pub fn index(&self) -> &ArchivedGemIndex {
        // Safety: archived pointer is valid as long as _mmap is alive
        unsafe { &*self.archived }
    }

    /// Look up a gem by name
    pub fn get_gem(&self, name: &str) -> Option<&ArchivedIndexedGem> {
        let idx = self.index().name_to_idx.get(name)?;
        let idx_val: u32 = (*idx).into();
        self.index().gems.get(idx_val as usize)
    }

    /// Get all gem names
    pub fn gem_names(&self) -> impl Iterator<Item = &str> {
        self.index().gems.iter().map(|g| g.name.as_str())
    }

    /// Get index statistics
    pub fn stats(&self) -> IndexStats {
        let index = self.index();
        let total_versions: usize = index.gems.iter().map(|g| g.versions.len()).sum();
        
        IndexStats {
            gem_count: index.gems.len(),
            version_count: total_versions,
            built_at: index.built_at.into(),
        }
    }
}

/// Statistics about the loaded index
#[derive(Debug)]
pub struct IndexStats {
    pub gem_count: usize,
    pub version_count: usize,
    pub built_at: u64,
}

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================

/// Parse compact index format into IndexedVersions
fn parse_compact_index(gem_name: &str, text: &str) -> Result<Vec<IndexedVersion>> {
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

    Ok(versions)
}

/// Simple directory walker
fn walkdir(dir: &PathBuf) -> Result<impl Iterator<Item = Result<PathBuf>>> {
    let entries: Vec<_> = fs::read_dir(dir)
        .with_context(|| format!("Failed to read {:?}", dir))?
        .collect();

    Ok(entries.into_iter().flat_map(move |entry| {
        match entry {
            Ok(e) => {
                let path = e.path();
                if path.is_dir() {
                    // Recurse into subdirectory
                    match walkdir(&path) {
                        Ok(iter) => Box::new(iter) as Box<dyn Iterator<Item = Result<PathBuf>>>,
                        Err(e) => Box::new(std::iter::once(Err(e))),
                    }
                } else {
                    Box::new(std::iter::once(Ok(path)))
                }
            }
            Err(e) => Box::new(std::iter::once(Err(anyhow::Error::from(e)))),
        }
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_compact_index() {
        let content = r#"---
3.0.0 rack-test:>= 1.0,other:>= 0&< 2|checksum:abc123
2.0.0 |checksum:def456
"#;
        let versions = parse_compact_index("rack", content).unwrap();
        
        assert_eq!(versions.len(), 2);
        assert_eq!(versions[0].version, "3.0.0");
        assert_eq!(versions[0].dependencies.len(), 2);
        assert_eq!(versions[0].dependencies[0].name, "rack-test");
        assert_eq!(versions[0].dependencies[0].requirements, ">= 1.0");
        assert_eq!(versions[0].dependencies[1].requirements, ">= 0, < 2");
    }

    #[test]
    fn test_index_serialization() {
        let index = GemIndex {
            gems: vec![IndexedGem {
                name: "rack".to_string(),
                versions: vec![IndexedVersion {
                    version: "3.0.0".to_string(),
                    dependencies: vec![IndexedDep {
                        name: "rack-test".to_string(),
                        requirements: ">= 1.0".to_string(),
                    }],
                    sha256: Some("abc123".to_string()),
                }],
            }],
            name_to_idx: [("rack".to_string(), 0)].into_iter().collect(),
            version: 1,
            built_at: 0,
        };

        // Serialize
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&index).unwrap();
        
        // Access without deserializing
        let archived = rkyv::access::<ArchivedGemIndex, rkyv::rancor::Error>(&bytes).unwrap();
        
        assert_eq!(archived.gems.len(), 1);
        assert_eq!(archived.gems[0].name.as_str(), "rack");
        assert_eq!(archived.gems[0].versions[0].version.as_str(), "3.0.0");
    }
}
