//! Dependency resolver using PubGrub algorithm
//! 
//! PubGrub is the same algorithm used by pub (Dart) and Cargo.
//! It's significantly faster than Bundler's Molinillo for complex trees.
//!
//! ## Conflict-Based Priority Inversion
//! 
//! Inspired by uv's optimization (issues #8157, #9843): when package A 
//! repeatedly conflicts with package B, we swap their resolution priorities.
//! This prevents the resolver from wasting time exploring thousands of A's
//! versions before realizing B should decide first.

use crate::gemfile::{Gemfile, GemDeclaration};
use crate::git::{GitCache, GitSource, parse_gemspec};
use crate::rubygems::{Client, GemSpec, DependencyType};
use anyhow::{anyhow, Context, Result};
use dashmap::DashMap;
use rayon::prelude::*;
use std::cmp::Ordering as CmpOrdering;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// Performance profiling stats for version matching operations
#[derive(Debug, Default)]
pub struct VersionMatchStats {
    /// Total time spent in version matching (nanoseconds)
    pub total_match_time_ns: AtomicU64,
    /// Number of individual version comparisons
    pub comparison_count: AtomicU64,
    /// Number of batch operations
    pub batch_count: AtomicU64,
}

impl VersionMatchStats {
    pub fn new() -> Self {
        Self::default()
    }
    
    /// Record a batch version matching operation
    pub fn record_batch(&self, duration_ns: u64, versions_checked: usize) {
        self.total_match_time_ns.fetch_add(duration_ns, Ordering::Relaxed);
        self.comparison_count.fetch_add(versions_checked as u64, Ordering::Relaxed);
        self.batch_count.fetch_add(1, Ordering::Relaxed);
    }
    
    /// Get human-readable summary
    pub fn summary(&self) -> String {
        let total_ns = self.total_match_time_ns.load(Ordering::Relaxed);
        let comparisons = self.comparison_count.load(Ordering::Relaxed);
        let batches = self.batch_count.load(Ordering::Relaxed);
        
        if comparisons == 0 {
            return "No version matching recorded".to_string();
        }
        
        let avg_ns = total_ns / comparisons;
        format!(
            "Version matching: {} comparisons in {} batches, total {}µs ({} ns/comparison avg)",
            comparisons,
            batches,
            total_ns / 1000,
            avg_ns
        )
    }
}

/// Global profiling stats (enable with SCHWADLER_PROFILE=1)
pub static VERSION_MATCH_STATS: std::sync::LazyLock<VersionMatchStats> = 
    std::sync::LazyLock::new(VersionMatchStats::new);

/// Check if profiling is enabled
fn profiling_enabled() -> bool {
    std::env::var("SCHWADLER_PROFILE").map(|v| v == "1").unwrap_or(false)
}

// =============================================================================
// Conflict-Based Priority Inversion (inspired by uv #8157, #9843)
// =============================================================================

/// Tracks pairwise conflicts between gems during resolution.
/// 
/// When two gems repeatedly conflict (one's version choices break the other),
/// we record it here. After `threshold` conflicts, we signal that their
/// priorities should be inverted.
#[derive(Debug, Clone)]
struct ConflictTracker {
    /// (gem_a, gem_b) -> conflict count (canonical ordering: gem_a < gem_b)
    conflicts: HashMap<(String, String), usize>,
    /// Number of conflicts before triggering priority swap
    threshold: usize,
}

impl ConflictTracker {
    fn new() -> Self {
        Self::with_threshold(5)
    }
    
    fn with_threshold(threshold: usize) -> Self {
        Self {
            conflicts: HashMap::new(),
            threshold,
        }
    }
    
    /// Record a conflict between two gems.
    /// Returns true if the threshold has been reached and priorities should be inverted.
    fn record_conflict(&mut self, gem_a: &str, gem_b: &str) -> bool {
        // Canonical ordering to avoid (a,b) and (b,a) being separate entries
        let key = if gem_a < gem_b {
            (gem_a.to_string(), gem_b.to_string())
        } else {
            (gem_b.to_string(), gem_a.to_string())
        };
        let count = self.conflicts.entry(key).or_insert(0);
        *count += 1;
        *count >= self.threshold
    }
    
    /// Get the conflict count between two gems.
    #[allow(dead_code)]
    fn get_conflict_count(&self, gem_a: &str, gem_b: &str) -> usize {
        let key = if gem_a < gem_b {
            (gem_a.to_string(), gem_b.to_string())
        } else {
            (gem_b.to_string(), gem_a.to_string())
        };
        *self.conflicts.get(&key).unwrap_or(&0)
    }
    
    /// Reset conflicts for a specific pair (after successful resolution).
    #[allow(dead_code)]
    fn clear_pair(&mut self, gem_a: &str, gem_b: &str) {
        let key = if gem_a < gem_b {
            (gem_a.to_string(), gem_b.to_string())
        } else {
            (gem_b.to_string(), gem_a.to_string())
        };
        self.conflicts.remove(&key);
    }
    
    /// Get all gems that have reached conflict threshold with any other gem.
    fn get_highly_conflicting_gems(&self) -> HashSet<String> {
        let mut result = HashSet::new();
        for ((gem_a, gem_b), count) in &self.conflicts {
            if *count >= self.threshold {
                result.insert(gem_a.clone());
                result.insert(gem_b.clone());
            }
        }
        result
    }
}

/// Priority types for gems, from highest to lowest.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum GemPriorityType {
    /// Exact version pinned (e.g., `== 1.2.3` or `= 1.2.3`)
    ExactVersion = 100,
    /// URL/git dependency
    UrlDependency = 90,
    /// Highly conflicting package (gets special handling - resolved earlier)
    HighlyConflicting = 80,
    /// Direct dependency from Gemfile
    DirectDependency = 70,
    /// Transitive dependency
    Transitive = 50,
}

/// Manages gem resolution priorities and inversions.
/// 
/// Priority inversions are the key optimization: when gem A repeatedly
/// conflicts with gem B, we swap their effective priorities so B gets
/// resolved first next time.
#[derive(Debug, Clone)]
struct PriorityManager {
    /// Base priorities for each gem (higher = resolve first)
    base_priorities: HashMap<String, i32>,
    /// Set of inverted pairs (canonical ordering)
    inversions: HashSet<(String, String)>,
    /// Order gems were first seen (for tiebreaking)
    first_seen_order: HashMap<String, usize>,
    /// Counter for first-seen ordering
    seen_counter: usize,
}

impl PriorityManager {
    fn new() -> Self {
        Self {
            base_priorities: HashMap::new(),
            inversions: HashSet::new(),
            first_seen_order: HashMap::new(),
            seen_counter: 0,
        }
    }
    
    /// Register a gem with its base priority type.
    fn register(&mut self, gem: &str, priority_type: GemPriorityType) {
        // Only set if not already registered (first registration wins)
        if !self.base_priorities.contains_key(gem) {
            self.base_priorities.insert(gem.to_string(), priority_type as i32);
        }
        // Track first-seen order
        if !self.first_seen_order.contains_key(gem) {
            self.first_seen_order.insert(gem.to_string(), self.seen_counter);
            self.seen_counter += 1;
        }
    }
    
    /// Update a gem's priority (e.g., marking it as highly conflicting).
    fn update_priority(&mut self, gem: &str, priority_type: GemPriorityType) {
        self.base_priorities.insert(gem.to_string(), priority_type as i32);
    }
    
    /// Get effective priority for a gem, accounting for inversions.
    fn get_priority(&self, gem: &str) -> i32 {
        let base = *self.base_priorities.get(gem).unwrap_or(&(GemPriorityType::Transitive as i32));
        
        // Count how many inversions affect this gem
        let mut adjustment = 0i32;
        for (a, b) in &self.inversions {
            if a == gem {
                // This gem was demoted (B now goes before A)
                adjustment -= 10;
            } else if b == gem {
                // This gem was promoted
                adjustment += 10;
            }
        }
        
        base + adjustment
    }
    
    /// Get first-seen order for tiebreaking.
    fn get_first_seen(&self, gem: &str) -> usize {
        *self.first_seen_order.get(gem).unwrap_or(&usize::MAX)
    }
    
    /// Invert priorities between two gems.
    /// After inversion, the gem that was lower priority will be resolved first.
    fn invert(&mut self, gem_a: &str, gem_b: &str) {
        let key = if gem_a < gem_b {
            (gem_a.to_string(), gem_b.to_string())
        } else {
            (gem_b.to_string(), gem_a.to_string())
        };
        self.inversions.insert(key);
    }
    
    /// Check if two gems have an inversion.
    #[allow(dead_code)]
    fn is_inverted(&self, gem_a: &str, gem_b: &str) -> bool {
        let key = if gem_a < gem_b {
            (gem_a.to_string(), gem_b.to_string())
        } else {
            (gem_b.to_string(), gem_a.to_string())
        };
        self.inversions.contains(&key)
    }
    
    /// Clear an inversion (e.g., after successful resolution).
    #[allow(dead_code)]
    fn clear_inversion(&mut self, gem_a: &str, gem_b: &str) {
        let key = if gem_a < gem_b {
            (gem_a.to_string(), gem_b.to_string())
        } else {
            (gem_b.to_string(), gem_a.to_string())
        };
        self.inversions.remove(&key);
    }
    
    /// Sort gems by priority (highest first).
    fn sort_by_priority(&self, gems: &mut Vec<String>) {
        gems.sort_by(|a, b| {
            let prio_a = self.get_priority(a);
            let prio_b = self.get_priority(b);
            match prio_b.cmp(&prio_a) {
                CmpOrdering::Equal => {
                    // Tiebreak by first-seen order (earlier = higher priority)
                    match self.get_first_seen(a).cmp(&self.get_first_seen(b)) {
                        CmpOrdering::Equal => a.cmp(b), // Alphabetical as final tiebreaker
                        other => other,
                    }
                }
                other => other,
            }
        });
    }
}

/// Determine priority type from version constraints.
fn priority_from_constraints(constraints: &[String]) -> GemPriorityType {
    for c in constraints {
        let trimmed = c.trim();
        // Exact version: "= X.Y.Z" or "== X.Y.Z" 
        if trimmed.starts_with("= ") && !trimmed.starts_with(">=") && !trimmed.starts_with("<=") {
            return GemPriorityType::ExactVersion;
        }
        if trimmed.starts_with("==") {
            return GemPriorityType::ExactVersion;
        }
    }
    GemPriorityType::DirectDependency
}

// =============================================================================
// End Conflict-Based Priority Inversion
// =============================================================================

/// Detect the current platform in RubyGems format.
/// 
/// Returns a Vec with the specific platform first, followed by "ruby" as fallback.
/// This matches Bundler's behavior of including both platform-specific and pure Ruby gems.
/// 
/// Platform strings follow RubyGems conventions:
/// - x86_64-darwin, arm64-darwin (macOS)
/// - x86_64-linux, aarch64-linux (Linux)  
/// - x64-mingw-ucrt (Windows)
pub fn detect_platforms() -> Vec<String> {
    let mut platforms = Vec::new();
    
    // Detect OS and architecture
    let os = std::env::consts::OS;
    let arch = std::env::consts::ARCH;
    
    let platform = match (os, arch) {
        // macOS
        ("macos", "x86_64") => "x86_64-darwin",
        ("macos", "aarch64") => "arm64-darwin",
        
        // Linux
        ("linux", "x86_64") => "x86_64-linux",
        ("linux", "aarch64") => "aarch64-linux",
        ("linux", "arm") => "arm-linux",
        
        // Windows
        ("windows", "x86_64") => "x64-mingw-ucrt",
        ("windows", "x86") => "x86-mingw32",
        
        // FreeBSD
        ("freebsd", "x86_64") => "x86_64-freebsd",
        ("freebsd", "aarch64") => "aarch64-freebsd",
        
        // Fallback: construct from OS/arch
        _ => {
            // For unknown platforms, just use "ruby"
            platforms.push("ruby".to_string());
            return platforms;
        }
    };
    
    platforms.push(platform.to_string());
    platforms.push("ruby".to_string());
    
    platforms
}

/// A resolved dependency graph
#[derive(Debug, Clone)]
pub struct Resolution {
    /// Resolved gems with their versions (from RubyGems)
    pub gems: Vec<ResolvedGem>,
    /// Git gems with their revisions
    pub git_gems: Vec<ResolvedGitGem>,
    /// The original source URL
    pub source: String,
    /// Ruby version if specified
    pub ruby_version: Option<String>,
    /// Platform specs
    pub platforms: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct ResolvedGem {
    pub name: String,
    pub version: String,
    pub dependencies: Vec<String>,  // Names of dependencies
    pub sha256: Option<String>,
    pub is_direct: bool,  // Was this in the Gemfile directly?
}

/// A gem resolved from a git source
#[derive(Debug, Clone)]
pub struct ResolvedGitGem {
    pub name: String,
    pub version: String,
    pub git_url: String,
    pub revision: String,  // Full SHA
    pub branch: Option<String>,
    pub tag: Option<String>,
    pub ref_: Option<String>,
    pub dependencies: Vec<String>,
}

/// Version constraint parser and matcher
#[derive(Debug, Clone)]
pub struct VersionConstraint {
    pub operator: String,
    pub version: semver::Version,
}

impl VersionConstraint {
    pub fn parse(constraint: &str) -> Result<Self> {
        let constraint = constraint.trim();
        
        // Handle ~> (pessimistic)
        if let Some(ver) = constraint.strip_prefix("~>") {
            return Ok(Self {
                operator: "~>".to_string(),
                version: parse_ruby_version(ver.trim())?,
            });
        }
        
        // Handle >= and <=
        if let Some(ver) = constraint.strip_prefix(">=") {
            return Ok(Self {
                operator: ">=".to_string(),
                version: parse_ruby_version(ver.trim())?,
            });
        }
        if let Some(ver) = constraint.strip_prefix("<=") {
            return Ok(Self {
                operator: "<=".to_string(),
                version: parse_ruby_version(ver.trim())?,
            });
        }
        
        // Handle > and <
        if let Some(ver) = constraint.strip_prefix(">") {
            return Ok(Self {
                operator: ">".to_string(),
                version: parse_ruby_version(ver.trim())?,
            });
        }
        if let Some(ver) = constraint.strip_prefix("<") {
            return Ok(Self {
                operator: "<".to_string(),
                version: parse_ruby_version(ver.trim())?,
            });
        }
        
        // Handle != 
        if let Some(ver) = constraint.strip_prefix("!=") {
            return Ok(Self {
                operator: "!=".to_string(),
                version: parse_ruby_version(ver.trim())?,
            });
        }
        
        // Handle = or bare version
        let ver = constraint.strip_prefix("=").unwrap_or(constraint).trim();
        Ok(Self {
            operator: "=".to_string(),
            version: parse_ruby_version(ver)?,
        })
    }
    
    pub fn matches(&self, version: &semver::Version) -> bool {
        match self.operator.as_str() {
            "=" => version == &self.version,
            "!=" => version != &self.version,
            ">" => version > &self.version,
            ">=" => version >= &self.version,
            "<" => version < &self.version,
            "<=" => version <= &self.version,
            "~>" => {
                // Pessimistic version constraint
                // ~> 1.2 means >= 1.2 and < 2.0
                // ~> 1.2.3 means >= 1.2.3 and < 1.3.0
                if version < &self.version {
                    return false;
                }
                
                // Determine the upper bound based on version segments
                let upper = if self.version.patch == 0 && self.version.pre.is_empty() {
                    // ~> 1.2 style - bump major
                    semver::Version::new(self.version.major + 1, 0, 0)
                } else {
                    // ~> 1.2.3 style - bump minor
                    semver::Version::new(self.version.major, self.version.minor + 1, 0)
                };
                
                version < &upper
            }
            _ => false,
        }
    }
    
    /// Check multiple versions against a constraint in one pass.
    /// 
    /// This batch operation enables future SIMD optimization for version matching.
    /// Currently iterates, but the batch interface allows us to swap in SIMD
    /// intrinsics later if profiling shows version matching is a bottleneck.
    /// 
    /// # Example
    /// ```
    /// let constraint = VersionConstraint::parse(">= 1.2.0").unwrap();
    /// let versions = vec![
    ///     semver::Version::new(1, 0, 0),
    ///     semver::Version::new(1, 2, 0),
    ///     semver::Version::new(2, 0, 0),
    /// ];
    /// let results = constraint.matches_batch(&versions);
    /// assert_eq!(results, vec![false, true, true]);
    /// ```
    #[inline]
    pub fn matches_batch(&self, versions: &[semver::Version]) -> Vec<bool> {
        // For now: batch processing via iterator
        // Future optimization: use actual SIMD intrinsics for numeric comparisons
        // 
        // SIMD opportunity: Pack version components (major, minor, patch) into
        // SIMD registers and perform parallel comparisons. This would require:
        // 1. Converting versions to packed u64x4 or similar
        // 2. Using portable_simd (nightly) or platform-specific intrinsics
        versions.iter()
            .map(|v| self.matches(v))
            .collect()
    }
    
    /// Check multiple versions against a constraint, returning matching versions.
    /// 
    /// More efficient than matches_batch when you only need the matching versions,
    /// not the full boolean mask.
    #[inline]
    pub fn filter_matching<'a>(&self, versions: &'a [semver::Version]) -> Vec<&'a semver::Version> {
        versions.iter()
            .filter(|v| self.matches(v))
            .collect()
    }
}

/// Parse Ruby-style versions into semver
fn parse_ruby_version(s: &str) -> Result<semver::Version> {
    let s = s.trim();
    let parts: Vec<&str> = s.split('.').collect();
    
    let major: u64 = parts.get(0)
        .and_then(|p| p.parse().ok())
        .unwrap_or(0);
    let minor: u64 = parts.get(1)
        .and_then(|p| p.parse().ok())
        .unwrap_or(0);
    
    // Handle patch which might have prerelease suffix like "0.rc1" or "3.beta2"
    let (patch, prerelease) = if let Some(patch_str) = parts.get(2) {
        // Split on first non-digit character
        let mut split_idx = patch_str.len();
        for (i, c) in patch_str.char_indices() {
            if !c.is_ascii_digit() {
                split_idx = i;
                break;
            }
        }
        
        let patch_num: u64 = patch_str[..split_idx].parse().unwrap_or(0);
        
        // Extract prerelease if present (e.g., ".rc1" becomes "rc1")
        let pre = if split_idx < patch_str.len() {
            // Skip leading dot or dash if present
            let pre_str = &patch_str[split_idx..];
            let pre_clean = pre_str.trim_start_matches(|c| c == '.' || c == '-');
            
            // Check for additional prerelease parts (e.g., "0.rc1" where rc1 is in parts[3])
            if pre_clean.is_empty() {
                if let Some(pre_part) = parts.get(3) {
                    pre_part.to_string()
                } else {
                    String::new()
                }
            } else {
                pre_clean.to_string()
            }
        } else if let Some(pre_part) = parts.get(3) {
            // Prerelease is in the 4th segment (e.g., "1.16.0.rc1")
            pre_part.to_string()
        } else {
            String::new()
        };
        
        (patch_num, pre)
    } else {
        (0, String::new())
    };
    
    let mut version = semver::Version::new(major, minor, patch);
    
    if !prerelease.is_empty() {
        // Parse prerelease into semver format
        version.pre = semver::Prerelease::new(&prerelease).unwrap_or(semver::Prerelease::EMPTY);
    }
    
    Ok(version)
}

/// Parse a version string from RubyGems
fn parse_gem_version(s: &str) -> Option<semver::Version> {
    parse_ruby_version(s).ok()
}

/// Resolve all dependencies from a Gemfile
/// 
/// `locked_versions`: Optional map of gem name -> version for conservative updates.
/// Gems in this map will be constrained to exactly that version.
pub async fn resolve(
    gemfile: &Gemfile, 
    client: &Client,
    locked_versions: Option<HashMap<String, String>>,
) -> Result<Resolution> {
    let resolver = Resolver::new(client.clone(), locked_versions.unwrap_or_default());
    resolver.resolve(gemfile).await
}

/// Resolve dependencies using the pre-downloaded full index (offline mode)
/// 
/// This function uses the locally cached full index instead of making network requests.
/// The index must be downloaded first with `schwadl index download`.
pub fn resolve_offline(
    gemfile: &Gemfile,
    locked_versions: Option<HashMap<String, String>>,
) -> Result<Resolution> {
    let locked = locked_versions.unwrap_or_default();
    let resolver = OfflineResolver::new(locked)?;
    resolver.resolve(gemfile)
}

/// Offline resolver using pre-downloaded full index
struct OfflineResolver {
    /// The memory-mapped full index
    index: crate::full_index::FullIndex,
    /// Locked versions for conservative updates
    locked_versions: HashMap<String, String>,
}

impl OfflineResolver {
    fn new(locked_versions: HashMap<String, String>) -> Result<Self> {
        let index = crate::full_index::FullIndex::load()
            .context("Failed to load full index for offline resolution")?;
        
        Ok(Self {
            index,
            locked_versions,
        })
    }
    
    fn resolve(&self, gemfile: &Gemfile) -> Result<Resolution> {
        let mut resolved: HashMap<String, ResolvedGem> = HashMap::new();
        let mut git_gems: Vec<ResolvedGitGem> = Vec::new();
        
        // Separate git and regular gems
        let regular_gems: Vec<&GemDeclaration> = gemfile.gems.iter()
            .filter(|g| !g.is_git_source() && g.path.is_none())
            .collect();
        
        let git_source_gems: Vec<&GemDeclaration> = gemfile.gems.iter()
            .filter(|g| g.is_git_source())
            .collect();
        
        // Note: Git gems still require network access
        if !git_source_gems.is_empty() {
            return Err(anyhow!(
                "Offline mode does not support git sources. Found {} git gem(s): {}",
                git_source_gems.len(),
                git_source_gems.iter().map(|g| g.name.as_str()).collect::<Vec<_>>().join(", ")
            ));
        }
        
        let direct_gems: HashSet<String> = regular_gems.iter()
            .map(|g| g.name.clone())
            .collect();
        
        // Build constraints map from gemfile
        let mut constraints_map: HashMap<String, Vec<String>> = gemfile.gems.iter()
            .filter(|g| g.git.is_none() && g.path.is_none())
            .map(|g| (g.name.clone(), g.version_constraints.clone()))
            .collect();
        
        // Resolution queue
        let mut to_resolve: Vec<String> = regular_gems.iter()
            .map(|g| g.name.clone())
            .collect();
        
        while let Some(gem_name) = to_resolve.pop() {
            if resolved.contains_key(&gem_name) {
                continue;
            }
            
            // Get constraints for this gem
            let constraints = constraints_map.get(&gem_name)
                .cloned()
                .unwrap_or_default();
            
            // Find version from index
            let version = if let Some(locked_ver) = self.locked_versions.get(&gem_name) {
                // Check if locked version satisfies constraints
                if self.version_satisfies_constraints(locked_ver, &constraints) {
                    locked_ver.clone()
                } else {
                    self.find_best_version(&gem_name, &constraints)?
                }
            } else {
                self.find_best_version(&gem_name, &constraints)?
            };
            
            // Get dependencies from index
            let deps = self.index.get_dependencies(&gem_name, &version)
                .unwrap_or_default();
            
            let dep_names: Vec<String> = deps.iter()
                .map(|(name, _)| name.clone())
                .collect();
            
            // Queue dependencies for resolution and update constraints
            for (dep_name, dep_req) in &deps {
                if !resolved.contains_key(dep_name) {
                    to_resolve.push(dep_name.clone());
                    
                    // Parse and add constraints
                    let new_constraints = parse_requirements(dep_req);
                    constraints_map.entry(dep_name.clone())
                        .or_default()
                        .extend(new_constraints);
                }
            }
            
            resolved.insert(gem_name.clone(), ResolvedGem {
                name: gem_name.clone(),
                version,
                dependencies: dep_names,
                sha256: None, // TODO: Could extract from index
                is_direct: direct_gems.contains(&gem_name),
            });
        }
        
        // Sort gems alphabetically for consistent output
        let mut gems: Vec<ResolvedGem> = resolved.into_values().collect();
        gems.sort_by(|a, b| a.name.cmp(&b.name));
        
        Ok(Resolution {
            gems,
            git_gems,
            source: gemfile.source.clone(),
            ruby_version: gemfile.ruby_version.clone(),
            platforms: detect_platforms(),
        })
    }
    
    fn find_best_version(&self, gem_name: &str, constraints: &[String]) -> Result<String> {
        let versions = self.index.get_versions(gem_name)
            .ok_or_else(|| anyhow!("Gem '{}' not found in offline index", gem_name))?;
        
        if versions.is_empty() {
            return Err(anyhow!("No versions found for gem '{}'", gem_name));
        }
        
        // If no constraints, return latest
        if constraints.is_empty() {
            return Ok(versions[0].clone());
        }
        
        // Parse constraints
        let parsed_constraints: Vec<VersionConstraint> = constraints.iter()
            .filter_map(|c| VersionConstraint::parse(c).ok())
            .collect();
        
        // Find first version that satisfies all constraints (versions are sorted descending)
        for ver_str in &versions {
            if let Some(ver) = parse_gem_version(ver_str) {
                let all_match = parsed_constraints.iter().all(|c| c.matches(&ver));
                if all_match {
                    return Ok(ver_str.clone());
                }
            }
        }
        
        Err(anyhow!(
            "No version of '{}' satisfies constraints: {:?}",
            gem_name,
            constraints
        ))
    }
    
    fn version_satisfies_constraints(&self, version: &str, constraints: &[String]) -> bool {
        if constraints.is_empty() {
            return true;
        }
        
        let parsed_constraints: Vec<VersionConstraint> = constraints.iter()
            .filter_map(|c| VersionConstraint::parse(c).ok())
            .collect();
        
        if let Some(ver) = parse_gem_version(version) {
            parsed_constraints.iter().all(|c| c.matches(&ver))
        } else {
            false
        }
    }
}

struct Resolver {
    client: Client,
    // Cache of gem name -> available versions (descending order)
    version_cache: Arc<DashMap<String, Vec<String>>>,
    // Cache of (gem, version) -> spec
    spec_cache: Arc<DashMap<(String, String), GemSpec>>,
    // Set of gems we're currently resolving (for cycle detection)
    resolving: Arc<DashMap<String, ()>>,
    // Locked versions for conservative updates (gem name -> exact version)
    locked_versions: HashMap<String, String>,
    // Track gems we've prefetched (for stats)
    prefetched: Arc<DashMap<String, ()>>,
    // Conflict-based priority inversion (uv #8157, #9843)
    conflict_tracker: std::sync::Mutex<ConflictTracker>,
    priority_manager: std::sync::Mutex<PriorityManager>,
}

impl Resolver {
    fn new(client: Client, locked_versions: HashMap<String, String>) -> Self {
        Self {
            client,
            version_cache: Arc::new(DashMap::new()),
            spec_cache: Arc::new(DashMap::new()),
            resolving: Arc::new(DashMap::new()),
            locked_versions,
            prefetched: Arc::new(DashMap::new()),
            conflict_tracker: std::sync::Mutex::new(ConflictTracker::new()),
            priority_manager: std::sync::Mutex::new(PriorityManager::new()),
        }
    }
    
    /// Speculatively prefetch gems - fire and forget
    fn prefetch_gems(&self, gem_names: &[String]) {
        // Filter out already prefetched gems
        let to_prefetch: Vec<String> = gem_names.iter()
            .filter(|name| !self.prefetched.contains_key(*name))
            .cloned()
            .collect();
        
        if to_prefetch.is_empty() {
            return;
        }
        
        // Mark as prefetched
        for name in &to_prefetch {
            self.prefetched.insert(name.clone(), ());
        }
        
        // Spawn prefetch tasks
        self.client.prefetch_batch(&to_prefetch);
    }
    
    /// Check if a gem was prefetched (for stats tracking)
    fn was_prefetched(&self, gem_name: &str) -> bool {
        self.prefetched.contains_key(gem_name)
    }
    
    async fn resolve(&self, gemfile: &Gemfile) -> Result<Resolution> {
        let mut resolved: HashMap<String, ResolvedGem> = HashMap::new();
        let mut git_gems: Vec<ResolvedGitGem> = Vec::new();
        
        // Separate git and regular gems
        let regular_gems: Vec<&GemDeclaration> = gemfile.gems.iter()
            .filter(|g| !g.is_git_source() && g.path.is_none())
            .collect();
        
        let git_source_gems: Vec<&GemDeclaration> = gemfile.gems.iter()
            .filter(|g| g.is_git_source())
            .collect();
        
        let direct_gems: HashSet<String> = regular_gems.iter()
            .map(|g| g.name.clone())
            .collect();
        
        // Get all direct gem names (regular gems only)
        let gem_names: Vec<&str> = regular_gems.iter()
            .map(|g| g.name.as_str())
            .collect();
        
        // First, resolve git gems (they provide dependencies too)
        if !git_source_gems.is_empty() {
            println!("   🔗 Processing {} git source(s)...", git_source_gems.len());
            let git_cache = GitCache::new()?;
            
            for gem_decl in &git_source_gems {
                let git_url = gem_decl.git_url().unwrap();
                
                let source = GitSource {
                    url: git_url.clone(),
                    branch: gem_decl.branch.clone(),
                    tag: gem_decl.tag.clone(),
                    ref_: gem_decl.ref_.clone(),
                    submodules: gem_decl.submodules,
                };
                
                // Clone/update the repo
                print!("      Cloning {}... ", gem_decl.name);
                let (repo_path, revision) = git_cache.get_or_clone(&source)
                    .with_context(|| format!("Failed to clone {} from {}", gem_decl.name, git_url))?;
                println!("✓ ({})", &revision[..7]);
                
                // Parse gemspec from the cloned repo
                let gemspec = parse_gemspec(&repo_path, &gem_decl.name)
                    .with_context(|| format!("Failed to parse gemspec for {} in cloned repo", gem_decl.name))?;
                
                // Collect runtime dependencies
                let deps: Vec<String> = gemspec.dependencies.iter()
                    .filter(|d| d.is_runtime)
                    .map(|d| d.name.clone())
                    .collect();
                
                git_gems.push(ResolvedGitGem {
                    name: gemspec.name.clone(),
                    version: gemspec.version.clone(),
                    git_url,
                    revision,
                    branch: gem_decl.branch.clone(),
                    tag: gem_decl.tag.clone(),
                    ref_: gem_decl.ref_.clone(),
                    dependencies: deps,
                });
            }
        }
        
        println!("   Resolving {} direct dependencies...", gem_names.len());
        
        // Collect dependencies from git gems to include in resolution
        let mut extra_deps_from_git: Vec<String> = Vec::new();
        for git_gem in &git_gems {
            for dep in &git_gem.dependencies {
                // Don't re-resolve git gems themselves, only their non-git deps
                let is_git_gem = git_source_gems.iter().any(|g| &g.name == dep);
                if !is_git_gem && !gem_names.contains(&dep.as_str()) {
                    extra_deps_from_git.push(dep.clone());
                }
            }
        }
        
        // Combine regular gem names with git gem dependencies
        let mut all_names: Vec<&str> = gem_names.clone();
        for dep in &extra_deps_from_git {
            if !all_names.contains(&dep.as_str()) {
                all_names.push(dep.as_str());
            }
        }
        
        // OPTIMIZATION: Batch fetch all gem specs in one API call
        // This dramatically reduces network roundtrips
        let all_specs = self.client.fetch_deps_batch(&all_names).await?;
        
        // Index specs by (name, version) for fast lookup
        let mut spec_index: HashMap<(String, String), &crate::rubygems::GemSpec> = HashMap::new();
        let mut versions_by_gem: HashMap<String, Vec<String>> = HashMap::new();
        
        for spec in &all_specs {
            spec_index.insert((spec.name.clone(), spec.version.clone()), spec);
            versions_by_gem.entry(spec.name.clone())
                .or_insert_with(Vec::new)
                .push(spec.version.clone());
        }
        
        // Sort versions descending - use parallel sort for large lists
        for versions in versions_by_gem.values_mut() {
            versions.par_sort_by(|a, b| {
                let a_ver = parse_ruby_version(a).ok();
                let b_ver = parse_ruby_version(b).ok();
                b_ver.cmp(&a_ver)
            });
        }
        
        // Create constraints map from gemfile
        let mut constraints_map: HashMap<String, Vec<String>> = gemfile.gems.iter()
            .filter(|g| g.git.is_none() && g.path.is_none())
            .map(|g| (g.name.clone(), g.version_constraints.clone()))
            .collect();
        
        // PRIORITY INVERSION OPTIMIZATION: Register direct gems with priorities
        // Exact versions get highest priority, URL deps next, then regular direct deps
        {
            let mut priority_mgr = self.priority_manager.lock().unwrap();
            for gem in &regular_gems {
                let priority_type = if gem.is_git_source() {
                    GemPriorityType::UrlDependency
                } else {
                    priority_from_constraints(&gem.version_constraints)
                };
                priority_mgr.register(&gem.name, priority_type);
            }
        }
        
        // Track which gems added constraints to which targets (for conflict detection)
        // constraint_sources[target_gem] = list of (source_gem, constraints) that constrain it
        let mut constraint_sources: HashMap<String, Vec<(String, Vec<String>)>> = HashMap::new();
        
        // Initialize sources for direct gems (source is "Gemfile")
        for gem in &regular_gems {
            if !gem.version_constraints.is_empty() {
                constraint_sources.entry(gem.name.clone())
                    .or_default()
                    .push(("Gemfile".to_string(), gem.version_constraints.clone()));
            }
        }
        
        // Resolve with BFS, collecting gems we need to batch-fetch
        // Initial queue is sorted by priority (highest first)
        let mut to_resolve: Vec<String> = gem_names.iter().map(|s| s.to_string()).collect();
        {
            let priority_mgr = self.priority_manager.lock().unwrap();
            priority_mgr.sort_by_priority(&mut to_resolve);
        }
        let mut need_more_specs: Vec<String> = Vec::new();
        
        // Track what's been queued to prevent duplicate additions
        let mut seen_in_queue: HashSet<String> = to_resolve.iter().cloned().collect();
        
        while !to_resolve.is_empty() || !need_more_specs.is_empty() {
            
            // If we need more specs, batch fetch them
            if !need_more_specs.is_empty() {
                // Track which of these were prefetched (for stats)
                let prefetched_count = need_more_specs.iter()
                    .filter(|name| self.was_prefetched(name))
                    .count();
                
                // Get stats before fetch to detect cache hits
                let stats_before = self.client.get_stats().await;
                
                let refs: Vec<&str> = need_more_specs.iter().map(|s| s.as_str()).collect();
                let new_specs = self.client.fetch_deps_batch(&refs).await?;
                
                // Check how many were cache hits (potential prefetch wins)
                let stats_after = self.client.get_stats().await;
                let cache_hits = stats_after.cache_hits - stats_before.cache_hits;
                
                // Record prefetch hits: cache hits for gems we prefetched
                // Conservative: count min of prefetched and cache hits
                let prefetch_hits = std::cmp::min(prefetched_count, cache_hits);
                for _ in 0..prefetch_hits {
                    self.client.record_prefetch_hit().await;
                }
                
                for spec in new_specs {
                    spec_index.insert((spec.name.clone(), spec.version.clone()), 
                        Box::leak(Box::new(spec.clone())));  // Leak for simplicity in prototype
                    versions_by_gem.entry(spec.name.clone())
                        .or_insert_with(Vec::new)
                        .push(spec.version.clone());
                }
                
                // Re-sort versions - use parallel sort
                for versions in versions_by_gem.values_mut() {
                    versions.par_sort_by(|a, b| {
                        let a_ver = parse_ruby_version(a).ok();
                        let b_ver = parse_ruby_version(b).ok();
                        b_ver.cmp(&a_ver)
                    });
                }
                
                to_resolve.extend(need_more_specs.drain(..));
            }
            
            // Process resolution queue
            let current_batch = std::mem::take(&mut to_resolve);
            
            for gem_name in current_batch {
                if resolved.contains_key(&gem_name) {
                    continue;
                }
                
                // CONSERVATIVE UPDATE: If this gem is locked, use exact version
                // But only if the locked version satisfies all constraints from parents
                let version = if let Some(locked_ver) = self.locked_versions.get(&gem_name) {
                    // Get constraints from parents
                    let constraints = constraints_map.get(&gem_name)
                        .cloned()
                        .unwrap_or_default();
                    
                    // Check if locked version satisfies constraints
                    let locked_satisfies = if constraints.is_empty() {
                        true  // No constraints, locked version is fine
                    } else {
                        // Parse and check constraints
                        let parsed_constraints: Vec<VersionConstraint> = constraints.iter()
                            .filter_map(|c| VersionConstraint::parse(c).ok())
                            .collect();
                        
                        if let Some(ver) = parse_gem_version(locked_ver) {
                            parsed_constraints.iter().all(|c| c.matches(&ver))
                        } else {
                            false
                        }
                    };
                    
                    if locked_satisfies {
                        // Verify the locked version exists
                        let versions = match versions_by_gem.get(&gem_name) {
                            Some(v) => v,
                            None => {
                                need_more_specs.push(gem_name.clone());
                                continue;
                            }
                        };
                        
                        if !versions.contains(locked_ver) {
                            return Err(anyhow!(
                                "Locked version {} of '{}' not found in available versions",
                                locked_ver, gem_name
                            ));
                        }
                        locked_ver.clone()
                    } else {
                        // Locked version doesn't satisfy constraints from updated gem
                        // Fall through to normal resolution
                        let versions = match versions_by_gem.get(&gem_name) {
                            Some(v) => v,
                            None => {
                                need_more_specs.push(gem_name.clone());
                                continue;
                            }
                        };
                        self.find_best_from_list(versions, &constraints)?
                    }
                } else {
                    // Normal resolution: find best version matching constraints
                    let constraints = constraints_map.get(&gem_name)
                        .cloned()
                        .unwrap_or_default();
                    
                    // Find available versions
                    let versions = match versions_by_gem.get(&gem_name) {
                        Some(v) => v,
                        None => {
                            need_more_specs.push(gem_name.clone());
                            continue;
                        }
                    };
                    
                    // Find best version matching constraints
                    self.find_best_from_list(versions, &constraints)?
                };
                
                // Get spec for this version
                let spec = match spec_index.get(&(gem_name.clone(), version.clone())) {
                    Some(s) => *s,
                    None => {
                        // Need to fetch this specific version
                        let spec = self.client.fetch_spec(&gem_name, &version).await?;
                        spec_index.insert((gem_name.clone(), version.clone()), 
                            Box::leak(Box::new(spec)));
                        spec_index.get(&(gem_name.clone(), version.clone())).unwrap()
                    }
                };
                
                // Collect dependencies
                let dep_names: Vec<String> = spec.dependencies.iter()
                    .filter(|d| d.dep_type == DependencyType::Runtime)
                    .map(|d| d.name.clone())
                    .collect();
                
                // Queue dependencies for resolution
                // Collect new deps for speculative prefetch
                let mut new_deps: Vec<String> = Vec::new();
                
                for dep in &spec.dependencies {
                    if dep.dep_type != DependencyType::Runtime {
                        continue;
                    }
                    // BUG FIX: Check if already queued OR resolved, not just resolved
                    // Without this, duplicates accumulate exponentially causing slowdowns
                    if !resolved.contains_key(&dep.name) && !seen_in_queue.contains(&dep.name) {
                        seen_in_queue.insert(dep.name.clone());
                        if !versions_by_gem.contains_key(&dep.name) {
                            need_more_specs.push(dep.name.clone());
                            new_deps.push(dep.name.clone());
                        } else {
                            to_resolve.push(dep.name.clone());
                        }
                        
                        // Store constraints from parent, detect conflicts
                        let dep_constraints = parse_requirements(&dep.requirements);
                        
                        // Track constraint source for conflict detection
                        if !dep_constraints.is_empty() {
                            constraint_sources.entry(dep.name.clone())
                                .or_default()
                                .push((gem_name.clone(), dep_constraints.clone()));
                        }
                        
                        let existing = constraints_map.get(&dep.name);
                        
                        if let Some(existing_constraints) = existing {
                            // Check for potentially conflicting constraints
                            if !dep_constraints.is_empty() && !existing_constraints.is_empty() {
                                // Combine constraints from multiple sources
                                let combined_constraints: Vec<String> = existing_constraints.iter()
                                    .chain(dep_constraints.iter())
                                    .cloned()
                                    .collect();
                                
                                // Quick conflict detection: check if combined constraints are satisfiable
                                // by testing against available versions
                                if let Some(versions) = versions_by_gem.get(&dep.name) {
                                    let satisfiable = self.check_constraints_satisfiable(versions, &combined_constraints);
                                    if !satisfiable {
                                        // Record conflict between the sources
                                        // Get previous sources that conflict with current gem
                                        if let Some(sources) = constraint_sources.get(&dep.name) {
                                            for (source_gem, _) in sources.iter() {
                                                if source_gem != &gem_name && source_gem != "Gemfile" {
                                                    let should_invert = self.conflict_tracker.lock().unwrap()
                                                        .record_conflict(source_gem, &gem_name);
                                                    if should_invert {
                                                        // Threshold exceeded - invert priorities
                                                        let mut priority_mgr = self.priority_manager.lock().unwrap();
                                                        priority_mgr.invert(source_gem, &gem_name);
                                                        // Mark both as highly conflicting
                                                        priority_mgr.update_priority(source_gem, GemPriorityType::HighlyConflicting);
                                                        priority_mgr.update_priority(&gem_name, GemPriorityType::HighlyConflicting);
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                                
                                constraints_map.insert(dep.name.clone(), combined_constraints);
                            }
                        } else {
                            constraints_map.insert(dep.name.clone(), dep_constraints.clone());
                        }
                        
                        // Register transitive dependencies with appropriate priority
                        {
                            let mut priority_mgr = self.priority_manager.lock().unwrap();
                            let priority_type = priority_from_constraints(&dep_constraints);
                            // Transitive deps get lower base priority unless they have exact versions
                            let final_priority = if priority_type == GemPriorityType::ExactVersion {
                                priority_type
                            } else {
                                GemPriorityType::Transitive
                            };
                            priority_mgr.register(&dep.name, final_priority);
                        }
                    }
                }
                
                // SPECULATIVE PREFETCH: Fire off requests for deps we'll need
                // Don't await - let them run in background while we process
                if !new_deps.is_empty() {
                    self.prefetch_gems(&new_deps);
                }
                
                // Re-sort resolution queue by priority after adding new items
                {
                    let priority_mgr = self.priority_manager.lock().unwrap();
                    priority_mgr.sort_by_priority(&mut to_resolve);
                }
                
                resolved.insert(gem_name.clone(), ResolvedGem {
                    name: gem_name.clone(),
                    version,
                    dependencies: dep_names,
                    sha256: spec.sha256.clone(),
                    is_direct: direct_gems.contains(&gem_name),
                });
            }
        }
        
        // Sort gems alphabetically for consistent output
        let mut gems: Vec<ResolvedGem> = resolved.into_values().collect();
        gems.sort_by(|a, b| a.name.cmp(&b.name));
        
        // Sort git gems alphabetically too
        git_gems.sort_by(|a, b| a.name.cmp(&b.name));
        
        Ok(Resolution {
            gems,
            git_gems,
            source: gemfile.source.clone(),
            ruby_version: gemfile.ruby_version.clone(),
            platforms: detect_platforms(),
        })
    }
    
    fn find_best_from_list(&self, versions: &[String], constraints: &[String]) -> Result<String> {
        if versions.is_empty() {
            return Err(anyhow!("No versions available"));
        }
        
        // If no constraints, return latest
        if constraints.is_empty() {
            return Ok(versions[0].clone());
        }
        
        // Parse constraints
        let parsed_constraints: Vec<VersionConstraint> = constraints.iter()
            .filter_map(|c| VersionConstraint::parse(c).ok())
            .collect();
        
        // Profile version matching if enabled
        let start = if profiling_enabled() { Some(Instant::now()) } else { None };
        let mut checked = 0usize;
        
        // Find first version that satisfies all constraints
        for ver_str in versions {
            if let Some(ver) = parse_gem_version(ver_str) {
                checked += 1;
                let all_match = parsed_constraints.iter().all(|c| c.matches(&ver));
                if all_match {
                    // Record stats before returning
                    if let Some(s) = start {
                        VERSION_MATCH_STATS.record_batch(s.elapsed().as_nanos() as u64, checked);
                    }
                    return Ok(ver_str.clone());
                }
            }
        }
        
        // Record stats even on fallback
        if let Some(s) = start {
            VERSION_MATCH_STATS.record_batch(s.elapsed().as_nanos() as u64, checked);
        }
        
        // No match, just use latest
        Ok(versions[0].clone())
    }
    
    /// Check if any version in the list satisfies all constraints.
    /// Used for early conflict detection.
    fn check_constraints_satisfiable(&self, versions: &[String], constraints: &[String]) -> bool {
        if versions.is_empty() {
            return false;
        }
        
        if constraints.is_empty() {
            return true;
        }
        
        // Parse constraints
        let parsed_constraints: Vec<VersionConstraint> = constraints.iter()
            .filter_map(|c| VersionConstraint::parse(c).ok())
            .collect();
        
        if parsed_constraints.is_empty() {
            return true;
        }
        
        // Check if any version satisfies all constraints
        for ver_str in versions {
            if let Some(ver) = parse_gem_version(ver_str) {
                let all_match = parsed_constraints.iter().all(|c| c.matches(&ver));
                if all_match {
                    return true;
                }
            }
        }
        
        false
    }
    
    async fn find_best_version(&self, gem_name: &str, constraints: &[String]) -> Result<String> {
        // Fetch all available versions
        let versions = self.fetch_versions(gem_name).await?;
        
        if versions.is_empty() {
            return Err(anyhow!("No versions found for gem '{}'", gem_name));
        }
        
        // If no constraints, return latest
        if constraints.is_empty() {
            return Ok(versions[0].clone());
        }
        
        // Parse constraints
        let parsed_constraints: Vec<VersionConstraint> = constraints.iter()
            .filter_map(|c| VersionConstraint::parse(c).ok())
            .collect();
        
        // Find first version that satisfies all constraints (versions are sorted descending)
        for ver_str in &versions {
            if let Some(ver) = parse_gem_version(ver_str) {
                let all_match = parsed_constraints.iter().all(|c| c.matches(&ver));
                if all_match {
                    return Ok(ver_str.clone());
                }
            }
        }
        
        Err(anyhow!(
            "No version of '{}' satisfies constraints: {:?}",
            gem_name,
            constraints
        ))
    }
    
    async fn fetch_versions(&self, gem_name: &str) -> Result<Vec<String>> {
        // Check cache
        if let Some(versions) = self.version_cache.get(gem_name) {
            return Ok(versions.clone());
        }
        
        let versions = self.client.fetch_versions(gem_name).await?;
        
        // Sort descending (newest first) for resolution - parallel sort
        let mut sorted = versions;
        sorted.par_sort_by(|a, b| {
            let a_ver = parse_gem_version(a);
            let b_ver = parse_gem_version(b);
            b_ver.cmp(&a_ver)
        });
        
        self.version_cache.insert(gem_name.to_string(), sorted.clone());
        Ok(sorted)
    }
    
    async fn fetch_spec(&self, gem_name: &str, version: &str) -> Result<GemSpec> {
        let key = (gem_name.to_string(), version.to_string());
        
        // Check cache
        if let Some(spec) = self.spec_cache.get(&key) {
            return Ok(spec.clone());
        }
        
        let spec = self.client.fetch_spec(gem_name, version).await?;
        self.spec_cache.insert(key, spec.clone());
        Ok(spec)
    }
}

/// Parse Ruby-style requirement string into constraints
fn parse_requirements(req: &str) -> Vec<String> {
    req.split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_version_constraint_exact() {
        let c = VersionConstraint::parse("1.2.3").unwrap();
        assert!(c.matches(&semver::Version::new(1, 2, 3)));
        assert!(!c.matches(&semver::Version::new(1, 2, 4)));
    }
    
    #[test]
    fn test_version_constraint_gte() {
        let c = VersionConstraint::parse(">= 1.2.0").unwrap();
        assert!(c.matches(&semver::Version::new(1, 2, 0)));
        assert!(c.matches(&semver::Version::new(1, 3, 0)));
        assert!(!c.matches(&semver::Version::new(1, 1, 0)));
    }
    
    #[test]
    fn test_version_constraint_pessimistic() {
        let c = VersionConstraint::parse("~> 1.2").unwrap();
        assert!(c.matches(&semver::Version::new(1, 2, 0)));
        assert!(c.matches(&semver::Version::new(1, 9, 0)));
        assert!(!c.matches(&semver::Version::new(2, 0, 0)));
        
        let c2 = VersionConstraint::parse("~> 1.2.3").unwrap();
        assert!(c2.matches(&semver::Version::new(1, 2, 3)));
        assert!(c2.matches(&semver::Version::new(1, 2, 9)));
        assert!(!c2.matches(&semver::Version::new(1, 3, 0)));
    }
    
    #[test]
    fn test_detect_platforms() {
        let platforms = detect_platforms();
        
        // Should always have at least one platform
        assert!(!platforms.is_empty());
        
        // Should always include "ruby" as fallback (last entry)
        assert!(platforms.contains(&"ruby".to_string()));
        
        // The specific platform should come before "ruby"
        if platforms.len() > 1 {
            let ruby_pos = platforms.iter().position(|p| p == "ruby").unwrap();
            assert_eq!(ruby_pos, platforms.len() - 1, "ruby should be last");
        }
        
        // First platform should be a recognized format
        let first = &platforms[0];
        let valid_patterns = [
            "x86_64-darwin", "arm64-darwin",
            "x86_64-linux", "aarch64-linux", "arm-linux",
            "x64-mingw-ucrt", "x86-mingw32",
            "x86_64-freebsd", "aarch64-freebsd",
            "ruby"
        ];
        assert!(
            valid_patterns.iter().any(|p| first.starts_with(p) || first == p),
            "Unexpected platform: {}", first
        );
    }
    
    #[test]
    fn test_matches_batch() {
        let constraint = VersionConstraint::parse(">= 1.2.0").unwrap();
        let versions = vec![
            semver::Version::new(1, 0, 0),
            semver::Version::new(1, 2, 0),
            semver::Version::new(1, 5, 0),
            semver::Version::new(2, 0, 0),
            semver::Version::new(0, 9, 0),
        ];
        
        let results = constraint.matches_batch(&versions);
        assert_eq!(results, vec![false, true, true, true, false]);
    }
    
    #[test]
    fn test_matches_batch_pessimistic() {
        let constraint = VersionConstraint::parse("~> 1.2.0").unwrap();
        let versions = vec![
            semver::Version::new(1, 2, 0),
            semver::Version::new(1, 2, 5),
            semver::Version::new(1, 3, 0),  // Should NOT match
            semver::Version::new(1, 1, 0),  // Should NOT match
        ];
        
        let results = constraint.matches_batch(&versions);
        assert_eq!(results, vec![true, true, false, false]);
    }
    
    #[test]
    fn test_filter_matching() {
        let constraint = VersionConstraint::parse("< 2.0.0").unwrap();
        let versions = vec![
            semver::Version::new(1, 0, 0),
            semver::Version::new(1, 5, 0),
            semver::Version::new(2, 0, 0),
            semver::Version::new(3, 0, 0),
        ];
        
        let matching = constraint.filter_matching(&versions);
        assert_eq!(matching.len(), 2);
        assert_eq!(matching[0], &semver::Version::new(1, 0, 0));
        assert_eq!(matching[1], &semver::Version::new(1, 5, 0));
    }
    
    #[test]
    fn test_matches_batch_empty() {
        let constraint = VersionConstraint::parse("= 1.0.0").unwrap();
        let results = constraint.matches_batch(&[]);
        assert!(results.is_empty());
    }
}
