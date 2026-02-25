//! Timing instrumentation for Schwadler
//!
//! Provides lightweight timing for key operations.
//! Enable detailed timing with SCHWADLER_TIMING=1 environment variable.

use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};
use std::time::{Duration, Instant};

/// Global timing statistics
pub struct TimingStats {
    /// Total resolution time
    pub resolution_total_ns: AtomicU64,
    /// Time spent on network fetches
    pub network_fetch_ns: AtomicU64,
    /// Time spent on cache lookups
    pub cache_lookup_ns: AtomicU64,
    /// Time spent on version matching
    pub version_matching_ns: AtomicU64,
    /// Time spent parsing Gemfile
    pub gemfile_parse_ns: AtomicU64,
    /// Time spent parsing gemspecs
    pub gemspec_parse_ns: AtomicU64,
    /// Time spent in PubGrub solver
    pub solver_ns: AtomicU64,
    /// Time spent on git operations
    pub git_ops_ns: AtomicU64,
    /// Number of network fetches
    pub network_fetch_count: AtomicU64,
    /// Number of cache hits
    pub cache_hits: AtomicU64,
    /// Number of cache misses
    pub cache_misses: AtomicU64,
    /// Number of version comparisons
    pub version_comparisons: AtomicU64,
}

impl TimingStats {
    pub const fn new() -> Self {
        Self {
            resolution_total_ns: AtomicU64::new(0),
            network_fetch_ns: AtomicU64::new(0),
            cache_lookup_ns: AtomicU64::new(0),
            version_matching_ns: AtomicU64::new(0),
            gemfile_parse_ns: AtomicU64::new(0),
            gemspec_parse_ns: AtomicU64::new(0),
            solver_ns: AtomicU64::new(0),
            git_ops_ns: AtomicU64::new(0),
            network_fetch_count: AtomicU64::new(0),
            cache_hits: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
            version_comparisons: AtomicU64::new(0),
        }
    }
    
    pub fn reset(&self) {
        self.resolution_total_ns.store(0, Ordering::Relaxed);
        self.network_fetch_ns.store(0, Ordering::Relaxed);
        self.cache_lookup_ns.store(0, Ordering::Relaxed);
        self.version_matching_ns.store(0, Ordering::Relaxed);
        self.gemfile_parse_ns.store(0, Ordering::Relaxed);
        self.gemspec_parse_ns.store(0, Ordering::Relaxed);
        self.solver_ns.store(0, Ordering::Relaxed);
        self.git_ops_ns.store(0, Ordering::Relaxed);
        self.network_fetch_count.store(0, Ordering::Relaxed);
        self.cache_hits.store(0, Ordering::Relaxed);
        self.cache_misses.store(0, Ordering::Relaxed);
        self.version_comparisons.store(0, Ordering::Relaxed);
    }
    
    pub fn add_network_fetch(&self, duration: Duration) {
        self.network_fetch_ns.fetch_add(duration.as_nanos() as u64, Ordering::Relaxed);
        self.network_fetch_count.fetch_add(1, Ordering::Relaxed);
    }
    
    pub fn add_cache_lookup(&self, duration: Duration, hit: bool) {
        self.cache_lookup_ns.fetch_add(duration.as_nanos() as u64, Ordering::Relaxed);
        if hit {
            self.cache_hits.fetch_add(1, Ordering::Relaxed);
        } else {
            self.cache_misses.fetch_add(1, Ordering::Relaxed);
        }
    }
    
    pub fn add_version_matching(&self, duration: Duration, comparisons: u64) {
        self.version_matching_ns.fetch_add(duration.as_nanos() as u64, Ordering::Relaxed);
        self.version_comparisons.fetch_add(comparisons, Ordering::Relaxed);
    }
    
    pub fn add_gemfile_parse(&self, duration: Duration) {
        self.gemfile_parse_ns.fetch_add(duration.as_nanos() as u64, Ordering::Relaxed);
    }
    
    pub fn add_solver(&self, duration: Duration) {
        self.solver_ns.fetch_add(duration.as_nanos() as u64, Ordering::Relaxed);
    }
    
    pub fn add_git_ops(&self, duration: Duration) {
        self.git_ops_ns.fetch_add(duration.as_nanos() as u64, Ordering::Relaxed);
    }
    
    pub fn set_total(&self, duration: Duration) {
        self.resolution_total_ns.store(duration.as_nanos() as u64, Ordering::Relaxed);
    }
    
    /// Print timing summary to stderr
    pub fn print_summary(&self) {
        if !timing_enabled() {
            return;
        }
        
        let total = Duration::from_nanos(self.resolution_total_ns.load(Ordering::Relaxed));
        let network = Duration::from_nanos(self.network_fetch_ns.load(Ordering::Relaxed));
        let cache = Duration::from_nanos(self.cache_lookup_ns.load(Ordering::Relaxed));
        let version = Duration::from_nanos(self.version_matching_ns.load(Ordering::Relaxed));
        let gemfile = Duration::from_nanos(self.gemfile_parse_ns.load(Ordering::Relaxed));
        let solver = Duration::from_nanos(self.solver_ns.load(Ordering::Relaxed));
        let git = Duration::from_nanos(self.git_ops_ns.load(Ordering::Relaxed));
        
        let net_count = self.network_fetch_count.load(Ordering::Relaxed);
        let cache_hits = self.cache_hits.load(Ordering::Relaxed);
        let cache_misses = self.cache_misses.load(Ordering::Relaxed);
        let ver_comps = self.version_comparisons.load(Ordering::Relaxed);
        
        eprintln!("\n📊 Timing Breakdown:");
        eprintln!("   Total:           {:>10.2?}", total);
        eprintln!("   ├─ Gemfile parse:{:>10.2?}", gemfile);
        eprintln!("   ├─ Network fetch:{:>10.2?} ({} requests)", network, net_count);
        eprintln!("   ├─ Cache lookup: {:>10.2?} ({} hits, {} misses)", cache, cache_hits, cache_misses);
        eprintln!("   ├─ Version match:{:>10.2?} ({} comparisons)", version, ver_comps);
        eprintln!("   ├─ PubGrub solve:{:>10.2?}", solver);
        eprintln!("   └─ Git ops:      {:>10.2?}", git);
        
        // Calculate overhead (unaccounted time)
        let accounted = network + cache + version + gemfile + solver + git;
        if total > accounted {
            let overhead = total - accounted;
            let overhead_pct = (overhead.as_nanos() as f64 / total.as_nanos() as f64) * 100.0;
            eprintln!("   Overhead:        {:>10.2?} ({:.1}%)", overhead, overhead_pct);
        }
    }
}

/// Global timing stats instance
pub static TIMING: TimingStats = TimingStats::new();

/// Check if detailed timing is enabled via environment
pub fn timing_enabled() -> bool {
    static ENABLED: AtomicBool = AtomicBool::new(false);
    static CHECKED: AtomicBool = AtomicBool::new(false);
    
    if !CHECKED.load(Ordering::Relaxed) {
        let enabled = std::env::var("SCHWADLER_TIMING")
            .map(|v| v == "1" || v.to_lowercase() == "true")
            .unwrap_or(false);
        ENABLED.store(enabled, Ordering::Relaxed);
        CHECKED.store(true, Ordering::Relaxed);
    }
    
    ENABLED.load(Ordering::Relaxed)
}

/// RAII guard for timing a scope
pub struct TimingGuard {
    start: Instant,
    category: TimingCategory,
}

#[derive(Clone, Copy)]
pub enum TimingCategory {
    NetworkFetch,
    CacheLookup,
    VersionMatching,
    GemfileParse,
    Solver,
    GitOps,
}

impl TimingGuard {
    pub fn new(category: TimingCategory) -> Self {
        Self {
            start: Instant::now(),
            category,
        }
    }
}

impl Drop for TimingGuard {
    fn drop(&mut self) {
        if !timing_enabled() {
            return;
        }
        
        let elapsed = self.start.elapsed();
        match self.category {
            TimingCategory::NetworkFetch => TIMING.add_network_fetch(elapsed),
            TimingCategory::CacheLookup => TIMING.add_cache_lookup(elapsed, false), // Hit/miss determined elsewhere
            TimingCategory::VersionMatching => TIMING.add_version_matching(elapsed, 1),
            TimingCategory::GemfileParse => TIMING.add_gemfile_parse(elapsed),
            TimingCategory::Solver => TIMING.add_solver(elapsed),
            TimingCategory::GitOps => TIMING.add_git_ops(elapsed),
        }
    }
}

/// Macro for timing a block
#[macro_export]
macro_rules! time_block {
    ($category:expr, $block:expr) => {{
        let _guard = $crate::timing::TimingGuard::new($category);
        $block
    }};
}
