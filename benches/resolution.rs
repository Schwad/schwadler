//! Benchmarks for Schwadler resolution performance
//! 
//! Run with: cargo bench
//! 
//! Benchmarks cover:
//! - Gemfile parsing
//! - Version constraint matching
//! - Cache operations
//! - Full resolution (with network mocking where possible)

use criterion::{criterion_group, criterion_main, Criterion, BenchmarkId, black_box};
use schwadler::{gemfile, VersionConstraint, PersistentCache};

const SIMPLE_GEMFILE: &str = include_str!("fixtures/simple.gemfile");
const MEDIUM_GEMFILE: &str = include_str!("fixtures/medium.gemfile");
const RAILS_GEMFILE: &str = include_str!("fixtures/rails.gemfile");
const MASSIVE_GEMFILE: &str = include_str!("fixtures/massive.gemfile");

// =============================================================================
// Gemfile Parsing Benchmarks
// =============================================================================

fn bench_gemfile_parsing(c: &mut Criterion) {
    let mut group = c.benchmark_group("gemfile_parsing");
    
    group.bench_function("simple_3_gems", |b| {
        b.iter(|| {
            black_box(gemfile::parse(SIMPLE_GEMFILE).unwrap())
        })
    });
    
    group.bench_function("medium_15_gems", |b| {
        b.iter(|| {
            black_box(gemfile::parse(MEDIUM_GEMFILE).unwrap())
        })
    });
    
    group.bench_function("rails_50_gems", |b| {
        b.iter(|| {
            black_box(gemfile::parse(RAILS_GEMFILE).unwrap())
        })
    });
    
    group.bench_function("massive_100_gems", |b| {
        b.iter(|| {
            black_box(gemfile::parse(MASSIVE_GEMFILE).unwrap())
        })
    });
    
    group.finish();
}

// =============================================================================
// Version Constraint Matching Benchmarks
// =============================================================================

fn bench_version_matching(c: &mut Criterion) {
    let mut group = c.benchmark_group("version_matching");
    
    // Generate test versions
    let versions: Vec<semver::Version> = (0..100)
        .flat_map(|major| {
            (0..10).map(move |minor| semver::Version::new(major, minor, 0))
        })
        .collect();
    
    // Common constraint patterns
    let constraints = vec![
        ("exact", "= 5.0.0"),
        ("gte", ">= 3.0"),
        ("pessimistic_major", "~> 5.0"),
        ("pessimistic_minor", "~> 5.2.0"),
        ("lt", "< 10.0"),
        ("range", ">= 3.0"),  // One half of a range constraint
    ];
    
    for (name, constraint_str) in constraints {
        let constraint = VersionConstraint::parse(constraint_str).unwrap();
        
        group.bench_with_input(
            BenchmarkId::new("single", name),
            &constraint,
            |b, c| {
                let test_version = semver::Version::new(5, 2, 3);
                b.iter(|| {
                    black_box(c.matches(&test_version))
                })
            },
        );
        
        group.bench_with_input(
            BenchmarkId::new("batch_1000", name),
            &constraint,
            |b, c| {
                b.iter(|| {
                    black_box(c.matches_batch(&versions))
                })
            },
        );
    }
    
    group.finish();
}

fn bench_constraint_parsing(c: &mut Criterion) {
    let mut group = c.benchmark_group("constraint_parsing");
    
    let constraint_strings = vec![
        "= 1.0.0",
        ">= 2.3.4",
        "~> 5.2",
        "~> 5.2.3",
        "< 10.0",
        "!= 3.0.0",
        "3.2.0",  // Bare version
    ];
    
    for constraint_str in constraint_strings {
        group.bench_with_input(
            BenchmarkId::from_parameter(constraint_str),
            constraint_str,
            |b, s| {
                b.iter(|| {
                    black_box(VersionConstraint::parse(s).unwrap())
                })
            },
        );
    }
    
    group.finish();
}

// =============================================================================
// Cache Operation Benchmarks
// =============================================================================

fn bench_cache_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("cache_operations");
    
    // Create a temporary cache directory
    let temp_dir = tempfile::tempdir().unwrap();
    
    group.bench_function("cache_init", |b| {
        b.iter(|| {
            let cache_dir = temp_dir.path().join(format!("cache_{}", rand_suffix()));
            black_box(PersistentCache::with_path(cache_dir).unwrap())
        })
    });
    
    // Pre-populate a cache for read benchmarks
    let populated_cache_dir = temp_dir.path().join("populated");
    {
        let mut cache = PersistentCache::with_path(populated_cache_dir.clone()).unwrap();
        // Add some test entries
        for i in 0..100 {
            let key = format!("test_gem_{}", i);
            let data = format!("version: {}.0.0\ndependencies: []", i);
            cache.put_gem_info(&key, &data, None, None).unwrap();
        }
    }
    
    group.bench_function("cache_get_hit", |b| {
        let cache = PersistentCache::with_path(populated_cache_dir.clone()).unwrap();
        b.iter(|| {
            black_box(cache.get_gem_info("test_gem_50"))
        })
    });
    
    group.bench_function("cache_get_miss", |b| {
        let cache = PersistentCache::with_path(populated_cache_dir.clone()).unwrap();
        b.iter(|| {
            black_box(cache.get_gem_info("nonexistent_gem"))
        })
    });
    
    group.bench_function("cache_stats", |b| {
        let cache = PersistentCache::with_path(populated_cache_dir.clone()).unwrap();
        b.iter(|| {
            black_box(cache.stats())
        })
    });
    
    group.finish();
}

// Helper to generate random suffix for unique cache dirs
fn rand_suffix() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
}

// =============================================================================
// Resolution Benchmarks (Synthetic - no network)
// =============================================================================

fn bench_resolution_parsing_only(c: &mut Criterion) {
    let mut group = c.benchmark_group("resolution_parse_phase");
    
    // Measure the parse + prepare phase (before network calls)
    let gemfiles = vec![
        ("simple", SIMPLE_GEMFILE),
        ("medium", MEDIUM_GEMFILE),
        ("rails", RAILS_GEMFILE),
        ("massive", MASSIVE_GEMFILE),
    ];
    
    for (name, content) in gemfiles {
        group.bench_with_input(
            BenchmarkId::from_parameter(name),
            content,
            |b, gemfile_content| {
                b.iter(|| {
                    // Parse Gemfile
                    let parsed = gemfile::parse(gemfile_content).unwrap();
                    
                    // Count dependencies (simulates prep work)
                    let dep_count = parsed.gems.len();
                    let constraint_count: usize = parsed.gems.iter()
                        .map(|g| g.version_constraints.len())
                        .sum();
                    
                    black_box((dep_count, constraint_count))
                })
            },
        );
    }
    
    group.finish();
}

// =============================================================================
// Throughput Benchmarks
// =============================================================================

fn bench_parsing_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("throughput");
    group.throughput(criterion::Throughput::Bytes(RAILS_GEMFILE.len() as u64));
    
    group.bench_function("rails_gemfile_parse", |b| {
        b.iter(|| {
            black_box(gemfile::parse(RAILS_GEMFILE).unwrap())
        })
    });
    
    group.finish();
}

criterion_group!(
    benches,
    bench_gemfile_parsing,
    bench_version_matching,
    bench_constraint_parsing,
    bench_cache_operations,
    bench_resolution_parsing_only,
    bench_parsing_throughput,
);

criterion_main!(benches);
