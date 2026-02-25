# Schwadler Speed Tracker

Tracking performance metrics and optimization progress.

## Baseline Metrics (2026-02-22)

Machine: Apple M1 Pro MacBook Pro (2021)  
Rust: 1.75+  
Build: Release with LTO

### Gemfile Parsing

| Gemfile Size | Time | Throughput |
|-------------|------|------------|
| Simple (3 gems) | ~10.7 µs | ~12.5 KB/s |
| Medium (15 gems) | ~56.8 µs | ~8.1 KB/s |
| Rails (50 gems) | ~190.8 µs | ~8.1 KB/s |
| Massive (100+ gems) | ~542.6 µs | ~6.4 KB/s |

**Observation:** Linear scaling with Gemfile size. Good performance.

### Version Constraint Matching

| Operation | Time |
|-----------|------|
| Single match (exact) | ~2.9 ns |
| Single match (>=) | ~3.2 ns |
| Single match (~>) | ~3.5 ns |
| Batch 1000 versions | ~2.8 µs |

**Observation:** Extremely fast. Version matching is NOT a bottleneck.

### Cache Operations

| Operation | Time |
|-----------|------|
| Cache init (new dir) | TBD |
| Cache get (hit) | TBD |
| Cache get (miss) | TBD |
| Cache stats | TBD |

### Full Resolution (End-to-End)

| Gemfile | Cold Cache | Warm Cache |
|---------|------------|------------|
| Simple (3 gems) | TBD | TBD |
| Medium (15 gems) | TBD | TBD |
| Rails (50 gems) | TBD | TBD |
| Massive (100+ gems) | TBD | TBD |

### Bundler Comparison

| Operation | Bundler | Schwadler | Speedup |
|-----------|---------|-----------|---------|
| Fresh lock (Rails) | TBD | TBD | TBD |
| Update single gem | TBD | TBD | TBD |
| Install (50 gems) | TBD | TBD | TBD |

## Performance Bottlenecks

Based on profiling:

1. **Network Latency** - Primary bottleneck. Mitigated by:
   - Persistent caching
   - Parallel fetching
   - HTTP/2 connection reuse

2. **PubGrub Solver** - Secondary bottleneck for complex trees. Mitigated by:
   - Conflict-driven learning
   - Version preference heuristics
   - Early termination

3. **Git Operations** - Tertiary bottleneck. Mitigated by:
   - Git SHA caching
   - Sparse checkouts

## Optimization Log

### 2026-02-22: Initial Baseline

- Set up criterion benchmarks
- Established baseline metrics for parsing and version matching
- Created profiling infrastructure

### Future Work

- [ ] SIMD-accelerated version batch matching
- [ ] Memory-mapped cache for faster reads
- [ ] Incremental resolution with lockfile delta
- [ ] Pre-warmed cache distribution

## Running Benchmarks

```bash
# Full benchmark suite
cargo bench

# Specific benchmark
cargo bench gemfile_parsing

# With HTML reports
cargo bench -- --save-baseline my_baseline
```

## Profiling

```bash
# Generate flamegraph (requires sudo on macOS)
./scripts/profile.sh benches/fixtures/rails.gemfile

# Enable timing instrumentation
SCHWADLER_TIMING=1 ./target/release/schwadl lock -g Gemfile
```

## Benchmark Artifacts

HTML reports are generated in `target/criterion/`.

Flamegraphs are saved to `target/profiles/`.
