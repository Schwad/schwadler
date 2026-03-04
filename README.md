# Schwadler 🦀

A faster gem dependency resolver and installer, written in Rust.

## Benchmarks

Tested against [Discourse](https://github.com/discourse/discourse) (301 gems):

| Operation | Schwadler | Bundler | Speedup |
|-----------|-----------|---------|---------|
| Lock | 1.04s | 5.85s | **5.6x** |
| Install | 52.67s | ~90s | **1.7x** |

## Installation

```bash
# Install from GitHub (requires Rust/Cargo)
cargo install --git https://github.com/Schwad/schwadler

# Or clone and build locally
git clone https://github.com/Schwad/schwadler
cd schwadler
cargo install --path .
```

## Usage

```bash
# Resolve dependencies and generate Gemfile.lock
schwadl lock

# Install gems from Gemfile.lock
schwadl install
```

## How It Works

Schwadler generates Bundler-compatible lockfiles, so you can use `bundle exec` as normal after installing with Schwadler.

### Key Optimizations

- **Parallel dependency fetching** — Fetches gem metadata concurrently
- **Efficient caching** — Caches RubyGems API responses in `~/.schwadler/cache`
- **Smart prefetching** — Predicts and prefetches likely dependencies
- **Rust performance** — Zero-cost abstractions and memory safety

## Status

🚧 **Alpha** — Works on many real-world Gemfiles but may have edge cases. Use alongside Bundler for verification.

## License

MIT

---

Built by [SchwadLabs](https://schwadlabs.io)
