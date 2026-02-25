//! Schwadler - A faster Bundler, written in Rust
//! 
//! This library provides dependency resolution for Ruby gems using
//! the PubGrub algorithm, with persistent caching and parallel fetching.

pub mod cache;
pub mod full_index;
pub mod gemfile;
pub mod gemspec;
pub mod git;
pub mod incremental;
pub mod index;
pub mod installer;
pub mod lockfile;
pub mod resolver;
pub mod rubygems;
pub mod timing;

// Re-export main types for convenience
pub use cache::PersistentCache;
pub use full_index::{FullIndex, download_full_index, update_index};
pub use gemfile::{Gemfile, GemDeclaration};
pub use resolver::{Resolution, ResolvedGem, VersionConstraint, resolve_offline};
pub use rubygems::Client;
pub use timing::{TimingStats, TIMING};
