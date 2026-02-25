use clap::{Parser, Subcommand};
use std::collections::HashSet;
use std::path::PathBuf;

mod cache;
mod full_index;
mod gemfile;
mod gemspec;
mod git;
mod incremental;
mod index;
mod installer;
mod lockfile;
mod resolver;
mod rubygems;

use anyhow::Result;

#[derive(Parser)]
#[command(name = "schwadl")]
#[command(about = "A faster Bundler, written in Rust 🦀")]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Resolve dependencies and generate Gemfile.lock (fresh resolution)
    Lock {
        /// Path to Gemfile (default: ./Gemfile)
        #[arg(short, long, default_value = "Gemfile")]
        gemfile: PathBuf,
        
        /// Number of parallel fetches
        #[arg(short, long, default_value = "16")]
        jobs: usize,
        
        /// Use offline mode (require pre-downloaded index)
        #[arg(long)]
        offline: bool,
    },
    
    /// Update gems (like bundle update)
    Update {
        /// Specific gems to update (conservative mode)
        /// If no gems specified, updates all (like bundle update --all)
        gems: Vec<String>,
        
        /// Path to Gemfile (default: ./Gemfile)
        #[arg(short, long, default_value = "Gemfile")]
        gemfile: PathBuf,
        
        /// Path to existing Gemfile.lock (default: ./Gemfile.lock)
        #[arg(short, long, default_value = "Gemfile.lock")]
        lockfile: PathBuf,
        
        /// Number of parallel fetches
        #[arg(short, long, default_value = "16")]
        jobs: usize,
    },
    
    /// Install gems from Gemfile.lock
    Install {
        /// Path to Gemfile.lock (default: ./Gemfile.lock)
        #[arg(short, long, default_value = "Gemfile.lock")]
        lockfile: PathBuf,
        
        /// Install path (default: vendor/bundle)
        #[arg(short, long, default_value = "vendor/bundle")]
        path: PathBuf,
        
        /// Number of parallel downloads
        #[arg(short, long, default_value = "16")]
        jobs: usize,
    },
    
    /// Show dependency tree
    List {
        /// Show all transitive dependencies
        #[arg(short, long)]
        all: bool,
    },
    
    /// Check for outdated gems
    Outdated,
    
    /// Manage the persistent cache
    Cache {
        #[command(subcommand)]
        action: CacheAction,
    },
    
    /// Manage the mmap index for fast startup
    Index {
        #[command(subcommand)]
        action: IndexAction,
    },
}

#[derive(Subcommand)]
enum IndexAction {
    /// Download the complete RubyGems index for offline resolution
    Download {
        /// Number of parallel downloads
        #[arg(short, long, default_value = "32")]
        jobs: usize,
    },
    /// Update the index (incremental - only fetch changes)
    Update {
        /// Number of parallel downloads
        #[arg(short, long, default_value = "32")]
        jobs: usize,
    },
    /// Build a local index from cached gem data (for gems already fetched)
    Build,
    /// Show index statistics
    Stats,
}

#[derive(Subcommand)]
enum CacheAction {
    /// Show cache statistics
    Stats,
    /// Clear all cached data
    Clear,
    /// Remove expired entries
    Prune,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    
    match cli.command {
        Commands::Lock { gemfile, jobs, offline } => {
            if offline {
                println!("🔒 schwadl lock --offline - resolving dependencies (no network)...");
            } else {
                println!("🔒 schwadl lock - resolving dependencies (fresh)...");
            }
            println!("   Gemfile: {:?}", gemfile);
            println!("   Parallelism: {} jobs", jobs);
            
            let start = std::time::Instant::now();
            
            // Parse Gemfile with gemspec directory context
            let gemfile_content = std::fs::read_to_string(&gemfile)?;
            // Get the canonical path to properly determine the directory
            let gemfile_canonical = gemfile.canonicalize().unwrap_or_else(|_| gemfile.clone());
            let gemfile_dir = gemfile_canonical.parent().unwrap_or(std::path::Path::new("."));
            let parsed = gemfile::parse_with_gemspec_dir(&gemfile_content, Some(gemfile_dir))?;
            
            if parsed.has_gemspec {
                println!("   📦 Found gemspec directive - loading dependencies from .gemspec");
            }
            println!("   Found {} direct dependencies", parsed.gems.len());
            
            // Check for offline mode
            if offline {
                // Resolve using the pre-downloaded full index
                if !full_index::FullIndex::exists() {
                    anyhow::bail!(
                        "Offline mode requires a pre-downloaded index.\n\
                         Run `schwadl index download` first to download the full RubyGems index."
                    );
                }
                
                println!("   📦 Using offline index...");
                let resolution = resolver::resolve_offline(&parsed, None)?;
                
                // Write lockfile
                lockfile::write(&resolution, "Gemfile.lock")?;
                
                let elapsed = start.elapsed();
                println!("✅ Lock completed in {:.2?} (offline)", elapsed);
            } else {
                // Online resolution - check if we have a full index to use
                let use_full_index = full_index::FullIndex::exists();
                
                if use_full_index {
                    println!("   📦 Using pre-downloaded index (run with --offline to skip network validation)");
                    let resolution = resolver::resolve_offline(&parsed, None)?;
                    
                    // Write lockfile
                    lockfile::write(&resolution, "Gemfile.lock")?;
                    
                    let elapsed = start.elapsed();
                    println!("✅ Lock completed in {:.2?}", elapsed);
                } else {
                    // Fetch specs and resolve (fresh - no locked constraints)
                    let client = rubygems::Client::new(jobs);
                    let resolution = resolver::resolve(&parsed, &client, None).await?;
                    
                    // Write lockfile
                    lockfile::write(&resolution, "Gemfile.lock")?;
                    
                    let elapsed = start.elapsed();
                    println!("✅ Lock completed in {:.2?}", elapsed);
                    
                    // Show cache stats
                    client.print_cache_stats().await;
                }
            }
            
            Ok(())
        }
        
        Commands::Update { gems, gemfile, lockfile: lockfile_path, jobs } => {
            let update_all = gems.is_empty();
            
            if update_all {
                println!("🔄 schwadl update --all - updating all dependencies...");
            } else {
                println!("🔄 schwadl update (conservative) - updating: {}", gems.join(", "));
            }
            println!("   Gemfile: {:?}", gemfile);
            println!("   Lockfile: {:?}", lockfile_path);
            println!("   Parallelism: {} jobs", jobs);
            
            let start = std::time::Instant::now();
            
            // Parse Gemfile
            let gemfile_content = std::fs::read_to_string(&gemfile)?;
            let parsed = gemfile::parse(&gemfile_content)?;
            
            println!("   Found {} direct dependencies", parsed.gems.len());
            
            // Check if we have an existing lockfile for incremental resolution
            let client = rubygems::Client::new(jobs);
            
            let resolution = if lockfile_path.exists() && !update_all {
                // Use incremental resolution for conservative updates
                let lock_content = std::fs::read_to_string(&lockfile_path)?;
                let existing_lock = lockfile::parse(&lock_content)?;
                
                println!("   ⚡ Using incremental resolution...");
                
                // Run incremental update
                let (resolution, diff) = incremental::incremental_update(
                    &parsed,
                    &existing_lock,
                    &gems,
                    &client,
                ).await?;
                
                // Print diff summary
                println!();
                incremental::print_diff_summary(&diff);
                
                resolution
            } else if lockfile_path.exists() {
                // update --all with existing lockfile: show diff after
                let lock_content = std::fs::read_to_string(&lockfile_path)?;
                let existing_lock = lockfile::parse(&lock_content)?;
                
                // Full resolution
                let resolution = resolver::resolve(&parsed, &client, None).await?;
                
                // Compute and show diff
                let new_lock = crate::lockfile::Lockfile {
                    source: resolution.source.clone(),
                    gems: resolution.gems.iter().map(|g| {
                        lockfile::LockedGem {
                            name: g.name.clone(),
                            version: g.version.clone(),
                            dependencies: g.dependencies.iter().map(|d| {
                                lockfile::LockedDependency {
                                    name: d.clone(),
                                    constraint: None,
                                }
                            }).collect(),
                        }
                    }).collect(),
                    platforms: resolution.platforms.clone(),
                    ruby_version: resolution.ruby_version.clone(),
                    bundled_with: Some("schwadl 0.1.0".to_string()),
                };
                let diff = incremental::diff_lockfiles(&existing_lock, &new_lock);
                
                println!();
                incremental::print_diff_summary(&diff);
                
                resolution
            } else {
                // No lockfile: fresh resolution
                println!("   ⚠️  No lockfile found, doing fresh resolution");
                resolver::resolve(&parsed, &client, None).await?
            };
            
            // Write lockfile
            lockfile::write(&resolution, "Gemfile.lock")?;
            
            let elapsed = start.elapsed();
            println!("✅ Update completed in {:.2?}", elapsed);
            
            // Show cache stats
            client.print_cache_stats().await;
            
            Ok(())
        }
        
        Commands::Install { lockfile, path, jobs } => {
            println!("📦 schwadl install - downloading gems...");
            println!("   Lockfile: {:?}", lockfile);
            println!("   Install path: {:?}", path);
            println!("   Parallelism: {} jobs", jobs);
            
            let start = std::time::Instant::now();
            
            // Parse lockfile
            let lock_content = std::fs::read_to_string(&lockfile)?;
            let locked = lockfile::parse(&lock_content)?;
            
            // Download and install gems
            let client = rubygems::Client::new(jobs);
            installer::install(&locked, &path, &client).await?;
            
            let elapsed = start.elapsed();
            println!("✅ Install completed in {:.2?}", elapsed);
            
            Ok(())
        }
        
        Commands::List { all } => {
            println!("📋 schwadl list");
            if all {
                println!("   Showing all transitive dependencies");
            }
            // TODO: implement
            Ok(())
        }
        
        Commands::Outdated => {
            println!("🔍 schwadl outdated - checking for updates...");
            // TODO: implement
            Ok(())
        }
        
        Commands::Cache { action } => {
            match action {
                CacheAction::Stats => {
                    println!("📊 Cache Statistics");
                    println!("   Location: ~/.schwadler/cache/");
                    
                    let cache = cache::PersistentCache::new()?;
                    let stats = cache.stats();
                    
                    println!("   {}", stats);
                    if stats.oldest_entry_age_secs > 0 {
                        let hours = stats.oldest_entry_age_secs / 3600;
                        let mins = (stats.oldest_entry_age_secs % 3600) / 60;
                        println!("   Oldest entry: {}h {}m ago", hours, mins);
                    }
                    
                    Ok(())
                }
                
                CacheAction::Clear => {
                    println!("🧹 Clearing cache...");
                    let mut cache = cache::PersistentCache::new()?;
                    cache.clear()?;
                    println!("✅ Cache cleared");
                    Ok(())
                }
                
                CacheAction::Prune => {
                    println!("🧹 Pruning expired entries...");
                    let mut cache = cache::PersistentCache::new()?;
                    let removed = cache.prune_expired()?;
                    println!("✅ Removed {} expired entries", removed);
                    Ok(())
                }
            }
        }
        
        Commands::Index { action } => {
            match action {
                IndexAction::Download { jobs } => {
                    println!("📥 schwadl index download - downloading complete RubyGems index...");
                    println!("   This will download metadata for ALL gems (~200,000+)");
                    println!("   Parallelism: {} concurrent requests", jobs);
                    println!();
                    
                    let stats = full_index::download_full_index(jobs).await?;
                    
                    println!();
                    println!("✅ Index download complete!");
                    println!("   {}", stats);
                    println!("   Location: {:?}", full_index::full_index_path());
                    println!();
                    println!("💡 You can now resolve offline: schwadl lock --offline");
                    
                    Ok(())
                }
                
                IndexAction::Update { jobs } => {
                    println!("🔄 schwadl index update - checking for updates...");
                    
                    let stats = full_index::update_index(jobs).await?;
                    
                    if stats.was_modified {
                        println!();
                        println!("✅ Index updated: {}", stats);
                    }
                    
                    Ok(())
                }
                
                IndexAction::Build => {
                    println!("🔨 Building mmap index from cache...");
                    let start = std::time::Instant::now();
                    
                    let stats = index::build_index()?;
                    let elapsed = start.elapsed();
                    
                    println!("✅ Index built in {:.2?}", elapsed);
                    println!("   {}", stats);
                    println!("   Location: {:?}", index::index_path());
                    
                    Ok(())
                }
                
                IndexAction::Stats => {
                    println!("📊 Index Statistics");
                    println!();
                    
                    // Full index stats (for offline resolution)
                    println!("Full Index (for offline resolution):");
                    match full_index::get_stats() {
                        Ok(stats) => {
                            println!("   {}", stats);
                            println!("   Location: {:?}", full_index::full_index_path());
                            
                            // Suggest update if old
                            let days = stats.age_secs / 86400;
                            if days > 7 {
                                println!();
                                println!("   ⚠️  Index is {} days old. Consider running:", days);
                                println!("      schwadl index update");
                            }
                        }
                        Err(_) => {
                            println!("   No full index found.");
                            println!("   Run `schwadl index download` to enable offline resolution.");
                        }
                    }
                    
                    println!();
                    
                    // Local cache index stats
                    println!("Cache Index (for online resolution):");
                    match index::MappedIndex::load() {
                        Ok(mapped) => {
                            let stats = mapped.stats();
                            println!("   {} gems, {} versions", stats.gem_count, stats.version_count);
                            
                            let age_secs = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap()
                                .as_secs()
                                .saturating_sub(stats.built_at);
                            let hours = age_secs / 3600;
                            let mins = (age_secs % 3600) / 60;
                            println!("   Built: {}h {}m ago", hours, mins);
                            println!("   Location: {:?}", index::index_path());
                        }
                        Err(_) => {
                            println!("   No cache index found.");
                            println!("   Run `schwadl index build` after resolving some gems.");
                        }
                    }
                    
                    Ok(())
                }
            }
        }
    }
}
