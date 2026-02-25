//! Gem installer - downloads and extracts gems in parallel
//!
//! .gem files are tar archives containing:
//! - metadata.gz (gzipped YAML)
//! - data.tar.gz (gzipped tar of gem files)
//! - checksums.yaml.gz

use crate::lockfile::Lockfile;
use crate::rubygems::Client;
use anyhow::{anyhow, Context, Result};
use flate2::read::GzDecoder;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use rayon::prelude::*;
use std::io::{Cursor, Read};
use std::path::{Path, PathBuf};
use std::process::Command;
use tar::Archive;
use tokio::fs;
use tokio::sync::Semaphore;
use std::sync::Arc;

/// Result of installing a single gem
#[derive(Debug, Clone)]
pub struct InstallResult {
    pub name: String,
    pub version: String,
    pub has_native_extension: bool,
    pub files_count: usize,
    pub gem_dir: PathBuf,
}

/// Result of compiling a native extension
#[derive(Debug)]
pub struct CompileResult {
    pub name: String,
    pub version: String,
    pub success: bool,
    pub duration_ms: u64,
    pub error: Option<String>,
}

/// Find the Ruby binary
fn find_ruby() -> Result<PathBuf> {
    // Try RUBY env var first
    if let Ok(ruby) = std::env::var("RUBY") {
        let path = PathBuf::from(&ruby);
        if path.exists() {
            return Ok(path);
        }
    }
    
    // Try which ruby
    let output = Command::new("which")
        .arg("ruby")
        .output()
        .context("Failed to find ruby")?;
    
    if output.status.success() {
        let path_str = String::from_utf8_lossy(&output.stdout);
        let path = PathBuf::from(path_str.trim());
        if path.exists() {
            return Ok(path);
        }
    }
    
    // Try common locations
    for path in &[
        "/usr/bin/ruby",
        "/usr/local/bin/ruby",
    ] {
        let p = PathBuf::from(path);
        if p.exists() {
            return Ok(p);
        }
    }
    
    Err(anyhow!("Ruby not found. Please install Ruby or set RUBY env var."))
}

/// Get Ruby version for display (e.g., "3.4.7")
fn get_ruby_version_full(ruby: &Path) -> Result<String> {
    let output = Command::new(ruby)
        .args(["-e", "puts RUBY_VERSION"])
        .output()
        .context("Failed to get Ruby version")?;
    
    if !output.status.success() {
        return Err(anyhow!("Failed to get Ruby version"));
    }
    
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

/// Get Ruby version for gem path (e.g., "3.4.0" - major.minor.0 convention)
fn get_ruby_version(ruby: &Path) -> Result<String> {
    let output = Command::new(ruby)
        .args(["-e", "puts RbConfig::CONFIG['ruby_version']"])
        .output()
        .context("Failed to get Ruby version")?;
    
    if !output.status.success() {
        // Fallback to RUBY_VERSION with .0 suffix
        let full = get_ruby_version_full(ruby)?;
        let parts: Vec<&str> = full.split('.').collect();
        if parts.len() >= 2 {
            return Ok(format!("{}.{}.0", parts[0], parts[1]));
        }
        return Err(anyhow!("Failed to get Ruby version"));
    }
    
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

/// Get Ruby platform (e.g., "x86_64-darwin22")
fn get_ruby_platform(ruby: &Path) -> Result<String> {
    let output = Command::new(ruby)
        .args(["-e", "puts RUBY_PLATFORM"])
        .output()
        .context("Failed to get Ruby platform")?;
    
    if !output.status.success() {
        return Err(anyhow!("Failed to get Ruby platform"));
    }
    
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

/// Check if gem has precompiled binaries (platform gem like nokogiri-x86_64-darwin)
/// These gems ship with .bundle/.so files for multiple Ruby versions
fn has_precompiled_binaries(gem_dir: &Path, ruby_version: &str) -> bool {
    let lib_dir = gem_dir.join("lib");
    if !lib_dir.exists() {
        return false;
    }
    
    // Extract major.minor from version (3.4.0 -> 3.4)
    let parts: Vec<&str> = ruby_version.split('.').collect();
    if parts.len() < 2 {
        return false;
    }
    let major_minor = format!("{}.{}", parts[0], parts[1]);
    
    // Walk lib/ looking for version-specific .bundle or .so files
    search_for_binaries(&lib_dir, &major_minor)
}

fn search_for_binaries(dir: &Path, major_minor: &str) -> bool {
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                let dir_name = path.file_name().unwrap_or_default().to_string_lossy();
                // Check for version directory (3.4, 3.3, etc.)
                if dir_name == major_minor {
                    // Look for .bundle or .so in this directory
                    if let Ok(files) = std::fs::read_dir(&path) {
                        for file in files.flatten() {
                            let name = file.file_name().to_string_lossy().to_string();
                            if name.ends_with(".bundle") || name.ends_with(".so") {
                                return true;
                            }
                        }
                    }
                }
                // Recurse into subdirectories (e.g., lib/nokogiri/3.4/)
                if search_for_binaries(&path, major_minor) {
                    return true;
                }
            }
        }
    }
    false
}

/// Extract require_paths from gemspec YAML
fn extract_require_paths(gemspec_yaml: &str) -> Vec<String> {
    let mut paths = Vec::new();
    let mut in_require_paths = false;
    
    for line in gemspec_yaml.lines() {
        let trimmed = line.trim();
        
        // Check if we're entering require_paths section
        if trimmed.starts_with("require_paths:") {
            in_require_paths = true;
            // Check for inline array format: require_paths: ["lib"]
            if let Some(rest) = trimmed.strip_prefix("require_paths:") {
                let rest = rest.trim();
                if rest.starts_with('[') && rest.ends_with(']') {
                    // Parse inline array
                    let inner = &rest[1..rest.len()-1];
                    for item in inner.split(',') {
                        let item = item.trim().trim_matches('"').trim_matches('\'');
                        if !item.is_empty() {
                            paths.push(item.to_string());
                        }
                    }
                    return paths;
                }
            }
            continue;
        }
        
        // If we're in require_paths, look for array items
        if in_require_paths {
            if trimmed.starts_with('-') {
                // Array item: "- lib" or "- lib/concurrent-ruby"
                let path = trimmed.trim_start_matches('-').trim();
                let path = path.trim_matches('"').trim_matches('\'');
                if !path.is_empty() {
                    paths.push(path.to_string());
                }
            } else if !trimmed.is_empty() && !trimmed.starts_with('#') {
                // New key, we're done with require_paths
                break;
            }
        }
    }
    
    if paths.is_empty() {
        paths.push("lib".to_string());
    }
    paths
}

/// Generate a Bundler-compatible stub gemspec
fn generate_stub_gemspec(name: &str, version: &str, _gem_dir: &Path, gemspec_yaml: Option<&str>) -> String {
    // Parse platform from version string (e.g., "1.19.1-x86_64-darwin" -> version="1.19.1", platform="x86_64-darwin")
    let (actual_version, platform) = parse_version_platform(version);
    
    // Extract require_paths from gemspec YAML
    let require_paths = if let Some(yaml) = gemspec_yaml {
        extract_require_paths(yaml)
    } else {
        vec!["lib".to_string()]
    };
    
    // Format require_paths as Ruby array
    let require_paths_str = require_paths
        .iter()
        .map(|p| format!("\"{}\".freeze", p))
        .collect::<Vec<_>>()
        .join(", ");
    
    // Platform line (only add if not ruby)
    let platform_line = if platform != "ruby" {
        format!("  s.platform = \"{}\".freeze\n", platform)
    } else {
        String::new()
    };
    
    // For stub header, use first require_path
    let first_path = require_paths.first().map(|s| s.as_str()).unwrap_or("lib");
    
    format!(
        r#"# -*- encoding: utf-8 -*-
# stub: {name} {version} {platform} {first_path}

Gem::Specification.new do |s|
  s.name = "{name}".freeze
  s.version = "{version}".freeze
{platform_line}  s.require_paths = [{require_paths_str}]
  s.authors = []
  s.summary = "".freeze
end
"#,
        name = name,
        version = actual_version,
        platform = platform,
        platform_line = platform_line,
        first_path = first_path,
        require_paths_str = require_paths_str,
    )
}

/// Parse version string to extract platform suffix
/// e.g., "1.19.1-x86_64-darwin" -> ("1.19.1", "x86_64-darwin")
/// e.g., "1.4.0" -> ("1.4.0", "ruby")
fn parse_version_platform(version: &str) -> (&str, &str) {
    // Common platform patterns
    let platform_patterns = [
        "-x86_64-darwin",
        "-arm64-darwin", 
        "-aarch64-darwin",
        "-x86_64-linux",
        "-aarch64-linux",
        "-x86_64-linux-gnu",
        "-x86_64-linux-musl",
        "-x86-mingw32",
        "-x64-mingw32",
        "-x64-mingw-ucrt",
        "-java",
        "-jruby",
    ];
    
    for pattern in &platform_patterns {
        if let Some(idx) = version.find(pattern) {
            let ver = &version[..idx];
            let plat = &version[idx + 1..];  // Skip the leading '-'
            return (ver, plat);
        }
    }
    
    (version, "ruby")
}

/// Find extconf.rb files in a gem directory (recursive search)
fn find_extconf_files(gem_dir: &Path) -> Vec<PathBuf> {
    let mut results = Vec::new();
    let ext_dir = gem_dir.join("ext");
    
    if !ext_dir.exists() {
        return results;
    }
    
    // Recursively search for extconf.rb files
    fn search_dir(dir: &Path, results: &mut Vec<PathBuf>) {
        if let Ok(entries) = std::fs::read_dir(dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    // Check if this directory contains extconf.rb
                    let extconf = path.join("extconf.rb");
                    if extconf.exists() {
                        results.push(path.clone());
                    }
                    // Continue searching subdirectories
                    search_dir(&path, results);
                }
            }
        }
    }
    
    // Also check ext/ itself for extconf.rb
    if ext_dir.join("extconf.rb").exists() {
        results.push(ext_dir.clone());
    }
    
    search_dir(&ext_dir, &mut results);
    results
}

/// Compile a native extension for a gem
async fn compile_extension(
    gem_name: &str,
    gem_version: &str,
    gem_dir: &Path,
    ruby: &Path,
) -> CompileResult {
    let start = std::time::Instant::now();
    
    let ext_dirs = find_extconf_files(gem_dir);
    if ext_dirs.is_empty() {
        return CompileResult {
            name: gem_name.to_string(),
            version: gem_version.to_string(),
            success: true,
            duration_ms: 0,
            error: None,
        };
    }
    
    for ext_dir in ext_dirs {
        // Step 1: Run ruby extconf.rb
        let extconf_result = Command::new(ruby)
            .arg("extconf.rb")
            .current_dir(&ext_dir)
            .output();
        
        match extconf_result {
            Ok(output) if !output.status.success() => {
                let stderr = String::from_utf8_lossy(&output.stderr);
                let stdout = String::from_utf8_lossy(&output.stdout);
                return CompileResult {
                    name: gem_name.to_string(),
                    version: gem_version.to_string(),
                    success: false,
                    duration_ms: start.elapsed().as_millis() as u64,
                    error: Some(format!(
                        "extconf.rb failed:\n{}\n{}",
                        stdout, stderr
                    )),
                };
            }
            Err(e) => {
                return CompileResult {
                    name: gem_name.to_string(),
                    version: gem_version.to_string(),
                    success: false,
                    duration_ms: start.elapsed().as_millis() as u64,
                    error: Some(format!("Failed to run extconf.rb: {}", e)),
                };
            }
            _ => {}
        }
        
        // Step 2: Run make
        let make_result = Command::new("make")
            .arg("-j2")  // Conservative parallelism within gem
            .current_dir(&ext_dir)
            .output();
        
        match make_result {
            Ok(output) if !output.status.success() => {
                let stderr = String::from_utf8_lossy(&output.stderr);
                let stdout = String::from_utf8_lossy(&output.stdout);
                return CompileResult {
                    name: gem_name.to_string(),
                    version: gem_version.to_string(),
                    success: false,
                    duration_ms: start.elapsed().as_millis() as u64,
                    error: Some(format!(
                        "make failed:\n{}\n{}",
                        stdout, stderr
                    )),
                };
            }
            Err(e) => {
                return CompileResult {
                    name: gem_name.to_string(),
                    version: gem_version.to_string(),
                    success: false,
                    duration_ms: start.elapsed().as_millis() as u64,
                    error: Some(format!("Failed to run make: {}", e)),
                };
            }
            _ => {}
        }
        
        // Step 3: Copy compiled .so/.bundle to lib/ preserving directory structure
        // Two patterns:
        // 1. ext/json/ext/generator/generator.bundle → lib/json/ext/generator.bundle
        //    (nested ext structure, use relative path from ext/)
        // 2. ext/puma_http11/puma_http11.bundle → lib/puma/puma_http11.bundle
        //    (flat ext structure with gem_name_ prefix, use gem name as subdir)
        let gem_ext_dir = gem_dir.join("ext");
        let lib_dir = gem_dir.join("lib");
        
        // Calculate relative path from ext/ to the extension directory
        let relative_path = ext_dir.strip_prefix(&gem_ext_dir).unwrap_or(&ext_dir);
        let relative_parent = relative_path.parent().unwrap_or(std::path::Path::new(""));
        
        // Find compiled extensions
        if let Ok(entries) = std::fs::read_dir(&ext_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                let name = path.file_name().unwrap_or_default().to_string_lossy();
                if name.ends_with(".so") || name.ends_with(".bundle") || name.ends_with(".dll") {
                    // Determine target directory
                    // If we have nested structure (relative_parent is not empty), use it
                    // Otherwise, check if extension name suggests a subdirectory
                    let target_dir = if !relative_parent.as_os_str().is_empty() {
                        lib_dir.join(relative_parent)
                    } else {
                        // Check for gem_name_ prefix pattern (e.g., puma_http11 → puma/)
                        // Or gem_name pattern where gem requires gem_name/gem_name (e.g., oj/oj)
                        let ext_basename = name.trim_end_matches(".so")
                            .trim_end_matches(".bundle")
                            .trim_end_matches(".dll");
                        let gem_prefix = gem_name.split('-').next().unwrap_or(gem_name);
                        
                        // Case 1: Extension name has underscore prefix matching gem (puma_http11 → puma/)
                        if let Some(underscore_pos) = ext_basename.find('_') {
                            let prefix = &ext_basename[..underscore_pos];
                            let subdir = lib_dir.join(prefix);
                            if subdir.exists() || prefix == gem_prefix {
                                subdir
                            } else {
                                lib_dir.clone()
                            }
                        }
                        // Case 2: Extension name equals gem name
                        // This is tricky - some gems require at top level (bigdecimal.so)
                        // while others require nested (oj/oj, bootsnap/bootsnap)
                        // Search all Ruby files in lib/ for the require pattern
                        else if ext_basename == gem_prefix {
                            // Search lib/ recursively for require 'gem/gem' pattern
                            fn search_for_nested_require(dir: &Path, pattern: &str) -> bool {
                                if let Ok(entries) = std::fs::read_dir(dir) {
                                    for entry in entries.flatten() {
                                        let path = entry.path();
                                        if path.is_dir() {
                                            if search_for_nested_require(&path, pattern) {
                                                return true;
                                            }
                                        } else if path.extension().map(|e| e == "rb").unwrap_or(false) {
                                            if let Ok(content) = std::fs::read_to_string(&path) {
                                                if content.contains(pattern) {
                                                    return true;
                                                }
                                            }
                                        }
                                    }
                                }
                                false
                            }
                            
                            let nested_pattern1 = format!("require '{}/{}'", gem_prefix, gem_prefix);
                            let nested_pattern2 = format!("require \"{}/{}\"", gem_prefix, gem_prefix);
                            
                            let requires_nested = search_for_nested_require(&lib_dir, &nested_pattern1) ||
                                                  search_for_nested_require(&lib_dir, &nested_pattern2);
                            
                            if requires_nested {
                                let subdir = lib_dir.join(gem_prefix);
                                std::fs::create_dir_all(&subdir).ok();
                                subdir
                            } else {
                                // Top-level require like bigdecimal.so
                                lib_dir.clone()
                            }
                        } else {
                            lib_dir.clone()
                        }
                    };
                    
                    std::fs::create_dir_all(&target_dir).ok();
                    let dest = target_dir.join(path.file_name().unwrap());
                    std::fs::copy(&path, &dest).ok();
                }
            }
        }
    }
    
    CompileResult {
        name: gem_name.to_string(),
        version: gem_version.to_string(),
        success: true,
        duration_ms: start.elapsed().as_millis() as u64,
        error: None,
    }
}

/// Filter gems to only include those matching the current platform.
/// 
/// For gems with platform-specific variants (e.g., nokogiri-1.19.1-arm64-darwin),
/// this selects only the variant matching the current platform instead of
/// downloading all 8 variants.
fn filter_gems_for_platform(gems: &[crate::lockfile::LockedGem], ruby_platform: &str) -> Vec<crate::lockfile::LockedGem> {
    use std::collections::HashMap;
    
    // Normalize Ruby platform to RubyGems format
    // e.g., "arm64-darwin22" -> "arm64-darwin" (strip OS version)
    let normalized_platform = normalize_platform(ruby_platform);
    
    // Group gems by base name
    let mut gem_groups: HashMap<String, Vec<&crate::lockfile::LockedGem>> = HashMap::new();
    
    for gem in gems {
        let (_, platform) = parse_version_platform(&gem.version);
        let base_name = gem.name.clone();
        gem_groups.entry(base_name).or_default().push(gem);
    }
    
    let mut filtered = Vec::new();
    
    for (_name, variants) in gem_groups {
        if variants.len() == 1 {
            // Single variant - just use it
            filtered.push(variants[0].clone());
        } else {
            // Multiple variants - pick the best match for current platform
            let best = select_best_platform_variant(&variants, &normalized_platform);
            filtered.push(best.clone());
        }
    }
    
    filtered
}

/// Normalize a Ruby platform string to match RubyGems conventions.
/// Strips OS version numbers (e.g., "arm64-darwin22" -> "arm64-darwin")
fn normalize_platform(platform: &str) -> String {
    // Common patterns: arm64-darwin22, x86_64-darwin21, x86_64-linux
    // Strip trailing version numbers from darwin
    if platform.contains("darwin") {
        // Find "darwin" and take everything up to and including it
        if let Some(idx) = platform.find("darwin") {
            return platform[..idx + 6].to_string(); // "darwin" is 6 chars
        }
    }
    platform.to_string()
}

/// Select the best platform variant for the current platform.
/// Priority: exact match > compatible variant > ruby (pure)
fn select_best_platform_variant<'a>(
    variants: &[&'a crate::lockfile::LockedGem],
    target_platform: &str,
) -> &'a crate::lockfile::LockedGem {
    // Score each variant
    let mut best: Option<(&crate::lockfile::LockedGem, i32)> = None;
    
    for variant in variants {
        let (_, platform) = parse_version_platform(&variant.version);
        let score = platform_match_score(platform, target_platform);
        
        if let Some((_, best_score)) = best {
            if score > best_score {
                best = Some((variant, score));
            }
        } else {
            best = Some((variant, score));
        }
    }
    
    best.map(|(v, _)| v).unwrap_or(variants[0])
}

/// Score how well a gem platform matches the target platform.
/// Higher score = better match.
fn platform_match_score(gem_platform: &str, target: &str) -> i32 {
    // Exact match is best
    if gem_platform == target {
        return 100;
    }
    
    // Handle darwin variations (arm64-darwin matches arm64-darwin22)
    if gem_platform.contains("darwin") && target.contains("darwin") {
        let gem_arch = gem_platform.split('-').next().unwrap_or("");
        let target_arch = target.split('-').next().unwrap_or("");
        if gem_arch == target_arch {
            return 90; // Same arch, darwin variant
        }
    }
    
    // Handle linux variations (x86_64-linux-gnu vs x86_64-linux)
    if gem_platform.contains("linux") && target.contains("linux") {
        let gem_arch = gem_platform.split('-').next().unwrap_or("");
        let target_arch = target.split('-').next().unwrap_or("");
        if gem_arch == target_arch {
            // Prefer -gnu over -musl if no specific match
            if gem_platform.contains("-gnu") {
                return 85;
            }
            return 80;
        }
    }
    
    // "ruby" (pure Ruby) is universal fallback
    if gem_platform == "ruby" {
        return 50;
    }
    
    // No platform suffix (old-style gem) works everywhere
    if gem_platform.is_empty() || gem_platform == "ruby" {
        return 50;
    }
    
    // Wrong platform - very low score but still valid (might need source compile)
    0
}

/// Install gems from a lockfile
pub async fn install(lockfile: &Lockfile, install_path: &Path, client: &Client) -> Result<()> {
    // Get Ruby version for proper directory structure
    let ruby = find_ruby()?;
    let ruby_version = get_ruby_version(&ruby)?;  // e.g., "3.4.0" for paths
    let ruby_version_full = get_ruby_version_full(&ruby)?;  // e.g., "3.4.7" for display
    let ruby_platform = get_ruby_platform(&ruby)?;
    
    println!("   Ruby: {} ({})", ruby_version_full, ruby_platform);
    
    // Filter gems to only include those for the current platform
    // This avoids downloading all 8 variants of gems like nokogiri
    let filtered_gems = filter_gems_for_platform(&lockfile.gems, &ruby_platform);
    let skipped_count = lockfile.gems.len() - filtered_gems.len();
    if skipped_count > 0 {
        println!("   Filtered {} platform variants (keeping {} gems)", skipped_count, filtered_gems.len());
    }
    
    // Create Bundler-compatible directory structure:
    // vendor/bundle/ruby/<VERSION>/gems/
    // vendor/bundle/ruby/<VERSION>/specifications/
    // vendor/bundle/ruby/<VERSION>/cache/
    // vendor/bundle/ruby/<VERSION>/extensions/<PLATFORM>/<VERSION>/
    let ruby_dir = install_path.join("ruby").join(&ruby_version);
    let gems_dir = ruby_dir.join("gems");
    let cache_dir = ruby_dir.join("cache");
    let specifications_dir = ruby_dir.join("specifications");
    let extensions_dir = ruby_dir.join("extensions").join(&ruby_platform).join(&ruby_version);
    let bin_dir = ruby_dir.join("bin");
    
    fs::create_dir_all(&gems_dir).await?;
    fs::create_dir_all(&cache_dir).await?;
    fs::create_dir_all(&specifications_dir).await?;
    fs::create_dir_all(&extensions_dir).await?;
    fs::create_dir_all(&bin_dir).await?;
    
    // Create .bundle/config for Bundler compatibility
    let bundle_dir = install_path.parent().unwrap_or(install_path).join(".bundle");
    fs::create_dir_all(&bundle_dir).await?;
    let config_path = bundle_dir.join("config");
    let config_content = format!(
        "---\nBUNDLE_PATH: \"{}\"\nBUNDLE_DISABLE_SHARED_GEMS: \"true\"\n",
        install_path.display()
    );
    fs::write(&config_path, config_content).await?;
    
    let gem_count = filtered_gems.len();
    println!("   Installing {} gems...", gem_count);
    
    // Create multi-progress for parallel display
    let mp = MultiProgress::new();
    let main_pb = mp.add(ProgressBar::new(gem_count as u64));
    main_pb.set_style(ProgressStyle::default_bar()
        .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({msg})")?
        .progress_chars("#>-"));
    main_pb.set_message("downloading");
    
    // Check which gems are already installed
    let mut to_install = Vec::new();
    let mut already_installed = 0;
    
    for gem in &filtered_gems {
        let gem_dir = gems_dir.join(format!("{}-{}", gem.name, gem.version));
        let marker = gem_dir.join(".schwadl-installed");
        
        if marker.exists() {
            already_installed += 1;
            main_pb.inc(1);
        } else {
            to_install.push(gem.clone());
        }
    }
    
    if already_installed > 0 {
        println!("   ✓ {} gems already installed", already_installed);
    }
    
    if to_install.is_empty() {
        main_pb.finish_with_message("complete");
        return Ok(());
    }
    
    // Download and install gems in parallel
    let mut handles = Vec::with_capacity(to_install.len());
    
    for gem in to_install {
        let client = client.clone();
        let gems_dir = gems_dir.clone();
        let cache_dir = cache_dir.clone();
        let specifications_dir = specifications_dir.clone();
        let pb = main_pb.clone();
        
        handles.push(tokio::spawn(async move {
            let result = install_single_gem(
                &client,
                &gem.name,
                &gem.version,
                &gems_dir,
                &cache_dir,
                &specifications_dir,
            ).await;
            pb.inc(1);
            result
        }));
    }
    
    // Collect results
    let mut installed = Vec::new();
    let mut native_extensions = Vec::new();
    let mut errors = Vec::new();
    
    for handle in handles {
        match handle.await? {
            Ok(result) => {
                if result.has_native_extension {
                    native_extensions.push(result.clone());
                }
                installed.push(result);
            }
            Err(e) => errors.push(e.to_string()),
        }
    }
    
    main_pb.finish_with_message("complete");
    
    // Report results
    println!("   ✓ Installed {} gems ({} files)", 
        installed.len(), 
        installed.iter().map(|r| r.files_count).sum::<usize>()
    );
    
    // Compile native extensions
    if !native_extensions.is_empty() {
        println!();
        println!("   🔨 Processing {} native extensions...", native_extensions.len());
        
        // Compile in parallel with semaphore (limit concurrent compilations)
        let sem = Arc::new(Semaphore::new(num_cpus::get().min(4)));
        let mut compile_handles = Vec::new();
        let mut precompiled_count = 0;
        
        for result in &native_extensions {
            let gem_dir = result.gem_dir.clone();
            let name = result.name.clone();
            let version = result.version.clone();
            
            // Check for precompiled binaries FIRST
            if has_precompiled_binaries(&gem_dir, &ruby_version) {
                println!("      ✓ {}-{} (precompiled)", name, version);
                precompiled_count += 1;
                continue;
            }
            
            let ruby = ruby.clone();
            let sem = sem.clone();
            
            compile_handles.push(tokio::spawn(async move {
                let _permit = sem.acquire().await.unwrap();
                compile_extension(&name, &version, &gem_dir, &ruby).await
            }));
        }
        
        // Collect compile results
        let mut compile_success = 0;
        let mut compile_errors = Vec::new();
        
        for handle in compile_handles {
            match handle.await {
                Ok(result) => {
                    if result.success {
                        compile_success += 1;
                        println!("      ✓ {}-{} ({}ms)", result.name, result.version, result.duration_ms);
                    } else {
                        println!("      ✗ {}-{}", result.name, result.version);
                        if let Some(err) = &result.error {
                            compile_errors.push(format!("{}-{}: {}", result.name, result.version, err.lines().next().unwrap_or("")));
                        }
                    }
                }
                Err(e) => {
                    compile_errors.push(format!("Task failed: {}", e));
                }
            }
        }
        
        let total_ok = compile_success + precompiled_count;
        if total_ok > 0 {
            println!("   ✓ {} native extensions ready ({} compiled, {} precompiled)", 
                total_ok, compile_success, precompiled_count);
        }
        
        if !compile_errors.is_empty() {
            println!();
            println!("   ⚠️  {} extensions failed to compile:", compile_errors.len());
            for err in compile_errors.iter().take(5) {
                println!("      • {}", err);
            }
            if compile_errors.len() > 5 {
                println!("      ... and {} more", compile_errors.len() - 5);
            }
        }
    }
    
    // Report errors
    if !errors.is_empty() {
        println!();
        println!("   ❌ {} gems failed to install:", errors.len());
        for e in errors.iter().take(5) {
            println!("      - {}", e);
        }
        if errors.len() > 5 {
            println!("      ... and {} more", errors.len() - 5);
        }
    }
    
    Ok(())
}

/// Install a single gem: download, extract, detect extensions
async fn install_single_gem(
    client: &Client,
    name: &str,
    version: &str,
    gems_dir: &Path,
    cache_dir: &Path,
    specifications_dir: &Path,
) -> Result<InstallResult> {
    let gem_dir = gems_dir.join(format!("{}-{}", name, version));
    let gem_cache_path = cache_dir.join(format!("{}-{}.gem", name, version));
    
    // Download gem
    let gem_data = client.download_gem(name, version).await?;
    
    // Save to cache
    fs::write(&gem_cache_path, &gem_data).await?;
    
    // Extract gem
    let (files_count, has_native_extension, gemspec_yaml) = 
        extract_gem(&gem_data, &gem_dir).await?;
    
    // Generate proper stub gemspec (Bundler-compatible format)
    // Uses actual require_paths from the gem's metadata
    let spec_path = specifications_dir.join(format!("{}-{}.gemspec", name, version));
    let stub_gemspec = generate_stub_gemspec(name, version, &gem_dir, gemspec_yaml.as_deref());
    fs::write(&spec_path, stub_gemspec).await?;
    
    // Write marker file
    let marker = gem_dir.join(".schwadl-installed");
    fs::write(&marker, format!("schwadl 0.1.0\n{}-{}", name, version)).await?;
    
    Ok(InstallResult {
        name: name.to_string(),
        version: version.to_string(),
        has_native_extension,
        files_count,
        gem_dir,
    })
}

/// Extract a .gem file (tar containing data.tar.gz)
async fn extract_gem(gem_data: &[u8], target_dir: &Path) -> Result<(usize, bool, Option<String>)> {
    // .gem is a tar archive
    let cursor = Cursor::new(gem_data);
    let mut archive = Archive::new(cursor);
    
    let mut data_tar_gz: Option<Vec<u8>> = None;
    let mut metadata_gz: Option<Vec<u8>> = None;
    
    // First pass: find data.tar.gz and metadata.gz
    for entry in archive.entries()? {
        let mut entry = entry?;
        let path = entry.path()?;
        let path_str = path.to_string_lossy();
        
        if path_str == "data.tar.gz" {
            let mut buf = Vec::new();
            entry.read_to_end(&mut buf)?;
            data_tar_gz = Some(buf);
        } else if path_str == "metadata.gz" {
            let mut buf = Vec::new();
            entry.read_to_end(&mut buf)?;
            metadata_gz = Some(buf);
        }
    }
    
    let data_tar_gz = data_tar_gz.ok_or_else(|| anyhow!("No data.tar.gz in gem"))?;
    
    // Extract gemspec from metadata
    let gemspec_content = if let Some(meta_gz) = metadata_gz {
        let mut decoder = GzDecoder::new(&meta_gz[..]);
        let mut content = String::new();
        decoder.read_to_string(&mut content).ok();
        Some(content)
    } else {
        None
    };
    
    // Decompress data.tar.gz
    let decoder = GzDecoder::new(&data_tar_gz[..]);
    let mut archive = Archive::new(decoder);
    
    // Create target directory
    std::fs::create_dir_all(target_dir)?;
    
    let mut files_count = 0;
    let mut has_native_extension = false;
    
    // Extract files
    for entry in archive.entries()? {
        let mut entry = entry?;
        let path = entry.path()?;
        let path_str = path.to_string_lossy();
        
        // Detect native extensions
        if path_str.starts_with("ext/") || 
           path_str.ends_with("extconf.rb") ||
           path_str.contains("/ext/") {
            has_native_extension = true;
        }
        
        // Build target path
        let target_path = target_dir.join(&*path);
        
        // Create parent directories
        if let Some(parent) = target_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        
        // Extract file
        if entry.header().entry_type().is_file() {
            let mut file = std::fs::File::create(&target_path)?;
            std::io::copy(&mut entry, &mut file)?;
            files_count += 1;
        } else if entry.header().entry_type().is_dir() {
            std::fs::create_dir_all(&target_path)?;
        }
    }
    
    Ok((files_count, has_native_extension, gemspec_content))
}

/// Check which gems need to be installed (uses Rayon for parallel filesystem checks)
pub async fn check_missing(lockfile: &Lockfile, install_path: &Path) -> Vec<(String, String)> {
    // Get Ruby version and platform for proper path and filtering
    let (ruby_version, ruby_platform) = match find_ruby().and_then(|r| {
        let v = get_ruby_version(&r)?;
        let p = get_ruby_platform(&r)?;
        Ok((v, p))
    }) {
        Ok((v, p)) => (v, p),
        Err(_) => ("unknown".to_string(), "unknown".to_string()),
    };
    
    let gems_dir = install_path.join("ruby").join(&ruby_version).join("gems");
    
    // Filter gems to only include those for the current platform
    let filtered_gems = filter_gems_for_platform(&lockfile.gems, &ruby_platform);
    
    // Check all gems in parallel using Rayon
    filtered_gems.par_iter()
        .filter_map(|gem| {
            let gem_dir = gems_dir.join(format!("{}-{}", gem.name, gem.version));
            let marker = gem_dir.join(".schwadl-installed");
            
            if !marker.exists() {
                Some((gem.name.clone(), gem.version.clone()))
            } else {
                None
            }
        })
        .collect()
}

/// Verify installed gems match lockfile (uses Rayon for parallel checks)
pub async fn verify(lockfile: &Lockfile, install_path: &Path) -> Result<Vec<String>> {
    // Get Ruby version and platform for proper path and filtering
    let (ruby_version, ruby_platform) = match find_ruby().and_then(|r| {
        let v = get_ruby_version(&r)?;
        let p = get_ruby_platform(&r)?;
        Ok((v, p))
    }) {
        Ok((v, p)) => (v, p),
        Err(_) => ("unknown".to_string(), "unknown".to_string()),
    };
    
    let gems_dir = install_path.join("ruby").join(&ruby_version).join("gems");
    
    // Filter gems to only include those for the current platform
    let filtered_gems = filter_gems_for_platform(&lockfile.gems, &ruby_platform);
    
    // Check all gems in parallel using Rayon
    let issues: Vec<String> = filtered_gems.par_iter()
        .filter_map(|gem| {
            let gem_dir = gems_dir.join(format!("{}-{}", gem.name, gem.version));
            
            if !gem_dir.exists() {
                return Some(format!("{}-{}: not installed", gem.name, gem.version));
            }
            
            let marker = gem_dir.join(".schwadl-installed");
            if !marker.exists() {
                return Some(format!("{}-{}: incomplete installation", gem.name, gem.version));
            }
            
            None
        })
        .collect();
    
    Ok(issues)
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_detect_native_extension() {
        // Test path detection
        assert!(detect_native("ext/foo/extconf.rb"));
        assert!(detect_native("ext/native.c"));
        assert!(!detect_native("lib/foo.rb"));
        assert!(!detect_native("spec/foo_spec.rb"));
    }
    
    fn detect_native(path: &str) -> bool {
        path.starts_with("ext/") || 
        path.ends_with("extconf.rb") ||
        path.contains("/ext/")
    }
}
