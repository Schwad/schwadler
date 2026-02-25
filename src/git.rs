//! Git source handling for Schwadler
//! 
//! Handles cloning, caching, and gemspec extraction from git sources.
//! Supports both `git:` and `github:` source types.

use anyhow::{anyhow, Context, Result};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Command;

/// Git source configuration for a gem
#[derive(Debug, Clone)]
pub struct GitSource {
    pub url: String,
    pub branch: Option<String>,
    pub tag: Option<String>,
    pub ref_: Option<String>,
    pub submodules: bool,
}

impl GitSource {
    /// Convert github shorthand to full URL
    pub fn from_github(repo: &str) -> Self {
        let url = format!("https://github.com/{}.git", repo);
        Self {
            url,
            branch: None,
            tag: None,
            ref_: None,
            submodules: false,
        }
    }
    
    /// Get the ref to checkout (branch, tag, or ref, with fallback to HEAD)
    pub fn checkout_ref(&self) -> &str {
        self.branch.as_deref()
            .or(self.tag.as_deref())
            .or(self.ref_.as_deref())
            .unwrap_or("HEAD")
    }
    
    /// Generate a unique cache key for this source
    pub fn cache_key(&self) -> String {
        use sha2::{Sha256, Digest};
        let mut hasher = Sha256::new();
        hasher.update(self.url.as_bytes());
        hasher.update(self.checkout_ref().as_bytes());
        let result = hasher.finalize();
        hex::encode(&result[..8])  // First 8 bytes = 16 hex chars
    }
}

/// Manager for git-sourced gems
pub struct GitCache {
    cache_dir: PathBuf,
}

impl GitCache {
    pub fn new() -> Result<Self> {
        let cache_dir = dirs::home_dir()
            .ok_or_else(|| anyhow!("Could not determine home directory"))?
            .join(".schwadler")
            .join("git");
        
        std::fs::create_dir_all(&cache_dir)?;
        
        Ok(Self { cache_dir })
    }
    
    /// Get or clone a git repository, returning the path and resolved commit SHA
    pub fn get_or_clone(&self, source: &GitSource) -> Result<(PathBuf, String)> {
        let cache_key = source.cache_key();
        let repo_path = self.cache_dir.join(&cache_key);
        
        if repo_path.exists() {
            // Update existing clone
            self.update_repo(&repo_path, source)?;
        } else {
            // Fresh clone
            self.clone_repo(source, &repo_path)?;
        }
        
        // Get current HEAD SHA
        let sha = self.get_head_sha(&repo_path)?;
        
        Ok((repo_path, sha))
    }
    
    fn clone_repo(&self, source: &GitSource, dest: &Path) -> Result<()> {
        let mut cmd = Command::new("git");
        cmd.arg("clone")
            .arg("--depth").arg("1")  // Shallow clone for speed
            .arg("--single-branch");
        
        if let Some(ref branch) = source.branch {
            cmd.arg("--branch").arg(branch);
        } else if let Some(ref tag) = source.tag {
            cmd.arg("--branch").arg(tag);
        }
        
        cmd.arg(&source.url)
            .arg(dest);
        
        let output = cmd.output()
            .context("Failed to execute git clone")?;
        
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(anyhow!("Git clone failed: {}", stderr));
        }
        
        // Handle specific ref if needed
        if source.ref_.is_some() || (source.tag.is_some() && source.branch.is_none()) {
            // Need to fetch the specific ref
            let checkout_ref = source.checkout_ref();
            
            // For refs, we need to unshallow first to get the ref
            if source.ref_.is_some() {
                let unshallow = Command::new("git")
                    .current_dir(dest)
                    .args(["fetch", "--unshallow"])
                    .output();
                // Ignore errors - might already be complete
                let _ = unshallow;
            }
            
            let checkout = Command::new("git")
                .current_dir(dest)
                .args(["checkout", checkout_ref])
                .output()
                .context("Failed to checkout ref")?;
            
            if !checkout.status.success() {
                let stderr = String::from_utf8_lossy(&checkout.stderr);
                return Err(anyhow!("Git checkout failed: {}", stderr));
            }
        }
        
        // Handle submodules if requested
        if source.submodules {
            let submodule = Command::new("git")
                .current_dir(dest)
                .args(["submodule", "update", "--init", "--recursive", "--depth", "1"])
                .output()
                .context("Failed to initialize submodules")?;
            
            if !submodule.status.success() {
                // Non-fatal - might not have submodules
                eprintln!("Warning: submodule init returned non-zero");
            }
        }
        
        Ok(())
    }
    
    fn update_repo(&self, repo_path: &Path, source: &GitSource) -> Result<()> {
        // Fetch latest
        let fetch = Command::new("git")
            .current_dir(repo_path)
            .args(["fetch", "--depth", "1", "origin"])
            .output()
            .context("Failed to fetch")?;
        
        if !fetch.status.success() {
            // Try without depth (might be already full)
            Command::new("git")
                .current_dir(repo_path)
                .args(["fetch", "origin"])
                .output()?;
        }
        
        // Reset to the right ref
        let checkout_ref = source.checkout_ref();
        let target_ref = if checkout_ref == "HEAD" {
            "origin/HEAD".to_string()
        } else {
            format!("origin/{}", checkout_ref)
        };
        
        // Try origin/branch first, fall back to just the ref
        let reset = Command::new("git")
            .current_dir(repo_path)
            .args(["reset", "--hard", &target_ref])
            .output();
        
        if reset.is_err() || !reset.as_ref().unwrap().status.success() {
            Command::new("git")
                .current_dir(repo_path)
                .args(["reset", "--hard", checkout_ref])
                .output()
                .context("Failed to reset to ref")?;
        }
        
        Ok(())
    }
    
    fn get_head_sha(&self, repo_path: &Path) -> Result<String> {
        let output = Command::new("git")
            .current_dir(repo_path)
            .args(["rev-parse", "HEAD"])
            .output()
            .context("Failed to get HEAD SHA")?;
        
        if !output.status.success() {
            return Err(anyhow!("git rev-parse failed"));
        }
        
        Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
    }
}

/// Parsed gemspec from a git repo
#[derive(Debug, Clone)]
pub struct GitGemSpec {
    pub name: String,
    pub version: String,
    pub dependencies: Vec<GitGemDependency>,
    pub summary: Option<String>,
}

#[derive(Debug, Clone)]
pub struct GitGemDependency {
    pub name: String,
    pub requirements: String,
    pub is_runtime: bool,
}

/// Parse a gemspec file from a cloned repo
pub fn parse_gemspec(repo_path: &Path, gem_name: &str) -> Result<GitGemSpec> {
    // Look for gemspec files
    let gemspec_path = find_gemspec(repo_path, gem_name)?;
    
    // Use Ruby to parse the gemspec and output JSON
    // This is the reliable way since gemspecs can contain arbitrary Ruby
    let output = Command::new("ruby")
        .arg("-e")
        .arg(format!(r#"
require 'json'
require 'rubygems'
require 'rubygems/specification'

# Stub common methods that might fail without full gem environment
module Gem
  def self.loaded_specs
    {{}}
  end
end

# Read and evaluate the gemspec
spec = Gem::Specification.load(ARGV[0])

if spec.nil?
  STDERR.puts "Failed to load gemspec"
  exit 1
end

deps = spec.dependencies.map do |d|
  {{
    name: d.name,
    requirements: d.requirement.to_s,
    is_runtime: d.type == :runtime
  }}
end

puts JSON.generate({{
  name: spec.name,
  version: spec.version.to_s,
  dependencies: deps,
  summary: spec.summary
}})
"#))
        .arg(&gemspec_path)
        .current_dir(repo_path)
        .output()
        .context("Failed to run Ruby for gemspec parsing")?;
    
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        
        // Fallback: try simple regex parsing for basic gemspecs
        let content = std::fs::read_to_string(&gemspec_path)?;
        return parse_gemspec_simple(&content, gem_name);
    }
    
    let json_str = String::from_utf8_lossy(&output.stdout);
    let parsed: serde_json::Value = serde_json::from_str(&json_str)
        .context("Failed to parse gemspec JSON output")?;
    
    let deps = parsed["dependencies"]
        .as_array()
        .unwrap_or(&vec![])
        .iter()
        .filter_map(|d| {
            Some(GitGemDependency {
                name: d["name"].as_str()?.to_string(),
                requirements: d["requirements"].as_str()?.to_string(),
                is_runtime: d["is_runtime"].as_bool().unwrap_or(true),
            })
        })
        .collect();
    
    Ok(GitGemSpec {
        name: parsed["name"].as_str().unwrap_or(gem_name).to_string(),
        version: parsed["version"].as_str().unwrap_or("0.0.0").to_string(),
        dependencies: deps,
        summary: parsed["summary"].as_str().map(|s| s.to_string()),
    })
}

/// Simple regex-based gemspec parser as fallback
fn parse_gemspec_simple(content: &str, gem_name: &str) -> Result<GitGemSpec> {
    use regex::Regex;
    
    // Extract version
    let version_re = Regex::new(r#"\.version\s*=\s*['"]([^'"]+)['"]"#)?;
    let version = version_re.captures(content)
        .and_then(|c| c.get(1))
        .map(|m| m.as_str().to_string())
        .unwrap_or_else(|| "0.0.0".to_string());
    
    // Extract name
    let name_re = Regex::new(r#"\.name\s*=\s*['"]([^'"]+)['"]"#)?;
    let name = name_re.captures(content)
        .and_then(|c| c.get(1))
        .map(|m| m.as_str().to_string())
        .unwrap_or_else(|| gem_name.to_string());
    
    // Extract dependencies (basic pattern)
    let dep_re = Regex::new(r#"add_(?:runtime_)?dependency\s*\(?['"]([^'"]+)['"](?:\s*,\s*['"]([^'"]+)['"])?\)?"#)?;
    let dev_dep_re = Regex::new(r#"add_development_dependency\s*\(?['"]([^'"]+)['"]"#)?;
    
    let mut dependencies = Vec::new();
    
    for cap in dep_re.captures_iter(content) {
        let dep_name = cap.get(1).map(|m| m.as_str().to_string()).unwrap_or_default();
        let requirements = cap.get(2).map(|m| m.as_str().to_string()).unwrap_or_else(|| ">= 0".to_string());
        if !dep_name.is_empty() {
            dependencies.push(GitGemDependency {
                name: dep_name,
                requirements,
                is_runtime: true,
            });
        }
    }
    
    Ok(GitGemSpec {
        name,
        version,
        dependencies,
        summary: None,
    })
}

/// Find the gemspec file in a repo
fn find_gemspec(repo_path: &Path, gem_name: &str) -> Result<PathBuf> {
    // First, try exact match: gem_name.gemspec
    let exact = repo_path.join(format!("{}.gemspec", gem_name));
    if exact.exists() {
        return Ok(exact);
    }
    
    // Try finding any .gemspec in root
    for entry in std::fs::read_dir(repo_path)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().map(|e| e == "gemspec").unwrap_or(false) {
            return Ok(path);
        }
    }
    
    // Check in lib/ directory (some gems put it there)
    let lib_dir = repo_path.join("lib");
    if lib_dir.exists() {
        for entry in std::fs::read_dir(&lib_dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().map(|e| e == "gemspec").unwrap_or(false) {
                return Ok(path);
            }
        }
    }
    
    Err(anyhow!(
        "No gemspec found for '{}' in {:?}",
        gem_name,
        repo_path
    ))
}

/// Get all gems from a git repo (handles multi-gem repos)
pub fn find_all_gemspecs(repo_path: &Path) -> Result<Vec<PathBuf>> {
    let mut gemspecs = Vec::new();
    
    // Check root
    for entry in std::fs::read_dir(repo_path)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().map(|e| e == "gemspec").unwrap_or(false) {
            gemspecs.push(path);
        }
    }
    
    Ok(gemspecs)
}

/// Resolved git gem ready for lockfile
#[derive(Debug, Clone)]
pub struct ResolvedGitGem {
    pub name: String,
    pub version: String,
    pub git_url: String,
    pub revision: String,  // Full SHA
    pub branch: Option<String>,
    pub tag: Option<String>,
    pub dependencies: Vec<GitGemDependency>,
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_github_shorthand() {
        let source = GitSource::from_github("rails/rails");
        assert_eq!(source.url, "https://github.com/rails/rails.git");
    }
    
    #[test]
    fn test_cache_key() {
        let source = GitSource::from_github("rails/rails");
        let key = source.cache_key();
        assert_eq!(key.len(), 16);  // 8 bytes = 16 hex chars
    }
    
    #[test]
    fn test_simple_gemspec_parse() {
        let content = r#"
Gem::Specification.new do |s|
  s.name = 'my_gem'
  s.version = '1.2.3'
  s.add_dependency 'activesupport', '>= 5.0'
  s.add_runtime_dependency 'rack'
  s.add_development_dependency 'rspec'
end
"#;
        let spec = parse_gemspec_simple(content, "my_gem").unwrap();
        assert_eq!(spec.name, "my_gem");
        assert_eq!(spec.version, "1.2.3");
        assert_eq!(spec.dependencies.len(), 2);  // Only runtime deps
    }
}
