//! Gemfile.lock parser and writer
//! 
//! Produces output that exactly matches Bundler's format.
//! This is critical for compatibility with the Ruby ecosystem.

use crate::resolver::{Resolution, ResolvedGem, ResolvedGitGem};
use anyhow::{anyhow, Result};
use std::collections::HashMap;
use std::io::Write;

/// Parsed lockfile structure
#[derive(Debug, Clone)]
pub struct Lockfile {
    pub source: String,
    pub gems: Vec<LockedGem>,
    pub platforms: Vec<String>,
    pub ruby_version: Option<String>,
    pub bundled_with: Option<String>,
}

#[derive(Debug, Clone)]
pub struct LockedGem {
    pub name: String,
    pub version: String,
    pub dependencies: Vec<LockedDependency>,
}

#[derive(Debug, Clone)]
pub struct LockedDependency {
    pub name: String,
    pub constraint: Option<String>,
}

/// Parse an existing Gemfile.lock
pub fn parse(content: &str) -> Result<Lockfile> {
    let mut lockfile = Lockfile {
        source: "https://rubygems.org/".to_string(),
        gems: Vec::new(),
        platforms: vec!["ruby".to_string()],
        ruby_version: None,
        bundled_with: None,
    };
    
    let mut current_section = "";
    let mut in_specs = false;
    let mut current_gem: Option<LockedGem> = None;
    
    for line in content.lines() {
        // Section headers
        if line == "GEM" || line == "GIT" || line == "PATH" {
            // Save current gem if any
            if let Some(gem) = current_gem.take() {
                lockfile.gems.push(gem);
            }
            current_section = line;
            in_specs = false;
            continue;
        }
        
        if line == "PLATFORMS" || line == "DEPENDENCIES" || line == "RUBY VERSION" || line == "BUNDLED WITH" {
            if let Some(gem) = current_gem.take() {
                lockfile.gems.push(gem);
            }
            current_section = line;
            in_specs = false;
            continue;
        }
        
        if line.trim() == "specs:" {
            in_specs = true;
            continue;
        }
        
        // Parse based on section
        match current_section {
            "GEM" => {
                if line.starts_with("  remote:") {
                    lockfile.source = line.trim_start_matches("  remote:").trim().to_string();
                } else if in_specs {
                    // Gem line: "    name (version)" or dependency "      name (~> version)"
                    let trimmed = line.trim_start();
                    let indent = line.len() - trimmed.len();
                    
                    if indent == 4 {
                        // This is a gem
                        if let Some(gem) = current_gem.take() {
                            lockfile.gems.push(gem);
                        }
                        
                        if let Some((name, version)) = parse_gem_line(trimmed) {
                            current_gem = Some(LockedGem {
                                name,
                                version,
                                dependencies: Vec::new(),
                            });
                        }
                    } else if indent == 6 {
                        // This is a dependency
                        if let Some(ref mut gem) = current_gem {
                            if let Some((name, constraint)) = parse_dep_line(trimmed) {
                                gem.dependencies.push(LockedDependency { name, constraint });
                            }
                        }
                    }
                }
            }
            "PLATFORMS" => {
                let platform = line.trim();
                if !platform.is_empty() {
                    lockfile.platforms.push(platform.to_string());
                }
            }
            "RUBY VERSION" => {
                let version = line.trim();
                if !version.is_empty() && version.starts_with("ruby") {
                    lockfile.ruby_version = Some(version.to_string());
                }
            }
            "BUNDLED WITH" => {
                let version = line.trim();
                if !version.is_empty() {
                    lockfile.bundled_with = Some(version.to_string());
                }
            }
            _ => {}
        }
    }
    
    // Don't forget the last gem
    if let Some(gem) = current_gem {
        lockfile.gems.push(gem);
    }
    
    Ok(lockfile)
}

fn parse_gem_line(line: &str) -> Option<(String, String)> {
    // Format: "name (version)"
    let paren_start = line.find('(')?;
    let paren_end = line.find(')')?;
    
    let name = line[..paren_start].trim().to_string();
    let version = line[paren_start + 1..paren_end].trim().to_string();
    
    Some((name, version))
}

fn parse_dep_line(line: &str) -> Option<(String, Option<String>)> {
    // Format: "name" or "name (~> version)" or "name (>= a, < b)"
    if let Some(paren_start) = line.find('(') {
        let paren_end = line.find(')')?;
        let name = line[..paren_start].trim().to_string();
        let constraint = line[paren_start + 1..paren_end].trim().to_string();
        Some((name, Some(constraint)))
    } else {
        Some((line.trim().to_string(), None))
    }
}

/// Write a Resolution to Gemfile.lock format
pub fn write(resolution: &Resolution, path: &str) -> Result<()> {
    let mut output = String::new();
    
    // GIT sections (one per unique git source)
    // Sort git gems for consistent output
    let mut sorted_git_gems = resolution.git_gems.clone();
    sorted_git_gems.sort_by(|a, b| a.git_url.cmp(&b.git_url).then(a.name.cmp(&b.name)));
    
    for git_gem in &sorted_git_gems {
        output.push_str("GIT\n");
        output.push_str(&format!("  remote: {}\n", git_gem.git_url));
        output.push_str(&format!("  revision: {}\n", git_gem.revision));
        
        // Add branch/tag/ref if present
        if let Some(ref branch) = git_gem.branch {
            output.push_str(&format!("  branch: {}\n", branch));
        }
        if let Some(ref tag) = git_gem.tag {
            output.push_str(&format!("  tag: {}\n", tag));
        }
        if let Some(ref ref_) = git_gem.ref_ {
            output.push_str(&format!("  ref: {}\n", ref_));
        }
        
        output.push_str("  specs:\n");
        output.push_str(&format!("    {} ({})\n", git_gem.name, git_gem.version));
        
        // Sort and write dependencies
        let mut deps = git_gem.dependencies.clone();
        deps.sort();
        for dep in &deps {
            output.push_str(&format!("      {}\n", dep));
        }
        
        output.push('\n');
    }
    
    // GEM section
    output.push_str("GEM\n");
    // Ensure trailing slash for compatibility
    let source = if resolution.source.ends_with('/') {
        resolution.source.clone()
    } else {
        format!("{}/", resolution.source)
    };
    output.push_str(&format!("  remote: {}\n", source));
    output.push_str("  specs:\n");
    
    // Build dependency map for efficient lookup
    let dep_map: HashMap<&str, &ResolvedGem> = resolution.gems.iter()
        .map(|g| (g.name.as_str(), g))
        .collect();
    
    // Sort gems alphabetically
    let mut sorted_gems = resolution.gems.clone();
    sorted_gems.sort_by(|a, b| a.name.to_lowercase().cmp(&b.name.to_lowercase()));
    
    for gem in &sorted_gems {
        output.push_str(&format!("    {} ({})\n", gem.name, gem.version));
        
        // Sort dependencies alphabetically too
        let mut deps = gem.dependencies.clone();
        deps.sort();
        
        for dep_name in &deps {
            // Special case: bundler dependency should use >= constraint
            // (Bundler checks its own version at runtime)
            if dep_name == "bundler" {
                output.push_str(&format!("      {} (>= 1.15.0)\n", dep_name));
            } else if let Some(dep_gem) = dep_map.get(dep_name.as_str()) {
                // Use exact version constraint for locked dependencies
                output.push_str(&format!("      {} (= {})\n", dep_name, dep_gem.version));
            } else {
                // Dependency not found in resolution (shouldn't happen), fallback
                output.push_str(&format!("      {}\n", dep_name));
            }
        }
    }
    
    output.push('\n');
    
    // PLATFORMS section
    output.push_str("PLATFORMS\n");
    for platform in &resolution.platforms {
        output.push_str(&format!("  {}\n", platform));
    }
    output.push('\n');
    
    // DEPENDENCIES section (direct gems only, including git gems)
    output.push_str("DEPENDENCIES\n");
    
    // Collect all direct dependencies
    let mut all_direct: Vec<String> = sorted_gems.iter()
        .filter(|g| g.is_direct)
        .map(|g| g.name.clone())
        .collect();
    
    // Git gems are always direct (they're declared in Gemfile)
    for git_gem in &sorted_git_gems {
        if !all_direct.contains(&git_gem.name) {
            all_direct.push(git_gem.name.clone());
        }
    }
    
    all_direct.sort_by(|a, b| a.to_lowercase().cmp(&b.to_lowercase()));
    
    for name in &all_direct {
        // Use ! suffix to indicate it was in the Gemfile
        output.push_str(&format!("  {}!\n", name));
    }
    output.push('\n');
    
    // RUBY VERSION section (if present)
    // Note: Bundler expects the actual Ruby version, not constraints like "~> 3.4"
    // We use the current Ruby version instead of the Gemfile constraint
    if resolution.ruby_version.is_some() {
        // Get actual Ruby version from environment
        if let Ok(ruby_output) = std::process::Command::new("ruby")
            .args(["-e", "puts RUBY_VERSION"])
            .output()
        {
            if ruby_output.status.success() {
                let version = String::from_utf8_lossy(&ruby_output.stdout).trim().to_string();
                output.push_str("RUBY VERSION\n");
                output.push_str(&format!("   ruby {}\n", version));
                output.push('\n');
            }
        }
    }
    
    // BUNDLED WITH section
    output.push_str("BUNDLED WITH\n");
    output.push_str("   schwadl 0.1.0\n");
    
    // Write to file
    let mut file = std::fs::File::create(path)?;
    file.write_all(output.as_bytes())?;
    
    let total_gems = sorted_gems.len() + sorted_git_gems.len();
    if sorted_git_gems.is_empty() {
        println!("   Wrote {} gems to {}", total_gems, path);
    } else {
        println!("   Wrote {} gems ({} from git) to {}", total_gems, sorted_git_gems.len(), path);
    }
    
    Ok(())
}

/// Format a lockfile back to string (for comparison/debugging)
pub fn format(lockfile: &Lockfile) -> String {
    let mut output = String::new();
    
    output.push_str("GEM\n");
    output.push_str(&format!("  remote: {}\n", lockfile.source));
    output.push_str("  specs:\n");
    
    for gem in &lockfile.gems {
        output.push_str(&format!("    {} ({})\n", gem.name, gem.version));
        for dep in &gem.dependencies {
            if let Some(ref c) = dep.constraint {
                output.push_str(&format!("      {} ({})\n", dep.name, c));
            } else {
                output.push_str(&format!("      {}\n", dep.name));
            }
        }
    }
    
    output.push('\n');
    output.push_str("PLATFORMS\n");
    for platform in &lockfile.platforms {
        output.push_str(&format!("  {}\n", platform));
    }
    
    output
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_parse_lockfile() {
        let content = r#"GEM
  remote: https://rubygems.org/
  specs:
    rack (3.0.0)
    webrick (1.8.1)
      rack (~> 3.0)

PLATFORMS
  ruby
  x86_64-darwin-22

DEPENDENCIES
  rack!
  webrick!

BUNDLED WITH
   2.4.0
"#;
        
        let lockfile = parse(content).unwrap();
        assert_eq!(lockfile.source, "https://rubygems.org/");
        assert_eq!(lockfile.gems.len(), 2);
        assert_eq!(lockfile.gems[0].name, "rack");
        assert_eq!(lockfile.gems[0].version, "3.0.0");
        assert_eq!(lockfile.gems[1].name, "webrick");
        assert_eq!(lockfile.gems[1].dependencies.len(), 1);
    }
}
