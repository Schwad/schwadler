//! Gemfile parser - extracts gem declarations, sources, groups
//! 
//! Supports common Gemfile patterns:
//! - gem 'name', 'version'
//! - gem 'name', '>= 1.0', '< 2.0'
//! - gem 'name', group: :development
//! - source 'https://rubygems.org'
//! - group :development do ... end
//! - ruby '3.2.0'
//! - gemspec (loads dependencies from .gemspec file)

use anyhow::{anyhow, Result};
use std::collections::HashSet;
use std::path::Path;

use crate::gemspec;

#[derive(Debug, Clone)]
pub struct Gemfile {
    pub source: String,
    pub ruby_version: Option<String>,
    pub gems: Vec<GemDeclaration>,
    pub has_gemspec: bool,
    pub gemspec_options: Option<GemspecOptions>,
}

#[derive(Debug, Clone, Default)]
pub struct GemspecOptions {
    pub path: Option<String>,
    pub name: Option<String>,
    pub development_group: Option<String>,
}

#[derive(Debug, Clone)]
pub struct GemDeclaration {
    pub name: String,
    pub version_constraints: Vec<String>,
    pub groups: HashSet<String>,
    pub require: Option<RequireOption>,
    pub platforms: HashSet<String>,
    pub git: Option<String>,
    pub github: Option<String>,  // Shorthand for github repos
    pub branch: Option<String>,
    pub tag: Option<String>,
    pub ref_: Option<String>,    // Git ref (commit SHA)
    pub path: Option<String>,
    pub submodules: bool,        // Whether to init submodules
}

#[derive(Debug, Clone)]
pub enum RequireOption {
    Bool(bool),
    Names(Vec<String>),
}

impl Default for GemDeclaration {
    fn default() -> Self {
        Self {
            name: String::new(),
            version_constraints: Vec::new(),
            groups: HashSet::from(["default".to_string()]),
            require: None,
            platforms: HashSet::new(),
            git: None,
            github: None,
            branch: None,
            tag: None,
            ref_: None,
            path: None,
            submodules: false,
        }
    }
}

impl GemDeclaration {
    /// Check if this gem uses a git source
    pub fn is_git_source(&self) -> bool {
        self.git.is_some() || self.github.is_some()
    }
    
    /// Get the full git URL (resolving github shorthand if needed)
    pub fn git_url(&self) -> Option<String> {
        if let Some(ref github) = self.github {
            Some(format!("https://github.com/{}.git", github))
        } else {
            self.git.clone()
        }
    }
}

/// Parse a Gemfile into structured data
pub fn parse(content: &str) -> Result<Gemfile> {
    parse_with_gemspec_dir(content, None)
}

/// Parse a Gemfile with a specific directory for gemspec lookup
pub fn parse_with_gemspec_dir(content: &str, gemspec_dir: Option<&Path>) -> Result<Gemfile> {
    let mut gemfile = Gemfile {
        source: "https://rubygems.org".to_string(),
        ruby_version: None,
        gems: Vec::new(),
        has_gemspec: false,
        gemspec_options: None,
    };
    
    let mut current_groups: HashSet<String> = HashSet::from(["default".to_string()]);
    let mut in_group_block = false;
    
    for line in content.lines() {
        let line = line.trim();
        
        // Skip empty lines and comments
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        
        // Handle source declaration
        if line.starts_with("source") {
            if let Some(url) = extract_string_arg(line) {
                gemfile.source = url;
            }
            continue;
        }
        
        // Handle ruby version
        if line.starts_with("ruby") && !line.starts_with("rubygems") {
            if let Some(version) = extract_string_arg(line) {
                gemfile.ruby_version = Some(version);
            }
            continue;
        }
        
        // Handle gemspec directive
        if line.starts_with("gemspec") {
            gemfile.has_gemspec = true;
            gemfile.gemspec_options = Some(parse_gemspec_options(line));
            
            // If we have a directory, load the gemspec now
            if let Some(dir) = gemspec_dir {
                if let Err(e) = load_gemspec_dependencies(&mut gemfile, dir) {
                    eprintln!("Warning: Failed to load gemspec: {}", e);
                }
            }
            continue;
        }
        
        // Handle group block start
        if line.starts_with("group") && line.contains("do") {
            current_groups = extract_groups(line);
            in_group_block = true;
            continue;
        }
        
        // Handle group block end
        if line == "end" && in_group_block {
            current_groups = HashSet::from(["default".to_string()]);
            in_group_block = false;
            continue;
        }
        
        // Handle gem declaration
        if line.starts_with("gem") && !line.starts_with("gemspec") {
            if let Some(mut gem) = parse_gem_line(line)? {
                // If in a group block, use those groups
                if in_group_block {
                    gem.groups = current_groups.clone();
                }
                gemfile.gems.push(gem);
            }
        }
    }
    
    // Empty Gemfiles are valid - they produce empty lockfiles
    // (gemspec-based Gemfiles also may have no explicit gems)
    Ok(gemfile)
}

/// Parse gemspec directive options
fn parse_gemspec_options(line: &str) -> GemspecOptions {
    let mut options = GemspecOptions::default();
    
    // Parse path: option
    if let Some(path) = extract_option_value(line, "path") {
        options.path = Some(path);
    }
    
    // Parse name: option
    if let Some(name) = extract_option_value(line, "name") {
        options.name = Some(name);
    }
    
    // Parse development_group: option
    if let Some(group) = extract_option_value(line, "development_group") {
        options.development_group = Some(group);
    }
    
    options
}

/// Load dependencies from a gemspec file and add them to the Gemfile
fn load_gemspec_dependencies(gemfile: &mut Gemfile, dir: &Path) -> Result<()> {
    let gemspec_path = if let Some(ref opts) = gemfile.gemspec_options {
        if let Some(ref path) = opts.path {
            let path_dir = dir.join(path);
            if let Some(ref name) = opts.name {
                path_dir.join(format!("{}.gemspec", name))
            } else {
                gemspec::find_gemspec(&path_dir)?
            }
        } else if let Some(ref name) = opts.name {
            dir.join(format!("{}.gemspec", name))
        } else {
            gemspec::find_gemspec(dir)?
        }
    } else {
        gemspec::find_gemspec(dir)?
    };
    
    let gemspec_content = std::fs::read_to_string(&gemspec_path)?;
    let spec = gemspec::parse(&gemspec_content)?;
    
    // Determine development group
    let dev_group = gemfile.gemspec_options
        .as_ref()
        .and_then(|o| o.development_group.clone())
        .unwrap_or_else(|| "development".to_string());
    
    // Add runtime dependencies
    for dep in spec.runtime_dependencies {
        let mut gem = GemDeclaration::default();
        gem.name = dep.name;
        gem.version_constraints = dep.version_constraints;
        gem.groups = HashSet::from(["default".to_string()]);
        gemfile.gems.push(gem);
    }
    
    // Add development dependencies
    for dep in spec.development_dependencies {
        let mut gem = GemDeclaration::default();
        gem.name = dep.name;
        gem.version_constraints = dep.version_constraints;
        gem.groups = HashSet::from([dev_group.clone()]);
        gemfile.gems.push(gem);
    }
    
    Ok(())
}

/// Extract a quoted string argument from a line
fn extract_string_arg(line: &str) -> Option<String> {
    // Handle both single and double quotes
    let start_single = line.find('\'');
    let start_double = line.find('"');
    
    let (start, quote) = match (start_single, start_double) {
        (Some(s), Some(d)) if s < d => (s, '\''),
        (Some(s), Some(d)) if d < s => (d, '"'),
        (Some(s), None) => (s, '\''),
        (None, Some(d)) => (d, '"'),
        _ => return None,
    };
    
    let rest = &line[start + 1..];
    let end = rest.find(quote)?;
    Some(rest[..end].to_string())
}

/// Extract groups from a group declaration
fn extract_groups(line: &str) -> HashSet<String> {
    let mut groups = HashSet::new();
    
    // Match :symbol patterns
    let mut i = 0;
    let chars: Vec<char> = line.chars().collect();
    
    while i < chars.len() {
        if chars[i] == ':' && (i == 0 || !chars[i-1].is_alphabetic()) {
            // Start of a symbol
            let start = i + 1;
            let mut end = start;
            while end < chars.len() && (chars[end].is_alphanumeric() || chars[end] == '_') {
                end += 1;
            }
            if end > start {
                let symbol: String = chars[start..end].iter().collect();
                if symbol != "do" {  // Skip 'do' keyword
                    groups.insert(symbol);
                }
            }
            i = end;
        } else {
            i += 1;
        }
    }
    
    if groups.is_empty() {
        groups.insert("default".to_string());
    }
    
    groups
}

/// Parse a single gem declaration line
fn parse_gem_line(line: &str) -> Result<Option<GemDeclaration>> {
    // Must start with gem
    if !line.starts_with("gem") {
        return Ok(None);
    }
    
    let mut gem = GemDeclaration::default();
    
    // Extract the gem name (first string argument)
    let after_gem = line.strip_prefix("gem").unwrap().trim();
    
    // Find all quoted strings
    let mut strings = Vec::new();
    let mut in_string = false;
    let mut quote_char = '"';
    let mut current_string = String::new();
    
    for c in after_gem.chars() {
        if !in_string {
            if c == '\'' || c == '"' {
                in_string = true;
                quote_char = c;
                current_string.clear();
            }
        } else {
            if c == quote_char {
                strings.push(current_string.clone());
                in_string = false;
            } else {
                current_string.push(c);
            }
        }
    }
    
    if strings.is_empty() {
        return Ok(None);
    }
    
    gem.name = strings[0].clone();
    
    // Remaining strings are version constraints
    for s in strings.iter().skip(1) {
        // Check if it looks like a version constraint
        let trimmed = s.trim();
        if trimmed.starts_with(|c: char| c == '>' || c == '<' || c == '=' || c == '~' || c == '!') {
            // Has an operator, use as-is
            gem.version_constraints.push(s.clone());
        } else if trimmed.starts_with(|c: char| c.is_ascii_digit()) {
            // Bare version string like '2.2.0' - treat as exact match
            gem.version_constraints.push(format!("= {}", s));
        }
    }
    
    // Parse options (group:, require:, platform:, git:, etc.)
    
    // Handle inline group
    if line.contains("group:") || line.contains(":group =>") {
        gem.groups = extract_inline_groups(line);
    }
    
    // Handle git
    if let Some(git_url) = extract_option_value(line, "git") {
        gem.git = Some(git_url);
    }
    
    // Handle github shorthand
    if let Some(github_repo) = extract_option_value(line, "github") {
        gem.github = Some(github_repo);
    }
    
    // Handle branch
    if let Some(branch) = extract_option_value(line, "branch") {
        gem.branch = Some(branch);
    }
    
    // Handle tag
    if let Some(tag) = extract_option_value(line, "tag") {
        gem.tag = Some(tag);
    }
    
    // Handle ref
    if let Some(ref_) = extract_option_value(line, "ref") {
        gem.ref_ = Some(ref_);
    }
    
    // Handle path
    if let Some(path) = extract_option_value(line, "path") {
        gem.path = Some(path);
    }
    
    // Handle submodules
    if line.contains("submodules: true") || line.contains(":submodules => true") {
        gem.submodules = true;
    }
    
    // Handle require: false
    if line.contains("require: false") || line.contains("require => false") {
        gem.require = Some(RequireOption::Bool(false));
    }
    
    Ok(Some(gem))
}

/// Extract inline groups from gem declaration
fn extract_inline_groups(line: &str) -> HashSet<String> {
    let mut groups = HashSet::new();
    
    // Find position after 'group:' or ':group =>'
    let group_start = line.find("group:")
        .or_else(|| line.find(":group =>"))
        .unwrap_or(line.len());
    
    let rest = &line[group_start..];
    
    // Look for :symbol or [:symbol, :symbol2]
    let mut i = 0;
    let chars: Vec<char> = rest.chars().collect();
    
    while i < chars.len() {
        if chars[i] == ':' {
            let start = i + 1;
            let mut end = start;
            while end < chars.len() && (chars[end].is_alphanumeric() || chars[end] == '_') {
                end += 1;
            }
            if end > start {
                let symbol: String = chars[start..end].iter().collect();
                if symbol != "group" {  // Skip 'group' itself
                    groups.insert(symbol);
                }
            }
            i = end;
        } else {
            i += 1;
        }
    }
    
    if groups.is_empty() {
        groups.insert("default".to_string());
    }
    
    groups
}

/// Extract a specific option value
fn extract_option_value(line: &str, option: &str) -> Option<String> {
    // Try new-style hash syntax: option: 'value'
    let pattern1 = format!("{}:", option);
    // Try old-style hash syntax: :option => 'value'  
    let pattern2 = format!(":{}=>", option.replace(" ", ""));
    let pattern3 = format!(":{} =>", option);
    
    let start = line.find(&pattern1)
        .or_else(|| line.find(&pattern2))
        .or_else(|| line.find(&pattern3))?;
    
    let rest = &line[start..];
    extract_string_arg(rest)
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_simple_gemfile() {
        let content = r#"
source 'https://rubygems.org'

gem 'rails', '~> 7.0'
gem 'pg'
"#;
        let gemfile = parse(content).unwrap();
        assert_eq!(gemfile.source, "https://rubygems.org");
        assert_eq!(gemfile.gems.len(), 2);
        assert_eq!(gemfile.gems[0].name, "rails");
        assert_eq!(gemfile.gems[0].version_constraints, vec!["~> 7.0"]);
        assert_eq!(gemfile.gems[1].name, "pg");
    }
    
    #[test]
    fn test_groups() {
        let content = r#"
source 'https://rubygems.org'

gem 'rails'

group :development, :test do
  gem 'rspec'
  gem 'pry'
end

group :production do
  gem 'newrelic_rpm'
end
"#;
        let gemfile = parse(content).unwrap();
        assert_eq!(gemfile.gems.len(), 4);
        
        // rspec should be in development and test
        let rspec = gemfile.gems.iter().find(|g| g.name == "rspec").unwrap();
        assert!(rspec.groups.contains("development"));
        assert!(rspec.groups.contains("test"));
        
        // newrelic_rpm should be in production
        let newrelic = gemfile.gems.iter().find(|g| g.name == "newrelic_rpm").unwrap();
        assert!(newrelic.groups.contains("production"));
    }
    
    #[test]
    fn test_git_sources() {
        let content = r#"
source 'https://rubygems.org'

gem 'rails'
gem 'puma', github: 'puma/puma', branch: 'master'
gem 'rack', git: 'https://github.com/rack/rack.git', tag: 'v2.0.0'
gem 'sinatra', github: 'sinatra/sinatra', ref: 'abc123'
"#;
        let gemfile = parse(content).unwrap();
        assert_eq!(gemfile.gems.len(), 4);
        
        // puma with github shorthand
        let puma = gemfile.gems.iter().find(|g| g.name == "puma").unwrap();
        assert_eq!(puma.github, Some("puma/puma".to_string()));
        assert_eq!(puma.branch, Some("master".to_string()));
        assert!(puma.is_git_source());
        assert_eq!(puma.git_url(), Some("https://github.com/puma/puma.git".to_string()));
        
        // rack with full git URL
        let rack = gemfile.gems.iter().find(|g| g.name == "rack").unwrap();
        assert_eq!(rack.git, Some("https://github.com/rack/rack.git".to_string()));
        assert_eq!(rack.tag, Some("v2.0.0".to_string()));
        
        // sinatra with ref
        let sinatra = gemfile.gems.iter().find(|g| g.name == "sinatra").unwrap();
        assert_eq!(sinatra.ref_, Some("abc123".to_string()));
    }
}
