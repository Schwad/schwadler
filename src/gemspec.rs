//! Gemspec parser - extracts dependencies from .gemspec files
//!
//! Supports common gemspec patterns:
//! - spec.add_dependency "name", "version"
//! - spec.add_runtime_dependency "name", ">= 1.0"
//! - spec.add_development_dependency "name", "~> 2.0"
//! - s.add_dependency "name" (shorthand variable)

use anyhow::{anyhow, Result};
use std::path::Path;

#[derive(Debug, Clone)]
pub struct Gemspec {
    pub name: String,
    pub runtime_dependencies: Vec<GemspecDependency>,
    pub development_dependencies: Vec<GemspecDependency>,
}

#[derive(Debug, Clone)]
pub struct GemspecDependency {
    pub name: String,
    pub version_constraints: Vec<String>,
}

/// Find the gemspec file in a directory
pub fn find_gemspec(dir: &Path) -> Result<std::path::PathBuf> {
    // Look for .gemspec files
    let entries = std::fs::read_dir(dir)?;
    
    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        if let Some(ext) = path.extension() {
            if ext == "gemspec" {
                return Ok(path);
            }
        }
    }
    
    Err(anyhow!("No .gemspec file found in {:?}", dir))
}

/// Parse a gemspec file
pub fn parse(content: &str) -> Result<Gemspec> {
    let mut gemspec = Gemspec {
        name: String::new(),
        runtime_dependencies: Vec::new(),
        development_dependencies: Vec::new(),
    };
    
    for line in content.lines() {
        let line = line.trim();
        
        // Skip empty lines and comments
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        
        // Extract gem name from spec.name = "name" or s.name = "name"
        if (line.contains(".name") && line.contains('=')) && !line.contains("homepage") {
            // Avoid matching homepage_uri, source_code_uri, etc.
            if !line.contains("_uri") && !line.contains("mfa") {
                if let Some(name) = extract_string_value(line) {
                    gemspec.name = name;
                }
            }
        }
        
        // Parse add_dependency / add_runtime_dependency
        if line.contains("add_dependency") || line.contains("add_runtime_dependency") {
            // Skip development dependencies
            if line.contains("add_development_dependency") {
                if let Some(dep) = parse_dependency_line(line) {
                    gemspec.development_dependencies.push(dep);
                }
            } else if let Some(dep) = parse_dependency_line(line) {
                gemspec.runtime_dependencies.push(dep);
            }
            continue;
        }
        
        // Parse add_development_dependency explicitly  
        if line.contains("add_development_dependency") {
            if let Some(dep) = parse_dependency_line(line) {
                gemspec.development_dependencies.push(dep);
            }
        }
    }
    
    Ok(gemspec)
}

/// Extract a string value from assignment like `spec.name = "rails"`
fn extract_string_value(line: &str) -> Option<String> {
    // Find position after =
    let eq_pos = line.find('=')?;
    let rest = &line[eq_pos + 1..];
    extract_quoted_string(rest)
}

/// Extract a quoted string (single or double quotes)
fn extract_quoted_string(text: &str) -> Option<String> {
    let text = text.trim();
    
    // Find opening quote
    let (start_pos, quote_char) = {
        let single = text.find('\'');
        let double = text.find('"');
        match (single, double) {
            (Some(s), Some(d)) if s < d => (s, '\''),
            (Some(s), Some(d)) if d < s => (d, '"'),
            (Some(s), None) => (s, '\''),
            (None, Some(d)) => (d, '"'),
            _ => return None,
        }
    };
    
    let rest = &text[start_pos + 1..];
    let end_pos = rest.find(quote_char)?;
    Some(rest[..end_pos].to_string())
}

/// Parse a dependency line like `spec.add_dependency "rails", "~> 7.0", ">= 7.0.1"`
fn parse_dependency_line(line: &str) -> Option<GemspecDependency> {
    // Extract all quoted strings from the line
    let strings = extract_all_quoted_strings(line);
    
    if strings.is_empty() {
        return None;
    }
    
    let name = strings[0].clone();
    let version_constraints: Vec<String> = strings[1..]
        .iter()
        .filter(|s| is_version_constraint(s))
        .cloned()
        .collect();
    
    Some(GemspecDependency {
        name,
        version_constraints,
    })
}

/// Extract all quoted strings from a line
fn extract_all_quoted_strings(text: &str) -> Vec<String> {
    let mut strings = Vec::new();
    let chars: Vec<char> = text.chars().collect();
    let mut i = 0;
    
    while i < chars.len() {
        if chars[i] == '\'' || chars[i] == '"' {
            let quote_char = chars[i];
            let start = i + 1;
            let mut end = start;
            
            while end < chars.len() && chars[end] != quote_char {
                end += 1;
            }
            
            if end > start && end < chars.len() {
                let s: String = chars[start..end].iter().collect();
                strings.push(s);
            }
            
            i = end + 1;
        } else {
            i += 1;
        }
    }
    
    strings
}

/// Check if a string looks like a version constraint
fn is_version_constraint(s: &str) -> bool {
    let trimmed = s.trim();
    // Version constraints start with digit, ~, >, <, =, or !
    trimmed.starts_with(|c: char| {
        c.is_ascii_digit() || c == '~' || c == '>' || c == '<' || c == '=' || c == '!'
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_parse_simple_gemspec() {
        let content = r#"
Gem::Specification.new do |spec|
  spec.name = "my_gem"
  spec.version = "1.0.0"
  
  spec.add_dependency "rails", "~> 7.0"
  spec.add_dependency "pg"
  spec.add_development_dependency "rspec", "~> 3.0"
end
"#;
        let gemspec = parse(content).unwrap();
        assert_eq!(gemspec.name, "my_gem");
        assert_eq!(gemspec.runtime_dependencies.len(), 2);
        assert_eq!(gemspec.runtime_dependencies[0].name, "rails");
        assert_eq!(gemspec.runtime_dependencies[0].version_constraints, vec!["~> 7.0"]);
        assert_eq!(gemspec.runtime_dependencies[1].name, "pg");
        assert!(gemspec.runtime_dependencies[1].version_constraints.is_empty());
        assert_eq!(gemspec.development_dependencies.len(), 1);
        assert_eq!(gemspec.development_dependencies[0].name, "rspec");
    }
    
    #[test]
    fn test_parse_rails_style_gemspec() {
        let content = r#"
Gem::Specification.new do |s|
  s.name        = "rails"
  s.version     = "7.2.0"
  
  s.add_dependency "activesupport", "7.2.0"
  s.add_dependency "actionpack",    "7.2.0"
  s.add_dependency "bundler", ">= 1.15.0"
end
"#;
        let gemspec = parse(content).unwrap();
        assert_eq!(gemspec.name, "rails");
        assert_eq!(gemspec.runtime_dependencies.len(), 3);
        assert_eq!(gemspec.runtime_dependencies[0].name, "activesupport");
        assert_eq!(gemspec.runtime_dependencies[2].name, "bundler");
        assert_eq!(gemspec.runtime_dependencies[2].version_constraints, vec![">= 1.15.0"]);
    }
    
    #[test]
    fn test_parse_multiple_constraints() {
        let content = r#"
Gem::Specification.new do |spec|
  spec.name = "test"
  spec.add_dependency "rubocop", ">= 1.48", "< 3.0"
end
"#;
        let gemspec = parse(content).unwrap();
        assert_eq!(gemspec.runtime_dependencies[0].name, "rubocop");
        assert_eq!(gemspec.runtime_dependencies[0].version_constraints, vec![">= 1.48", "< 3.0"]);
    }
}
