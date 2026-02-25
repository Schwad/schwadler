# frozen_string_literal: true

Gem::Specification.new do |spec|
  spec.name = "test_gem"
  spec.version = "1.0.0"
  spec.authors = ["Test"]
  spec.summary = "A test gem"

  spec.add_dependency "rake", "~> 13.0"
  spec.add_dependency "minitest", "~> 5.0"
end
