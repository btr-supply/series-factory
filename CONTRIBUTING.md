# Contributing to Series Factory

We welcome contributions to Series Factory! This document provides guidelines for contributing to the project.

## Table of Contents
- [Getting Started](#getting-started)
- [Development Setup](#development-setup)
- [How to Contribute](#how-to-contribute)
- [Code Style](#code-style)
- [Testing](#testing)
- [Pull Request Process](#pull-request-process)
- [Adding New Features](#adding-new-features)

## Getting Started

1. Fork the repository on GitHub
2. Clone your fork locally:
   ```bash
   git clone https://github.com/YOUR_USERNAME/series_factory.git
   cd series_factory
   ```
3. Add the upstream repository as a remote:
   ```bash
   git remote add upstream https://github.com/ORIGINAL_OWNER/series_factory.git
   ```

## Development Setup

### Prerequisites
- Rust 1.70 or higher
- Cargo
- Git

### Building the Project
```bash
cargo build
cargo test
```

### Running Tests
```bash
# Run all tests
cargo test

# Run tests with output
cargo test -- --nocapture

# Run specific test
cargo test test_name
```

## How to Contribute

### Reporting Bugs
- Use the GitHub issue tracker
- Check if the issue already exists
- Include:
  - Clear description of the bug
  - Steps to reproduce
  - Expected behavior
  - Actual behavior
  - System information (OS, Rust version)
  - Relevant logs or error messages

### Suggesting Features
- Open a GitHub issue with the "enhancement" label
- Clearly describe the feature and its use case
- Explain why this feature would be useful
- Include examples if possible

### Code Contributions

1. **Create a branch** for your feature or bugfix:
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Make your changes**:
   - Write clean, documented code
   - Follow the existing code style
   - Add tests for new functionality
   - Update documentation as needed

3. **Commit your changes**:
   ```bash
   git add .
   git commit -m "[feat] Add new feature X"
   ```

   Follow commit format `[type] Description` (capitalized after tag):
   - `[feat]` for new features
   - `[fix]` for bug fixes
   - `[refac]` for code refactoring
   - `[docs]` for documentation changes
   - `[ops]` for CI/CD, dependencies, infrastructure

4. **Keep your branch updated**:
   ```bash
   git fetch upstream
   git rebase upstream/main
   ```

## Code Style

### Rust Style Guidelines
- Follow standard Rust naming conventions
- Use `rustfmt` for formatting:
  ```bash
  cargo fmt
  ```
- Use `clippy` for linting:
  ```bash
  cargo clippy -- -D warnings
  ```
- Document public APIs with doc comments
- Keep functions small and focused
- Prefer explicit error handling over `unwrap()`

### Documentation
- Add doc comments for all public items
- Include examples in doc comments where appropriate
- Update README.md for user-facing changes
- Document complex algorithms or business logic

## Testing

### Test Requirements
- All new features must include tests
- Bug fixes should include a test that reproduces the bug
- Maintain or improve code coverage
- Tests should be deterministic and fast

### Test Organization
- Unit tests go in the same file as the code
- Integration tests go in `tests/` directory
- Use descriptive test names
- Mock external dependencies (network, filesystem)

### Performance Tests
For performance-critical code:
- Add benchmarks using `criterion`
- Document performance expectations
- Compare against baseline performance

## Pull Request Process

1. **Before submitting**:
   - Ensure all tests pass
   - Run `cargo fmt` and `cargo clippy`
   - Update documentation
   - Rebase on latest main branch

2. **PR Description**:
   - Reference any related issues
   - Describe what changes were made
   - Explain why the changes were necessary
   - Include screenshots for UI changes
   - List any breaking changes

3. **Review Process**:
   - Address reviewer feedback promptly
   - Keep the PR focused on a single concern
   - Be open to suggestions and criticism
   - Maintain a professional tone

4. **After Approval**:
   - Squash commits if requested
   - Ensure CI passes
   - PR will be merged by maintainers

## Adding New Features

### Adding a New Data Source
1. Create a new module in `src/sources/`
2. Implement the `TickSource` trait
3. Add source parsing in `src/cli.rs`
4. Update documentation with examples
5. Add integration tests

### Adding a New Generative Model
1. Add model variant to `GenerativeModel` enum
2. Implement generation logic in `src/sources/synthetic.rs`
3. Add parameter parsing in `src/cli.rs`
4. Document the model in README.md
5. Add unit tests for the model

### Adding New Aggregate Fields
1. Update `Aggregate` struct in `src/types.rs`
2. Implement calculation in `src/aggregation/mod.rs`
3. Update Parquet schema in `src/cache/mod.rs`
4. Document the new field in README.md
5. Add tests for the calculation

## Code of Conduct

### Our Standards
- Be respectful and inclusive
- Welcome newcomers and help them get started
- Focus on constructive criticism
- Accept responsibility for mistakes
- Prioritize the community's best interests

### Unacceptable Behavior
- Harassment or discrimination
- Trolling or insulting comments
- Personal or political attacks
- Public or private harassment
- Publishing private information without permission

## Questions?

If you have questions about contributing:
1. Check existing documentation
2. Search closed issues
3. Ask in discussions
4. Open a new issue with the "question" label

Thank you for contributing to Series Factory!

## Automation Policy

This repository makes use of automation tools for code generation, testing, and CI/CD. Commits created through automation do not include AI tool attribution in commit messages or code comments. All automated contributions follow the same code review and quality standards as manual contributions.