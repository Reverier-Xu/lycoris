# AGENTS.md

## Guidelines for AI Coding Agents in This Rust Repository

This document defines **non-negotiable standards and workflows** for all AI agents (and human contributors) working on this Rust codebase. All requirements below must be followed for every task, commit, and pull request. No exceptions are permitted without explicit user approval.

---

## 1. Core Fundamental Principles

- **Zero-Warning Mandate**: All code must pass all quality checks with **zero warnings, errors, or pending suggestions** before any commit or task completion.
- **Workspace Cleanliness**: The working directory must be left in a pristine state after every task. No leftover temporary files, uncommitted changes, or untracked assets are permitted.
- **Local Tool Priority**: Always prefer pre-installed native system tools for file system operations, search, and inspection before falling back to generic cross-platform commands.
- **Atomic Changes**: One logical change per commit. Large refactors or feature implementations must be split into incremental, reviewable commits that each pass all quality gates.

---

## 2. Preferred Native Local Tooling

You **MUST prioritize using the following pre-installed local tools** for all relevant operations. Only use fallback commands if these tools are unavailable, and confirm availability first.

| Tool           | Purpose                         | Mandatory Usage Scenarios                                                                                            |
| -------------- | ------------------------------- | -------------------------------------------------------------------------------------------------------------------- |
| `fd`           | Fast, user-friendly file search | Replace all uses of `find` for file discovery, pattern matching, and recursive file listing.                         |
| `ripgrep` (rg) | High-performance regex search   | Replace all uses of `grep` for content search, pattern matching, and codebase scanning.                              |
| `bat`          | Syntax-aware file preview       | Replace all uses of `cat` for file content inspection, with automatic syntax highlighting for Rust and config files. |
| `eza`          | Modern directory listing        | Replace all uses of `ls` for directory navigation and file system inspection.                                        |
| `fish`         | Modern shell                    | Use `fish` for all shell command execution, leveraging its built-in auto-completion and safety features.             |

---

## 3. Rust Code Quality & Formatting Standards

All Rust code changes **MUST pass the following checks before any commit**. No code may be committed with unresolved lints, formatting issues, or warnings.

### 3.1 Code Formatting

- **Mandatory Format Command**: Use `cargo +nightly fmt --all` for all code formatting. This ensures consistent formatting across the entire workspace using the nightly Rust toolchain.
- **Pre-Commit Validation**: Run `cargo +nightly fmt --all -- --check` before every commit to confirm no formatting violations exist. Fix all issues before proceeding.
- **Rust Edition Compliance**: All code must adhere to the latest stable Rust edition standards, with full compatibility with the nightly toolchain for formatting.

### 3.2 Linting & Static Analysis

- **Mandatory Clippy Check**: Run the full Clippy suite with the following command:
  ```bash
  cargo +nightly clippy --workspace --all-targets --all-features -- -D warnings
  ```
- **Zero-Warning Enforcement**: All Clippy warnings are treated as errors (`-D warnings`). You **MUST fix every Clippy lint** before committing code. This includes pedantic and nursery lints where enabled in the workspace configuration.
- **Unsafe Code Restrictions**: `unsafe` Rust is strictly forbidden unless explicitly requested by the user. Any approved `unsafe` code must include comprehensive safety comments documenting all invariants.
- **Error Handling**: Never use `.unwrap()` or `.expect()` in production code. Use proper error propagation with `thiserror` (for libraries) or `anyhow` (for applications), with meaningful context for all error paths.

### 3.3 Testing & Validation

- **Test Execution**: Run `cargo test --workspace --all-features` to ensure all unit, integration, and doc tests pass before every commit.
- **Test Coverage**: All new features and bug fixes must include corresponding test coverage. Tests must live alongside the code they validate (in `#[cfg(test)]` modules).

---

## 4. CI/CD Workflow Testing

For all changes to GitHub Actions workflows (`.github/workflows/` directory):

- **Mandatory Local Validation**: You **MUST test all workflow changes locally using the pre-installed `act` tool** before committing or pushing to the remote repository.
- **Test Command**: Use `act` to run the full workflow suite locally, replicating the CI environment exactly. Validate that all jobs pass successfully with your changes.
- **No Broken CI**: Never commit workflow changes that have not been validated with `act`. CI pipeline failures from untested workflow changes are strictly prohibited.

---

## 5. Git Commit Standards

All commits **MUST follow these exact rules** with zero exceptions.

### 5.1 Gitmoji Specification

Every commit must lead with a valid [gitmoji](https://gitmoji.dev/) that accurately describes the primary purpose of the change. Common valid gitmojis include:

- `:sparkles:` for new features
- `:bug:` for bug fixes
- `:memo:` for documentation changes
- `:recycle:` for code refactoring
- `:art:` for formatting/structural changes
- `:white_check_mark:` for test updates
- `:wrench:` for config/tooling changes

### 5.2 Commit Message Format

```
<gitmoji> <all-lowercase summary>

- detailed bullet point 1 describing the change
- detailed bullet point 2 describing the change
- additional context as needed
```

#### Non-Negotiable Rules:

1. **All Lowercase Summary**: The commit summary (first line) must be entirely lowercase. No uppercase letters are permitted, even for proper nouns or acronyms.
2. **List-Form Body**: All commit details must be in a bulleted list format. No paragraph text is allowed in the commit body.
3. **Atomic Scope**: Each commit must address a single logical change. Do not combine unrelated fixes, features, or refactors in a single commit.
4. **Imperative Mood**: All summary and detail text must use imperative, present-tense language (e.g., "add feature" not "added feature").

---

## 6. End-of-Task Validation Checklist

Before marking any task as complete, you **MUST verify and confirm all of the following**:

1. All code is formatted with `cargo +nightly fmt --all` and passes the `--check` validation.
2. `cargo +nightly clippy` runs with zero warnings or errors (using the mandatory command above).
3. All `cargo test` suite passes with no failures.
4. All workflow changes have been tested locally with `act` and pass successfully.
5. The working directory is clean: no untracked files, uncommitted changes, or temporary assets remain.
6. All commits follow the gitmoji, all-lowercase summary, and list-form body standards.
7. All preferred local tools were used for file system and search operations where applicable.
8. No forbidden commands or operations were executed during the task.

---

## 7. Forbidden Actions

The following actions are **strictly prohibited** unless explicitly approved by the user in writing:

- Running destructive commands: `git reset --hard`, `git clean -fd`, `rm -rf`, or any command that can irreversibly delete or overwrite code/data.
- Committing code that fails formatting, Clippy, or test checks.
- Using `unwrap()` or `expect()` in production code.
- Modifying protected files (defined in the repository's `SECURITY.md` or `CONTRIBUTING.md`) without following the formal change approval process.
- Adding unnecessary dependencies or features beyond the scope of the requested task.
- Pushing untested workflow changes to the remote repository without local `act` validation.
- Using generic `find`, `grep`, `cat`, or `ls` commands when the preferred native tools are available.
