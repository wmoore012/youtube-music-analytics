# Codex Reviewer Guide for This Repository

This document orients Codex (and human reviewers) to the expectations, constraints, and review checklist derived from the repository's internal standards under `.kiro/`.

## Repository Purpose
- Enterprise-grade YouTube ETL + analytics for the music industry
- Sentiment analysis, data quality monitoring, and storytelling notebooks

## Canonical Standards (source of truth)
- `.kiro/steering/development_standards.md` — code quality, DB, UX, config, cleanup
- `.kiro/steering/structure.md` — project layout, naming, imports, data flow
- `.kiro/specs/robust-ci-and-git-setup/tasks.md` — CI/CD philosophy and guardrails

Codex should evaluate changes against these standards.

## Golden Rules (must pass)
- TDD-first: tests accompany or precede code; no large untested features
- Fail loudly: no silent except; raise or log clearly with user-facing hints
- No magic values: configurable via `.env`; never hardcode secrets or URLs
- Database discipline: lowercase_snake_case; avoid booleans (use enums/strings)
- Human-readable: small, single-purpose functions; extract helpers over duplication

## Notebook Expectations
- All visualizations are interactive (Plotly/Altair)
- Consistent global color mapping for artists/categories
- Storytelling: descriptive → diagnostic → predictive → prescriptive
- Executed notebooks must reflect current data before merge

## Configuration & Cleanup
- Channels configured via env vars like `YT_ARTISTNAME_YT`
- Cleanup tools must validate configuration vs DB before deletion, require confirmation, and emit human-readable SQL

## CI/CD & Quality Gates
- Lint: flake8 (line length 120, no F541 etc.), complexity under control
- Typecheck: mypy with stubs; resolve duplicate module names
- Security: bandit scans; no credentials in repo
- Tests: unit/integration/system; notebook execution checks in CI
- Enhanced CI script must not crash (no KeyErrors) and should produce actionable guidance

## Review Checklist (Codex)
1) Standards alignment
   - Naming, imports, file layout follow `.kiro/steering/structure.md`
   - Code quality matches `.kiro/steering/development_standards.md`
2) Tests & notebooks
   - New logic has tests; notebooks updated/executed if outputs change
   - Artist counts and expected KPIs consistent across notebooks/tests
3) Config & safety
   - New parameters live in `.env` with clear names/comments
   - No hardcoded channel lists or secrets; uses env-driven config
4) Database & SQL
   - Schemas explicit; human-readable SQL formatting
   - Cleanup/data mutations require confirmations and backups where applicable
5) Lint & types
   - No new flake8 violations introduced by this PR
   - mypy passes or has justified ignores; no module duplication
6) Security & compliance
   - YouTube ToS compliance preserved; data retention paths unchanged or improved
   - Bandit passes; no insecure patterns added
7) Performance & complexity
   - Large functions split; helpers extracted; avoid AI-bloat code
   - No unnecessary heavy dependencies added
8) Docs & DX
   - README or inline docs updated for user-facing behavior
   - Clear error messages and remediation steps

## Known Open Issues (context for reviewers)
- Linting debt: numerous F541/E501/E226/C901 violations in legacy files
- Typecheck: missing stubs (yaml, pymysql); duplicate module names in `src/youtubeviz`
- Enhanced CI: KeyError on `overall_health` in `generate_ai_agent_report`
- Test suite breadth: `pytest.ini` limits discovery; full suite has import gaps

PRs should avoid regressing these areas and preferably improve them incrementally (boy-scout rule).

## Preferred PR Structure
- Small, focused changes; meaningful commit messages
- Include before/after notes for user-visible behavior
- Provide quick validation steps (make target or single command)

## How to trigger Codex review
- In the PR, comment exactly: `@codex review`
- Codex will acknowledge with 👀 and post a standard review
