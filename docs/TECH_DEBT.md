Tech Debt Management Workflow

Scope: Applies to this monorepo and mirrors for OSS bricks under oss/.

Debt Metrics
- Technical Debt Ratio (TDR, SonarQube): remediation_cost ÷ development_cost (baseline ~30 min/LOC).
  - Quality gate on new/changed code: fail if TDR > 5%.
- Debt Health Index (DHI): Principal ÷ (Principal + Interest) — quick local signal.
  - Principal: Estimated full remediation (hours).
  - Interest: Ongoing cost per iteration (hours/iteration).
- DHI thresholds:
  - ≤ 0.15: Informational. Track but defer.
  - 0.16–0.35: Schedule if adjacent work touches area.
  - > 0.35: Create priority issue and assign an owner this cycle.

Intake and Tracking
- Use label: tech-debt with optional area labels (db, etl, twitter, oss-bricks).
- Template fields per item: Context, Impact, Principal, Interest, Risk, Proposed Fix, Acceptance Criteria.
- Link to code hotspots (files, functions) and tests.

Prioritization Heuristics
- Safety first: security and data-loss risks trump other work.
- High-interest first: recurring pain, flaky tests, or frequent merge conflicts.
- Align with roadmap: fix debt blocking upcoming features to minimize context switching.

Execution Workflow
1) Write/extend tests first (happy path + 1–2 edge cases).
2) Add a short ADR in the PR description when changing public behavior.
3) Keep changes small and isolated; feature-flag anything risky by default off.
4) Update docs and examples; include a quickstart or migration note when helpful.
5) Run policy gates green before merge: lint/type/test + security (Bandit) + Semgrep SQL-safety.

Deprecation Strategy
- Move replaced modules into archive/MARKED_FOR_DELETION/YYYY-MM-DD/… with a top docstring:
  - Created at, Reason, Replacement, Removal window.
- Do not delete until the removal window elapses and downstreams migrate.

Dependency Hygiene
- Dependabot weekly for pip and GitHub Actions in each oss/ brick.
- Pin minimal major versions; avoid unnecessary transitive bloat.
- Security: run bandit minimal in CI; escalate findings with severity ≥ medium.

Code Review Checklist (abbrev.)
- Tests: coverage for new/changed behavior; readable, isolated.
- Types: mypy strict; avoid Any; justify exceptions.
- Style: Ruff (fix), then Black, then isort (profile=black) or ruff-format only.
- Policy: Semgrep SQL-safety; feature flags default off; no secrets.
- Docs: README/Quickstart updated; migration notes when needed.

Metrics and Cadence
- Track opened vs. closed tech-debt issues per sprint.
- Review top 5 hotspots monthly; schedule one improvement PR per sprint.
