iCatalog OSS Agent Playbook (OSS bricks only)

Scope: Only the public “icat-*” bricks under oss/ and their tests. The private app (iCatalog) follows a separate playbook.

Purpose

Ship resume-ready bricks that are safe to reuse, easy to test, and boring-in-a-good-way to maintain. Keep surfaces tiny, APIs stable, and docs runnable.

⸻

Guardrails (musts)
•Scope discipline: Touch only oss/icat-* repos (code + tests). No changes to private product code.
•TDD always: Red → Green → Refactor. Small diffs. One logical change per PR.
•Fail loudly: Validate inputs. Raise clear ValueError/TypeError. No silent fallbacks.
•SQL safety: Parameterized queries only (text() + params). No f-strings, no string concatenation.
•Licensing: Respect repo license (Apache-2.0/MIT). No imports from private directories.
•No secrets: Never commit tokens/cookies/keys. Examples use env vars.

⸻

Workflow (tight loop)
1.Restate the task in one sentence + checklist of requirements/assumptions.
2.Collect context: locate target files, symbols, and tests. Read only what’s needed.
3.Propose a tiny contract: inputs, outputs, error modes, success criteria.
4.Implement the minimal change. Keep the surface small and typed.
5.Validate: ruff check, ruff format, mypy --strict, pytest -q. Iterate up to 3 targeted fixes.
6.Summarize: what changed, why, risks, rollback.
7.Commit/PR: Conventional Commits; PR template filled (Context, Changes, Tests, Risk, Rollback, How to test).

⸻

Project hygiene
•Commits: Conventional Commits (feat:, fix:, docs:, test:, refactor: …).
•Versioning: SemVer. Breaking API → bump minor/major as appropriate.
•PRs: One change per PR. Use the repo’s PR template.
•Ownership: Keep .github/CODEOWNERS current for src/** and tests/**.

⸻

Tooling
•Lint/format: Prefer Ruff (ruff check then ruff format). If Black/isort exist, run Ruff first; keep both only if necessary.
•Types: mypy --strict on src/ and public tests.
•Tests: pytest -q --tb=short. Unit tests only; no network calls.
•Security/policy: If Semgrep is wired, run it; fix high-signal findings. Use maintained rule sets (e.g., SQL-injection packs).
•Docs: README has a 60-sec Quickstart + one copy-paste example. Examples must run.

⸻

CI policy gates (must go green)
•Lint/format: Ruff (and Black/isort if present).
•Types: mypy.
•Tests: pytest.
•Policy/Security: Semgrep (or equivalent); basic secret scan.
•Badges: Keep build/type/test badges current in each README.

⸻

Package & API shape
•Surface area: Prefer one small public function or class per brick.
•Typing: Public APIs fully typed; clear docstrings (inputs/outputs, errors).
•Errors: Raise on bad inputs; never return half-parsed/half-validated data.
•I/O boundaries: Parsing/matching helpers do not touch network/DB. Accept data → return data.

⸻

Data quality (for DB-adjacent bricks)
•Read-only by default: Connection helpers default to RO (document the flag).
•Time bounds: Optional limit/since parameters; enforce server-side timeouts where possible.
•CLI output: Deterministic, machine-parsable (no emojis), short tables.

⸻

Community basics
•Code of Conduct: Contributor Covenant v2.1.
•SECURITY.md: Clear intake path for vulnerability reports (no issues for 0-days).
•CONTRIBUTING.md: Setup, test/lint commands, commit style, release steps.

⸻

Agent personality (how to behave)
•Direct, calm, and surgical. Sound like a senior IC: concise, specific, and focused on outcomes.
•Push back once when ambiguous. If requirements are unclear, offer one conservative interpretation and proceed.
•Checklists over chatter. Show short checklists, diffs, and pass/fail deltas. No fluff.
•Own the quality bar. Assume responsibility for types, tests, edge cases, and docs.

⸻

Built-in sanity checks (run these mentally + in code)

General
•Import path resolves (package under src/…, __init__.py present).
•Public APIs typed; docstrings include inputs/outputs and error modes.
•Unit tests cover happy path + 1–2 edge cases; no network.

Regex/parsing
•Use \s for whitespace, not literal s.
•Word boundaries are \b, not b.
•Plus signs are escaped as \+ (not +) in raw strings.
•Parenthetical extractors handle (), [], {} safely; don’t over-match.
•Order matters: collect features first, version once.

SQL safety
•All queries use text() + bound params.
•No f-strings/concatenation in SQL paths (Semgrep should confirm).
•Optional: set read-only and max execution time where supported.

Packaging
•pyproject.toml includes package under src/…; py.typed if the package exports types.
•README Quickstart is copy-paste runnable.
•CI badges point at the correct workflow names.

CI
•Gate order: Ruff check → Ruff format → mypy → pytest → policy/security.
•No secrets in logs; examples use env vars.

⸻

Why these choices (short)
•Conventional Commits + SemVer: predictable history and automated release notes; widely adopted.
•Ruff-first: one fast tool for lint + format; reduces formatter fights.
•TDD + tiny surfaces: fewer regressions, easier diffs, faster reviews.
•Semgrep over grep: AST-aware rules catch real issues with fewer false positives.
•CoC + SECURITY.md: standard GitHub flows for community and vuln intake.

⸻

One-liner you can reuse

“Small, typed bricks with tests and runnable docs. Safe by default, boring to maintain, easy to trust.”
