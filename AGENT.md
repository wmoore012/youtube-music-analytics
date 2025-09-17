# AI Reviewer & CI/CD Agent Playbook (TDD-first, bulletproof)

This is the operational guide for Codex (and human reviewers) to validate
changes with a test-first mindset and hard CI gates. It encodes where to look,
what to check, and how to override defaults when a different setup is
acceptable.

## Purpose

- Enterprise-grade YouTube ETL + analytics for the music industry
- Sentiment analysis, data quality monitoring, and storytelling notebooks

## Canonical Standards (source of truth)

- `.kiro/steering/development_standards.md` — code quality, DB, UX, config,
  cleanup
- `.kiro/steering/structure.md` — project layout, naming, imports, data flow
- `.kiro/specs/robust-ci-and-git-setup/tasks.md` — CI/CD philosophy and
  guardrails

Codex must evaluate changes against these standards.

## Operating Modes

- Fast checks (pre-PR): make ci-local, lint, types, notebook smoke tests
- Full gate (pre-merge): enterprise tests + comprehensive artist/data
  validation + security + compliance

## TDD-First Golden Rules (must pass)

- Tests precede or accompany code; no significant untested features
- Every public function requires: happy-path test + 1-2 edge-case tests
- Bug fix requires a failing test reproducer first
- Avoid broad except; raise/log with user-action hints
- No magic values; configure via `.env`/config files; never commit secrets/URLs
- DB discipline: lowercase_snake_case; prefer enums/strings over booleans
- Keep functions small and cohesive; extract helpers; avoid duplication

## Required CI/CD Gates (what to run, where to look)

1. Code quality

- Lint: flake8 (max line length 120) — Make: `lint`
- Types: mypy (stubs where needed) — Make: `typecheck`
- Security: bandit + safety — Make: `security-scan`

2. Tests (local + notebooks)

- Unit/Integration: Make: `test`
- Notebook execution (hard gate): Make: `test-notebook-execution` (runs
  `tests/test_notebook_execution.py`)
- Comprehensive validation (hard gate): Make: `ci-comprehensive` →
  `scripts/comprehensive_artist_validation.py`

3. Data assets (must match expectations)

- CSV tables (hard gate):
  - `music_analysis_tables/artist_music_summary.csv` columns must exist and be
    non-null:
    `artist_name, total_videos, total_views, total_likes, total_comments, total_est_revenue_usd, avg_engagement_rate, revenue_per_video`
  - `music_analysis_tables/normalized_music_videos.csv` required non-null
    columns:
    `video_id, title, artist_name, video_type, published_at, view_count, like_count, comment_count, est_revenue_usd, metrics_date, fetched_at`
  - Allowed-null: `isrc`, `has_isrc` (boolean-like), derived percentage fields
    may be zero but must be present
  - Artists in both files must equal expected set; fail on missing or unexpected
    unless overrides are set
- Notebook outputs (hard gate):
  - Execute: `execute_music_analytics.py`, `execute_data_quality.py`,
    `execute_artist_comparison.py`
  - Each must emit: a line matching `Artists?:\s*(\d+)` and contain all expected
    artist names
  - Required sections in outputs:
    - Music Analytics: `MUSIC INDUSTRY PERFORMANCE DASHBOARD`,
      `Market Share Analysis`, `Revenue Analysis`, `INVESTMENT RECOMMENDATIONS`
    - Data Quality: `DATA QUALITY ASSESSMENT RESULTS`,
      `OVERALL DATA QUALITY SCORE`
    - Artist Comparison: `Artist Comparison Metrics`, `ARTIST RANKING SUMMARY`,
      `Top Performing Videos by Artist`

4. Database checks (hard gate when DB available)

- Schema files: `tools/setup/create_tables.py` (source of truth)
- Validate core tables exist: `youtube_videos`, `youtube_metrics`,
  `youtube_comments`, `youtube_videos_raw`, `youtube_playlists_raw`
- Non-null assertions:
  - youtube_videos: `video_id` (PK), `fetched_at`; `channel_title` must be
    non-null for tracked artists
  - youtube_metrics: `video_id`, `metrics_date`, `fetched_at`; metric columns
    must be non-null for latest date for tracked artists
  - youtube_comments: `comment_id`, `video_id` non-null when `comment_text`
    present
- Freshness: for tracked artists, latest `metrics_date` within
  `required_freshness_hours` (default 48; see overrides)

5. System health JSON (hard gate)

- File: `system_health_dashboard.json`
- Required keys: `sentiment_accuracy.status`, `bot_detection.status`,
  `data_quality.status`, `momentum_stability.total_artists`,
  `overall_health.score`, `overall_health.status`, `alerts`
- Consistency checks:
  - `momentum_stability.total_artists` must equal expected artist count
  - `overall_health.status` should align with component scores (document if not)
  - Alert levels reflect component statuses (at least warning when below
    thresholds)

## Expected Artists & Configuration Overrides

Source of truth (default): `config/expected_artists.json`

- Keys: `expected_artists` (array of strings), `minimum_artists` (int)

Override knobs (set via environment variables for flexibility by other
contributors):

- AGENT_EXPECTED_ARTISTS_JSON: path to a JSON file with `expected_artists` and
  `minimum_artists`
- AGENT_EXPECTED_ARTISTS: comma-separated list to override expected artists
  (takes precedence)
- AGENT_MINIMUM_ARTISTS: integer expected count (defaults to JSON value)
- AGENT_ALLOW_ARTIST_SUPERSET: true|false — if true, allow extra artists but
  require all expected present (default: false)
- AGENT_REQUIRED_FRESHNESS_HOURS: integer hours for DB freshness checks
  (default: 48; aligns with `config/production.json`)
- AGENT_FAIL_ON_NULLS: true|false — fail CI on unexpected nulls in required
  columns (default: true)
- AGENT_HEALTH_MIN_SCORE: integer 0-100 minimal acceptable
  `overall_health.score` (default: 60)

Resolution order: env overrides → AGENT_EXPECTED_ARTISTS_JSON →
`config/expected_artists.json` (fallback).

## Exact Validation Procedures (what the agent must do)

1. Load expected artists

- Read from AGENT_EXPECTED_ARTISTS/AGENT_EXPECTED_ARTISTS_JSON else
  `config/expected_artists.json`
- Compute EXPECTED_SET and EXPECTED_COUNT

2. Run quick CI (fail fast on red)

- make ci-local
- make lint; make typecheck; make security-scan

3. Run notebook gates

- make test-notebook-execution
- Parse outputs of each execute_* script:
  - Verify all names in EXPECTED_SET appear ≥ 1 time
  - Extract first `Artists?: N` and assert N == EXPECTED_COUNT (or ≥ minimum if
    ALLOW_ARTIST_SUPERSET=true)
  - Verify required section headers are present

4. Validate CSV tables

- For each listed CSV, assert required columns exist
- Scan required columns for nulls and NaNs; fail when AGENT_FAIL_ON_NULLS=true
- Compare set of artists in file to EXPECTED_SET according to
  AGENT_ALLOW_ARTIST_SUPERSET

5. Validate database (if DB reachable)

- Ensure tables exist from schema list
- For tracked artists (from EXPECTED_SET), assert non-null in required columns
  and most recent `metrics_date` is within AGENT_REQUIRED_FRESHNESS_HOURS

6. Validate health JSON

- Load `system_health_dashboard.json`
- Assert presence of required keys
- Assert `momentum_stability.total_artists == EXPECTED_COUNT` (or ≥ minimum if
  ALLOW_ARTIST_SUPERSET)
- If AGENT_HEALTH_MIN_SCORE is set, assert `overall_health.score >= threshold`;
  otherwise warn

7. Comprehensive artist validation

- Run: `python scripts/comprehensive_artist_validation.py`
- Require final line contains `ALL VALIDATIONS PASSED` to pass this gate

8. Produce PR report (what to post)

- Summarize gates and statuses with a checklist
- Include any override values in effect
- List any failing files/paths and exact mismatches (missing artists, null
  columns, stale freshness, bad health score)

## Review Checklist (Codex)

1. Standards alignment
   - Naming/imports/layout follow `.kiro/steering/structure.md`
   - Development standards adhered to per
     `.kiro/steering/development_standards.md`
2. Tests & notebooks (TDD-first)
   - New logic includes tests and notebook updates as needed
   - Artist counts/KPIs consistent across notebooks/tests
3. Config & safety
   - New parameters added to `.env` and documented; no hardcoded lists/secrets
4. Database & SQL
   - Schemas explicit; human-readable SQL; destructive ops gated/confirmed
5. Lint & types
   - No new flake8 violations; mypy passes or justified ignores
6. Security & compliance
   - Bandit/safety pass; YouTube ToS compliance unchanged or improved
7. Performance & complexity
   - Refactor long functions; no heavy deps without need
8. Docs & DX
   - README/inline docs updated for behavior changes; actionable errors

## Known Open Issues (context for reviewers)

- Linting debt in legacy code (F541/E501/E226/C901)
- Type checking gaps (yaml, pymysql stubs), duplicate module names in
  `src/youtubeviz`
- Enhanced CI occasionally KeyError on `overall_health` generation
- Full test discovery may have import gaps depending on environment

PRs should not regress these and should improve incrementally (boy-scout rule).

## Preferred PR Structure

- Small, focused changes with meaningful commits
- Include before/after for user-visible behavior
- Provide quick validation steps (Make target or single command)

## How to trigger Codex review

- In the PR, comment exactly: `@codex review`
- Codex will acknowledge with 👀 and post a structured review

## Agent Reference: Filepaths and Columns to Validate

CSV tables

- `music_analysis_tables/artist_music_summary.csv`
  - Required columns:
    `artist_name,total_videos,total_views,total_likes,total_comments,total_est_revenue_usd,avg_engagement_rate,revenue_per_video`
  - Allowed-null: none of the above (values may be 0)
- `music_analysis_tables/normalized_music_videos.csv`
  - Required columns:
    `video_id,title,artist_name,video_type,isrc,has_isrc,published_at,view_count,like_count,comment_count,est_revenue_usd,metrics_date,fetched_at`
  - Allowed-null: `isrc`; `has_isrc` may be boolean-like; others must be
    non-null

Database tables (from `tools/setup/create_tables.py`)

- `youtube_videos(video_id PK, isrc, title, channel_title, published_at, view_count, like_count, comment_count, fetched_at)`
- `youtube_metrics(video_id, metrics_date, view_count, like_count, comment_count, fetched_at)`
- `youtube_comments(id PK, video_id, comment_id, comment_text, like_count, published_at, sentiment_score, fetched_at)`

Notebooks / scripts to execute

- `execute_music_analytics.py`
- `execute_data_quality.py`
- `execute_artist_comparison.py`

Health file

- `system_health_dashboard.json` (keys listed above)

## Optional Overrides via PR Labels (if labels are available)

- `allow-artist-superset` → sets AGENT_ALLOW_ARTIST_SUPERSET=true
- `relax-null-checks` → sets AGENT_FAIL_ON_NULLS=false (warn only)
- `lower-health-threshold` → sets AGENT_HEALTH_MIN_SCORE to 40 for this PR

These are advisory for reviewers; agents may map labels to env overrides in
their runtime.

## Minimal Runbook (local reproduction)

1. Install dev tools: `make dev`
2. Initialize DB (optional for DB checks): `make db-init`
3. Quick CI: `make ci-local`
4. Notebook gates: `make test-notebook-execution`
5. Comprehensive artist validation: `make ci-comprehensive`
6. Security: `make security-scan`

If any gate fails, the PR must not merge unless an override is explicitly
documented in the PR with justification.

# Repo-Scoped Agent Charter (Staging Only)

SCOPE

- You are restricted to THIS repository and its default remote. Do not
  reference, open, link to, or mirror any other repos unless I explicitly paste
  a URL and say “allow.”
- Treat any mention of “public repo,” “other repo,” or “mirror” as out of scope
  unless I explicitly authorize.

SECRETS & CLOSED MATERIAL

- The “formula numbers,” model weights, and any private scoring/tuning artifacts
  are proprietary. Do not print them, summarize them, or embed them into code
  comments, tests, or commit messages.
- Do not attempt to infer, compress, or re-express the formula via ML or
  brute-force search (“model extraction” is disallowed).

CODE + DATA CHANGES

- Work only on branches named `feat/*`, `fix/*`, or `chore/*`. Never push to
  `main` directly. Always open a PR.
- Never add secrets or large binary artifacts to git history. Use env vars and
  secret stores for config, and LFS for any unavoidable large files.
- Tests: you may add/update tests in this repo. Do not fetch tests from other
  repos.

I/O & NETWORK

- Do not fetch external code without explicit URLs and permission. No
  auto-syncs, submodules, or subtree pulls.
- Do not send repository contents to external services unless I explicitly say
  “ok to share.”

DOCS & DISCLOSURE

- In all docs/READMEs here, refer to this repo as “private staging.” Do not
  mention any other repo by name.
- If you need info that might exist in the other repo, ask me instead of
  guessing or linking.

SAFETY CHECKS (always)

- Before any operation that could reveal proprietary formulas or internal
  datasets (printing, logging, exporting), stop and ask for approval.
- Prefer minimal diffs; include rationale in PR descriptions.
