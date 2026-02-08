# ROLLOUT MOMENT LOG (2026-02-08)

## Why this exists
This log captures a high-impact product direction change so future agents do not "optimize" the dashboard back into a pretty-but-not-actionable report.

## LOUD RULES (DO NOT REGRESS)
1. The dashboard must answer: "What should the label do today?"
2. KPI math must be explicit arithmetic and readable by non-technical stakeholders.
3. Do not reintroduce pseudo-finance KPI cards (for example, Estimated Revenue) in the exec strip.
4. New entries must never render fake gigantic percentage deltas; mark as `NEW ENTRY`.
5. Snapshot freshness must follow ETL heartbeat and daily snapshot regeneration.
6. Use duration-only wording for short-form (`Short video (<60s)`), not product claims.

## What changed in this moment
### 1) Executive-first KPI emphasis
- Moved KPI focus to action-driving signals:
  - discovery speed (`views/day`)
  - engagement quality (`(likes + comments) / views * 100`)
  - release cadence and content-mix lift in the action center
- Removed revenue from the top KPI strip and KPI delta arithmetic panel.

### 2) True single-artist coaching view
- Artist Deep Dive now has a focus-artist dropdown.
- Focus artist is visually highlighted; benchmark artists are grayscale context.
- The deep-dive story is now "how to level up one artist" instead of "compare everyone equally."

### 3) Daily demo snapshot guardrail
- Nightly ingestion now regenerates demo snapshot artifacts after successful ingestion.
- Snapshot refresh is timeout-guarded and test-covered.
- Demo snapshot defaults are now portfolio-sized (`8` artists, up to `200` ranked videos per artist).

### 4) Label-safe content taxonomy cleanup
- Removed pseudo-finance wiring from Streamlit KPI paths (no estimated revenue rollups/cards/tooltips).
- Replaced `Shorts` UI language with `Short video (<60s)` and removed title hashtag forcing.
- Added age/velocity sentinel handling so unknown publish age cannot create fake views/day spikes.

## Decision rationale
| Decision | Reason |
| --- | --- |
| Lead with speed + lift | These produce immediate rollout decisions in meetings. |
| Use overall engagement arithmetic | More robust than noisy per-video means for exec readouts. |
| Keep benchmark context but de-emphasize it | Preserves strategic context while keeping one artist as the story. |
| Keep loud comments and tests | Prevents future regressions by AI/code-assist tools. |

## Regression checklist
- [ ] KPI strip still shows speed + engagement-first metrics.
- [ ] No `Estimated Revenue` card in the exec KPI strip.
- [ ] Delta table does not include finance rows.
- [ ] `NEW ENTRY` behavior is intact for near-zero baselines.
- [ ] `Avg views/day` uses `NEW ENTRY` guard when baseline is near-zero.
- [ ] Deep Dive supports one focus artist with grayscale benchmarks.
- [ ] Nightly ETL path refreshes demo snapshot.
- [ ] Short-form language in UI is `Short video (<60s)` (duration-based).
