# TRACKSTATS LAYOUT STORY - PART 2 (2026-02-08)

## WHY THIS DOCUMENT EXISTS
Part 1 documented the foundation. Part 2 captures the level-up path now that the foundation is stable.

North star:

> What should we do today for each artist?

If a section does not answer that quickly, it moves out of the main path.

## CELEBRATION: WHAT WE ALREADY BUILT WELL

### We have real product bones
- Clear tab scaffold: `Overview`, `Artist Deep Dive`, `Velocity Analysis`.
- Reliable source/freshness callouts.
- Action table logic already wired.
- Artist color system already in place.
- Comment investigation links now include thumbnail + YouTube URL.

### Why the old layout was a smart first move
The old layout prioritized breadth first: one place to scan roster totals, trend intent, velocity, and top videos.
That was the right phase-one strategy because it made the dataset visible end-to-end.

## CURRENT VS NEXT (ASCII SCAFFOLDS)

### OVERVIEW

Current scaffold:

```text
[Header + source banner]
[Tabs]
[Totals KPI strip]
[Trend chart] [Velocity scatter]
[Content mix + rollout tables + top videos]
```

Next scaffold:

```text
[Header + source + freshness]
[Tabs]
[HERO: Heat vs normal] [HERO: Fans talking] [HERO: Cadence gap]

[MEETING TABLE]
| Artist | Heat | Direction | Fans talking | Cadence | Today move |

[Support visuals]
[Velocity zones + short ranked lists]
[Format mix by artist - horizontal]
[What to look at next (2 videos per artist)]
```

### ARTIST DEEP DIVE

Current scaffold:

```text
[Focus dropdown]
[Generic KPI row]
[Focus + benchmark tables mixed]
```

Next scaffold:

```text
[Focus dropdown (name + assigned color)]
[Large focus artist header]
[Focus hero cards]
[Focus story first]
[Benchmarks in grayscale context]
```

### VELOCITY

Current scaffold:

```text
[Scatter]
[Dense legend]
```

Next scaffold:

```text
[Scatter with clear labels]
Big & loved | Small but loved | Big but low reaction | Quiet

[Three short lists]
1) Big & loved right now
2) Small but loved - worth a push
3) Big but low reaction
```

## METRIC LANGUAGE RULES (3AM READ)
- `Heat vs normal` (not abstract score names).
- `Fans talking` as `comments gained / views gained x 1,000`.
- `Cadence gap` as days since last official.
- `Today move` as a nudge, not a command.
- No pseudo-finance KPI in exec flow.

## ARITHMETIC RULES (AUDITABLE)

### Heat vs normal
- Per video: `views_gained_window = max(view_count) - min(view_count)`.
- Per artist:
  - `daily_7 = views_gained_last_7 / 7`
  - `daily_90 = views_gained_last_90 / 90`
  - `heat = daily_7 / daily_90`

### Direction tag
- `last_3 = views_gained_last_3 / 3`
- `prior_4 = views_gained_prior_4 / 4`
- `last_3 > prior_4 * 1.10` -> accelerating
- `last_3 < prior_4 * 0.90` -> slowing
- else -> steady

### Fans talking
- `fans_talking_per_1k = (comments_gained_last_7 / views_gained_last_7) * 1000`
- Benchmark line uses same window + same selected roster.

### Cadence
- `cadence_gap_days = today - latest_official_release_date`
- Compare against each artist typical official-release gap.

## VISUAL DIRECTION (PREATTENTIVE)
Palette direction:
- Main theme: deep modern red family (YouTube-adjacent, not neon).
- Focus artist: assigned accent color.
- Benchmarks: grayscale.
- Warnings only for true data risks.

Micro-motion direction:
- Subtle card entrance and hover emphasis only.
- No looping animation noise.

## DELIVERY OPERATING RULES (PART 2)
To keep shipping speed high without risky refactors:

1. Pre-commit quality gate:
   - `ruff --fix`
   - `ruff-format`
2. Keep arithmetic-only KPI language:
   - no pseudo-finance KPI reintroduction
   - no hidden score naming
3. Keep short-form taxonomy explicit:
   - `Short video (<60s)` only
   - no title hashtag forcing
4. Tight exception handling on config loaders:
   - `FileNotFoundError`, `PermissionError`, `OSError`, `JSONDecodeError`
   - no silent broad catch in these paths

## HARD GUARDS
1. No `Estimated revenue` anywhere in exec flow.
2. No `Shorts` product claims; use `Short video (<60s)` from duration only.
3. No title hashtag forcing for short-form labeling.
4. No fan-identifying/comment-text fields in exported demo CSV artifacts.
5. New entry deltas must not show fake giant percentages.

## IMPLEMENTATION STATUS (PART 2)
- [x] Hero-card arithmetic model implemented in Overview flow.
- [x] Meeting table under hero cards implemented.
- [x] Velocity zones and follow-up lists implemented.
- [x] 2-per-artist comment investigation links + thumbnails implemented.
- [x] Snapshot CSV export hardened against PII/finance leakage.
- [ ] Extend same hero pattern to the Deep Dive top strip.
- [ ] Add conversation-shift cards using arithmetic-only week-over-week word counts.
