# TRACKSTATS: CELEBRATION + NEXT FORMAT (2026-02-08)

## THE GOOD NEWS FIRST

We already built the hard part:
- Clean three-tab scaffold: `Overview`, `Artist Deep Dive`, `Velocity Analysis`.
- Real arithmetic metrics wired end-to-end.
- Focus artist workflow exists and is usable.
- Action language exists in the rollout area.

That is the foundation. We are not starting over.

---

## CURRENT LAYOUT (WHAT IS WORKING NOW)

```text
[Header + source banner]
[Tabs: Overview | Artist Deep Dive | Velocity Analysis]
[KPI cards]
[Trend chart] [Velocity scatter]
[Content mix + rollout tables + top videos]
```

### Why this already works
- Fast first read.
- Shows roster health in one screen.
- Gives enough data depth to debug artist-level behavior.

---

## NEXT LAYOUT (WHAT WE ARE LEVELING TO)

```text
[Header + freshness + source truth]
[Tabs]
[TODAY-FIRST KPI row: Daily plays | vs normal | Fans responding | Status]
[Context KPI row: totals + sample size]

[Who needs a push and what to do]
| Artist | Daily plays | vs normal | Fans responding | Status | Full video boost | Cadence | Today move |

[Visual support]
- Horizontal artist-format mix
- Velocity zones (Big & loved / Small but loved / Big but low reaction / Quiet)
- Comment investigation links (2 videos per artist, with thumbnail)
```

### Why this is the upgrade
- Lead with momentum, not vanity totals.
- Keep one clear question: **what do we do today?**
- Keep arithmetic visible and simple.

---

## SIDE-BY-SIDE: CURRENT VS NEXT

| Area | Current strength | Next upgrade |
| --- | --- | --- |
| KPI row | Big numbers are clear | Reorder so momentum + action come first |
| Trend chart | Has time-series intent | Single-day mode shifts to release-anchored trajectory |
| Scatter | Rich but noisy | Zone labels + short ranked lists under chart |
| Rollout table | Lots of data | Shorter labels + explicit `Today move` |
| Artist deep dive | Good concept | Focus artist becomes hero, benchmarks fade to grayscale |
| Talking points | Logic is solid | Artist-color callout blocks for instant scan |

---

## VISUAL DIRECTION (PREATTENTIVE, PREMIUM, MODERN)

```text
Primary palette: deep YouTube-inspired reds
Focus artist: assigned accent color
Benchmarks: controlled grayscale
Risk states: amber/red only for true data issues
```

- Keep cards and calls-to-action crisp.
- Keep animation subtle and meaningful.
- Keep benchmark visuals intentionally quiet.

---

## LANGUAGE RULES (3AM READABILITY)

- Prefer: `Fans responding %` over heavy jargon.
- Prefer: `Today move` over long advisory paragraphs.
- Prefer short tooltips that point to simple on-page arithmetic.
- One idea per card, one decision per chart.

---

## DELIVERY GUARDRAILS (SPEED + SAFETY)

This project now follows a low-refactor execution rule for fast, safe iteration:

- `pre-commit` runs `ruff --fix` + `ruff-format` before commits.
- Black/Ruff formatting is enforced so PR diffs stay readable.
- Revenue language and pseudo-finance KPI paths stay removed from exec flow.
- `Shorts` product wording stays removed from UI labels (`Short video (<60s)` only).
- Broad `except Exception` in config-load paths is replaced with specific exceptions.

Result: fewer regressions, faster review cycles, cleaner handoff between agents.

---

## THE NON-NEGOTIABLE QUESTION

Every section must answer at least one of these:
1. Who is moving?
2. Why are they moving?
3. What should we do today?

If a chart does not answer one of those, it does not stay on the main path.
