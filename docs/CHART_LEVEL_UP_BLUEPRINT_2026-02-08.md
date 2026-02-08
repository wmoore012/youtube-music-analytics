# TRACKSTATS CHART LEVEL-UP BLUEPRINT (2026-02-08)

## PURPOSE
This is the visual strategy memo for the dashboard refresh.
It keeps what already works, removes clutter, and upgrades every view to answer one question fast:

**What should we do today for each artist?**

---

## WHAT WE ALREADY NAILED

### Strong foundations worth celebrating
- Tabs split the app into clear mental modes: `Overview`, `Artist Deep Dive`, `Velocity Analysis`.
- KPI arithmetic is explicit and readable.
- Focus-artist mode exists and can be turned into true coaching mode.
- Content mix + rollout tables already contain useful signal inputs.
- Latest snapshot date and data-source visibility reduce blind trust.

### Why this matters
The app already has the right raw material.  
The level-up is about **story order**, **visual hierarchy**, and **decision framing**.

---

## OLD LAYOUT INTENT (KEEP THE GOOD PARTS)

| Existing intent | Keep it? | Why it was right |
| --- | --- | --- |
| Show full roster quickly | Keep | Good for daily triage. |
| Compare artists against each other | Keep (as context) | Useful benchmark layer. |
| Show multiple chart types | Keep selectively | Variety helps only if each chart earns its space. |
| Add deep-dive section | Keep and sharpen | Correct direction for artist-by-artist strategy. |

---

## LEVEL-UP MAP (CURRENT -> NEXT)

| Current component | Current strength | Current pain at 3am | New format | Why this wins |
| --- | --- | --- | --- | --- |
| KPI strip | Fast read of totals | Totals can bury momentum | Lead with `Daily plays`, `vs normal`, `Fans responding`, `Status` | Puts action first, context second. |
| View growth scatter/line | Has trend data | Single-day snapshots can look broken | Dual mode: real time-series when multi-day, release-anchored curve when single-day | Always readable, never fake precision. |
| Content mix grouped bars | Shows format counts | Hard to compare artists quickly | Horizontal stacked bars, sorted by focus metric | Faster scan, cleaner strategy comparison. |
| Large rollout table | Rich metrics | Dense, hard to act on quickly | “Today-first rollout KPIs” with short labels + clear `Today move` | Turns metrics into decisions. |
| Talking points text list | Has useful logic | Easy to ignore visually | Artist-colored callout cards | High salience with low cognitive load. |
| Artist deep dive | Good concept | Still too roster-comparison-heavy | Focus artist headline + benchmark grayscale context | One artist is the story, benchmarks are reference only. |

---

## NEW TAB BLUEPRINT

## 1) OVERVIEW TAB (ROSTER PULSE)
### Lead section
- KPI cards in this order:
  1) `Daily plays (last 7 days)`
  2) `vs normal`
  3) `Fans responding %`
  4) `Status`
- Totals become secondary context, not lead signal.

### Decision section
- “Who needs a push and what to do” table:
  - plain-language columns
  - one short “Today move” per artist
  - no heavy language

---

## 2) ARTIST DEEP DIVE TAB (COACHING MODE)
### Visual hierarchy
- Big focus-artist header in assigned palette color.
- Benchmark artists in grayscale by default.
- Focus metrics get color accents; benchmarks stay low-noise.

### Story order
1) Focus snapshot
2) What is improving / slipping
3) Format mix recommendation
4) Concrete next move

---

## 3) VELOCITY TAB (WHO IS RUNNING RIGHT NOW)
### Chart framing
- Title: “How big each video is vs how hard fans react”
- Axes:
  - `Plays per day (last 7 days)`
  - `Fans responding %`
- Zone labels:
  - `Big & loved`
  - `Small but loved`
  - `Big but low reaction`
  - `Quiet`

### Support blocks under chart
- `Big & loved right now`
- `Small but loved - worth a push`
- `Big but low reaction`

Each block is a short ranked table, not a wall of prose.

---

## VISUAL SYSTEM (PREATTENTIVE, PREMIUM, CLEAN)

### Color direction
- Primary action color: YouTube red family.
- Focus artist accent: artist-assigned palette color.
- Benchmark context: grayscale neutrals.
- Warning color: amber/red only for true data-quality risk.

### Motion direction
- Subtle only:
  - soft card entrance
  - hover emphasis on active artist rows
  - no distracting continuous animation loops

### Typography and spacing
- Short labels, strong contrast, larger section headers.
- One idea per row/card.
- Avoid dense paragraph blocks in key decision areas.

---

## LANGUAGE STYLE RULES
- Use plain words over analytics jargon.
- Keep labels short and literal.
- Avoid score-like wording unless arithmetic is shown.
- Always pair a metric with “what to do now.”
- Avoid burying the lead in long tooltips.

---

## CHART-BY-CHART UPGRADE CHECKLIST

- [ ] Every chart has one clear decision purpose.
- [ ] Every table has a visible “today move” output.
- [ ] Focus artist stands out immediately in deep dive.
- [ ] Benchmark context is visible but visually de-emphasized.
- [ ] No pseudo-complex metric names.
- [ ] No chart kept “just because it looks good.”
- [ ] Snapshot freshness is explicit and truthful.

---

## NORTH STAR
If someone opens this dashboard half-awake, they should still be able to answer:
1) Who is moving?
2) Why are they moving?
3) What should we do today?
