# TRACKSTATS CHART LEVEL-UP BLUEPRINT (2026-02-08)

## PURPOSE
Visual strategy for the dashboard refresh.

We keep what already works, strip clutter, and make every view answer one fast question:

> **What should we do today for each artist?**

All metrics stay simple and explicit:
- **Daily plays** = views over last 7 days / 7
- **vs normal** = comparison against last 90-day average daily plays
- **Fans responding %** = (likes + comments) / views
- **Full video boost** = full-video plays/day / audio-or-lyric plays/day

No hidden scoring systems.

---

## WHAT WE ALREADY NAILED

### Strong foundations
- Tabs already split the app into clear modes: `Overview`, `Artist Deep Dive`, `Velocity`.
- KPI arithmetic is explicit and easy to explain.
- Focus-artist mode exists and can become true coaching mode.
- Content mix + rollout tables already encode strategy-ready questions.
- Latest snapshot date and source callouts make freshness visible.

**Translation:** the raw material is right. The level-up is story order, visual hierarchy, and decision framing.

---

## OLD LAYOUT INTENT (KEEP THE GOOD PARTS)

| Existing intent | Keep it? | Why it was right |
| --- | --- | --- |
| Show full roster quickly | Keep | Daily triage. |
| Compare artists against each other | Keep (as context) | Useful benchmark layer. |
| Show multiple chart types | Keep selectively | Variety only helps if each chart earns space. |
| Add deep-dive section | Keep and sharpen | Correct direction for artist coaching. |

---

## LEVEL-UP MAP (CURRENT -> NEXT)

| Current component | Current strength | Current pain at 3am | New format | Why this wins |
| --- | --- | --- | --- | --- |
| KPI strip | Fast totals | Totals bury momentum | Lead with `Daily plays`, `vs normal`, `Fans responding`, `Status` | Action first, vanity context second. |
| View growth chart | Trend intent exists | Single-day snapshots look broken | Multi-day -> true time-series; single-day -> day-0 release-anchored cumulative curve | Always readable, never fake precision. |
| Content mix grouped bars | Shows format distribution | Hard to compare quickly | Horizontal stacked bars sorted by key metric | Faster scan and cleaner strategy view. |
| Large rollout table | Rich metric set | Dense and slow to parse | Today-first table with short labels + `Today move` | Turns KPIs into immediate plays. |
| Talking points text list | Logic is good | Easy to ignore visually | Artist-color callout cards | High salience with low reading load. |
| Artist deep dive | Good concept | Still too roster-comparison-heavy | Focus artist headline + grayscale benchmark context | One artist is the story. |

---

## CURRENT VS NEXT SCAFFOLDS (ASCII)

## 1) OVERVIEW TAB (ROSTER PULSE)

### Current scaffold
```text
[Header + source]
[Tabs]
[KPI: totals row]
[Chart: view growth] [Chart: engagement vs velocity]
[Content strategy signals]
[Top videos table]
```

### Next scaffold
```text
[Header + source + freshness]
[Tabs]
[KPI ROW: Daily plays | vs normal | Fans responding | Status]
[Secondary context KPIs: totals, videos analyzed]

[WHO NEEDS A PUSH + WHAT TO DO]
| Artist | Daily plays | vs normal | Fans responding | Last drop | Status | Full video boost | Shorts/other share | Cadence | Today move |

[Supporting visuals]
[Format mix by artist - horizontal]
[Top videos making noise right now]
```

---

## 2) ARTIST DEEP DIVE TAB (COACHING MODE)

### Current scaffold
```text
[Focus dropdown]
[KPI strip]
[Focus + benchmark tables/charts mixed]
```

### Next scaffold
```text
[Focus artist dropdown]
[FOCUS ARTIST HERO: BIG NAME + assigned color]
[Focus KPI row]

[What changed]
- Daily plays trend
- Fans responding trend
- Full video boost trend

[Where people watch this artist]
[Stacked format mix]

[Today move card]
- 2-3 concrete actions

[Benchmark context in grayscale]
```

---

## 3) VELOCITY TAB (WHO'S RUNNING RIGHT NOW)

### Current scaffold
```text
[Scatter chart]
[Legend clutter]
```

### Next scaffold
```text
[Scatter: plays/day vs fans responding]
[Quadrant labels]
- Big & loved
- Small but loved
- Big but low reaction
- Quiet

[Three short ranked tables]
1) Big & loved right now
2) Small but loved - worth a push
3) Big but low reaction
```

---

## VISUAL SYSTEM (PREATTENTIVE, PREMIUM, CLEAN)

### Color
- Primary action color: YouTube red family.
- Focus artist accent: assigned artist palette color.
- Benchmark context: grayscale neutrals.
- Warning color: amber/red only for true data risk.

### Motion
- Subtle only:
  - soft card entrance
  - hover emphasis on active rows
- No distracting perpetual animation loops.

### Type and spacing
- Short labels, high contrast, larger section headers.
- One idea per row/card.
- No wall-of-text blocks in decision areas.

---

## LANGUAGE STYLE RULES
- Plain words over heavy analytics jargon.
- Short, literal labels.
- Avoid score-like naming unless arithmetic is visible.
- Pair each metric with a clear "what to do now" direction.
- Tooltips should point to concise on-page explanations, not long paragraphs.

---

## CHART-BY-CHART UPGRADE CHECKLIST
- [ ] Each chart answers one decision question.
- [ ] Each table exposes a visible **Today move**.
- [ ] Focus artist pops immediately in Deep Dive.
- [ ] Benchmark context stays visible but softer.
- [ ] No pseudo-complex metric names.
- [ ] No chart kept only because it looks cool.
- [ ] Snapshot freshness is explicit and truthful.

---

## NORTH STAR
If someone opens this dashboard half-awake, they should still be able to answer:
1. **Who is moving?**
2. **Why are they moving?**
3. **What should we do today?**
