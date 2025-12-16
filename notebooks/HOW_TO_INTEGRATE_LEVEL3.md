# How to Integrate Level 3 Sections into MusicScope™ Dashboard

## ✅ **Prerequisites**

1. **Git commit created** - Safe restore point at commit `a6850c8`
2. **Design system imports added** - Already done in cell 5 of the notebook
3. **Level 3 modules created** - All 3 files compile successfully:
   - `notebooks/musicscope_momentum_level3.py`
   - `notebooks/musicscope_sentiment_level3.py`
   - `notebooks/musicscope_performance_level3.py`

---

## 🎯 **Integration Steps**

### **Step 1: Open the Notebook**

Open `notebooks/MusicScope™_Professional_Dashboard.ipynb` in Jupyter/VS Code.

---

### **Step 2: Replace Momentum Section (Cells 11-19)**

**Current cells 11-19:** Old momentum code (9 cells)

**Replace with 2 new cells:**

#### **Cell 11 (Markdown):**
```markdown
## ⚡ Momentum Intelligence

Track who is heating up, when to lean in, and how thresholds affect the breakout narrative.
```

#### **Cell 12 (Code):**
```python
# ============================================================================
# SECTION 2: ⚡ MOMENTUM INTELLIGENCE (LEVEL 3)
# ============================================================================

from musicscope_momentum_level3 import render_momentum_section

# Render the complete Momentum Intelligence section with REAL data
momentum_metadata = render_momentum_section(videos_df=videos_df, demo=False)

print(f"\n📊 Momentum section metadata:")
print(f"  - Charts rendered: {momentum_metadata['charts_rendered']}")
print(f"  - Artists analyzed: {momentum_metadata['artists_analyzed']}")
print(f"  - Breakout count: {momentum_metadata['breakout_count']}")
```

**Delete cells 13-19** (old momentum code)

---

### **Step 3: Replace Sentiment Section (Cells 20-26)**

**Current cells 20-26:** Old sentiment code (7 cells)

**Replace with 2 new cells:**

#### **Cell 20 (Markdown):**
```markdown
## ❤️ Sentiment Intelligence

Understand how your audience feels about your content.
```

#### **Cell 21 (Code):**
```python
# ============================================================================
# SECTION 3: ❤️ SENTIMENT INTELLIGENCE (LEVEL 3)
# ============================================================================

from musicscope_sentiment_level3 import render_sentiment_section

# Render the complete Sentiment Intelligence section with REAL data
sentiment_metadata = render_sentiment_section(videos_df=videos_df, comments_df=comments_df, demo=False)

print(f"\n📊 Sentiment section metadata:")
print(f"  - Charts rendered: {sentiment_metadata['charts_rendered']}")
print(f"  - Comments analyzed: {sentiment_metadata['comments_analyzed']:,}")
print(f"  - Net Sentiment Score: {sentiment_metadata['net_sentiment_score']:.1f}")
```

**Delete cells 22-26** (old sentiment code)

---

### **Step 4: Replace Performance Section (Cells 27+)**

**Current cells 27+:** Old performance code (24 cells)

**Replace with 2 new cells:**

#### **Cell 27 (Markdown):**
```markdown
## 🚀 Performance Intelligence

Identify which assets are actually converting attention into engagement.
```

#### **Cell 28 (Code):**
```python
# ============================================================================
# SECTION 4: 🚀 PERFORMANCE INTELLIGENCE (LEVEL 3)
# ============================================================================

from musicscope_performance_level3 import render_performance_section

# Render the complete Performance Intelligence section with REAL data
performance_metadata = render_performance_section(videos_df=videos_df, comments_df=comments_df, demo=False)

print(f"\n📊 Performance section metadata:")
print(f"  - Charts rendered: {performance_metadata['charts_rendered']}")
print(f"  - Hidden gems: {performance_metadata['hidden_gems_count']}")
print(f"  - Total engagement: {performance_metadata['total_engagement']:,}")
```

**Delete cells 29-50** (old performance code)

---

## 🧪 **Testing**

### **Step 5: Run the Notebook**

1. **Restart kernel** - Ensure clean state
2. **Run all cells** - Execute from top to bottom
3. **Verify output:**
   - ✅ Design system imports load successfully
   - ✅ All 3 sections render without errors
   - ✅ Hero cards appear with gradient backgrounds
   - ✅ Subsection cards show business questions
   - ✅ Charts display correctly
   - ✅ Insight cards appear after charts
   - ✅ Closing cards show metrics
   - ✅ Metadata prints correctly

---

## 🐛 **Troubleshooting**

### **Import Error: `musicscope_design_system`**

**Problem:** `ModuleNotFoundError: No module named 'musicscope_design_system'`

**Solution:** Ensure `notebooks/musicscope_design_system.py` exists and the notebook's working directory is correct.

```python
# Add to top of notebook if needed:
import sys
sys.path.insert(0, 'notebooks')
```

---

### **Import Error: `tools.advanced_charts`**

**Problem:** `ModuleNotFoundError: No module named 'tools'`

**Solution:** Ensure the project root is on `sys.path` (should already be done in cell 1).

---

### **Missing Data Columns**

**Problem:** `ValueError: Required column 'artist_name' not found`

**Solution:** The Level 3 modules use `resolve_artist_column()` to auto-detect artist columns. If you see this error, check that your data has at least one of: `artist_name`, `artist`, `channel_title`, `uploader`.

---

### **No Charts Rendering**

**Problem:** Code runs but no charts appear

**Solution:**
1. Check that `videos_df` and `comments_df` are not empty
2. Verify Plotly/Matplotlib are installed: `pip install plotly matplotlib`
4. Remember that **HTML cards only render inside Jupyter/VS Code notebooks**. When you run `python notebooks/test_level3_modules.py` from a terminal, you will see plain text logs like `[MUSICSCOPE HERO] ⚡ Momentum Intelligence` instead of gradient cards. This is expected and comes from the design system's IPython-aware display helpers.

---


3. Check Jupyter display settings

---

## 📊 **Expected Output**

After successful integration, you should see:

### **Momentum Section:**
- Purple gradient hero card
- Interactive threshold sliders
- KPI-22 dual-panel histogram (duration + warning)
- Closing card with 4 metrics

### **Sentiment Section:**
- Pink gradient hero card
- Donut chart with Net Sentiment Score
- Dual-axis volatility timeline
- Asset-level sentiment bars
- Closing card with 4 metrics

### **Performance Section:**
- Purple gradient hero card
- Hidden Gems scatter plot
- Content leaderboard bars
- Artist performance bars
- Engagement efficiency box plot
- Closing card with 4 metrics

---

## 🎉 **Success Criteria**

- [ ] All 3 sections render without errors
- [ ] All charts display correctly
- [ ] Insight cards appear with actionable messages
- [ ] Closing cards show correct metrics
- [ ] Metadata prints correctly
- [ ] Notebook runs end-to-end without manual intervention

---

## 🔄 **Rollback (If Needed)**

If something goes wrong, you can easily rollback:

```bash
# Restore from git commit
git checkout a6850c8 -- "notebooks/MusicScope™_Professional_Dashboard.ipynb"
```

Or restore from the backup created by the integration script:

```bash
cp "notebooks/MusicScope™_Professional_Dashboard.ipynb.backup" "notebooks/MusicScope™_Professional_Dashboard.ipynb"
```

---

## ✅ **Final Commit**

After successful integration and testing:

```bash
git add notebooks/musicscope_*_level3.py
git add "notebooks/MusicScope™_Professional_Dashboard.ipynb"
git commit -m "feat(notebooks): integrate Level 3 sections with modular architecture

- Created 3 modular Level 3 files (momentum, sentiment, performance)
- Hybrid architecture: design system + existing helpers
- Replaced old sections with clean function calls
- All charts render with Level 3 business framing
- Metadata returned for testing/debugging"
```

---

## 📝 **Notes**

- The old notebook had **51 cells total** (9 momentum + 7 sentiment + 24 performance + 11 other)
- The new notebook will have **~17 cells total** (2 momentum + 2 sentiment + 2 performance + 11 other)
- **Reduction:** 34 cells removed, replaced with 6 clean cells
- **Benefit:** Easier to maintain, test, and extend

