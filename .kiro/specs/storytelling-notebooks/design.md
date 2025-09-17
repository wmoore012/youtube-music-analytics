# Design Document

## Overview

This design addresses the critical import failures preventing notebook execution and git readiness, while establishing a foundation for compelling storytelling notebooks. The primary focus is fixing the `ModuleNotFoundError: No module named 'src'` issue and creating a minimal viable storytelling system that can be committed to git immediately.

The approach follows Test-Driven Development (TDD) principles: write failing tests first, implement minimal code to pass tests, then refactor for quality. This ensures git readiness takes priority over advanced features, with storytelling enhancements built incrementally on a stable foundation.

## Architecture

### Core Components (Prioritized for Git Readiness)

#### 1. Import Resolution System (CRITICAL - Phase 1)
**Design Rationale**: Import failures block all notebook execution and prevent git commits. This must be resolved first before any storytelling features can be implemented.

- **Package Installation Validation**: Ensure `pip install -e .` works correctly for `src.youtubeviz` package
- **Import Path Fixes**: Resolve `ModuleNotFoundError: No module named 'src'` in all notebooks
- **Dependency Resolution**: Validate all required modules are available and importable
- **Error Handling**: Provide clear error messages when imports fail

#### 2. Minimal Storytelling Framework (Phase 1)
**Design Rationale**: Build the simplest possible storytelling system that enables notebook execution while preserving existing functionality.

- **Core Storytelling Functions**: Enhance existing `story_block()` and `quick_takeaways()` functions
- **Basic Narrative Helpers**: Simple introduction and conclusion generators
- **Error Recovery**: Graceful handling of missing data or chart failures
- **Existing Chart Preservation**: Maintain all current visualizations while improving quality

#### 3. Notebook Execution System (Phase 1)
**Design Rationale**: Notebooks must execute end-to-end without errors to achieve git readiness and provide value to users.

- **Data Loading**: Reliable database connections and data retrieval
- **Chart Generation**: All existing charts must render properly with data
- **Output Validation**: Ensure executed notebooks contain complete analysis
- **Error Handling**: Clear messages when data is missing or operations fail

#### 4. Compassionate Analytics Framework (Phase 2)
**Design Rationale**: Music industry analysis should treat artists as humans with careers and dreams, not just data points.

- **Descriptive Analytics**: Present facts honestly while maintaining respect for artists
- **Sentiment Integration**: Include fan feedback and engagement analysis
- **Growth-Focused Recommendations**: Emphasize opportunities rather than failures
- **Educational Context**: Explain industry concepts for students and stakeholders

#### 5. Enhanced Visualization System (Phase 2)
**Design Rationale**: Beautiful, interactive charts improve engagement and make analysis more compelling for stakeholders.

- **Interactive Charts**: All visualizations use Plotly/Altair for interactivity
- **Consistent Color Schemes**: Global artist color mapping from .env configuration
- **Chart Enhancement**: Apply data visualization best practices and emotional impact
- **Mobile Compatibility**: Charts work across different screen sizes

#### 6. Educational Content Framework (Phase 3 - Future)
**Design Rationale**: Notebooks should serve as learning tools for data science students entering the music industry.

- **Concept Explanations**: Define technical and industry terms
- **Business Context**: Connect data analysis to music industry decisions
- **Learning Progression**: Structure content for different skill levels
- **Practical Applications**: Show how analysis informs real business decisions

## Components and Interfaces

### Phase 1: Critical Import Resolution

#### Package Installation Validation System
**Design Rationale**: Import failures must be detected and resolved systematically to ensure reliable notebook execution.

```python
# Import validation and resolution system
def validate_package_installation() -> bool:
    """Verify that 'pip install -e .' was run and src.youtubeviz is importable"""
    try:
        import src.youtubeviz.storytelling
        import src.youtubeviz.charts
        import src.youtubeviz.data
        return True
    except ImportError as e:
        print(f"Package installation issue: {e}")
        print("Please run: pip install -e .")
        return False

def fix_notebook_imports() -> None:
    """Update notebook imports to use correct package structure"""
    # Scan notebooks for import statements
    # Replace problematic imports with working ones
    # Validate imports work in Jupyter environment
```

#### Advanced Chart Specification System

**Design Rationale**: Charts must follow cognitive design principles and handle uncertainty properly for new artists with small sample sizes.

##### Core Chart Functions (Data-Science Grade)

```python
# 1. Sentiment Breakdown Charts
def create_sentiment_diverging_bars(df: pd.DataFrame, artists: List[str]) -> plotly.Figure:
    """
    Diverging stacked bars (negatives left, positives right) per artist
    - Wilson 95% CI error whiskers for proportion uncertainty
    - Small-multiples by video for detailed analysis
    - Interactive hover with raw counts + confidence intervals
    """

def create_sentiment_cluster_heatmap(df: pd.DataFrame) -> plotly.Figure:
    """
    Clustered heatmap of sentiment aspects × rates per artist
    - Bayesian shrinkage toward roster mean (beta-binomial partial pooling)
    - Seriation for optimal row/column ordering
    - Brush selection to reorder by artist profile
    """

# 2. Fan Feedback Analysis
def create_theme_lollipop_charts(themes_data: pd.DataFrame, sentiment: str) -> plotly.Figure:
    """
    Lollipop charts for top 3 positive/negative themes per artist
    - Wilson CI whiskers for each proportion
    - Collapse near-ties visually (overlapping CIs)
    - Side panel with extractive quotes linked to timestamps
    """

# 3. Content Performance Analysis
def create_standout_videos_scatter(df: pd.DataFrame) -> plotly.Figure:
    """
    Scatterplot: Positive rate (y) vs Views (log x) with LOWESS trend
    - Highlight large positive residuals (above trend) for promotion candidates
    - Gray 95% confidence band around trend line
    - Interactive hover with residual values
    """

def create_content_mix_charts(df: pd.DataFrame) -> Dict[str, plotly.Figure]:
    """
    Multiple chart types for content analysis:
    - 100% stacked bars (ISRC vs non-ISRC) with Wilson whiskers
    - Dumbbell charts (short-form vs long-form percentages)
    - Cleveland dot plots (MV vs lyric vs visualizer counts)
    - Stacked area charts (total views by category over time)
    """

# 4. Roster-Wide Analysis
def create_tour_compatibility_analysis(df: pd.DataFrame) -> Dict[str, plotly.Figure]:
    """
    UMAP scatter + cluster heatmap for tour compatibility
    - Video/comment embeddings colored by artist, shaped by content type
    - Density contours with nearest neighbor identification
    - Artist × artist similarity matrix with bootstrap CIs
    """

def create_upset_plot(features_df: pd.DataFrame) -> plotly.Figure:
    """
    UpSet plot for feature intersections (ISRC, short-form, visualizer, etc.)
    - Ranked by views/engagement
    - Click intersection to filter all other charts
    - Better than Venn diagrams for >3 sets
    """

# 5. Uncertainty and Smoothing Helpers
def apply_wilson_intervals(counts: np.array, totals: np.array) -> Tuple[np.array, np.array]:
    """Wilson confidence intervals for proportions (protects from low-n volatility)"""

def apply_bayesian_shrinkage(rates: pd.Series, roster_mean: float) -> pd.Series:
    """Beta-binomial partial pooling toward roster mean for stable estimates"""

def apply_loess_smoothing(x: np.array, y: np.array, frac: float = 0.3) -> np.array:
    """LOWESS/LOESS robust local regression for trend lines"""
```

##### Interactive Behavior System

```python
class NotebookInteractivity:
    """
    Implements Shneiderman mantra: overview first → zoom/filter → details on demand
    """

    def setup_cross_filtering(self) -> None:
        """
        Persistent filters that affect entire notebook:
        - Date range slider
        - Artist multi-select
        - Content type checkboxes
        - "Use shrinkage?" toggle
        """

    def create_linked_brushing(self) -> None:
        """
        Altair/Vega-Lite linked selections:
        - Brush artist in one chart → filter all others
        - Click intersection in UpSet → filter all charts
        - Select cluster in heatmap → highlight in scatter
        """

    def setup_drill_down_panels(self) -> None:
        """
        Details on demand:
        - Click sentiment bar → show representative quotes
        - Hover standout video → show residual calculation
        - Click artist rank → show trajectory details
        """
```

### Notebook Structure Framework

```python
class NotebookSection:
    title: str
    description: str
    educational_content: Optional[str]
    code_cells: List[CodeCell]
    narrative_cells: List[MarkdownCell]

class StorytellingNotebook:
    sections: List[NotebookSection]
    artist_list: List[str]
    color_scheme: Dict[str, str]

    def generate_intro() -> MarkdownCell
    def generate_conclusion() -> MarkdownCell
    def add_educational_context(section: str, concept: str) -> MarkdownCell
```

### Color System and Visual Consistency

**Design Rationale**: Follow ColorBrewer recommendations from chart specification - blue↔orange diverging palette for sentiment, avoid red/green for color-blind accessibility.

```python
class ColorSystemManager:
    """Manages consistent color schemes following ColorBrewer best practices"""

    def __init__(self, config_source: str = "env"):
        self.artist_colors = self.load_artist_colors(config_source)
        # Primary diverging palette for sentiment (color-blind safe)
        self.sentiment_palette = self.get_blue_orange_diverging()
        # Alternative diverging palette
        self.sentiment_alt_palette = self.get_purple_green_diverging()
        # Categorical palette for artists/categories
        self.category_palette = self.get_colorbrewer_categorical()

    def get_blue_orange_diverging(self) -> Dict[str, str]:
        """ColorBrewer blue-orange diverging palette for sentiment (primary choice)"""
        return {
            'very_negative': '#d7191c',    # Red-orange
            'negative': '#fdae61',         # Light orange
            'neutral': '#ffffbf',          # Light yellow
            'positive': '#abd9e9',         # Light blue
            'very_positive': '#2c7bb6'     # Blue
        }

    def get_purple_green_diverging(self) -> Dict[str, str]:
        """ColorBrewer purple-green diverging palette (alternative)"""
        return {
            'very_negative': '#762a83',    # Purple
            'negative': '#c2a5cf',         # Light purple
            'neutral': '#f7f7f7',          # Light gray
            'positive': '#a6dba0',         # Light green
            'very_positive': '#1b7837'     # Green
        }

    def get_colorbrewer_categorical(self) -> List[str]:
        """ColorBrewer Set2 categorical palette for artists (8 colors max)"""
        return [
            '#66c2a5',  # Teal
            '#fc8d62',  # Orange
            '#8da0cb',  # Blue
            '#e78ac3',  # Pink
            '#a6d854',  # Green
            '#ffd92f',  # Yellow
            '#e5c494',  # Beige
            '#b3b3b3'   # Gray
        ]

    def apply_artist_colors(self, fig: plotly.Figure, artists: List[str]) -> plotly.Figure:
        """Apply consistent artist colors from .env/config with ColorBrewer fallback"""
        # Try to load from config/artist_colors.json first
        # Fall back to ColorBrewer Set2 categorical palette
        # Ensure colors are consistent across all charts in notebook

    def get_sentiment_colors_for_chart(self, chart_type: str) -> Dict[str, str]:
        """Get appropriate sentiment colors based on chart type"""
        if chart_type in ['diverging_bars', 'lollipop_negative']:
            return self.sentiment_palette
        elif chart_type == 'heatmap':
            return self.sentiment_alt_palette  # Purple-green for variety
        else:
            return self.sentiment_palette  # Default to blue-orange

class ChartEnhancementSystem:
    """Applies data visualization best practices following chart specification"""

    def enhance_chart_beauty(self, fig: plotly.Figure, theme: str = "professional") -> plotly.Figure:
        """
        Apply visual enhancements per specification:
        - Maximize data-to-ink ratio (minimize chartjunk)
        - Use position & length encodings (most accurate perceptually)
        - Reserve color hue for categories, color intensity for quantitative
        - Label directly on lines/bars to reduce legend scanning
        - Appropriate chart sizing for mobile compatibility
        """

    def apply_cognitive_design_principles(self, fig: plotly.Figure, insights: List[str]) -> plotly.Figure:
        """
        Apply pre-attentive attributes and Gestalt principles:
        - Position (most accurate) for key comparisons
        - Length (bar charts) for magnitude comparisons
        - Color hue for categories, intensity for quantitative values
        - Size for magnitude, Shape for distinction
        - Proximity (group related metrics)
        - Similarity (consistent colors across charts)
        - Enclosure (highlight important sections)
        - Continuity (trend lines and connections)
        """

    def add_uncertainty_by_default(self, fig: plotly.Figure, uncertainty_data: Dict) -> plotly.Figure:
        """
        Show uncertainty indicators by default per specification:
        - Wilson confidence intervals for proportions (thin error whiskers)
        - Gray 95% confidence bands on LOESS trend lines
        - Bootstrap CIs for similarity matrices
        - "Needs more data" badges for n < 20 comments
        - Display both raw and shrunken values in tooltips
        """

    def apply_smoothing_and_stabilization(self, data: pd.DataFrame, chart_type: str) -> pd.DataFrame:
        """
        Apply appropriate smoothing per chart specification:
        - Time trends: LOWESS/LOESS robust local regression + EWMA
        - Proportions: Empirical Bayes shrinkage toward roster mean
        - Composition: Wilson intervals on bars
        - Rolling metrics: 7-day or 28-day rolling means
        - Week-level aggregation to avoid daily noise
        """
```

### Integration with Existing Configuration

**Design Rationale**: Leverage existing .env and config/artist_colors.json while implementing ColorBrewer best practices.

```python
class ConfigurationIntegration:
    """Integrates chart specification with existing project configuration"""

    def load_artist_colors_with_fallback(self) -> Dict[str, str]:
        """
        Load artist colors with ColorBrewer fallback:
        1. Try ARTIST_COLORS_JSON from .env
        2. Try config/artist_colors.json file
        3. Fall back to ColorBrewer Set2 categorical palette
        4. Ensure consistent colors across all 15 chart types
        """

    def setup_sentiment_color_scheme(self) -> Dict[str, str]:
        """
        Implement blue↔orange diverging palette per specification:
        - Primary: Blue-orange (color-blind safe)
        - Alternative: Purple-green for variety in heatmaps
        - Avoid red/green combinations
        - Consistent across diverging bars, lollipops, scatter plots
        """

    def configure_chart_defaults(self) -> Dict[str, Any]:
        """
        Set up chart defaults per specification:
        - Altair/Vega-Lite for linked brushing and interactivity
        - Plotly for heavy drilldowns and complex tooltips
        - Wilson intervals for all proportion-based charts
        - LOESS smoothing for time trends
        - Gray confidence bands by default
        """

    def setup_cross_filtering_system(self) -> Dict[str, Any]:
        """
        Configure persistent filters per specification:
        - Date range slider
        - Artist multi-select (from .env channel configuration)
        - Content type checkboxes
        - "Use shrinkage?" toggle for Bayesian stabilization
        - All filters cross-filter entire notebook
        """

### Auto-Generated Summary System

**Design Rationale**: Pull top insights from currently filtered view and render actionable markdown narratives.

```python
class AutoSummaryGenerator:
    """Generates markdown summaries based on current chart state"""

    def generate_insight_summary(self, filtered_data: pd.DataFrame) -> str:
        """
        Extract top insights from currently filtered view per specification:
        - Top themes with non-overlapping Wilson confidence intervals
        - Standout videos by LOESS residual analysis (above trend candidates)
        - Tour compatibility based on UMAP clustering and fan overlap
        - Content mix optimization using p-chart control limits
        - Roster ranking changes from bump chart analysis
        """

    def create_compassionate_narrative(self, artist_data: Dict) -> str:
        """
        Generate artist-focused narrative following requirements:
        - Present hard truths honestly with fan sentiment context
        - Focus on growth opportunities rather than failures
        - Include actual fan quotes and engagement drivers
        - Provide specific, actionable next steps based on data
        - Treat artists as humans with careers, not just data points
        """

    def format_educational_content(self, concept: str, context: str) -> str:
        """
        Create educational explanations for students:
        - Define statistical concepts (Wilson intervals, LOESS, EB shrinkage)
        - Connect data analysis to music business decisions
        - Explain why certain chart types work better (position > angle/area)
        - Describe cognitive design principles in accessible language
        """
```

## Data Models

### Notebook Configuration
```python
@dataclass
class NotebookConfig:
    title: str
    artists: List[str]
    analysis_type: str  # "comparison", "deep_dive", "overview"
    timeframe_days: int
    educational_level: str  # "beginner", "intermediate", "advanced"
    color_scheme: str  # "vibrant", "professional", "academic"
    include_revenue: bool = True
    include_sentiment: bool = True
```

### Story Elements
```python
@dataclass
class StoryElement:
    section_type: str  # "intro", "analysis", "insight", "recommendation"
    content_type: str  # "markdown", "code", "visualization"
    narrative_text: str
    educational_notes: Optional[str]
    business_context: Optional[str]
```

### Chart Enhancement Metadata
```python
@dataclass
class ChartMetadata:
    chart_type: str
    emotional_tone: str  # "celebratory", "analytical", "cautionary"
    key_insights: List[str]
    what_to_look_for: List[str]
    business_implications: List[str]
```

## Error Handling

### Graceful Degradation
- **Missing Data**: Provide clear explanations when data is unavailable
- **Chart Failures**: Fall back to data tables with narrative explanations
- **Color Scheme Issues**: Use default palettes with warnings about configuration

### Educational Error Messages
- **Student-Friendly**: Explain what went wrong and why it matters
- **Learning Opportunities**: Turn errors into teaching moments about data quality
- **Recovery Suggestions**: Provide clear steps to resolve issues

### Data Quality Integration
- **Validation Checks**: Ensure data quality before generating insights
- **Quality Warnings**: Alert users to potential data issues that affect analysis
- **Confidence Indicators**: Show reliability levels for different metrics

## Testing Strategy (Comprehensive Validation)

### Notebook Cell Count Validation
**Design Rationale**: Notebooks must have the correct number of cells to ensure complete execution and prevent silent failures.

```python
class TestNotebookStructure:
    """Validate notebook structure and cell count"""

    def test_notebook_has_expected_cell_count(self):
        """Test that notebook contains all required cells"""
        expected_cells = {
            'markdown': 15,  # Introduction, section headers, explanations
            'code': 25,      # Data loading, chart generation, analysis
            'total': 40      # Must match exactly
        }

        actual_cells = self.count_notebook_cells(notebook_path)
        assert actual_cells['total'] == expected_cells['total'], \
            f"Expected {expected_cells['total']} cells, got {actual_cells['total']}"

    def test_all_code_cells_execute_successfully(self):
        """Test that every code cell executes without errors"""
        # Must pass for git readiness

    def test_all_charts_render_with_data(self):
        """Test that all 15 chart specifications produce valid outputs"""
        # Each chart from the specification must render
```

### Chart-Specific Testing
**Design Rationale**: Each chart specification must be validated for data accuracy, visual correctness, and interactive behavior.

```python
class TestSentimentCharts:
    """Test sentiment analysis chart functions"""

    def test_diverging_bars_have_wilson_intervals(self):
        """Test that sentiment bars include Wilson CI whiskers"""

    def test_cluster_heatmap_applies_bayesian_shrinkage(self):
        """Test that heatmap uses beta-binomial partial pooling"""

    def test_lollipop_charts_show_top_3_themes(self):
        """Test that theme charts display exactly 3 themes per artist"""

class TestContentAnalysisCharts:
    """Test content categorization and performance charts"""

    def test_standout_videos_scatter_has_loess_trend(self):
        """Test scatter plot includes LOWESS trend line with confidence band"""

    def test_upset_plot_handles_feature_intersections(self):
        """Test UpSet plot correctly calculates set intersections"""

    def test_tour_compatibility_umap_clustering(self):
        """Test UMAP embedding and cluster analysis"""

class TestInteractiveFeatures:
    """Test interactive chart behaviors"""

    def test_cross_filtering_works_across_charts(self):
        """Test that selecting in one chart filters others"""

    def test_drill_down_panels_show_details(self):
        """Test that clicking elements reveals additional information"""

    def test_hover_tooltips_show_confidence_intervals(self):
        """Test that tooltips display uncertainty measures"""
```

### Data Quality and Uncertainty Testing
**Design Rationale**: New artists have small sample sizes requiring special handling for statistical validity.

```python
class TestUncertaintyHandling:
    """Test statistical uncertainty and smoothing methods"""

    def test_wilson_intervals_calculated_correctly(self):
        """Test Wilson confidence intervals for proportions"""

    def test_bayesian_shrinkage_stabilizes_small_samples(self):
        """Test that EB shrinkage prevents small-n volatility"""

    def test_minimum_n_thresholds_enforced(self):
        """Test that cells with n < 20 show 'needs more data' badge"""

    def test_loess_smoothing_robust_to_outliers(self):
        """Test that LOWESS handles outliers appropriately"""

class TestNewArtistDataHandling:
    """Test specific handling for new artists with limited data"""

    def test_rate_stabilization_with_beta_binomial(self):
        """Test beta-binomial EB for all per-artist rates"""

    def test_time_normalization_views_per_day(self):
        """Test views/day since upload calculation"""

    def test_engagement_per_1000_views_metric(self):
        """Test normalized engagement metrics"""
```

### Git Readiness Validation
**Design Rationale**: All tests must pass before code can be committed to ensure system stability.

```python
class TestGitReadiness:
    """Critical tests that must pass for git commit"""

    def test_package_imports_work_in_clean_environment(self):
        """Test pip install -e . works from scratch"""

    def test_all_notebooks_execute_end_to_end(self):
        """Test complete notebook execution without failures"""

    def test_no_hardcoded_paths_or_dependencies(self):
        """Test system works across different environments"""

    def test_all_15_chart_specifications_implemented(self):
        """Test that every chart from specification is implemented"""

    def test_interactive_features_work_in_jupyter(self):
        """Test that Altair/Plotly interactivity works in notebooks"""
```

## TDD Implementation Approach (Test-Obsessed)

### Phase 1: Test-First Import Resolution (Git Readiness Priority)
1. **Write Import Tests First**: Create comprehensive test suite for all import scenarios
2. **Implement Import Fixes**: Write minimal code to pass import tests
3. **Test Package Installation**: Validate `pip install -e .` works through tests
4. **Refactor for Quality**: Clean up import code while maintaining test coverage

### TDD Workflow for Each Feature
```python
# Step 1: Write failing test
def test_story_block_handles_none_figure():
    result = story_block(None, "Test", ["bullet"])
    assert "Error: No figure provided" in result

# Step 2: Run test (should fail)
# pytest tests/test_storytelling.py::test_story_block_handles_none_figure

# Step 3: Write minimal implementation to pass test
def story_block(fig, title, bullets, caption=None):
    if fig is None:
        return "Error: No figure provided"
    # ... rest of implementation

# Step 4: Run test (should pass)
# Step 5: Refactor while keeping tests green
```

### Test Coverage Requirements
- **Minimum Coverage**: 95% for all new code
- **Critical Functions**: 100% coverage for import resolution and core storytelling
- **Error Paths**: Every error scenario must have corresponding tests
- **Integration Points**: All module interactions must be tested

### Git Readiness Checklist (All Tests Must Pass)
```python
class GitReadinessTests:
    def test_package_installs_cleanly_in_fresh_environment()
    def test_all_notebooks_execute_without_import_errors()
    def test_storytelling_functions_work_with_real_data()
    def test_error_handling_provides_helpful_messages()
    def test_no_hardcoded_dependencies_or_paths()
```

### Continuous Integration Integration
- **Pre-commit Hooks**: Run full test suite before any commit
- **Test-Driven Refactoring**: Only refactor when all tests are green
- **Coverage Reporting**: Fail CI if coverage drops below 95%
- **Property-Based Testing**: Generate edge cases automatically

## TDD Success Metrics (Test-Obsessed Quality Gates)

### Test Coverage Metrics (Mandatory for Git Commit)
- **Unit Test Coverage**: 100% for critical functions (import resolution, storytelling core)
- **Integration Test Coverage**: 95% for notebook execution workflows
- **Error Path Coverage**: 100% for all error handling scenarios
- **Property-Based Test Coverage**: All public functions tested with generated inputs

### Git Readiness Metrics (Must Pass Before Commit)
- **Import Success Rate**: 100% of notebook imports work without errors
- **Package Installation**: `pip install -e .` works in clean environment
- **Notebook Execution**: All notebooks execute end-to-end without failures
- **Test Suite Execution**: All tests pass in under 30 seconds

### TDD Quality Metrics
- **Red-Green-Refactor Cycles**: Every feature follows strict TDD workflow
- **Test-First Development**: No production code written without failing test first
- **Refactoring Safety**: All refactoring done with green test suite
- **Continuous Testing**: Tests run on every code change

### Code Quality Through Testing
- **Error Handling**: Every error scenario has corresponding test
- **Edge Cases**: Property-based testing covers unexpected inputs
- **Integration Points**: All module interactions validated through tests
- **Regression Prevention**: Comprehensive test suite prevents future breakage
