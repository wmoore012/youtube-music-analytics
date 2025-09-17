# Requirements Document

## Introduction

This feature enhances the existing sentiment analysis system with production-grade improvements including deterministic dataset generation, music-domain VADER customizations, comprehensive evaluation framework, and robust data quality controls. The system will provide more accurate sentiment analysis for YouTube music comments while maintaining reproducibility and operational excellence.

## Requirements

### Requirement 1: Deterministic Dataset Generation

**User Story:** As a data scientist, I want deterministic dataset IDs and fingerprints, so that I can reproduce experiments and track dataset versions across builds.

#### Acceptance Criteria

1. WHEN a dataset is built THEN the system SHALL generate UUID-5 based deterministic IDs using schema version namespace
2. WHEN the same phrase with identical labels is processed THEN the system SHALL produce the same ID consistently
3. WHEN dataset fingerprint is requested THEN the system SHALL return SHA-256 hash of all entries for build provenance
4. WHEN dataset is exported THEN the system SHALL include schema version and build fingerprint in metadata

### Requirement 2: Unicode-Aware Text Normalization

**User Story:** As a data engineer, I want robust text normalization that handles emoji and Unicode properly, so that duplicate detection works reliably across different text encodings.

#### Acceptance Criteria

1. WHEN text contains Unicode characters THEN the system SHALL apply NFKC normalization
2. WHEN text contains emoji sequences THEN the system SHALL handle skin-tone modifiers and ZWJ sequences consistently
3. WHEN generating deduplication keys THEN the system SHALL use casefold and whitespace collapse
4. WHEN comparing phrases THEN the system SHALL detect near-duplicates using normalized keys

### Requirement 3: Music-Domain VADER Enhancement Variants

**User Story:** As a sentiment analyst, I want multiple VADER enhancement approaches tested, so that we can identify the most effective customization strategy for music YouTube comments.

#### Acceptance Criteria

1. WHEN creating enhancement variants THEN the system SHALL implement 3-5 different approaches with varying levels of customization
2. WHEN processing music slang terms THEN variant models SHALL apply different valence score strategies for terms like "slaps", "banger", "goated"
3. WHEN encountering modern boosters THEN variant models SHALL test different intensifier recognition approaches for "af", "frfr", "deadass"
4. WHEN processing multi-word idioms THEN variant models SHALL experiment with different phrase handling strategies
5. WHEN scoring emoji THEN variant models SHALL test conservative vs aggressive emoji weight adjustments
6. WHEN applying custom weights THEN all variants SHALL maintain VADER's booster math consistency
7. WHEN comparing variants THEN the system SHALL evaluate minimal-change vs comprehensive-enhancement approaches

### Requirement 4: SemEval-Aligned Quality Controls

**User Story:** As a dataset curator, I want automated quality checks that enforce sentiment labeling consistency, so that the dataset meets academic standards.

#### Acceptance Criteria

1. WHEN intent is REQUEST or INFO THEN the system SHALL enforce neutral sentiment unless opinion boosters are present
2. WHEN duplicate phrases are detected THEN the system SHALL prevent entries with same normalized text and identical label triples
3. WHEN NSFW content is marked THEN the system SHALL validate toxicity level consistency
4. WHEN quality checks fail THEN the system SHALL provide specific error messages with phrase examples

### Requirement 5: Comprehensive Evaluation Framework

**User Story:** As a machine learning engineer, I want rigorous A/B testing capabilities for sentiment model improvements, so that I can measure performance gains with statistical confidence.

#### Acceptance Criteria

1. WHEN comparing models THEN the system SHALL support multi-way comparison: current model, stock VADER, and 3-5 enhanced VADER variants
2. WHEN evaluating models THEN the system SHALL support paired testing on identical comment sets
3. WHEN evaluating generalization THEN the system SHALL use GroupKFold by video_id to prevent data leakage
4. WHEN computing significance THEN the system SHALL apply McNemar's test for paired classifier comparison
5. WHEN reporting metrics THEN the system SHALL provide bootstrap confidence intervals for performance deltas
6. WHEN benchmarking THEN the system SHALL preserve current model performance as baseline for comparison
7. WHEN testing multiple slices THEN the system SHALL apply Benjamini-Hochberg FDR correction

### Requirement 6: Production-Grade Data Pipeline

**User Story:** As a data platform engineer, I want robust export capabilities with timeout controls and schema validation, so that the system can run reliably in production environments.

#### Acceptance Criteria

1. WHEN exporting data THEN the system SHALL support configurable timeout controls
2. WHEN schema changes occur THEN the system SHALL export JSON Schema for downstream validation
3. WHEN data is cached THEN the system SHALL use cached properties for DataFrame generation
4. WHEN CLI operations are needed THEN the system SHALL provide command-line interface for stats and exports

### Requirement 7: Experiment Reproducibility

**User Story:** As a research scientist, I want complete experiment traceability and reproducibility, so that results can be validated and experiments can be replicated.

#### Acceptance Criteria

1. WHEN running experiments THEN the system SHALL log patch_id, code commit, data timestamp, and fold indices
2. WHEN using randomization THEN the system SHALL set and record random seeds
3. WHEN fetching YouTube data THEN the system SHALL log API query parameters and pagination tokens
4. WHEN generating reports THEN the system SHALL include experiment metadata and configuration details

### Requirement 8: Multi-Level Testing Strategy

**User Story:** As a quality assurance engineer, I want comprehensive testing at multiple levels, so that sentiment improvements are validated across different data slices and use cases.

#### Acceptance Criteria

1. WHEN testing overall performance THEN the system SHALL report macro-F1 on labeled comments for current model, stock VADER, and all enhancement variants
2. WHEN testing by video THEN the system SHALL compute per-video macro-F1 metrics comparing current vs enhanced models
3. WHEN testing data slices THEN the system SHALL evaluate emoji-heavy, booster-present, and idiom-present subsets
4. WHEN testing comment length THEN the system SHALL compare performance on long vs short comments
5. WHEN benchmarking current model THEN the system SHALL establish performance baseline before implementing enhancements
6. WHEN multiple comparisons are made THEN the system SHALL adjust p-values for false discovery rate control
