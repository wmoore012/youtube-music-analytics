"""
UMAP clustering analysis for tour compatibility and artist similarity.
Bulletproof implementation with proper error handling and validation.
"""

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from .chart_models import (
    UMAPClusteringData,
    validate_data_quality,
    validate_dataframe_schema,
    validate_required_columns,
)


class UMAPNotAvailableError(Exception):
    """Raised when UMAP is not available but required."""

    pass


class InsufficientDataError(Exception):
    """Raised when there's insufficient data for clustering analysis."""

    pass


@dataclass
class ClusteringResult:
    """Results from UMAP clustering analysis."""

    embeddings: np.ndarray
    cluster_labels: np.ndarray
    similarity_matrix: np.ndarray
    artist_names: List[str]
    n_clusters: int
    silhouette_score: float


class UMAPClusteringAnalyzer:
    """
    Bulletproof UMAP clustering analyzer for tour compatibility analysis.

    Fails loudly with clear error messages when requirements aren't met.
    """

    def __init__(self, min_samples_per_artist: int = 10, min_total_samples: int = 50, random_state: int = 42):
        """
        Initialize UMAP clustering analyzer.

        Args:
            min_samples_per_artist: Minimum samples required per artist
            min_total_samples: Minimum total samples for analysis
            random_state: Random state for reproducibility

        Raises:
            UMAPNotAvailableError: If UMAP dependencies are not available
        """
        self.min_samples_per_artist = min_samples_per_artist
        self.min_total_samples = min_total_samples
        self.random_state = random_state

        # Check for required dependencies
        self._validate_dependencies()

    def _validate_dependencies(self) -> None:
        """Validate that required dependencies are available."""
        try:
            from sklearn.cluster import KMeans
            from sklearn.feature_extraction.text import TfidfVectorizer
            from sklearn.metrics import silhouette_score
            import umap
        except ImportError as e:
            raise UMAPNotAvailableError(
                f"Required dependencies not available: {e}. " "Install with: pip install umap-learn scikit-learn"
            )

    def validate_input_data(self, df: pd.DataFrame) -> None:
        """
        Validate input data for clustering analysis.

        Args:
            df: DataFrame with clustering data

        Raises:
            ChartDataValidationError: If data validation fails
            InsufficientDataError: If insufficient data for analysis
        """
        # Check required columns
        required_columns = [
            "artist_name",
            "video_id",
            "comment_text",
            "sentiment_category",
            "content_type",
            "views",
            "engagement_rate",
        ]
        validate_required_columns(df, required_columns)

        # Validate data quality
        validate_data_quality(df, self.min_total_samples)

        # Validate against Pydantic schema
        validate_dataframe_schema(df, UMAPClusteringData, sample_size=50)

        # Check per-artist sample counts
        artist_counts = df["artist_name"].value_counts()
        insufficient_artists = artist_counts[artist_counts < self.min_samples_per_artist]

        if len(insufficient_artists) > 0:
            raise InsufficientDataError(
                f"Artists with insufficient data (< {self.min_samples_per_artist} samples): "
                f"{insufficient_artists.index.tolist()}. "
                f"Counts: {insufficient_artists.to_dict()}"
            )

        # Check total sample count
        if len(df) < self.min_total_samples:
            raise InsufficientDataError(
                f"Total samples ({len(df)}) below minimum ({self.min_total_samples}) "
                "for reliable clustering analysis"
            )

    def create_text_embeddings(self, texts: List[str]) -> np.ndarray:
        """
        Create text embeddings using TF-IDF.

        Args:
            texts: List of comment texts

        Returns:
            TF-IDF embeddings matrix

        Raises:
            ValueError: If texts are empty or invalid
        """
        if not texts or len(texts) == 0:
            raise ValueError("Cannot create embeddings from empty text list")

        # Filter out very short texts
        valid_texts = [text for text in texts if len(text.strip()) >= 10]

        if len(valid_texts) < len(texts) * 0.8:  # Lost more than 20% of texts
            raise ValueError(
                f"Too many short texts: {len(texts) - len(valid_texts)} out of {len(texts)} "
                "texts are too short (< 10 characters)"
            )

        try:
            from sklearn.feature_extraction.text import TfidfVectorizer

            # Create TF-IDF embeddings
            vectorizer = TfidfVectorizer(
                max_features=1000, stop_words="english", ngram_range=(1, 2), min_df=2, max_df=0.8
            )

            embeddings = vectorizer.fit_transform(valid_texts)
            return embeddings.toarray()

        except Exception as e:
            raise ValueError(f"Failed to create text embeddings: {e}")

    def perform_umap_reduction(self, embeddings: np.ndarray) -> np.ndarray:
        """
        Perform UMAP dimensionality reduction.

        Args:
            embeddings: High-dimensional embeddings

        Returns:
            2D UMAP embeddings

        Raises:
            ValueError: If UMAP reduction fails
        """
        try:
            import umap

            reducer = umap.UMAP(
                n_components=2,
                random_state=self.random_state,
                n_neighbors=min(15, embeddings.shape[0] - 1),
                min_dist=0.1,
                metric="cosine",
            )

            umap_embeddings = reducer.fit_transform(embeddings)

            # Validate output
            if umap_embeddings.shape[1] != 2:
                raise ValueError(f"UMAP should produce 2D embeddings, got {umap_embeddings.shape[1]}D")

            if np.any(np.isnan(umap_embeddings)):
                raise ValueError("UMAP produced NaN values-check input data quality")

            return umap_embeddings

        except Exception as e:
            raise ValueError(f"UMAP reduction failed: {e}")

    def perform_clustering(self, embeddings: np.ndarray, n_clusters: Optional[int] = None) -> Tuple[np.ndarray, float]:
        """
        Perform K-means clustering on embeddings.

        Args:
            embeddings: 2D embeddings for clustering
            n_clusters: Number of clusters (auto-determined if None)

        Returns:
            Tuple of (cluster_labels, silhouette_score)

        Raises:
            ValueError: If clustering fails
        """
        try:
            from sklearn.cluster import KMeans
            from sklearn.metrics import silhouette_score

            # Auto-determine number of clusters if not specified
            if n_clusters is None:
                n_clusters = min(max(2, len(np.unique(embeddings, axis=0)) // 10), 8)

            # Ensure we don't have more clusters than samples
            n_clusters = min(n_clusters, embeddings.shape[0] - 1)

            if n_clusters < 2:
                raise ValueError(f"Need at least 2 clusters, but only {n_clusters} possible")

            # Perform clustering
            kmeans = KMeans(n_clusters=n_clusters, random_state=self.random_state, n_init=10)
            cluster_labels = kmeans.fit_predict(embeddings)

            # Calculate silhouette score
            if len(np.unique(cluster_labels)) > 1:
                sil_score = silhouette_score(embeddings, cluster_labels)
            else:
                sil_score = 0.0

            return cluster_labels, sil_score

        except Exception as e:
            raise ValueError(f"Clustering failed: {e}")

    def analyze_clustering(self, df: pd.DataFrame) -> ClusteringResult:
        """
        Perform complete clustering analysis.

        Args:
            df: DataFrame with comment and artist data

        Returns:
            ClusteringResult with all analysis results

        Raises:
            ChartDataValidationError: If data validation fails
            InsufficientDataError: If insufficient data
            ValueError: If analysis fails
        """
        # Validate input data
        self.validate_input_data(df)

        # Create text embeddings
        texts = df["comment_text"].tolist()
        embeddings = self.create_text_embeddings(texts)

        # Perform UMAP reduction
        umap_embeddings = self.perform_umap_reduction(embeddings)

        # Perform clustering
        cluster_labels, silhouette_score = self.perform_clustering(umap_embeddings)

        # Calculate artist similarity matrix
        similarity_matrix = self._calculate_artist_similarity_matrix(df, umap_embeddings)

        return ClusteringResult(
            embeddings=umap_embeddings,
            cluster_labels=cluster_labels,
            similarity_matrix=similarity_matrix,
            artist_names=df["artist_name"].unique().tolist(),
            n_clusters=len(np.unique(cluster_labels)),
            silhouette_score=silhouette_score,
        )

    def _calculate_artist_similarity_matrix(self, df: pd.DataFrame, embeddings: np.ndarray) -> np.ndarray:
        """Calculate similarity matrix between artists."""
        from sklearn.metrics.pairwise import cosine_similarity

        artists = df["artist_name"].unique()
        n_artists = len(artists)
        similarity_matrix = np.zeros((n_artists, n_artists))

        for i, artist1 in enumerate(artists):
            artist1_mask = df["artist_name"] == artist1
            artist1_embeddings = embeddings[artist1_mask]

            for j, artist2 in enumerate(artists):
                if i <= j:  # Only calculate upper triangle
                    artist2_mask = df["artist_name"] == artist2
                    artist2_embeddings = embeddings[artist2_mask]

                    # Calculate average similarity between artist embeddings
                    if len(artist1_embeddings) > 0 and len(artist2_embeddings) > 0:
                        similarity = cosine_similarity(
                            artist1_embeddings.mean(axis=0).reshape(1, -1),
                            artist2_embeddings.mean(axis=0).reshape(1, -1),
                        )[0, 0]
                        similarity_matrix[i, j] = similarity
                        similarity_matrix[j, i] = similarity  # Symmetric matrix

        return similarity_matrix


def calculate_artist_similarity_matrix(df: pd.DataFrame, embeddings: np.ndarray) -> np.ndarray:
    """
    Calculate artist similarity matrix from embeddings.

    Args:
        df: DataFrame with artist data
        embeddings: UMAP embeddings

    Returns:
        Artist similarity matrix
    """
    analyzer = UMAPClusteringAnalyzer()
    return analyzer._calculate_artist_similarity_matrix(df, embeddings)


def analyze_tour_compatibility(
    similarity_matrix: np.ndarray, artist_names: List[str], threshold: float = 0.7
) -> Dict[str, List[str]]:
    """
    Analyze tour compatibility based on similarity matrix.

    Args:
        similarity_matrix: Artist similarity matrix
        artist_names: List of artist names
        threshold: Similarity threshold for compatibility

    Returns:
        Dictionary mapping artists to compatible tour partners
    """
    if len(artist_names) != similarity_matrix.shape[0]:
        raise ValueError("Artist names length must match similarity matrix dimensions")

    compatibility = {}

    for i, artist in enumerate(artist_names):
        # Find artists with similarity above threshold (excluding self)
        compatible_indices = np.where((similarity_matrix[i] >= threshold) & (np.arange(len(artist_names)) != i))[0]

        compatible_artists = [artist_names[j] for j in compatible_indices]
        compatibility[artist] = compatible_artists

    return compatibility
