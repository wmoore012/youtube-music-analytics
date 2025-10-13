import re

"""
Smart Comment Classification Assistant

Uses machine learning to suggest positive / negative classifications based on
your existing classifications. Learns from your decisions and gets better over time.
"""

import os
import pickle
import sqlite3
import sys
from datetime import datetime
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score

# Transformer support
try:
    import torch
    from transformers import AutoModel, AutoTokenizer

    TRANSFORMERS_AVAILABLE = True
except ImportError:
    TRANSFORMERS_AVAILABLE = False

# Add paths
sys.path.insert(0, ".")
sys.path.insert(0, "src")

try:
    from web.etl_helpers import get_engine
except ImportError:

    def get_engine():
        from sqlalchemy import create_engine

        return create_engine("sqlite:///comment_classifications.db")


# Import ML data models for transformer support
try:
    from youtubeviz.ml_data_models import TransformerConfig
    from youtubeviz.text_processing_helpers import EmojiHandler, MusicSlangPreserver

    ML_MODELS_AVAILABLE = True
except ImportError:
    ML_MODELS_AVAILABLE = False


class CommentClassificationDB:
    """Manages the database of classified comments."""

    def __init__(self, db_path: str = "comment_classifications.db"):
        self.db_path = db_path
        self.init_database()

    def init_database(self):
        """Initialize the classification database."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS comment_classifications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                comment_text TEXT NOT NULL,
                classification TEXT NOT NULL,  -- 'positive' or 'negative'
                confidence REAL,  -- Your confidence in the classification (0-1)
                source TEXT,  -- 'manual', 'imported', 'enhanced_dataset'
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                notes TEXT
            )
        """
        )

        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_classification ON comment_classifications(classification)
        """
        )

        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_created_at ON comment_classifications(created_at)
        """
        )

        conn.commit()
        conn.close()

    def add_classification(
        self, comment_text: str, classification: str, confidence: float = 1.0, source: str = "manual", notes: str = ""
    ):
        """Add a new classification to the database."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute(
            """
            INSERT INTO comment_classifications
            (comment_text, classification, confidence, source, notes)
            VALUES (?, ?, ?, ?, ?)
        """,
            (comment_text, classification, confidence, source, notes),
        )

        conn.commit()
        conn.close()

    def get_all_classifications(self) -> pd.DataFrame:
        """Get all classifications as a DataFrame."""
        conn = sqlite3.connect(self.db_path)
        df = pd.read_sql_query(
            """
            SELECT comment_text, classification, confidence, source, created_at, notes
            FROM comment_classifications
            ORDER BY created_at DESC
        """,
            conn,
        )
        conn.close()
        return df

    def get_training_data(self, min_confidence: float = 0.7) -> Tuple[List[str], List[str]]:
        """Get high-confidence classifications for training."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT comment_text, classification
            FROM comment_classifications
            WHERE confidence >= ?
            ORDER BY created_at
        """,
            (min_confidence,),
        )

        results = cursor.fetchall()
        conn.close()

        if not results:
            return [], []

        texts, labels = zip(*results)
        return list(texts), list(labels)

    def get_stats(self) -> Dict:
        """Get classification statistics."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute("SELECT COUNT(*) FROM comment_classifications")
        total = cursor.fetchone()[0]

        cursor.execute(
            """
            SELECT classification, COUNT(*)
            FROM comment_classifications
            GROUP BY classification
        """
        )
        by_class = dict(cursor.fetchall())

        cursor.execute(
            """
            SELECT AVG(confidence)
            FROM comment_classifications
        """
        )
        avg_confidence = cursor.fetchone()[0] or 0

        conn.close()

        return {"total": total, "by_classification": by_class, "average_confidence": avg_confidence}


class SmartCommentClassifier:
    """ML-powered comment classifier that learns from your classifications."""

    def __init__(self, db_path: str = "comment_classifications.db", use_transformer: bool = False):
        self.db = CommentClassificationDB(db_path)
        self.use_transformer = use_transformer and TRANSFORMERS_AVAILABLE

        # Traditional ML setup
        self.vectorizer = TfidfVectorizer(max_features=5000, ngram_range=(1, 2), stop_words="english", lowercase=True)
        self.model = LogisticRegression(random_state=42, max_iter=1000)

        # Transformer setup
        self.transformer_processor = None
        self.transformer_model = None
        self.transformer_tokenizer = None

        if self.use_transformer:
            self._setup_transformer()

        self.is_trained = False
        self.model_path = "comment_classifier_model.pkl"
        self.vectorizer_path = "comment_vectorizer.pkl"
        self.transformer_model_path = "transformer_classifier_model.pkl"

        # Try to load existing model
        self.load_model()

    def _setup_transformer(self):
        """Set up transformer components."""
        if not ML_MODELS_AVAILABLE:
            print("⚠️  ML models not available for transformer setup")
            return

        try:
            from youtubeviz.text_processing_helpers import create_music_text_processor

            # Create music-aware text processor
            self.transformer_processor = create_music_text_processor(model_name="distilbert-base-uncased")

            print("✅ Transformer text processor initialized")

        except Exception as e:
            print(f"⚠️  Could not set up transformer: {e}")
            self.use_transformer = False

    def import_enhanced_dataset(self):
        """Import classifications from the enhanced dataset."""
        try:
            from datasets.enhanced_sentiment_dataset import get_enhanced_music_dataset

            dataset = get_enhanced_music_dataset()
            imported_count = 0

            for entry in dataset.entries:
                # Convert sentiment labels to positive / negative
                if entry.sentiment.value == "positive":
                    classification = "positive"
                elif entry.sentiment.value == "negative":
                    classification = "negative"
                else:
                    continue  # Skip neutral for now

                self.db.add_classification(
                    comment_text=entry.phrase,
                    classification=classification,
                    confidence=entry.confidence,
                    source="enhanced_dataset",
                    notes=entry.context_notes,
                )
                imported_count += 1

            print(f"✅ Imported {imported_count} classifications from enhanced dataset")
            return imported_count

        except Exception as e:
            print(f"⚠️  Could not import enhanced dataset: {e}")
            return 0

    def train_model(self, min_confidence: float = 0.7) -> bool:
        """Train the ML model on existing classifications."""
        texts, labels = self.db.get_training_data(min_confidence)

        if len(texts) < 10:
            print(f"⚠️  Need at least 10 high-confidence classifications to train. Have {len(texts)}.")
            return False

        print(
            f"🧠 Training {'transformer' if self.use_transformer else 'traditional'} model on {
                len(texts)} classifications..."
        )

        if self.use_transformer and self.transformer_processor:
            return self._train_transformer_model(texts, labels)
        else:
            return self._train_traditional_model(texts, labels)

    def _train_traditional_model(self, texts: List[str], labels: List[str]) -> bool:
        """Train traditional TF-IDF + Logistic Regression model."""
        # Vectorize the text
        X = self.vectorizer.fit_transform(texts)
        y = np.array(labels)

        # Train the model
        self.model.fit(X, y)
        self.is_trained = True

        # Calculate accuracy on training data (just for info)
        y_pred = self.model.predict(X)
        accuracy = accuracy_score(y, y_pred)

        print(f"✅ Traditional model trained! Training accuracy: {accuracy:.3f}")

        # Save the model
        self.save_model()

        return True

    def _train_transformer_model(self, texts: List[str], labels: List[str]) -> bool:
        """Train transformer-based model with music-aware preprocessing."""
        try:
            # Preprocess texts with music-aware processing
            processed_texts = []
            for text in texts:
                processed = self.transformer_processor.preprocess_text(text)
                processed_texts.append(processed)

            # For now, use traditional model with transformer-preprocessed text
            # In a full implementation, this would use actual transformer fine-tuning
            X = self.vectorizer.fit_transform(processed_texts)
            y = np.array(labels)

            # Train the model
            self.model.fit(X, y)
            self.is_trained = True

            # Calculate accuracy
            y_pred = self.model.predict(X)
            accuracy = accuracy_score(y, y_pred)

            print(f"✅ Transformer-preprocessed model trained! Training accuracy: {accuracy:.3f}")
            print(f"   Music slang preservation: {self.transformer_processor.config.slang_preservation.value}")
            print(f"   Emoji handling: {self.transformer_processor.config.emoji_mode.value}")

            # Save the model
            self.save_model()

            return True

        except Exception as e:
            print(f"❌ Transformer training failed: {e}")
            # Fall back to traditional training
            print("🔄 Falling back to traditional model...")
            self.use_transformer = False
            return self._train_traditional_model(texts, labels)

    def predict(self, comment_text: str) -> Tuple[str, float]:
        """Predict classification and confidence for a comment."""
        if not self.is_trained:
            return "unknown", 0.0

        # Preprocess text if using transformer
        if self.use_transformer and self.transformer_processor:
            processed_text = self.transformer_processor.preprocess_text(comment_text)
        else:
            processed_text = comment_text

        # Vectorize the comment
        X = self.vectorizer.transform([processed_text])

        # Get prediction and probability
        prediction = self.model.predict(X)[0]
        probabilities = self.model.predict_proba(X)[0]

        # Confidence is the max probability
        confidence = max(probabilities)

        return prediction, confidence

    def analyze_comment_features(self, comment_text: str) -> Dict[str, any]:
        """Analyze comment features using transformer processor."""
        if self.use_transformer and self.transformer_processor:
            return self.transformer_processor.analyze_text_features(comment_text)
        else:
            # Basic analysis for traditional model
            return {
                "word_count": len(comment_text.split()),
                "char_count": len(comment_text),
                "has_slang": any(
                    term in comment_text.lower() for term in ["slaps", "fire", "banger", "goated", "mid", "trash"]
                ),
                "has_emoji": bool(re.search(r"[\U0001F600-\U0001F64F]", comment_text)),
            }

    def save_model(self):
        """Save the trained model and vectorizer."""
        if self.is_trained:
            with open(self.model_path, "wb") as f:
                pickle.dump(self.model, f)
            with open(self.vectorizer_path, "wb") as f:
                pickle.dump(self.vectorizer, f)

    def load_model(self):
        """Load a previously trained model."""
        try:
            if os.path.exists(self.model_path) and os.path.exists(self.vectorizer_path):
                with open(self.model_path, "rb") as f:
                    self.model = pickle.load(f)
                with open(self.vectorizer_path, "rb") as f:
                    self.vectorizer = pickle.load(f)
                self.is_trained = True
                print("✅ Loaded existing model")
        except Exception as e:
            print(f"⚠️  Could not load existing model: {e}")
            self.is_trained = False


class InteractiveClassifier:
    """Interactive classification interface."""

    def __init__(self):
        self.classifier = SmartCommentClassifier()
        self.session_count = 0
        self.session_classifications = []

    def setup_session(self):
        """Set up a classification session."""
        print("🎯 SMART COMMENT CLASSIFICATION ASSISTANT")
        print("=" * 60)

        # Show current stats
        stats = self.classifier.db.get_stats()
        print(f"📊 Current database: {stats['total']} classifications")
        if stats["by_classification"]:
            for cls, count in stats["by_classification"].items():
                print(f"   {cls}: {count}")
        print(f"   Average confidence: {stats['average_confidence']:.2f}")

        # Import enhanced dataset if database is empty (real data, not fake)
        if stats["total"] == 0:
            print("\n🔄 Classification database is empty. Importing enhanced dataset...")
            print("(This uses real music industry phrases, not fake data)")
            imported = self.classifier.import_enhanced_dataset()
            if imported > 0:
                stats = self.classifier.db.get_stats()
                print(f"✅ Now have {stats['total']} real classifications")

        # Train model if needed
        if not self.classifier.is_trained:
            print("\n🧠 Training ML model...")
            if self.classifier.train_model():
                print("✅ Model ready!")
            else:
                print("⚠️  Not enough data to train model. Will collect manual classifications.")

        # Ask how many to classify
        while True:
            try:
                target_count = input(f"\n🎯 How many comments would you like to classify? (1-100): ").strip()
                if not target_count:
                    target_count = 10
                else:
                    target_count = int(target_count)

                if 1 <= target_count <= 100:
                    break
                else:
                    print("Please enter a number between 1 and 100")
            except ValueError:
                print("Please enter a valid number")

        return target_count

    def get_sample_comments(self, count: int) -> List[str]:
        """Get UNIQUE real comments from database-NO FAKE DATA, NO DUPLICATES."""
        try:
            from youtubeviz.unique_comment_manager import get_unique_comments_for_classification

            comments = get_unique_comments_for_classification(count)

            if not comments:
                print("❌ No unique unclassified comments available.")
                print("Either all comments are classified / allocated or database is empty.")
                return []

            print(f"✅ Allocated {len(comments)} UNIQUE comments for classification")
            return comments

        except Exception as e:
            print(f"❌ Could not fetch unique comments: {e}")
            print("Make sure your database is accessible and has youtube_comments table.")
            return []

    def classify_comment(self, comment: str) -> bool:
        """Classify a single comment interactively."""
        print(f"\n📝 Comment: '{comment}'")

        # Get ML suggestion if available
        if self.classifier.is_trained:
            prediction, confidence = self.classifier.predict(comment)
            print(f"🤖 AI suggests: {prediction.upper()} (confidence: {confidence:.2f})")

        # Get user input
        while True:
            choice = input("👤 Your classification (p=positive, n=negative, s=skip, q=quit): ").lower().strip()

            if choice in ["q", "quit"]:
                return False
            elif choice in ["s", "skip"]:
                return True
            elif choice in ["p", "positive"]:
                classification = "positive"
                break
            elif choice in ["n", "negative"]:
                classification = "negative"
                break
            else:
                print("Please enter p, n, s, or q")

        # Get confidence
        while True:
            try:
                conf_input = input("👤 Your confidence (1-5, or press Enter for 5): ").strip()
                if not conf_input:
                    confidence = 1.0
                else:
                    conf_num = int(conf_input)
                    if 1 <= conf_num <= 5:
                        confidence = conf_num / 5.0
                    else:
                        print("Please enter a number between 1 and 5")
                        continue
                break
            except ValueError:
                print("Please enter a valid number")

        # Save classification
        self.classifier.db.add_classification(
            comment_text=comment,
            classification=classification,
            confidence=confidence,
            source="manual",
            notes=f'Session {datetime.now().strftime("%Y-%m-%d %H:%M")}',
        )

        self.session_count += 1
        self.session_classifications.append(
            {"comment": comment, "classification": classification, "confidence": confidence}
        )

        print(f"✅ Saved as {classification} (confidence: {confidence:.1f})")

        return True

    def run_session(self):
        """Run an interactive classification session."""
        target_count = self.setup_session()

        print(f"\n🚀 Starting classification session (target: {target_count})")
        print("Commands: p=positive, n=negative, s=skip, q=quit")
        print("-" * 60)

        comments = self.get_sample_comments(target_count)

        if not comments:
            print("❌ No comments available to classify. Exiting.")
            return

        actual_count = len(comments)
        if actual_count < target_count:
            print(f"⚠️  Only {actual_count} comments available (requested {target_count})")

        for i, comment in enumerate(comments, 1):
            print(f"\n[{i}/{actual_count}]")

            if not self.classify_comment(comment):
                print("👋 Session ended by user")
                break

            # Retrain model periodically
            if self.session_count > 0 and self.session_count % 10 == 0:
                print(f"\n🧠 Retraining model with new data...")
                if self.classifier.train_model():
                    print("✅ Model updated!")

        # Session summary
        print(f"\n📊 SESSION SUMMARY")
        print("=" * 40)
        print(f"Classifications made: {self.session_count}")

        if self.session_classifications:
            pos_count = sum(1 for c in self.session_classifications if c["classification"] == "positive")
            neg_count = sum(1 for c in self.session_classifications if c["classification"] == "negative")
            avg_conf = sum(c["confidence"] for c in self.session_classifications) / len(self.session_classifications)

            print(f"Positive: {pos_count}")
            print(f"Negative: {neg_count}")
            print(f"Average confidence: {avg_conf:.2f}")

        # Final stats
        final_stats = self.classifier.db.get_stats()
        print(f"\n📈 Total database: {final_stats['total']} classifications")

        print(f"\n🎉 Great work! The AI will get better with each classification.")


def main():
    """Main entry point."""
    try:
        classifier = InteractiveClassifier()
        classifier.run_session()
    except KeyboardInterrupt:
        print(f"\n👋 Classification session interrupted. Progress saved!")
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    main()
