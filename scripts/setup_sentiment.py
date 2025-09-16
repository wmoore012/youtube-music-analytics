#!/usr/bin/env python3
"""
Quick setup for sentiment analysis if it's not working.
This creates a basic VADER-based sentiment analyzer for benchmarking.
"""

import os
import sys
from pathlib import Path


def create_basic_sentiment_analyzer():
    """Create a basic sentiment analyzer if none exists."""
    
    sentiment_file = Path("src/youtubeviz/music_sentiment.py")
    
    if sentiment_file.exists():
        print(f"✅ Sentiment analyzer already exists at {sentiment_file}")
        return True
    
    # Create the directory if it doesn't exist
    sentiment_file.parent.mkdir(parents=True, exist_ok=True)
    
    # Basic sentiment analyzer using VADER
    sentiment_code = '''"""
Basic music-aware sentiment analysis using VADER.
Created for benchmarking purposes.
"""

try:
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
    VADER_AVAILABLE = True
except ImportError:
    VADER_AVAILABLE = False
    print("Warning: vaderSentiment not installed. Install with: pip install vaderSentiment")


# Music-specific positive terms
MUSIC_POSITIVE = {
    'fire': 2.0, 'slaps': 2.0, 'banger': 2.0, 'vibes': 1.5,
    'hits different': 2.0, 'goes hard': 2.0, 'chef kiss': 2.0,
    'no skip': 1.8, 'on repeat': 1.5, 'obsessed': 1.8
}

# Music-specific negative terms  
MUSIC_NEGATIVE = {
    'mid': -1.5, 'trash': -2.0, 'skip': -1.8, 'boring': -1.5,
    'overrated': -1.2, 'generic': -1.0
}


def analyze_comment(comment_text):
    """
    Analyze sentiment of a music comment.
    
    Args:
        comment_text (str): The comment to analyze
        
    Returns:
        dict: Sentiment analysis results
    """
    if not VADER_AVAILABLE:
        # Fallback to basic keyword matching
        return _basic_sentiment_fallback(comment_text)
    
    analyzer = SentimentIntensityAnalyzer()
    
    # Add music-specific terms to lexicon
    for term, score in MUSIC_POSITIVE.items():
        analyzer.lexicon[term] = score
    for term, score in MUSIC_NEGATIVE.items():
        analyzer.lexicon[term] = score
    
    # Get VADER scores
    scores = analyzer.polarity_scores(comment_text)
    
    # Determine overall sentiment
    compound = scores['compound']
    if compound >= 0.05:
        sentiment = 'positive'
    elif compound <= -0.05:
        sentiment = 'negative'
    else:
        sentiment = 'neutral'
    
    return {
        'sentiment': sentiment,
        'compound': compound,
        'positive': scores['pos'],
        'negative': scores['neg'],
        'neutral': scores['neu'],
        'confidence': abs(compound)
    }


def _basic_sentiment_fallback(comment_text):
    """Basic sentiment analysis without VADER."""
    text_lower = comment_text.lower()
    
    positive_score = sum(1 for term in MUSIC_POSITIVE if term in text_lower)
    negative_score = sum(1 for term in MUSIC_NEGATIVE if term in text_lower)
    
    # Simple positive/negative words
    basic_positive = ['good', 'great', 'love', 'amazing', 'awesome', 'best']
    basic_negative = ['bad', 'hate', 'terrible', 'worst', 'awful', 'sucks']
    
    positive_score += sum(1 for term in basic_positive if term in text_lower)
    negative_score += sum(1 for term in basic_negative if term in text_lower)
    
    if positive_score > negative_score:
        sentiment = 'positive'
        compound = 0.5
    elif negative_score > positive_score:
        sentiment = 'negative'
        compound = -0.5
    else:
        sentiment = 'neutral'
        compound = 0.0
    
    return {
        'sentiment': sentiment,
        'compound': compound,
        'positive': positive_score / max(positive_score + negative_score, 1),
        'negative': negative_score / max(positive_score + negative_score, 1),
        'neutral': 0.5,
        'confidence': abs(compound)
    }


# Test function
if __name__ == "__main__":
    test_comments = [
        "This song is fire! Absolutely love it",
        "This is trash, worst song ever",
        "Pretty good track, decent vibes",
        "This slaps so hard, no skip album"
    ]
    
    print("Testing sentiment analyzer:")
    for comment in test_comments:
        result = analyze_comment(comment)
        print(f"'{comment}' -> {result['sentiment']} ({result['compound']:.2f})")
'''
    
    # Write the sentiment analyzer
    with open(sentiment_file, 'w') as f:
        f.write(sentiment_code)
    
    print(f"✅ Created basic sentiment analyzer at {sentiment_file}")
    
    # Test it
    try:
        sys.path.insert(0, '.')
        from src.youtubeviz.music_sentiment import analyze_comment
        result = analyze_comment("This song is fire!")
        print(f"✅ Test successful: {result}")
        return True
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return False


def main():
    """Set up sentiment analysis for benchmarking."""
    print("🎵 Setting up sentiment analysis for benchmarking...")
    
    if create_basic_sentiment_analyzer():
        print("\n✅ Sentiment analysis is ready!")
        print("You can now run: make benchmark")
    else:
        print("\n❌ Failed to set up sentiment analysis")
        print("You may need to install vaderSentiment: pip install vaderSentiment")


if __name__ == "__main__":
    main()