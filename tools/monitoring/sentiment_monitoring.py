#!/usr/bin/env python3
"""
Sentiment Analysis Monitoring & Alerting

Monitors sentiment analysis accuracy, bot detection rates, and data quality.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

import pandas as pd
import numpy as np
from sqlalchemy import text
from web.etl_helpers import get_engine
from youtubeviz.weak_supervision_sentiment import WeakSupervisionSentimentAnalyzer
from tools.sentiment.deploy_bot_detection import EnhancedBotDetector
import logging
from datetime import datetime, timedelta
import json
from decimal import Decimal


class DecimalEncoder(json.JSONEncoder):
    """JSON encoder that handles Decimal types."""
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SentimentMonitor:
    """Monitor sentiment analysis performance and quality."""
    
    def __init__(self):
        self.engine = get_engine()
        self.analyzer = WeakSupervisionSentimentAnalyzer()
        self.bot_detector = EnhancedBotDetector()
        
    def check_sentiment_accuracy(self) -> dict:
        """Check sentiment analysis accuracy on test cases."""
        test_cases = [
            ("this is fire 🔥", "positive"),
            ("my nigga snapped", "positive"), 
            ("she ate that", "positive"),
            ("who produced this?", "neutral"),
            ("lyrics?", "neutral"),
            ("this is trash", "negative"),
            ("mid", "negative"),
            ("overrated", "negative")
        ]
        
        texts = [case[0] for case, _ in test_cases]
        weak_labels = self.analyzer.apply_labeling_functions(texts)
        
        correct = 0
        results = []
        
        for i, (text, expected) in enumerate(test_cases):
            if i < len(weak_labels) and weak_labels[i].final_label:
                predicted_value = weak_labels[i].final_label.value
                if predicted_value > 0:
                    predicted = "positive"
                elif predicted_value < 0:
                    predicted = "negative"
                else:
                    predicted = "neutral"
                    
                is_correct = predicted == expected
                if is_correct:
                    correct += 1
                    
                results.append({
                    "text": text,
                    "expected": expected,
                    "predicted": predicted,
                    "correct": is_correct,
                    "confidence": weak_labels[i].confidence
                })
            else:
                results.append({
                    "text": text,
                    "expected": expected,
                    "predicted": "unknown",
                    "correct": False,
                    "confidence": 0.0
                })
        
        accuracy = correct / len(test_cases) * 100
        
        return {
            "accuracy_percent": accuracy,
            "total_tests": len(test_cases),
            "correct_predictions": correct,
            "test_results": results,
            "status": "healthy" if accuracy >= 75 else "degraded" if accuracy >= 50 else "critical"
        }
    
    def check_bot_detection_rates(self) -> dict:
        """Check bot detection false positive/negative rates."""
        with self.engine.connect() as conn:
            # Get recent bot detection stats
            result = conn.execute(text("""
                SELECT 
                    COUNT(*) as total_comments,
                    SUM(CASE WHEN is_bot_suspected THEN 1 ELSE 0 END) as bot_suspected,
                    AVG(CASE WHEN is_bot_suspected THEN 1.0 ELSE 0.0 END) as bot_rate
                FROM youtube_comments 
                WHERE created_at >= DATE_SUB(NOW(), INTERVAL 7 DAY)
            """)).fetchone()
            
            total_comments = result[0] or 0
            bot_suspected = result[1] or 0
            bot_rate = result[2] or 0
            
            # Check for anomalies
            expected_bot_rate = 0.057  # 5.7% baseline
            bot_rate_float = float(bot_rate) if bot_rate else 0.0
            rate_deviation = abs(bot_rate_float - expected_bot_rate) / expected_bot_rate
            
            status = "healthy"
            if rate_deviation > 0.5:  # More than 50% deviation
                status = "anomaly_detected"
            elif rate_deviation > 0.3:  # More than 30% deviation
                status = "monitoring_required"
                
            return {
                "total_comments": total_comments,
                "bot_suspected": bot_suspected,
                "bot_rate_percent": bot_rate_float * 100,
                "expected_rate_percent": expected_bot_rate * 100,
                "rate_deviation_percent": rate_deviation * 100,
                "status": status
            }
    
    def check_data_quality_metrics(self) -> dict:
        """Check overall data quality metrics."""
        with self.engine.connect() as conn:
            # Check for data quality issues
            metrics = {}
            
            # Missing sentiment analysis
            result = conn.execute(text("""
                SELECT COUNT(*) FROM youtube_comments yc
                LEFT JOIN comment_sentiment cs ON yc.comment_id = cs.comment_id
                WHERE cs.comment_id IS NULL
            """)).fetchone()
            metrics["missing_sentiment"] = result[0]
            
            # NULL channel titles
            result = conn.execute(text("""
                SELECT COUNT(*) FROM youtube_videos 
                WHERE channel_title IS NULL OR channel_title = ''
            """)).fetchone()
            metrics["null_channel_titles"] = result[0]
            
            # Orphaned metrics
            result = conn.execute(text("""
                SELECT COUNT(*) FROM youtube_metrics ym
                LEFT JOIN youtube_videos yv ON ym.video_id = yv.video_id
                WHERE yv.video_id IS NULL
            """)).fetchone()
            metrics["orphaned_metrics"] = result[0]
            
            # Calculate overall quality score
            total_issues = sum(metrics.values())
            quality_score = max(0, 100 - total_issues)
            
            status = "excellent" if quality_score >= 95 else \
                    "good" if quality_score >= 85 else \
                    "needs_attention" if quality_score >= 70 else "critical"
            
            return {
                "quality_score": quality_score,
                "metrics": metrics,
                "total_issues": total_issues,
                "status": status
            }
    
    def check_momentum_calculation_stability(self) -> dict:
        """Check momentum calculation stability."""
        with self.engine.connect() as conn:
            # Get recent momentum data
            result = conn.execute(text("""
                SELECT 
                    yv.channel_title as artist_name,
                    COUNT(DISTINCT yv.video_id) as video_count,
                    AVG(ym.view_count) as avg_views,
                    STDDEV(ym.view_count) as view_stddev
                FROM youtube_videos yv
                JOIN youtube_metrics ym ON yv.video_id = ym.video_id
                WHERE yv.channel_title IS NOT NULL
                GROUP BY yv.channel_title
            """)).fetchall()
            
            artists_data = []
            for row in result:
                artist_name, video_count, avg_views, view_stddev = row
                
                # Calculate coefficient of variation (stability metric)
                avg_views_float = float(avg_views) if avg_views else 0.0
                view_stddev_float = float(view_stddev) if view_stddev else 0.0
                cv = (view_stddev_float / avg_views_float) if avg_views_float > 0 else 0
                stability = "stable" if cv < 1.0 else "moderate" if cv < 2.0 else "volatile"
                
                artists_data.append({
                    "artist_name": artist_name,
                    "video_count": int(video_count),
                    "avg_views": avg_views_float,
                    "coefficient_of_variation": cv,
                    "stability": stability
                })
            
            # Overall stability assessment
            avg_cv = np.mean([a["coefficient_of_variation"] for a in artists_data])
            overall_status = "stable" if avg_cv < 1.0 else "moderate" if avg_cv < 2.0 else "volatile"
            
            return {
                "overall_stability": overall_status,
                "average_coefficient_of_variation": avg_cv,
                "artist_stability": artists_data,
                "total_artists": len(artists_data)
            }
    
    def generate_health_dashboard(self) -> dict:
        """Generate comprehensive system health dashboard."""
        logger.info("🔍 Generating system health dashboard...")
        
        dashboard = {
            "timestamp": datetime.now().isoformat(),
            "system_version": "2.0.0",
            "monitoring_status": "active"
        }
        
        try:
            # Run all health checks
            dashboard["sentiment_accuracy"] = self.check_sentiment_accuracy()
            dashboard["bot_detection"] = self.check_bot_detection_rates()
            dashboard["data_quality"] = self.check_data_quality_metrics()
            dashboard["momentum_stability"] = self.check_momentum_calculation_stability()
            
            # Calculate overall system health
            health_scores = []
            
            # Sentiment accuracy (0-100)
            health_scores.append(dashboard["sentiment_accuracy"]["accuracy_percent"])
            
            # Bot detection (100 if healthy, 50 if monitoring, 0 if anomaly)
            bot_status = dashboard["bot_detection"]["status"]
            if bot_status == "healthy":
                health_scores.append(100)
            elif bot_status == "monitoring_required":
                health_scores.append(50)
            else:
                health_scores.append(0)
            
            # Data quality score
            health_scores.append(dashboard["data_quality"]["quality_score"])
            
            # Momentum stability (100 if stable, 50 if moderate, 0 if volatile)
            momentum_status = dashboard["momentum_stability"]["overall_stability"]
            if momentum_status == "stable":
                health_scores.append(100)
            elif momentum_status == "moderate":
                health_scores.append(50)
            else:
                health_scores.append(0)
            
            overall_health = np.mean(health_scores)
            
            dashboard["overall_health"] = {
                "score": overall_health,
                "status": "healthy" if overall_health >= 80 else 
                         "degraded" if overall_health >= 60 else "critical",
                "component_scores": {
                    "sentiment_accuracy": health_scores[0],
                    "bot_detection": health_scores[1], 
                    "data_quality": health_scores[2],
                    "momentum_stability": health_scores[3]
                }
            }
            
            dashboard["alerts"] = self.generate_alerts(dashboard)
            
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            dashboard["error"] = str(e)
            dashboard["overall_health"] = {"score": 0, "status": "error"}
        
        return dashboard
    
    def generate_alerts(self, dashboard: dict) -> list:
        """Generate alerts based on dashboard metrics."""
        alerts = []
        
        # Sentiment accuracy alerts
        sentiment_accuracy = dashboard["sentiment_accuracy"]["accuracy_percent"]
        if sentiment_accuracy < 50:
            alerts.append({
                "level": "critical",
                "component": "sentiment_analysis",
                "message": f"Sentiment accuracy critically low: {sentiment_accuracy:.1f}%",
                "action_required": "Immediate model retraining required"
            })
        elif sentiment_accuracy < 75:
            alerts.append({
                "level": "warning",
                "component": "sentiment_analysis", 
                "message": f"Sentiment accuracy below threshold: {sentiment_accuracy:.1f}%",
                "action_required": "Monitor and consider model updates"
            })
        
        # Bot detection alerts
        bot_status = dashboard["bot_detection"]["status"]
        if bot_status == "anomaly_detected":
            alerts.append({
                "level": "warning",
                "component": "bot_detection",
                "message": "Bot detection rate anomaly detected",
                "action_required": "Review bot detection parameters"
            })
        
        # Data quality alerts
        quality_score = dashboard["data_quality"]["quality_score"]
        if quality_score < 70:
            alerts.append({
                "level": "critical",
                "component": "data_quality",
                "message": f"Data quality score critically low: {quality_score}",
                "action_required": "Immediate data cleanup required"
            })
        elif quality_score < 85:
            alerts.append({
                "level": "warning",
                "component": "data_quality",
                "message": f"Data quality below target: {quality_score}",
                "action_required": "Schedule data quality maintenance"
            })
        
        return alerts


def main():
    """Main monitoring function."""
    monitor = SentimentMonitor()
    
    try:
        dashboard = monitor.generate_health_dashboard()
        
        # Save dashboard
        with open("system_health_dashboard.json", "w") as f:
            json.dump(dashboard, f, indent=2, cls=DecimalEncoder)
        
        # Print summary
        overall_health = dashboard.get("overall_health", {})
        score = overall_health.get("score", 0)
        status = overall_health.get("status", "unknown")
        
        logger.info(f"📊 System Health Score: {score:.1f}/100 ({status.upper()})")
        
        # Print alerts
        alerts = dashboard.get("alerts", [])
        if alerts:
            logger.warning(f"⚠️  {len(alerts)} alerts generated:")
            for alert in alerts:
                logger.warning(f"   {alert['level'].upper()}: {alert['message']}")
        else:
            logger.info("✅ No alerts - system operating normally")
        
        return score >= 60  # Return success if score is acceptable
        
    except Exception as e:
        logger.error(f"❌ Monitoring failed: {e}")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)