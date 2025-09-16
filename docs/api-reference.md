# 🔌 API Reference
## *Production-Grade Music Analytics API*

[![Grammy Nominated Producer](https://img.shields.io/badge/Grammy-Nominated%20Producer-gold?style=flat-square)](https://www.grammy.com)
[![M.S. Data Science](https://img.shields.io/badge/M.S.-Data%20Science-blue?style=flat-square)](https://github.com/wmoore012)
[![API Version](https://img.shields.io/badge/API-v2.1-blue?style=flat-square)](https://api.musicanalytics.com)

---

## 🎯 **API Overview**

The **YouTube Music Analytics API** provides programmatic access to music industry insights with Grammy-level reliability and M.S. Data Science rigor. Built specifically for music industry professionals who need real-time, actionable data.

### **🚀 Base URL**
```
Production:  https://api.musicanalytics.com/v2
Staging:     https://staging-api.musicanalytics.com/v2
```

### **🔐 Authentication**
```http
Authorization: Bearer YOUR_API_TOKEN
Content-Type: application/json
```

### **📊 Rate Limits**
- **Free Tier**: 1,000 requests/hour
- **Professional**: 10,000 requests/hour
- **Enterprise**: 100,000 requests/hour

---

## 🎵 **Artist Analytics Endpoints**

### **GET /artists/{artist_id}/overview**

Get comprehensive artist performance overview with music industry context.

#### **Parameters**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `artist_id` | string | Yes | Artist identifier or channel ID |
| `date_range` | string | No | Date range (7d, 30d, 90d, 1y) |
| `include_predictions` | boolean | No | Include Grammy-level trend predictions |

#### **Example Request**
```bash
curl -X GET "https://api.musicanalytics.com/v2/artists/taylor-swift/overview?date_range=30d" \
  -H "Authorization: Bearer YOUR_TOKEN"
```

#### **Example Response**
```json
{
  "artist": {
    "id": "taylor-swift",
    "name": "Taylor Swift",
    "channel_id": "UCqECaJ8Gagnn7YCbPEzWH6g",
    "genre": "Pop",
    "grammy_nominations": 46,
    "grammy_wins": 12
  },
  "performance": {
    "total_views": 15420000000,
    "total_subscribers": 54200000,
    "engagement_rate": 4.8,
    "momentum_score": 87.3,
    "industry_percentile": 99.2
  },
  "trends": {
    "view_growth_30d": 12.5,
    "subscriber_growth_30d": 3.2,
    "engagement_trend": "increasing",
    "viral_potential": 0.85
  },
  "predictions": {
    "chart_probability": 0.92,
    "grammy_potential": 0.78,
    "next_milestone": "60M subscribers",
    "confidence_level": "high"
  },
  "business_insights": {
    "investment_recommendation": "strong_buy",
    "marketing_optimization": "increase_budget_25_percent",
    "collaboration_opportunities": ["Olivia Rodrigo", "Billie Eilish"],
    "optimal_release_window": "2024-03-15"
  }
}
```

### **GET /artists/{artist_id}/videos**

Get detailed video performance data with music industry analytics.

#### **Parameters**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `artist_id` | string | Yes | Artist identifier |
| `limit` | integer | No | Number of videos (default: 50, max: 1000) |
| `sort_by` | string | No | Sort criteria (views, engagement, momentum) |
| `video_type` | string | No | Filter by type (music_video, lyric_video, live) |

#### **Example Response**
```json
{
  "videos": [
    {
      "video_id": "YQHsXMglC9A",
      "title": "Anti-Hero",
      "published_at": "2022-10-21T04:00:00Z",
      "performance": {
        "view_count": 284000000,
        "like_count": 3200000,
        "comment_count": 156000,
        "engagement_rate": 5.2
      },
      "music_analysis": {
        "genre": "Pop",
        "tempo": 97,
        "key": "C major",
        "energy_level": 0.73,
        "danceability": 0.68
      },
      "business_metrics": {
        "revenue_estimate": 850000,
        "chart_performance": {
          "billboard_hot_100": 1,
          "weeks_on_chart": 28
        },
        "streaming_impact": {
          "spotify_boost": 45.2,
          "apple_music_boost": 38.7
        }
      },
      "sentiment_analysis": {
        "overall_sentiment": 0.82,
        "fan_engagement_quality": "exceptional",
        "comment_themes": ["relatable", "catchy", "vulnerable"],
        "bot_percentage": 2.1
      }
    }
  ],
  "pagination": {
    "total": 247,
    "page": 1,
    "per_page": 50,
    "has_next": true
  }
}
```

---

## 📊 **Analytics Endpoints**

### **POST /analytics/compare**

Compare multiple artists with Grammy-level insights and statistical significance testing.

#### **Request Body**
```json
{
  "artists": ["taylor-swift", "olivia-rodrigo", "billie-eilish"],
  "metrics": ["engagement_rate", "momentum_score", "viral_potential"],
  "date_range": "90d",
  "statistical_analysis": true,
  "business_context": true
}
```

#### **Example Response**
```json
{
  "comparison": {
    "artists": [
      {
        "name": "Taylor Swift",
        "metrics": {
          "engagement_rate": 4.8,
          "momentum_score": 87.3,
          "viral_potential": 0.85
        },
        "ranking": 1,
        "statistical_significance": {
          "vs_olivia_rodrigo": {
            "p_value": 0.003,
            "effect_size": 0.72,
            "significant": true
          }
        }
      }
    ],
    "insights": {
      "market_leader": "Taylor Swift",
      "fastest_growing": "Olivia Rodrigo",
      "most_consistent": "Billie Eilish",
      "investment_recommendations": {
        "immediate_opportunity": "Olivia Rodrigo",
        "safe_investment": "Taylor Swift",
        "emerging_potential": "Billie Eilish"
      }
    },
    "statistical_summary": {
      "confidence_level": 0.95,
      "sample_size": 1247,
      "methodology": "Welch's t-test with Bonferroni correction"
    }
  }
}
```

### **GET /analytics/trends**

Get industry trend analysis with predictive insights.

#### **Parameters**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `genre` | string | No | Filter by genre (pop, hip-hop, country, etc.) |
| `time_period` | string | No | Analysis period (1m, 3m, 6m, 1y) |
| `prediction_horizon` | string | No | Forecast period (1m, 3m, 6m) |

#### **Example Response**
```json
{
  "trends": {
    "emerging_genres": [
      {
        "genre": "Afrobeats",
        "growth_rate": 156.7,
        "key_artists": ["Burna Boy", "Wizkid", "Davido"],
        "market_opportunity": "high",
        "investment_timeline": "immediate"
      }
    ],
    "declining_trends": [
      {
        "trend": "Traditional Country",
        "decline_rate": -12.3,
        "replacement_trend": "Country Pop Fusion"
      }
    ],
    "predictions": {
      "next_viral_genre": "Latin Trap",
      "probability": 0.73,
      "timeline": "Q2 2024",
      "key_indicators": ["TikTok usage", "Streaming growth", "Radio adoption"]
    }
  }
}
```

---

## 💬 **Sentiment Analysis Endpoints**

### **GET /sentiment/{video_id}**

Get comprehensive sentiment analysis with music industry context.

#### **Example Response**
```json
{
  "video_id": "YQHsXMglC9A",
  "sentiment_analysis": {
    "overall_score": 0.82,
    "confidence": 0.94,
    "comment_count": 156000,
    "analyzed_comments": 145230,
    "bot_filtered": 10770
  },
  "sentiment_breakdown": {
    "positive": 0.73,
    "neutral": 0.19,
    "negative": 0.08
  },
  "music_specific_sentiment": {
    "lyrics_appreciation": 0.89,
    "production_quality": 0.91,
    "vocal_performance": 0.87,
    "visual_aesthetics": 0.84
  },
  "fan_engagement_quality": {
    "authentic_fans": 0.87,
    "casual_listeners": 0.11,
    "potential_bots": 0.02,
    "engagement_depth": "high"
  },
  "cultural_analysis": {
    "demographic_breakdown": {
      "gen_z": 0.45,
      "millennial": 0.38,
      "gen_x": 0.12,
      "boomer": 0.05
    },
    "geographic_sentiment": {
      "north_america": 0.85,
      "europe": 0.79,
      "asia": 0.81,
      "latin_america": 0.88
    }
  },
  "business_implications": {
    "marketing_receptivity": "very_high",
    "brand_safety": "excellent",
    "collaboration_potential": "strong",
    "tour_demand_indicator": 0.91
  }
}
```

---

## 🎯 **Prediction Endpoints**

### **POST /predictions/chart-performance**

Predict chart performance using Grammy-level industry insights.

#### **Request Body**
```json
{
  "video_id": "new-release-123",
  "artist_id": "emerging-artist",
  "release_strategy": {
    "marketing_budget": 500000,
    "playlist_placements": ["Today's Top Hits", "Pop Rising"],
    "radio_strategy": "pop_mainstream",
    "social_media_push": true
  },
  "prediction_timeframe": "12_weeks"
}
```

#### **Example Response**
```json
{
  "predictions": {
    "billboard_hot_100": {
      "peak_position": 23,
      "probability": 0.67,
      "weeks_on_chart": 16,
      "confidence_interval": [15, 35]
    },
    "streaming_performance": {
      "spotify_first_week": 12500000,
      "apple_music_first_week": 4200000,
      "youtube_views_4_weeks": 85000000
    },
    "commercial_success": {
      "revenue_projection": 2800000,
      "roi_estimate": 5.6,
      "break_even_timeline": "6_weeks"
    }
  },
  "success_factors": {
    "positive_indicators": [
      "Strong pre-release buzz",
      "High engagement rate on teasers",
      "Favorable playlist placement"
    ],
    "risk_factors": [
      "Competitive release window",
      "Limited radio support initially"
    ]
  },
  "optimization_recommendations": {
    "increase_tiktok_budget": 25000,
    "target_additional_playlists": ["Chill Pop", "Indie Pop"],
    "optimal_music_video_release": "week_2"
  }
}
```

---

## 🔍 **Search & Discovery Endpoints**

### **GET /search/artists**

Search for artists with advanced filtering and music industry context.

#### **Parameters**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `query` | string | Yes | Search query |
| `genre` | string | No | Filter by genre |
| `momentum_threshold` | float | No | Minimum momentum score (0-100) |
| `subscriber_range` | string | No | Subscriber count range (1k-10k, 10k-100k, etc.) |
| `grammy_status` | string | No | Grammy status (nominated, winner, none) |

#### **Example Response**
```json
{
  "results": [
    {
      "artist": {
        "id": "olivia-rodrigo",
        "name": "Olivia Rodrigo",
        "genre": "Pop",
        "subscribers": 13200000,
        "momentum_score": 94.2
      },
      "discovery_insights": {
        "breakout_potential": "very_high",
        "investment_timing": "immediate",
        "comparable_artists": ["Taylor Swift", "Billie Eilish"],
        "market_opportunity": 8500000
      },
      "grammy_analysis": {
        "nomination_probability": 0.89,
        "category_predictions": ["Best New Artist", "Song of the Year"],
        "historical_comparisons": ["Billie Eilish 2020", "Dua Lipa 2021"]
      }
    }
  ]
}
```

---

## 📈 **Real-Time Endpoints**

### **GET /realtime/trending**

Get real-time trending analysis with immediate business implications.

#### **Example Response**
```json
{
  "trending_now": [
    {
      "video_id": "trending-123",
      "artist": "Emerging Artist",
      "title": "Viral Hit Song",
      "trend_metrics": {
        "velocity": 156.7,
        "acceleration": 23.4,
        "viral_coefficient": 1.8,
        "time_to_peak": "estimated_6_hours"
      },
      "business_urgency": {
        "action_required": "immediate",
        "opportunity_window": "12_hours",
        "potential_impact": "major_breakthrough",
        "recommended_actions": [
          "Increase marketing spend immediately",
          "Secure playlist placements",
          "Prepare follow-up content"
        ]
      }
    }
  ],
  "market_alerts": [
    {
      "type": "genre_surge",
      "genre": "Afrobeats",
      "growth_rate": 234.5,
      "key_drivers": ["TikTok viral dance", "Celebrity endorsement"],
      "investment_window": "48_hours"
    }
  ]
}
```

---

## 🛠️ **Utility Endpoints**

### **GET /health**

System health check with detailed status information.

#### **Example Response**
```json
{
  "status": "healthy",
  "timestamp": "2024-01-15T10:30:00Z",
  "services": {
    "api": "operational",
    "database": "operational",
    "etl_pipeline": "operational",
    "ml_models": "operational"
  },
  "performance": {
    "avg_response_time": "127ms",
    "uptime": "99.97%",
    "requests_per_second": 1247
  },
  "data_freshness": {
    "youtube_data": "2_minutes_ago",
    "sentiment_analysis": "5_minutes_ago",
    "trend_calculations": "15_minutes_ago"
  }
}
```

### **GET /limits**

Get current API usage and limits.

#### **Example Response**
```json
{
  "current_usage": {
    "requests_this_hour": 2847,
    "requests_today": 45230,
    "requests_this_month": 892340
  },
  "limits": {
    "hourly_limit": 10000,
    "daily_limit": 100000,
    "monthly_limit": 2000000
  },
  "tier": "professional",
  "reset_times": {
    "hourly_reset": "2024-01-15T11:00:00Z",
    "daily_reset": "2024-01-16T00:00:00Z",
    "monthly_reset": "2024-02-01T00:00:00Z"
  }
}
```

---

## 🔐 **Authentication & Security**

### **API Token Management**

#### **Generate Token**
```bash
curl -X POST "https://api.musicanalytics.com/v2/auth/token" \
  -H "Content-Type: application/json" \
  -d '{
    "email": "your@email.com",
    "password": "your_password",
    "scope": "analytics:read predictions:read"
  }'
```

#### **Token Scopes**
- `analytics:read` - Access to analytics endpoints
- `analytics:write` - Create custom analytics
- `predictions:read` - Access to prediction models
- `predictions:write` - Create custom predictions
- `realtime:read` - Access to real-time data
- `admin:all` - Full administrative access

### **Security Best Practices**

#### **Rate Limiting**
```http
X-RateLimit-Limit: 10000
X-RateLimit-Remaining: 9847
X-RateLimit-Reset: 1642248000
```

#### **Error Handling**
```json
{
  "error": {
    "code": "RATE_LIMIT_EXCEEDED",
    "message": "API rate limit exceeded. Please wait before making more requests.",
    "details": {
      "retry_after": 3600,
      "limit": 10000,
      "window": "1 hour"
    },
    "documentation": "https://docs.musicanalytics.com/rate-limits"
  }
}
```

---

## 📚 **SDKs & Libraries**

### **Python SDK**
```python
from music_analytics import MusicAnalyticsClient

client = MusicAnalyticsClient(api_token="your_token")

# Get artist overview
artist = client.artists.get_overview("taylor-swift", date_range="30d")

# Compare artists
comparison = client.analytics.compare_artists([
    "taylor-swift", "olivia-rodrigo", "billie-eilish"
])

# Predict chart performance
prediction = client.predictions.chart_performance(
    video_id="new-release-123",
    marketing_budget=500000
)
```

### **JavaScript SDK**
```javascript
import { MusicAnalyticsClient } from '@music-analytics/sdk';

const client = new MusicAnalyticsClient({
  apiToken: 'your_token'
});

// Get trending analysis
const trending = await client.realtime.getTrending();

// Search for emerging artists
const artists = await client.search.artists({
  momentum_threshold: 80,
  subscriber_range: '10k-100k'
});
```

### **R Package**
```r
library(musicanalytics)

# Set up client
client <- music_analytics_client(api_token = "your_token")

# Get artist data for analysis
artist_data <- get_artist_overview(client, "taylor-swift")

# Statistical analysis
comparison <- compare_artists_statistical(
  client,
  c("taylor-swift", "olivia-rodrigo"),
  confidence_level = 0.95
)
```

---

## 🎯 **Use Case Examples**

### **A&R Discovery Workflow**
```python
# Find emerging artists with high potential
emerging = client.search.artists(
    momentum_threshold=85,
    subscriber_range="50k-500k",
    grammy_status="none"
)

# Analyze their trajectory
for artist in emerging.results:
    prediction = client.predictions.grammy_potential(artist.id)
    if prediction.probability > 0.7:
        print(f"High Grammy potential: {artist.name}")
```

### **Marketing Optimization**
```python
# Optimize marketing spend across artists
portfolio = ["artist1", "artist2", "artist3"]
budget = 1000000

optimization = client.analytics.optimize_marketing_spend(
    artists=portfolio,
    total_budget=budget,
    objective="maximize_roi"
)

print(f"Recommended allocation: {optimization.allocation}")
```

### **Real-Time Monitoring**
```python
# Monitor for viral opportunities
while True:
    trending = client.realtime.get_trending()

    for trend in trending.urgent_opportunities:
        if trend.potential_impact == "major_breakthrough":
            send_alert(f"Viral opportunity: {trend.artist}")

    time.sleep(300)  # Check every 5 minutes
```

---

## 📞 **Support & Resources**

### **🆘 Getting Help**
- **Documentation**: [docs.musicanalytics.com](https://docs.musicanalytics.com)
- **Support Email**: [support@musicanalytics.com](mailto:support@musicanalytics.com)
- **Discord Community**: [discord.gg/musicanalytics](https://discord.gg/musicanalytics)
- **Stack Overflow**: Tag questions with `music-analytics-api`

### **📈 Status & Updates**
- **Status Page**: [status.musicanalytics.com](https://status.musicanalytics.com)
- **API Changelog**: [changelog.musicanalytics.com](https://changelog.musicanalytics.com)
- **Developer Blog**: [blog.musicanalytics.com](https://blog.musicanalytics.com)

### **🎓 Learning Resources**
- **API Tutorial**: Interactive guide for beginners
- **Video Tutorials**: YouTube channel with use case examples
- **Webinar Series**: Monthly deep-dives with industry experts
- **Case Studies**: Real-world success stories and implementations

---

<div align="center">

## 🎵 **API Built for Grammy-Level Excellence** 📊

**Where music industry expertise meets data science rigor**
**Production-ready • Scalable • Business-focused**

*Designed by a Grammy-nominated producer with M.S. Data Science*

**Ready to power the next generation of music industry decisions**

</div>
