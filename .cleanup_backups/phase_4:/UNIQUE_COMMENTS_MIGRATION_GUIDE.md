
# MIGRATION GUIDE: Using Unique Comments

## Overview
All systems must use the UniqueCommentManager to ensure no comment overlap.

## Quick Migration Steps

### 1. Import the manager
```python
from youtubeviz.unique_comment_manager import (
    get_unique_comments_for_classification,
    get_unique_comments_for_training,
    get_unique_comments_for_testing,
    get_unique_comments_for_benchmark,
    get_unique_comments_for_evaluation
)
```

### 2. Replace direct database queries

#### OLD (DON'T USE):
```python
query = "SELECT comment_text FROM youtube_comments ORDER BY RANDOM() LIMIT 100"
```

#### NEW (USE THIS):
```python
# For classification
comments = get_unique_comments_for_classification(100)

# For ML training
comments = get_unique_comments_for_training(100)

# For testing
comments = get_unique_comments_for_testing(100)

# For benchmarking
comments_data = get_unique_comments_for_benchmark("system_name", 100)

# For evaluation
comments_data = get_unique_comments_for_evaluation("system_name", 100)
```

### 3. System-specific usage

#### Smart Classifier
```python
def get_sample_comments(self, count: int) -> List[str]:
    return get_unique_comments_for_classification(count)
```

#### Model Benchmark System
```python
def fetch_evaluation_comments(self, count: int) -> List[Dict]:
    return get_unique_comments_for_benchmark("model_benchmark", count)
```

#### VADER Evaluation
```python
def get_test_comments(self, count: int) -> List[Dict]:
    return get_unique_comments_for_evaluation("vader_evaluation", count)
```

## Benefits
- ✅ No data leakage between train/test/validation
- ✅ No duplicate comments across systems
- ✅ Proper ML evaluation methodology
- ✅ Trackable comment usage
- ✅ Reproducible results

## Usage Statistics
Check comment allocation with:
```python
from youtubeviz.unique_comment_manager import comment_manager
stats = comment_manager.get_usage_stats()
print(stats)
```

## Reset System Allocation
If you need to reset a system's comment allocation:
```python
comment_manager.reset_system_allocation("system_name")
```
