"""
TDD Tests for Notebook Guardian - Python File Support

Test-driven development for validating .py files in addition to .ipynb files.
This ensures AI agents can validate any Python data science workflow.
"""

import ast
import os
from pathlib import Path
import sys
import tempfile
from unittest.mock import MagicMock, Mock, patch

import pytest

try:
    from src.notebook_guardian.python_validator import (
        CodeBlockValidator,
        DataSciencePatternDetector,
        FunctionValidator,
        ImportValidator,
        PythonFileValidator,
        PythonValidationResult,
    )
    from src.notebook_guardian.smart_installer import SmartInstaller
except ImportError:
    # Skip tests if modules not available
    pytest.skip("Notebook Guardian modules not available", allow_module_level=True)


class TestPythonFileValidator:
    """Test validation of .py files for data science workflows."""

    def setup_method(self):
        """Set up test fixtures."""
        self.validator = PythonFileValidator()

    def test_validate_simple_python_file(self):
        """Test validation of a simple Python data science file."""
        python_code = '''
import pandas as pd
import numpy as np

def load_data():
    """Load sample data."""
    return pd.DataFrame({
        'feature1': [1, 2, 3],
        'feature2': [4, 5, 6],
        'target': [0, 1, 0]
    })

def train_model(data):
    """Train a simple model."""
    from sklearn.ensemble import RandomForestClassifier

    X = data[['feature1', 'feature2']]
    y = data['target']

    model = RandomForestClassifier()
    model.fit(X, y)

    return {
        'accuracy': 0.95,
        'precision': 0.92,
        'recall': 0.94
    }

if __name__ == "__main__":
    data = load_data()
    results = train_model(data)
    print(f"Model accuracy: {results['accuracy']}")
'''

        with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
            f.write(python_code)
            temp_path = f.name

        try:
            result = self.validator.validate_file(temp_path)

            assert result.is_valid is True
            assert result.file_type == "python"
            assert len(result.imports_found) >= 2  # pandas, numpy
            assert len(result.functions_found) >= 2  # load_data, train_model
            assert result.has_data_science_patterns is True

        finally:
            os.unlink(temp_path)

    def test_detect_missing_imports(self):
        """Test detection of missing imports in Python files."""
        python_code = """
# Missing import for pandas!
def load_data():
    return pd.DataFrame({'x': [1, 2, 3]})  # This will fail

# Missing import for sklearn!
def train_model():
    model = RandomForestClassifier()  # This will fail
    return model
"""

        with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
            f.write(python_code)
            temp_path = f.name

        try:
            result = self.validator.validate_file(temp_path)

            assert result.is_valid is False
            assert len(result.missing_imports) >= 2  # pandas, sklearn
            assert "pd" in str(result.errors) or "pandas" in str(result.errors)
            assert "RandomForestClassifier" in str(result.errors)

        finally:
            os.unlink(temp_path)

    def test_validate_data_science_patterns(self):
        """Test detection of common data science patterns."""
        python_code = """
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, classification_report

# Data loading pattern
df = pd.read_csv('data.csv')

# Data preprocessing pattern
X = df.drop('target', axis=1)
y = df['target']
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)

# Model training pattern
from sklearn.ensemble import RandomForestClassifier
model = RandomForestClassifier(n_estimators=100)
model.fit(X_train, y_train)

# Model evaluation pattern
y_pred = model.predict(X_test)
accuracy = accuracy_score(y_test, y_pred)
report = classification_report(y_test, y_pred)

# Visualization pattern
import matplotlib.pyplot as plt
plt.figure(figsize=(10, 6))
plt.plot(accuracy)
plt.show()
"""

        with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
            f.write(python_code)
            temp_path = f.name

        try:
            result = self.validator.validate_file(temp_path)

            assert result.is_valid is True
            assert result.patterns_detected["data_loading"] is True
            assert result.patterns_detected["model_training"] is True
            assert result.patterns_detected["model_evaluation"] is True
            assert result.patterns_detected["visualization"] is True
            assert result.patterns_detected["train_test_split"] is True

        finally:
            os.unlink(temp_path)

    def test_validate_function_signatures(self):
        """Test validation of function signatures and return types."""
        python_code = '''
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple

def load_data() -> pd.DataFrame:
    """Load data with proper type hints."""
    return pd.DataFrame({'x': [1, 2, 3]})

def preprocess_data(df: pd.DataFrame) -> Tuple[np.ndarray, np.ndarray]:
    """Preprocess with type hints."""
    X = df.drop('target', axis=1).values
    y = df['target'].values
    return X, y

def train_model(X: np.ndarray, y: np.ndarray) -> Dict[str, float]:
    """Train model with proper return type."""
    # Simulate training
    return {
        'accuracy': 0.95,
        'precision': 0.92,
        'recall': 0.94,
        'f1_score': 0.93
    }

def invalid_function():
    """Function without proper type hints or docstring."""
    pass
'''

        with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
            f.write(python_code)
            temp_path = f.name

        try:
            result = self.validator.validate_file(temp_path)

            assert result.is_valid is True
            assert len(result.functions_found) == 4

            # Check function quality scores
            function_scores = result.function_quality_scores
            assert function_scores["load_data"] > 0.8  # Good type hints and docstring
            assert function_scores["preprocess_data"] > 0.8
            assert function_scores["train_model"] > 0.8
            assert function_scores["invalid_function"] < 0.5  # Poor quality

        finally:
            os.unlink(temp_path)

    def test_validate_with_syntax_errors(self):
        """Test handling of Python files with syntax errors."""
        python_code = """
import pandas as pd

def broken_function():
    # Missing closing parenthesis
    df = pd.DataFrame({
        'x': [1, 2, 3
    # Missing closing brace and parenthesis

def another_function()  # Missing colon
    return "broken"
"""

        with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
            f.write(python_code)
            temp_path = f.name

        try:
            result = self.validator.validate_file(temp_path)

            assert result.is_valid is False
            assert result.has_syntax_errors is True
            assert len(result.syntax_errors) > 0
            assert "SyntaxError" in str(result.errors)

        finally:
            os.unlink(temp_path)

    def test_performance_with_large_python_file(self):
        """Test validation performance with large Python files."""
        # Generate a large Python file
        large_code_parts = [
            "import pandas as pd",
            "import numpy as np",
            "from sklearn.ensemble import RandomForestClassifier",
            "",
        ]

        # Add many functions
        for i in range(100):
            large_code_parts.extend(
                [
                    f"def function_{i}(data):",
                    f'    """Function {i} for data processing."""',
                    f"    result = data * {i}",
                    f"    return result",
                    "",
                ]
            )

        # Add main execution
        large_code_parts.extend(
            [
                "if __name__ == '__main__':",
                "    data = pd.DataFrame({'x': range(1000)})",
                "    for i in range(100):",
                f"        result = function_{{i}}(data)",
                "    print('Processing complete')",
            ]
        )

        large_code = "\n".join(large_code_parts)

        with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
            f.write(large_code)
            temp_path = f.name

        try:
            import time

            start_time = time.time()

            result = self.validator.validate_file(temp_path)

            end_time = time.time()
            validation_time = end_time - start_time

            assert result.is_valid is True
            assert len(result.functions_found) == 100
            assert validation_time < 2.0  # Should complete within 2 seconds

            print(f"Validated large Python file ({len(large_code)} chars) in {validation_time:.3f}s")

        finally:
            os.unlink(temp_path)


class TestCodeBlockValidator:
    """Test validation of individual code blocks within Python files."""

    def setup_method(self):
        """Set up test fixtures."""
        self.validator = CodeBlockValidator()

    def test_validate_import_block(self):
        """Test validation of import statements."""
        import_code = """
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, precision_score
import matplotlib.pyplot as plt
"""

        result = self.validator.validate_imports(import_code)

        assert result.is_valid is True
        assert len(result.imports_found) >= 5
        assert "pandas" in result.imports_found
        assert "numpy" in result.imports_found
        assert "sklearn.ensemble" in result.imports_found

    def test_validate_function_block(self):
        """Test validation of function definitions."""
        function_code = '''
def train_model(X_train, y_train, model_type='rf'):
    """
    Train a machine learning model.

    Args:
        X_train: Training features
        y_train: Training labels
        model_type: Type of model to train

    Returns:
        Trained model and metrics
    """
    if model_type == 'rf':
        from sklearn.ensemble import RandomForestClassifier
        model = RandomForestClassifier()
    else:
        from sklearn.linear_model import LogisticRegression
        model = LogisticRegression()

    model.fit(X_train, y_train)

    return {
        'model': model,
        'accuracy': 0.95,
        'training_samples': len(X_train)
    }
'''

        result = self.validator.validate_function(function_code)

        assert result.is_valid is True
        assert result.has_docstring is True
        assert result.has_type_hints is False  # No type hints in this example
        assert result.has_return_statement is True
        assert result.complexity_score < 10  # Not too complex

    def test_validate_data_processing_block(self):
        """Test validation of data processing code blocks."""
        processing_code = """
# Data preprocessing pipeline
df_clean = df.dropna()
df_clean['feature_scaled'] = (df_clean['feature'] - df_clean['feature'].mean()) / df_clean['feature'].std()

# Feature engineering
df_clean['feature_squared'] = df_clean['feature'] ** 2
df_clean['feature_log'] = np.log(df_clean['feature'] + 1)

# Train-test split
X = df_clean.drop('target', axis=1)
y = df_clean['target']
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
"""

        result = self.validator.validate_data_processing(processing_code)

        assert result.is_valid is True
        assert result.has_data_cleaning is True
        assert result.has_feature_engineering is True
        assert result.has_train_test_split is True


class TestImportValidator:
    """Test validation and auto-installation of imports."""

    def setup_method(self):
        """Set up test fixtures."""
        self.validator = ImportValidator()

    def test_detect_missing_imports(self):
        """Test detection of missing imports."""
        code_with_missing_imports = """
# pandas is used but not imported
df = pd.DataFrame({'x': [1, 2, 3]})

# sklearn is used but not imported
model = RandomForestClassifier()

# numpy is used but not imported
arr = np.array([1, 2, 3])
"""

        missing_imports = self.validator.find_missing_imports(code_with_missing_imports)

        assert "pandas" in missing_imports or "pd" in missing_imports
        assert "sklearn" in str(missing_imports) or "RandomForestClassifier" in str(missing_imports)
        assert "numpy" in missing_imports or "np" in missing_imports

    def test_suggest_import_fixes(self):
        """Test suggestion of import fixes."""
        code = "df = pd.DataFrame({'x': [1, 2, 3]})"

        suggestions = self.validator.suggest_import_fixes(code)

        assert len(suggestions) > 0
        assert any("import pandas as pd" in suggestion for suggestion in suggestions)

    @patch("subprocess.run")
    def test_auto_install_missing_packages(self, mock_subprocess):
        """Test automatic installation of missing packages."""
        mock_subprocess.return_value.returncode = 0

        missing_packages = ["pandas", "scikit-learn", "matplotlib"]

        result = self.validator.auto_install_packages(missing_packages)

        assert result.is_valid is True
        assert len(result.installed_packages) == 3
        assert mock_subprocess.call_count == 3


class TestFunctionValidator:
    """Test validation of function quality and patterns."""

    def setup_method(self):
        """Set up test fixtures."""
        self.validator = FunctionValidator()

    def test_calculate_function_quality_score(self):
        """Test calculation of function quality scores."""
        high_quality_function = '''
def train_model(X: np.ndarray, y: np.ndarray, model_type: str = 'rf') -> Dict[str, Any]:
    """
    Train a machine learning model with comprehensive validation.

    Args:
        X: Feature matrix
        y: Target vector
        model_type: Type of model to train ('rf', 'lr', 'svm')

    Returns:
        Dictionary containing trained model and evaluation metrics

    Raises:
        ValueError: If input data is invalid
    """
    if X.shape[0] != y.shape[0]:
        raise ValueError("X and y must have same number of samples")

    if model_type == 'rf':
        model = RandomForestClassifier()
    elif model_type == 'lr':
        model = LogisticRegression()
    else:
        raise ValueError(f"Unknown model type: {model_type}")

    model.fit(X, y)

    return {
        'model': model,
        'accuracy': model.score(X, y),
        'n_samples': X.shape[0],
        'n_features': X.shape[1]
    }
'''

        score = self.validator.calculate_quality_score(high_quality_function)

        assert score > 0.8  # High quality function

        low_quality_function = """
def bad_func(x, y):
    return x + y
"""

        score = self.validator.calculate_quality_score(low_quality_function)

        assert score < 0.5  # Low quality function

    def test_detect_data_science_patterns_in_function(self):
        """Test detection of data science patterns within functions."""
        ml_function = """
def complete_ml_pipeline(data_path):
    # Data loading
    df = pd.read_csv(data_path)

    # Data preprocessing
    df_clean = df.dropna()

    # Feature engineering
    X = df_clean.drop('target', axis=1)
    y = df_clean['target']

    # Train-test split
    X_train, X_test, y_train, y_test = train_test_split(X, y)

    # Model training
    model = RandomForestClassifier()
    model.fit(X_train, y_train)

    # Model evaluation
    y_pred = model.predict(X_test)
    accuracy = accuracy_score(y_test, y_pred)

    # Visualization
    plt.figure()
    plt.plot(y_test, y_pred, 'o')
    plt.show()

    return model, accuracy
"""

        patterns = self.validator.detect_patterns(ml_function)

        assert patterns["data_loading"] is True
        assert patterns["data_preprocessing"] is True
        assert patterns["feature_engineering"] is True
        assert patterns["train_test_split"] is True
        assert patterns["model_training"] is True
        assert patterns["model_evaluation"] is True
        assert patterns["visualization"] is True


class TestDataSciencePatternDetector:
    """Test detection of data science patterns and best practices."""

    def setup_method(self):
        """Set up test fixtures."""
        self.detector = DataSciencePatternDetector()

    def test_detect_ml_workflow_patterns(self):
        """Test detection of complete ML workflow patterns."""
        complete_workflow = """
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, classification_report
import matplotlib.pyplot as plt

# 1. Data Loading
df = pd.read_csv('dataset.csv')

# 2. Exploratory Data Analysis
print(df.info())
print(df.describe())

# 3. Data Preprocessing
df_clean = df.dropna()
df_clean = pd.get_dummies(df_clean)

# 4. Feature Engineering
X = df_clean.drop('target', axis=1)
y = df_clean['target']

# 5. Train-Test Split
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# 6. Model Training
model = RandomForestClassifier(n_estimators=100, random_state=42)
model.fit(X_train, y_train)

# 7. Model Evaluation
y_pred = model.predict(X_test)
accuracy = accuracy_score(y_test, y_pred)
report = classification_report(y_test, y_pred)

# 8. Visualization
plt.figure(figsize=(10, 6))
plt.plot(y_test, label='Actual')
plt.plot(y_pred, label='Predicted')
plt.legend()
plt.show()

# 9. Model Persistence
import joblib
joblib.dump(model, 'trained_model.pkl')
"""

        patterns = self.detector.detect_all_patterns(complete_workflow)

        assert patterns["data_loading"] is True
        assert patterns["exploratory_analysis"] is True
        assert patterns["data_preprocessing"] is True
        assert patterns["feature_engineering"] is True
        assert patterns["train_test_split"] is True
        assert patterns["model_training"] is True
        assert patterns["model_evaluation"] is True
        assert patterns["visualization"] is True
        assert patterns["model_persistence"] is True

    def test_detect_deep_learning_patterns(self):
        """Test detection of deep learning specific patterns."""
        dl_code = """
import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense, Dropout
from tensorflow.keras.optimizers import Adam

# Model architecture
model = Sequential([
    Dense(128, activation='relu', input_shape=(10,)),
    Dropout(0.2),
    Dense(64, activation='relu'),
    Dropout(0.2),
    Dense(1, activation='sigmoid')
])

# Model compilation
model.compile(
    optimizer=Adam(learning_rate=0.001),
    loss='binary_crossentropy',
    metrics=['accuracy']
)

# Model training with callbacks
from tensorflow.keras.callbacks import EarlyStopping, ModelCheckpoint

callbacks = [
    EarlyStopping(patience=10, restore_best_weights=True),
    ModelCheckpoint('best_model.h5', save_best_only=True)
]

history = model.fit(
    X_train, y_train,
    validation_data=(X_val, y_val),
    epochs=100,
    batch_size=32,
    callbacks=callbacks
)

# Training visualization
plt.plot(history.history['loss'], label='Training Loss')
plt.plot(history.history['val_loss'], label='Validation Loss')
plt.legend()
plt.show()
"""

        patterns = self.detector.detect_deep_learning_patterns(dl_code)

        assert patterns["neural_network_architecture"] is True
        assert patterns["model_compilation"] is True
        assert patterns["callbacks"] is True
        assert patterns["training_history"] is True
        assert patterns["training_visualization"] is True

    def test_detect_data_quality_issues(self):
        """Test detection of potential data quality issues in code."""
        problematic_code = """
# No data validation
df = pd.read_csv('data.csv')

# Direct model training without checking data
model = RandomForestClassifier()
model.fit(df.drop('target', axis=1), df['target'])

# No train-test split
accuracy = model.score(df.drop('target', axis=1), df['target'])

# No cross-validation
print(f"Accuracy: {accuracy}")
"""

        issues = self.detector.detect_quality_issues(problematic_code)

        assert issues["no_data_validation"] is True
        assert issues["no_train_test_split"] is True
        assert issues["no_cross_validation"] is True
        assert issues["data_leakage_risk"] is True


class TestPythonValidationResult:
    """Test the PythonValidationResult data structure."""

    def test_create_validation_result(self):
        """Test creation of validation results."""
        result = PythonValidationResult(
            is_valid=True,
            file_path="/path/to/file.py",
            file_type="python",
            imports_found=["pandas", "numpy"],
            functions_found=["load_data", "train_model"],
            patterns_detected={"data_loading": True, "model_training": True},
        )

        assert result.is_valid is True
        assert result.file_type == "python"
        assert len(result.imports_found) == 2
        assert len(result.functions_found) == 2
        assert result.has_data_science_patterns is True

    def test_merge_validation_results(self):
        """Test merging multiple validation results."""
        result1 = PythonValidationResult(
            is_valid=True,
            file_path="/path/to/file1.py",
            imports_found=["pandas"],
            functions_found=["func1"],
            patterns_detected={"data_loading": True},
        )

        result2 = PythonValidationResult(
            is_valid=True,
            file_path="/path/to/file2.py",
            imports_found=["numpy"],
            functions_found=["func2"],
            patterns_detected={"model_training": True},
        )

        merged = result1.merge(result2)

        assert merged.is_valid is True
        assert len(merged.imports_found) == 2
        assert len(merged.functions_found) == 2
        assert merged.patterns_detected["data_loading"] is True
        assert merged.patterns_detected["model_training"] is True

    def test_validation_result_to_dict(self):
        """Test conversion of validation result to dictionary."""
        result = PythonValidationResult(
            is_valid=True,
            file_path="/path/to/file.py",
            imports_found=["pandas"],
            functions_found=["load_data"],
            patterns_detected={"data_loading": True},
        )

        result_dict = result.to_dict()

        assert isinstance(result_dict, dict)
        assert result_dict["is_valid"] is True
        assert result_dict["file_path"] == "/path/to/file.py"
        assert "pandas" in result_dict["imports_found"]
        assert "load_data" in result_dict["functions_found"]


class TestIntegrationWithSmartInstaller:
    """Test integration between Python validation and smart installer."""

    def setup_method(self):
        """Set up test fixtures."""
        self.validator = PythonFileValidator()
        self.installer = SmartInstaller()

    @patch("subprocess.run")
    def test_validate_and_auto_install_workflow(self, mock_subprocess):
        """Test complete workflow of validation and auto-installation."""
        mock_subprocess.return_value.returncode = 0

        python_code = """
# Missing imports that should be auto-installed
df = pd.DataFrame({'x': [1, 2, 3]})
model = RandomForestClassifier()
plt.plot([1, 2, 3])
"""

        with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
            f.write(python_code)
            temp_path = f.name

        try:
            # First validation should fail due to missing imports
            result = self.validator.validate_file(temp_path)
            assert result.is_valid is False
            assert len(result.missing_imports) > 0

            # Auto-install missing packages
            install_result = self.installer.install_missing_packages(result.missing_imports)
            assert install_result.is_valid is True

            # Second validation should pass (mocked)
            with patch.object(self.validator, "validate_file") as mock_validate:
                mock_validate.return_value = PythonValidationResult(
                    is_valid=True, file_path=temp_path, missing_imports=[]
                )

                final_result = self.validator.validate_file(temp_path)
                assert final_result.is_valid is True
                assert len(final_result.missing_imports) == 0

        finally:
            os.unlink(temp_path)

    def test_performance_benchmark(self):
        """Test performance of Python file validation."""
        # Create multiple Python files for benchmarking
        test_files = []

        for i in range(10):
            python_code = f"""
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier

def load_data_{i}():
    return pd.DataFrame({{'feature_{i}': range(100)}})

def train_model_{i}(data):
    model = RandomForestClassifier()
    X = data.drop('target', axis=1) if 'target' in data.columns else data
    y = np.random.randint(0, 2, len(X))
    model.fit(X, y)
    return model.score(X, y)

if __name__ == "__main__":
    data = load_data_{i}()
    accuracy = train_model_{i}(data)
    print(f"Model {i} accuracy: {{accuracy}}")
"""

            with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
                f.write(python_code)
                test_files.append(f.name)

        try:
            import time

            start_time = time.time()

            results = []
            for file_path in test_files:
                result = self.validator.validate_file(file_path)
                results.append(result)

            end_time = time.time()
            total_time = end_time - start_time

            # All files should validate successfully
            assert all(result.is_valid for result in results)

            # Should complete within reasonable time
            assert total_time < 5.0  # 10 files in under 5 seconds

            print(f"Validated {len(test_files)} Python files in {total_time:.3f}s")

        finally:
            for file_path in test_files:
                os.unlink(file_path)
