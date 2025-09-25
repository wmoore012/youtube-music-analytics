#!/usr/bin/env python3
"""
Demonstration of Notebook Guardian with Chart Visualization

This script shows how Notebook Guardian works with existing data science setups,
validates real code, and creates interactive charts to visualize the results.
"""

import os
from pathlib import Path
import sys
import tempfile
import time

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# Add current directory to path
sys.path.append(".")

from src.data_organization.notebook_validator import MetricExplainer, NotebookValidator
from src.notebook_guardian.python_validator import PythonFileValidator
from src.notebook_guardian.security import create_security_report, enable_safe_mode
from src.notebook_guardian.smart_installer import FastDependencyDetector, check_dependencies


def create_sample_data_science_files():
    """Create sample data science files to demonstrate validation."""

    files_created = []

    # 1. Simple ML script
    ml_script = '''
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score
import matplotlib.pyplot as plt

def load_data():
    """Load sample dataset."""
    return pd.DataFrame({
        'feature1': np.random.randn(1000),
        'feature2': np.random.randn(1000),
        'target': np.random.randint(0, 2, 1000)
    })

def train_model(data):
    """Train a simple ML model."""
    X = data[['feature1', 'feature2']]
    y = data['target']

    model = RandomForestClassifier(n_estimators=100, random_state=42)
    model.fit(X, y)

    accuracy = model.score(X, y)
    return model, accuracy

def visualize_results(data, model):
    """Create visualizations."""
    plt.figure(figsize=(10, 6))
    plt.scatter(data['feature1'], data['feature2'], c=data['target'])
    plt.title('Data Distribution')
    plt.show()

if __name__ == "__main__":
    data = load_data()
    model, accuracy = train_model(data)
    print(f"Model accuracy: {accuracy:.3f}")
    visualize_results(data, model)
'''

    with tempfile.NamedTemporaryFile(mode="w", suffix="_ml_pipeline.py", delete=False) as f:
        f.write(ml_script)
        files_created.append(f.name)

    # 2. Deep Learning script
    dl_script = '''
import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense, Dropout
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

def create_model(input_dim):
    """Create a simple neural network."""
    model = Sequential([
        Dense(128, activation='relu', input_shape=(input_dim,)),
        Dropout(0.2),
        Dense(64, activation='relu'),
        Dropout(0.2),
        Dense(1, activation='sigmoid')
    ])

    model.compile(
        optimizer='adam',
        loss='binary_crossentropy',
        metrics=['accuracy']
    )

    return model

def train_neural_network():
    """Train neural network with sample data."""
    # Generate sample data
    X = np.random.randn(1000, 10)
    y = np.random.randint(0, 2, 1000)

    model = create_model(X.shape[1])

    history = model.fit(
        X, y,
        epochs=50,
        batch_size=32,
        validation_split=0.2,
        verbose=0
    )

    return model, history

if __name__ == "__main__":
    model, history = train_neural_network()
    print("Neural network training completed")
'''

    with tempfile.NamedTemporaryFile(mode="w", suffix="_deep_learning.py", delete=False) as f:
        f.write(dl_script)
        files_created.append(f.name)

    # 3. Data analysis script with potential issues
    problematic_script = '''
# Missing imports - should trigger warnings
df = pd.DataFrame({'x': [1, 2, 3]})  # pandas not imported
arr = np.array([1, 2, 3])  # numpy not imported
model = RandomForestClassifier()  # sklearn not imported

# Typosquatting example (intentional for demo)
# import pandass  # This would be flagged as suspicious

def analyze_data():
    """Analyze data without proper imports."""
    result = df.mean()
    return result

def train_model_unsafe():
    """Train model without proper validation."""
    # No train-test split - data leakage risk
    model.fit(df[['x']], df['y'])
    return model.score(df[['x']], df['y'])
'''

    with tempfile.NamedTemporaryFile(mode="w", suffix="_problematic.py", delete=False) as f:
        f.write(problematic_script)
        files_created.append(f.name)

    # 4. Comprehensive analytics script
    analytics_script = '''
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, classification_report
import seaborn as sns
import matplotlib.pyplot as plt

def comprehensive_analysis():
    """Complete data science workflow."""
    # Data loading
    data = pd.read_csv('sample_data.csv') if os.path.exists('sample_data.csv') else generate_sample_data()

    # Exploratory data analysis
    print(data.info())
    print(data.describe())

    # Data preprocessing
    data_clean = data.dropna()
    data_encoded = pd.get_dummies(data_clean)

    # Feature engineering
    X = data_encoded.drop('target', axis=1)
    y = data_encoded['target']

    # Train-test split
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Model training
    model = RandomForestClassifier(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)

    # Model evaluation
    y_pred = model.predict(X_test)
    accuracy = accuracy_score(y_test, y_pred)
    report = classification_report(y_test, y_pred)

    # Visualization
    fig = px.scatter(data, x='feature1', y='feature2', color='target')
    fig.show()

    return model, accuracy, report

def generate_sample_data():
    """Generate sample data for analysis."""
    return pd.DataFrame({
        'feature1': np.random.randn(1000),
        'feature2': np.random.randn(1000),
        'feature3': np.random.randint(0, 5, 1000),
        'target': np.random.randint(0, 2, 1000)
    })

if __name__ == "__main__":
    model, accuracy, report = comprehensive_analysis()
    print(f"Final model accuracy: {accuracy:.3f}")
'''

    with tempfile.NamedTemporaryFile(mode="w", suffix="_comprehensive.py", delete=False) as f:
        f.write(analytics_script)
        files_created.append(f.name)

    return files_created


def validate_files_and_collect_data(file_paths):
    """Validate files and collect performance/validation data."""

    print("🔍 VALIDATING FILES WITH NOTEBOOK GUARDIAN")
    print("=" * 60)

    # Initialize validators
    python_validator = PythonFileValidator()
    dependency_detector = FastDependencyDetector()

    # Enable safe mode for security
    enable_safe_mode()

    validation_results = []
    performance_data = []
    security_data = []

    for i, file_path in enumerate(file_paths):
        file_name = Path(file_path).name
        print(f"\n📄 Validating {file_name}")
        print("-" * 40)

        # Time the validation
        start_time = time.perf_counter()

        # Validate Python file
        result = python_validator.validate_file(file_path)

        # Detect dependencies
        with open(file_path, "r") as f:
            code = f.read()
        dependencies = dependency_detector.detect_dependencies(code)

        # Security analysis
        security_report = create_security_report(list(dependencies))

        end_time = time.perf_counter()
        validation_time = (end_time - start_time) * 1000  # Convert to ms

        # Collect data for charts
        validation_results.append(
            {
                "file": file_name,
                "file_index": i + 1,
                "is_valid": result.is_valid,
                "functions_found": len(result.functions_found),
                "imports_found": len(result.imports_found),
                "missing_imports": len(result.missing_imports),
                "patterns_detected": sum(result.patterns_detected.values()),
                "has_ml_patterns": result.patterns_detected.get("model_training", False),
                "has_viz_patterns": result.patterns_detected.get("visualization", False),
                "has_data_patterns": result.patterns_detected.get("data_loading", False),
                "validation_time_ms": validation_time,
                "file_size": result.file_size_bytes,
                "lines_of_code": result.lines_of_code,
            }
        )

        performance_data.append(
            {
                "file": file_name,
                "validation_time_ms": validation_time,
                "throughput_loc_per_sec": result.lines_of_code / (validation_time / 1000) if validation_time > 0 else 0,
                "dependencies_found": len(dependencies),
            }
        )

        security_data.append(
            {
                "file": file_name,
                "total_packages": security_report["total_packages"],
                "safe_packages": len(security_report["safe_packages"]),
                "suspicious_packages": len(security_report["suspicious_packages"]),
                "blocked_packages": len(security_report["blocked_packages"]),
                "security_score": len(security_report["safe_packages"]) / max(security_report["total_packages"], 1),
            }
        )

        # Print results
        print(f"✅ Validation: {'PASS' if result.is_valid else 'FAIL'}")
        print(f"📊 Functions: {len(result.functions_found)}")
        print(f"📦 Dependencies: {len(dependencies)}")
        print(f"⚡ Time: {validation_time:.2f}ms")
        print(f"🔒 Security: {len(security_report['safe_packages'])}/{security_report['total_packages']} safe")

        if result.missing_imports:
            print(f"⚠️  Missing imports: {result.missing_imports}")

        if security_report["suspicious_packages"]:
            print(f"🚨 Suspicious packages: {security_report['suspicious_packages']}")

    return validation_results, performance_data, security_data


def create_validation_charts(validation_results, performance_data, security_data):
    """Create interactive charts showing validation results."""

    print("\n📊 CREATING VALIDATION CHARTS")
    print("=" * 60)

    # Convert to DataFrames
    validation_df = pd.DataFrame(validation_results)
    performance_df = pd.DataFrame(performance_data)
    security_df = pd.DataFrame(security_data)

    # Create subplots
    fig = make_subplots(
        rows=3,
        cols=2,
        subplot_titles=[
            "Validation Results Overview",
            "Performance Metrics",
            "Code Complexity Analysis",
            "Security Analysis",
            "Pattern Detection",
            "Throughput Analysis",
        ],
        specs=[
            [{"type": "bar"}, {"type": "scatter"}],
            [{"type": "bar"}, {"type": "bar"}],
            [{"type": "bar"}, {"type": "scatter"}],
        ],
    )

    # 1. Validation Results Overview
    fig.add_trace(
        go.Bar(
            x=validation_df["file"],
            y=validation_df["functions_found"],
            name="Functions Found",
            marker_color="lightblue",
        ),
        row=1,
        col=1,
    )

    # 2. Performance Metrics (Validation Time)
    fig.add_trace(
        go.Scatter(
            x=validation_df["lines_of_code"],
            y=validation_df["validation_time_ms"],
            mode="markers+text",
            text=validation_df["file"],
            textposition="top center",
            name="Validation Time",
            marker=dict(size=10, color="red"),
        ),
        row=1,
        col=2,
    )

    # 3. Code Complexity (Functions vs Imports)
    fig.add_trace(
        go.Bar(x=validation_df["file"], y=validation_df["imports_found"], name="Imports Found", marker_color="green"),
        row=2,
        col=1,
    )

    # 4. Security Analysis
    fig.add_trace(
        go.Bar(x=security_df["file"], y=security_df["safe_packages"], name="Safe Packages", marker_color="lightgreen"),
        row=2,
        col=2,
    )

    fig.add_trace(
        go.Bar(
            x=security_df["file"],
            y=security_df["suspicious_packages"],
            name="Suspicious Packages",
            marker_color="orange",
        ),
        row=2,
        col=2,
    )

    # 5. Pattern Detection
    pattern_data = []
    for result in validation_results:
        pattern_data.extend(
            [
                {"file": result["file"], "pattern": "ML Patterns", "detected": result["has_ml_patterns"]},
                {"file": result["file"], "pattern": "Viz Patterns", "detected": result["has_viz_patterns"]},
                {"file": result["file"], "pattern": "Data Patterns", "detected": result["has_data_patterns"]},
            ]
        )

    pattern_df = pd.DataFrame(pattern_data)
    pattern_summary = pattern_df.groupby("pattern")["detected"].sum().reset_index()

    fig.add_trace(
        go.Bar(
            x=pattern_summary["pattern"], y=pattern_summary["detected"], name="Patterns Detected", marker_color="purple"
        ),
        row=3,
        col=1,
    )

    # 6. Throughput Analysis
    fig.add_trace(
        go.Scatter(
            x=performance_df["dependencies_found"],
            y=performance_df["throughput_loc_per_sec"],
            mode="markers+text",
            text=performance_df["file"],
            textposition="top center",
            name="Throughput (LOC/sec)",
            marker=dict(size=12, color="blue"),
        ),
        row=3,
        col=2,
    )

    # Update layout
    fig.update_layout(
        height=1200, title_text="🛡️ Notebook Guardian Validation Results Dashboard", title_x=0.5, showlegend=True
    )

    # Update axes labels
    fig.update_xaxes(title_text="Files", row=1, col=1)
    fig.update_yaxes(title_text="Function Count", row=1, col=1)

    fig.update_xaxes(title_text="Lines of Code", row=1, col=2)
    fig.update_yaxes(title_text="Validation Time (ms)", row=1, col=2)

    fig.update_xaxes(title_text="Files", row=2, col=1)
    fig.update_yaxes(title_text="Import Count", row=2, col=1)

    fig.update_xaxes(title_text="Files", row=2, col=2)
    fig.update_yaxes(title_text="Package Count", row=2, col=2)

    fig.update_xaxes(title_text="Pattern Type", row=3, col=1)
    fig.update_yaxes(title_text="Files with Pattern", row=3, col=1)

    fig.update_xaxes(title_text="Dependencies Found", row=3, col=2)
    fig.update_yaxes(title_text="Throughput (LOC/sec)", row=3, col=2)

    # Save chart
    chart_path = "notebook_guardian_validation_dashboard.html"
    fig.write_html(chart_path)
    print(f"📊 Interactive dashboard saved: {chart_path}")

    return fig, validation_df, performance_df, security_df


def create_performance_comparison_chart(performance_df):
    """Create a detailed performance comparison chart."""

    fig = go.Figure()

    # Add validation time bars
    fig.add_trace(
        go.Bar(
            x=performance_df["file"],
            y=performance_df["validation_time_ms"],
            name="Validation Time (ms)",
            marker_color="lightcoral",
            yaxis="y",
        )
    )

    # Add throughput line on secondary y-axis
    fig.add_trace(
        go.Scatter(
            x=performance_df["file"],
            y=performance_df["throughput_loc_per_sec"],
            mode="lines+markers",
            name="Throughput (LOC/sec)",
            line=dict(color="blue", width=3),
            marker=dict(size=8),
            yaxis="y2",
        )
    )

    # Update layout with secondary y-axis
    fig.update_layout(
        title="🚀 Notebook Guardian Performance Analysis",
        xaxis_title="Files Analyzed",
        yaxis=dict(title="Validation Time (ms)", side="left", color="red"),
        yaxis2=dict(title="Throughput (Lines of Code per Second)", side="right", overlaying="y", color="blue"),
        height=500,
        hovermode="x unified",
    )

    # Save performance chart
    perf_chart_path = "notebook_guardian_performance.html"
    fig.write_html(perf_chart_path)
    print(f"⚡ Performance chart saved: {perf_chart_path}")

    return fig


def create_security_analysis_chart(security_df):
    """Create security analysis visualization."""

    fig = go.Figure()

    # Stacked bar chart for security analysis
    fig.add_trace(
        go.Bar(x=security_df["file"], y=security_df["safe_packages"], name="Safe Packages", marker_color="lightgreen")
    )

    fig.add_trace(
        go.Bar(
            x=security_df["file"],
            y=security_df["suspicious_packages"],
            name="Suspicious Packages",
            marker_color="orange",
        )
    )

    fig.add_trace(
        go.Bar(x=security_df["file"], y=security_df["blocked_packages"], name="Blocked Packages", marker_color="red")
    )

    fig.update_layout(
        title="🔒 Security Analysis Results",
        xaxis_title="Files Analyzed",
        yaxis_title="Package Count",
        barmode="stack",
        height=400,
    )

    # Save security chart
    security_chart_path = "notebook_guardian_security.html"
    fig.write_html(security_chart_path)
    print(f"🔒 Security chart saved: {security_chart_path}")

    return fig


def generate_summary_report(validation_df, performance_df, security_df):
    """Generate a comprehensive summary report."""

    print("\n📋 VALIDATION SUMMARY REPORT")
    print("=" * 60)

    # Overall statistics
    total_files = len(validation_df)
    valid_files = validation_df["is_valid"].sum()
    total_functions = validation_df["functions_found"].sum()
    total_imports = validation_df["imports_found"].sum()
    avg_validation_time = performance_df["validation_time_ms"].mean()
    avg_throughput = performance_df["throughput_loc_per_sec"].mean()

    # Security statistics
    total_packages = security_df["total_packages"].sum()
    safe_packages = security_df["safe_packages"].sum()
    suspicious_packages = security_df["suspicious_packages"].sum()

    print(f"📊 Files Analyzed: {total_files}")
    print(f"✅ Valid Files: {valid_files}/{total_files} ({valid_files/total_files*100:.1f}%)")
    print(f"🔧 Functions Found: {total_functions}")
    print(f"📦 Imports Detected: {total_imports}")
    print(f"⚡ Avg Validation Time: {avg_validation_time:.2f}ms")
    print(f"🚀 Avg Throughput: {avg_throughput:,.0f} LOC/sec")
    print(
        f"🔒 Package Security: {safe_packages}/{total_packages} safe ({safe_packages/max(total_packages,1)*100:.1f}%)"
    )

    if suspicious_packages > 0:
        print(f"⚠️  Suspicious Packages Found: {suspicious_packages}")

    # Pattern detection summary
    ml_files = validation_df["has_ml_patterns"].sum()
    viz_files = validation_df["has_viz_patterns"].sum()
    data_files = validation_df["has_data_patterns"].sum()

    print(f"\n🧠 Pattern Detection:")
    print(f"  ML Workflows: {ml_files}/{total_files} files")
    print(f"  Visualization: {viz_files}/{total_files} files")
    print(f"  Data Processing: {data_files}/{total_files} files")

    # Performance insights
    fastest_file = performance_df.loc[performance_df["validation_time_ms"].idxmin()]
    slowest_file = performance_df.loc[performance_df["validation_time_ms"].idxmax()]

    print(f"\n⚡ Performance Insights:")
    print(f"  Fastest validation: {fastest_file['file']} ({fastest_file['validation_time_ms']:.2f}ms)")
    print(f"  Slowest validation: {slowest_file['file']} ({slowest_file['validation_time_ms']:.2f}ms)")

    return {
        "total_files": total_files,
        "valid_files": valid_files,
        "avg_validation_time": avg_validation_time,
        "avg_throughput": avg_throughput,
        "security_score": safe_packages / max(total_packages, 1),
    }


def demonstrate_metric_explanations():
    """Demonstrate the metric explanation system."""

    print("\n📝 METRIC EXPLANATION SYSTEM DEMO")
    print("=" * 60)

    explainer = MetricExplainer()

    # Sample metrics from validation
    sample_metrics = {
        "validation_accuracy": 0.95,
        "processing_speed": 0.87,
        "security_score": 0.92,
        "pattern_detection_rate": 0.78,
    }

    print("🎯 Sample Metric Explanations:")
    for metric, value in sample_metrics.items():
        explanation = explainer.generate_tooltip_text(metric, value)
        print(f"  {metric}: {explanation}")

    # Create legend definitions
    metrics_list = list(sample_metrics.keys())
    legends = explainer.create_legend_definitions(metrics_list)

    print(f"\n📚 Legend Definitions:")
    for metric, definition in legends.items():
        print(f"  {metric}: {definition}")


def main():
    """Main demonstration function."""

    print("🛡️ NOTEBOOK GUARDIAN DEMONSTRATION WITH CHARTS")
    print("=" * 80)
    print("This demo shows how Notebook Guardian validates real data science code")
    print("and creates interactive charts to visualize the results.")
    print()

    try:
        # Step 1: Create sample files
        print("📁 Creating sample data science files...")
        file_paths = create_sample_data_science_files()
        print(f"✅ Created {len(file_paths)} sample files")

        # Step 2: Validate files and collect data
        validation_results, performance_data, security_data = validate_files_and_collect_data(file_paths)

        # Step 3: Create interactive charts
        dashboard_fig, validation_df, performance_df, security_df = create_validation_charts(
            validation_results, performance_data, security_data
        )

        # Step 4: Create additional specialized charts
        performance_fig = create_performance_comparison_chart(performance_df)
        security_fig = create_security_analysis_chart(security_df)

        # Step 5: Generate summary report
        summary = generate_summary_report(validation_df, performance_df, security_df)

        # Step 6: Demonstrate metric explanations
        demonstrate_metric_explanations()

        print("\n" + "=" * 80)
        print("✅ DEMONSTRATION COMPLETED SUCCESSFULLY")
        print("=" * 80)
        print("📊 Interactive charts created:")
        print("  • notebook_guardian_validation_dashboard.html - Complete dashboard")
        print("  • notebook_guardian_performance.html - Performance analysis")
        print("  • notebook_guardian_security.html - Security analysis")
        print()
        print("🎯 Key Results:")
        print(f"  • Validated {summary['total_files']} files in {summary['avg_validation_time']:.2f}ms avg")
        print(f"  • Achieved {summary['avg_throughput']:,.0f} LOC/sec throughput")
        print(f"  • Security score: {summary['security_score']*100:.1f}%")
        print()
        print("🚀 Notebook Guardian is ready for production use!")
        print("Open the HTML files in your browser to see interactive charts.")

    except Exception as e:
        print(f"❌ Error during demonstration: {str(e)}")
        import traceback

        traceback.print_exc()

    finally:
        # Clean up temporary files
        print("\n🧹 Cleaning up temporary files...")
        for file_path in file_paths:
            try:
                os.unlink(file_path)
            except Exception:
                pass
        print("✅ Cleanup completed")


if __name__ == "__main__":
    main()
