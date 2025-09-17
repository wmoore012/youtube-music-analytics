"""
TDD Tests for Notebook Validation System

These tests ensure the notebook validation system works correctly
and makes it IMPOSSIBLE to miss broken charts.

NO FAKE DATA. WELL COMMENTED. BULLETPROOF.
"""

import json
import os
import tempfile
from unittest.mock import MagicMock, patch

import pytest

from create_and_validate_notebooks import NotebookValidator


class TestNotebookValidator:
    """Test the notebook validation system."""

    def test_validator_initialization(self):
        """Test validator initializes with correct settings."""
        validator = NotebookValidator()

        assert validator.total_expected_charts == 20
        assert validator.validation_results == {}

    def test_create_production_notebook(self):
        """Test production notebook creation."""
        validator = NotebookValidator()

        with tempfile.TemporaryDirectory() as temp_dir:
            # Mock the notebook path to use temp directory
            with patch("create_and_validate_notebooks.NotebookTemplateManager") as mock_manager:
                mock_instance = MagicMock()
                mock_manager.return_value = mock_instance

                # Mock notebook generation
                mock_notebook = {
                    "cells": [{"cell_type": "code"} for _ in range(44)],  # Expected cell count
                    "metadata": {},
                }
                mock_instance.generate_notebook_template.return_value = mock_notebook

                # Create notebook
                notebook_path = validator.create_production_notebook()

                # Verify calls
                mock_instance.generate_notebook_template.assert_called_once()
                mock_instance.save_notebook.assert_called_once()

                # Check expected path
                assert "MusicScope™_Validated_Dashboard.ipynb" in notebook_path

    def test_parse_validation_output_success_case(self):
        """Test parsing validation output for successful case."""
        validator = NotebookValidator()

        # Mock successful output
        output_text = """
🎯 REAL DATA ANALYTICS SUMMARY
==================================================
📊 Charts with REAL data: 15/20
📋 Charts showing data requirements: 3/20
❌ Charts with errors: 2/20

✅ Working with real data: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]
📋 Need data columns: [16, 17, 18]
❌ Have errors: [19, 20]

🎉 SUCCESS: 15 charts working with REAL data!
💝 No fake data used - authentic analytics only!

🎵 MusicScope™ Real Data Analytics Complete! 🎵

✅ CI/CD: PASS - Excellent chart health
        """

        result = validator._parse_validation_output(output_text)

        assert result["real_charts"] == 15
        assert result["requirement_charts"] == 3
        assert result["error_charts"] == 2
        assert result["total_expected"] == 20
        assert result["success_rate"] == 0.75  # 15/20
        assert result["ci_cd_status"] == "PASS"

    def test_parse_validation_output_failure_case(self):
        """Test parsing validation output for failure case."""
        validator = NotebookValidator()

        # Mock failure output
        output_text = """
🎯 REAL DATA ANALYTICS SUMMARY
==================================================
📊 Charts with REAL data: 0/20
📋 Charts showing data requirements: 0/20
❌ Charts with errors: 20/20

❌ Have errors: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20]

📋 All charts show data requirements - add real data to see analytics!

🎵 MusicScope™ Real Data Analytics Complete! 🎵

❌ CI/CD: FAIL - Poor chart health
        """

        result = validator._parse_validation_output(output_text)

        assert result["real_charts"] == 0
        assert result["requirement_charts"] == 0
        assert result["error_charts"] == 20
        assert result["success_rate"] == 0.0
        assert result["ci_cd_status"] == "FAIL"

    def test_validate_chart_counts_correct_total(self):
        """Test validation passes when chart counts are correct."""
        validator = NotebookValidator()

        # Correct validation output
        validation_output = {
            "real_charts": 10,
            "requirement_charts": 5,
            "error_charts": 5,
            "success_rate": 0.5,
            "ci_cd_status": "WARNING",
        }

        issues = validator.validate_chart_counts(validation_output)

        # Should have no issues for correct totals
        assert len(issues) == 0

    def test_validate_chart_counts_incorrect_total(self):
        """Test validation fails when chart counts don't add up."""
        validator = NotebookValidator()

        # Incorrect validation output (totals to 15 instead of 20)
        validation_output = {
            "real_charts": 5,
            "requirement_charts": 5,
            "error_charts": 5,  # Total = 15, should be 20
            "success_rate": 0.33,
            "ci_cd_status": "FAIL",
        }

        issues = validator.validate_chart_counts(validation_output)

        # Should detect the mismatch
        assert len(issues) > 0
        assert any("Total charts mismatch" in issue for issue in issues)

    def test_validate_chart_counts_all_failed(self):
        """Test validation detects when all charts failed."""
        validator = NotebookValidator()

        # All charts failed
        validation_output = {
            "real_charts": 0,
            "requirement_charts": 0,
            "error_charts": 20,
            "success_rate": 0.0,
            "ci_cd_status": "FAIL",
        }

        issues = validator.validate_chart_counts(validation_output)

        # Should detect system failure
        assert len(issues) > 0
        assert any("ALL charts failed" in issue for issue in issues)

    def test_validate_chart_counts_negative_values(self):
        """Test validation detects negative chart counts."""
        validator = NotebookValidator()

        # Negative values (impossible)
        validation_output = {
            "real_charts": -1,  # Impossible
            "requirement_charts": 10,
            "error_charts": 11,
            "success_rate": 0.0,
            "ci_cd_status": "FAIL",
        }

        issues = validator.validate_chart_counts(validation_output)

        # Should detect negative values
        assert len(issues) > 0
        assert any("Negative real chart count" in issue for issue in issues)

    def test_create_validation_report_success(self):
        """Test validation report generation for successful case."""
        validator = NotebookValidator()

        validation_output = {
            "real_charts": 18,
            "requirement_charts": 2,
            "error_charts": 0,
            "success_rate": 0.9,
            "ci_cd_status": "PASS",
        }

        report = validator.create_validation_report(validation_output)

        # Check report content
        assert "NOTEBOOK VALIDATION REPORT" in report
        assert "18/20" in report
        assert "90.0%" in report
        assert "PASS" in report
        assert "Excellent!" in report

    def test_create_validation_report_failure(self):
        """Test validation report generation for failure case."""
        validator = NotebookValidator()

        validation_output = {
            "real_charts": 0,
            "requirement_charts": 5,
            "error_charts": 15,
            "success_rate": 0.0,
            "ci_cd_status": "FAIL",
        }

        report = validator.create_validation_report(validation_output)

        # Check report content
        assert "NOTEBOOK VALIDATION REPORT" in report
        assert "0/20" in report
        assert "0.0%" in report
        assert "FAIL" in report
        assert "Fix data loading" in report
        assert "Investigate chart errors" in report

    def test_extract_last_cell_output_finds_validation(self):
        """Test extraction of validation output from notebook."""
        validator = NotebookValidator()

        # Mock executed notebook
        notebook = {
            "cells": [
                {
                    "cell_type": "code",
                    "outputs": [
                        {
                            "output_type": "stream",
                            "name": "stdout",
                            "text": [
                                "🎯 REAL DATA ANALYTICS SUMMARY\n",
                                "==================================================\n",
                                "📊 Charts with REAL data: 10/20\n",
                                "📋 Charts showing data requirements: 5/20\n",
                                "❌ Charts with errors: 5/20\n",
                            ],
                        }
                    ],
                }
            ]
        }

        result = validator._extract_last_cell_output(notebook)

        assert result["real_charts"] == 10
        assert result["requirement_charts"] == 5
        assert result["error_charts"] == 5

    def test_extract_last_cell_output_no_validation(self):
        """Test extraction when no validation output found."""
        validator = NotebookValidator()

        # Mock notebook without validation output
        notebook = {
            "cells": [
                {
                    "cell_type": "code",
                    "outputs": [{"output_type": "stream", "name": "stdout", "text": ["Some other output\n"]}],
                }
            ]
        }

        result = validator._extract_last_cell_output(notebook)

        # Should return empty dict
        assert result == {}


class TestNotebookValidationIntegration:
    """Integration tests for the complete validation system."""

    def test_validation_detects_broken_system(self):
        """Test that validation system detects when charts are broken."""
        validator = NotebookValidator()

        # Mock broken system output
        broken_output = {
            "real_charts": 0,
            "requirement_charts": 0,
            "error_charts": 20,
            "success_rate": 0.0,
            "ci_cd_status": "FAIL",
        }

        issues = validator.validate_chart_counts(broken_output)

        # Should detect multiple critical issues
        assert len(issues) > 0

        # Should detect all charts failed
        critical_issues = [issue for issue in issues if "ALL charts failed" in issue]
        assert len(critical_issues) > 0

    def test_validation_passes_healthy_system(self):
        """Test that validation system passes for healthy charts."""
        validator = NotebookValidator()

        # Mock healthy system output
        healthy_output = {
            "real_charts": 16,
            "requirement_charts": 3,
            "error_charts": 1,
            "success_rate": 0.8,
            "ci_cd_status": "PASS",
        }

        issues = validator.validate_chart_counts(healthy_output)

        # Should have no critical issues
        assert len(issues) == 0

    def test_validation_report_provides_actionable_feedback(self):
        """Test that validation report provides clear, actionable feedback."""
        validator = NotebookValidator()

        # Mock system needing improvement
        mixed_output = {
            "real_charts": 3,
            "requirement_charts": 10,
            "error_charts": 7,
            "success_rate": 0.15,
            "ci_cd_status": "FAIL",
        }

        report = validator.create_validation_report(mixed_output)

        # Should provide specific recommendations
        assert "Add missing data columns" in report
        assert "Investigate chart errors" in report
        assert "RECOMMENDATIONS" in report

        # Should show clear metrics
        assert "3/20" in report  # Real charts
        assert "15.0%" in report  # Success rate


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
