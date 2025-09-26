#!/usr/bin/env python3
"""
Blueprint Execution System

Maintains a blueprint notebook and creates executed versions with datetime stamps.
Validates execution outputs and FAILS LOUDLY on errors.

System maintains exactly 2 files in /notebooks:
1. Blueprint file (clean name) - used to create executed versions
2. Executed file (with datetime) - result of running the blueprint
"""

from datetime import datetime
import json
import logging
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
from typing import Any, Dict, List, Optional

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BlueprintExecutionManager:
    """
    Professional blueprint and execution management system.

    Features:
    - Maintains blueprint notebook (clean name)
    - Creates executed versions with datetime stamps
    - Archives old executed versions to datetime folders
    - Validates execution outputs for errors
    - FAILS LOUDLY when issues are detected
    """

    def __init__(self, notebooks_dir: Path):
        """Initialize the blueprint execution manager."""
        self.notebooks_dir = Path(notebooks_dir)
        self.archive_dir = self.notebooks_dir / "archive"

        # Ensure directories exist
        self.notebooks_dir.mkdir(parents=True, exist_ok=True)
        self.archive_dir.mkdir(parents=True, exist_ok=True)

        # Error patterns to detect in executed notebooks
        self.error_patterns = [
            r"❌.*Not Available",
            r"🚨 CRITICAL ERROR",
            r"CRITICAL FAILURE:",
            r"🚨 FIX YOUR DATABASE",
            r"Chart returned None",
            r"Database connection failed",
            r"🚨.*ERROR",
            r"FAILED.*charts?",
            r"KeyError:",
            r"ConnectionError:",
            r"DatabaseError:",
        ]

        # Success patterns to detect
        self.success_patterns = [
            r"✅.*SUCCESS",
            r"✅ REAL DATA",
            r"Charts: \d+/\d+ \(100% success\)",
            r"✅ All systems operational",
            r"📊.*generated successfully",
            r"MISSION ACCOMPLISHED",
        ]

        logger.info("📊 BlueprintExecutionManager initialized")
        logger.info(f"   📂 Notebooks: {self.notebooks_dir}")
        logger.info(f"   📦 Archive: {self.archive_dir}")

    def create_blueprint_notebook(self) -> Path:
        """
        Create the blueprint notebook using the existing create_notebook system.

        Returns:
            Path to the created blueprint notebook
        """
        logger.info("📝 Creating blueprint notebook...")

        # Use the existing create_notebook system
        from create_notebook import create_professional_notebook

        result = create_professional_notebook()
        blueprint_path = Path(result["notebook_path"])

        logger.info(f"✅ Blueprint created: {blueprint_path}")
        return blueprint_path

    def archive_old_executed_versions(self) -> List[Path]:
        """
        Archive any existing executed versions to datetime folders.

        Returns:
            List of paths to archived files
        """
        # Find existing executed versions
        executed_files = list(self.notebooks_dir.glob("*_executed.ipynb"))

        if not executed_files:
            logger.info("📦 No old executed versions to archive")
            return []

        logger.info(f"📦 Archiving {len(executed_files)} old executed versions...")

        archived_paths = []
        for executed_file in executed_files:
            # Create datetime folder for archive
            now = datetime.now()
            datetime_folder = now.strftime("%Y%m%d_%H%M%S")
            archive_datetime_dir = self.archive_dir / datetime_folder
            archive_datetime_dir.mkdir(parents=True, exist_ok=True)

            # Move to archive with original name
            archive_path = archive_datetime_dir / executed_file.name
            shutil.move(str(executed_file), str(archive_path))

            logger.info(f"   📄 Archived: {executed_file.name} → {datetime_folder}/")
            archived_paths.append(archive_path)

        return archived_paths

    def execute_blueprint_file(self, notebook_path: Path) -> Path:
        """
        Execute any notebook file to create an executed version.

        Args:
            notebook_path: Path to the notebook to execute

        Returns:
            Path to the executed notebook

        Raises:
            RuntimeError: If notebook doesn't exist or execution fails
        """
        if not notebook_path.exists():
            raise RuntimeError(f"Notebook not found: {notebook_path}")

        # Create executed filename with datetime
        now = datetime.now()
        datetime_str = now.strftime("%Y%m%d_%H%M%S")
        base_name = notebook_path.stem
        executed_name = f"{base_name}_{datetime_str}_executed.ipynb"
        executed_path = notebook_path.parent / executed_name

        logger.info(f"🚀 Executing notebook...")
        logger.info(f"   📄 Source: {notebook_path.name}")
        logger.info(f"   📄 Output: {executed_name}")

        try:
            # Execute notebook using nbconvert with explicit working directory
            cmd = [
                sys.executable,
                "-m",
                "nbconvert",
                "--to",
                "notebook",
                "--execute",
                "--output",
                str(executed_path),
                str(notebook_path),
                "--ExecutePreprocessor.timeout=600",
                "--ExecutePreprocessor.kernel_name=python3",
            ]

            # Set environment to ensure proper path resolution
            env = os.environ.copy()
            env["PYTHONPATH"] = str(self.notebooks_dir.parent)

            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                cwd=self.notebooks_dir.parent,  # Run from parent directory
                env=env,  # Pass environment with PYTHONPATH
                timeout=660,  # Extra buffer
            )

            if result.returncode != 0:
                logger.error(f"🚨 Notebook execution failed!")
                logger.error(f"   💥 Return code: {result.returncode}")
                logger.error(f"   🚨 STDERR: {result.stderr}")
                raise RuntimeError(f"Notebook execution failed: {result.stderr}")

            logger.info(f"✅ Notebook executed successfully: {executed_path}")
            return executed_path

        except Exception as e:
            logger.error(f"🚨 Notebook execution error: {e}")
            raise RuntimeError(f"Notebook execution error: {e}")

    def execute_blueprint(self) -> Path:
        """
        Execute the blueprint notebook to create an executed version.

        Returns:
            Path to the executed notebook

        Raises:
            RuntimeError: If blueprint doesn't exist or execution fails
        """
        blueprint_path = self.notebooks_dir / "MusicScope™_Professional_Dashboard.ipynb"
        return self.execute_blueprint_file(blueprint_path)

    def validate_executed_notebook(self, executed_path: Path) -> Dict[str, Any]:
        """
        Validate executed notebook outputs for error patterns.

        Args:
            executed_path: Path to the executed notebook

        Returns:
            Dictionary with validation results
        """
        if not executed_path.exists():
            return {
                "success": False,
                "errors": [f"Executed notebook not found: {executed_path}"],
                "summary": "Executed notebook file not found",
            }

        logger.info(f"🔍 Validating executed notebook: {executed_path.name}")

        try:
            # Load executed notebook
            with open(executed_path, "r", encoding="utf-8") as f:
                notebook = json.load(f)

            errors = []
            success_indicators = []

            # Check each cell's outputs
            for i, cell in enumerate(notebook.get("cells", [])):
                if cell.get("cell_type") == "code":
                    outputs = cell.get("outputs", [])

                    for output in outputs:
                        # Check stdout/stderr text
                        if output.get("output_type") == "stream":
                            text_lines = output.get("text", [])
                            if isinstance(text_lines, str):
                                text_lines = [text_lines]

                            # Detect errors
                            for line in text_lines:
                                for pattern in self.error_patterns:
                                    if re.search(pattern, line, re.IGNORECASE):
                                        errors.append(f"Cell {i+1}: {pattern} detected in: {line.strip()}")
                                        logger.warning(f"🚨 Error detected: {pattern}")

                            # Detect success indicators
                            for line in text_lines:
                                for pattern in self.success_patterns:
                                    if re.search(pattern, line, re.IGNORECASE):
                                        success_indicators.append(f"Cell {i+1}: {line.strip()}")

                        # Check error outputs
                        elif output.get("output_type") == "error":
                            error_name = output.get("ename", "Unknown")
                            error_value = output.get("evalue", "Unknown error")
                            errors.append(f"Cell {i+1}: {error_name}: {error_value}")
                            logger.error(f"🚨 Cell {i+1} error: {error_name}: {error_value}")

            # Determine overall success
            success = len(errors) == 0

            # Create summary
            if success:
                summary = f"✅ Validation passed: {len(success_indicators)} success indicators, 0 errors"
            else:
                summary = f"🚨 Validation failed: {len(errors)} errors detected"

            logger.info(f"📊 Validation complete: {summary}")

            return {
                "success": success,
                "errors": errors,
                "success_indicators": success_indicators,
                "summary": summary,
                "total_cells": len(notebook.get("cells", [])),
                "error_count": len(errors),
            }

        except Exception as e:
            error_msg = f"Failed to validate executed notebook: {e}"
            logger.error(f"🚨 {error_msg}")
            return {"success": False, "errors": [error_msg], "summary": "Validation failed due to exception"}

    def execute_complete_workflow(self) -> Dict[str, Any]:
        """
        Execute the complete workflow:
        1. Archive old executed versions
        2. Create/update blueprint
        3. Execute blueprint to create new executed version
        4. Validate executed version outputs
        5. FAIL LOUDLY if validation fails

        Returns:
            Dictionary with workflow results
        """
        logger.info("🔄 Starting complete blueprint execution workflow...")

        try:
            # Step 1: Archive old executed versions
            logger.info("📦 Step 1: Archiving old executed versions...")
            archived_files = self.archive_old_executed_versions()
            logger.info(f"✅ Step 1 complete: Archived {len(archived_files)} files")

            # Step 2: Ensure blueprint exists
            logger.info("📝 Step 2: Ensuring blueprint exists...")
            blueprint_path = self.notebooks_dir / "MusicScope™_Professional_Dashboard.ipynb"
            if not blueprint_path.exists():
                blueprint_path = self.create_blueprint_notebook()
            logger.info(f"✅ Step 2 complete: Blueprint ready at {blueprint_path}")

            # Step 3: Execute blueprint
            logger.info("🚀 Step 3: Executing blueprint...")
            executed_path = self.execute_blueprint()
            logger.info(f"✅ Step 3 complete: Executed version created at {executed_path}")

            # Step 4: Validate executed version
            logger.info("🔍 Step 4: Validating executed version...")
            validation_result = self.validate_executed_notebook(executed_path)

            if not validation_result["success"]:
                logger.error("🚨 Step 4 FAILED: Validation detected errors!")
                logger.error(f"   💥 Errors: {len(validation_result['errors'])}")
                for error in validation_result["errors"]:
                    logger.error(f"      - {error}")

                # FAIL LOUDLY
                raise RuntimeError(
                    f"🚨 EXECUTED NOTEBOOK VALIDATION FAILED! "
                    f"Detected {len(validation_result['errors'])} errors. "
                    f"FIX YOUR DATA AND CHARTS!"
                )

            logger.info(f"✅ Step 4 complete: Validation passed")
            logger.info(f"🎉 COMPLETE WORKFLOW SUCCESS!")

            return {
                "success": True,
                "blueprint_path": blueprint_path,
                "executed_path": executed_path,
                "archived_files": archived_files,
                "validation_result": validation_result,
                "summary": f"✅ Complete workflow successful: {validation_result['summary']}",
            }

        except Exception as e:
            error_msg = f"🚨 COMPLETE WORKFLOW FAILED: {e}"
            logger.error(error_msg)
            raise RuntimeError(error_msg)

    def get_current_files_status(self) -> Dict[str, Any]:
        """
        Get current status of blueprint and executed files.

        Returns:
            Dictionary with file status information
        """
        blueprint_path = self.notebooks_dir / "MusicScope™_Professional_Dashboard.ipynb"
        executed_files = list(self.notebooks_dir.glob("*_executed.ipynb"))

        status = {
            "blueprint_exists": blueprint_path.exists(),
            "blueprint_path": blueprint_path if blueprint_path.exists() else None,
            "executed_exists": len(executed_files) > 0,
            "executed_path": executed_files[0] if executed_files else None,
            "executed_count": len(executed_files),
            "total_notebook_files": len(list(self.notebooks_dir.glob("*.ipynb"))),
        }

        return status


def main():
    """Example usage of the BlueprintExecutionManager."""
    import tempfile

    # Example usage
    with tempfile.TemporaryDirectory() as temp_dir:
        notebooks_dir = Path(temp_dir) / "notebooks"
        manager = BlueprintExecutionManager(notebooks_dir)

        try:
            # Execute complete workflow
            result = manager.execute_complete_workflow()

            print(f"✅ Workflow completed successfully!")
            print(f"📄 Blueprint: {result['blueprint_path']}")
            print(f"📄 Executed: {result['executed_path']}")
            print(f"📊 Summary: {result['summary']}")

        except Exception as e:
            print(f"🚨 Workflow failed: {e}")


if __name__ == "__main__":
    main()
