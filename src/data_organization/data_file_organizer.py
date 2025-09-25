#!/usr/bin/env python3
"""
Data File Organizer - TDD Implementation

Organizes scattered CSV/JSON files throughout the codebase into a structured system.
This addresses the real problem of data files scattered in root, config/,
music_analysis_tables/, and other directories.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import hashlib
import json
from pathlib import Path
import shutil
import time
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

# --------------------------- Enums ---------------------------


class FileCategory(Enum):
    """Categories for organizing data files."""

    CONFIG = "config"
    ANALYSIS_RESULTS = "analysis_results"
    BENCHMARKS = "benchmarks"
    DATASETS = "datasets"
    EXPORTS = "exports"
    TEMPORARY = "temporary"
    UNKNOWN = "unknown"


# --------------------------- Data Models ---------------------------


@dataclass
class DataFileInfo:
    """Information about a discovered data file."""

    path: Path
    name: str
    size_bytes: int
    file_type: str
    category: FileCategory
    last_modified: datetime
    content_hash: Optional[str] = None

    def calculate_content_hash(self) -> str:
        """Calculate SHA-256 hash of file content for duplicate detection."""
        if self.content_hash is not None:
            return self.content_hash

        try:
            with open(self.path, "rb") as f:
                content = f.read()
                self.content_hash = hashlib.sha256(content).hexdigest()
                return self.content_hash
        except Exception:
            # If we can't read the file, return a hash of the path
            self.content_hash = hashlib.sha256(str(self.path).encode()).hexdigest()
            return self.content_hash

    def is_valid_format(self) -> bool:
        """Check if file format is valid (JSON or CSV)."""
        return self.file_type.lower() in ["json", "csv"]

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for reporting."""
        return {
            "path": str(self.path),
            "name": self.name,
            "size_bytes": self.size_bytes,
            "file_type": self.file_type,
            "category": self.category.value,
            "last_modified": self.last_modified.isoformat(),
            "content_hash": self.content_hash,
        }


@dataclass
class ValidationResult:
    """Result of file validation."""

    file_path: Path
    is_valid: bool
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    file_size_bytes: int = 0

    def add_error(self, error: str) -> None:
        """Add an error and set is_valid to False."""
        self.errors.append(error)
        self.is_valid = False

    def add_warning(self, warning: str) -> None:
        """Add a warning (doesn't affect validity)."""
        self.warnings.append(warning)


@dataclass
class OrganizationResult:
    """Result of file organization operation."""

    success: bool
    files_moved: int
    created_directories: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    duration_seconds: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for reporting."""
        return {
            "success": self.success,
            "files_moved": self.files_moved,
            "created_directories": self.created_directories,
            "errors": self.errors,
            "duration_seconds": self.duration_seconds,
        }

    def generate_report(self) -> str:
        """Generate human-readable report."""
        status = "SUCCESS" if self.success else "FAILED"
        report = f"Organization {status}\n"
        report += f"Files moved: {self.files_moved}\n"
        report += f"Directories created: {len(self.created_directories)}\n"
        report += f"Duration: {self.duration_seconds:.1f} seconds\n"

        if self.errors:
            report += f"Errors: {len(self.errors)}\n"
            for error in self.errors[:5]:  # Show first 5 errors
                report += f"  - {error}\n"

        return report


@dataclass
class BackupResult:
    """Result of backup operation."""

    success: bool
    backup_location: Optional[str] = None
    files_backed_up: int = 0
    errors: List[str] = field(default_factory=list)


@dataclass
class DuplicateGroup:
    """Group of duplicate files with same content."""

    files: List[DataFileInfo]
    content_hash: str
    total_size_bytes: int

    def __post_init__(self):
        """Calculate total size after initialization."""
        if self.total_size_bytes == 0:
            self.total_size_bytes = sum(f.size_bytes for f in self.files)


# --------------------------- Main Organizer Class ---------------------------


class DataFileOrganizer:
    """
    Organizes scattered CSV/JSON files throughout the codebase.

    Discovers, categorizes, validates, and organizes data files into
    a structured directory system.
    """

    def __init__(self):
        self.workspace_root: Optional[Path] = None
        self.discovered_files: List[DataFileInfo] = []
        self.file_categories: Dict[FileCategory, List[DataFileInfo]] = {}

        # Define category patterns for file classification
        self.category_patterns = {
            FileCategory.CONFIG: [
                "config/",
                "artist_aliases",
                "artist_colors",
                "expected_artists",
                "video_isrc_overrides",
                "production.json",
                "personal_issue_videos",
            ],
            FileCategory.ANALYSIS_RESULTS: [
                "music_analysis_tables/",
                "artist_music_summary",
                "normalized_music_videos",
                "video_type_analysis",
                "artist_performance",
            ],
            FileCategory.BENCHMARKS: [
                "benchmark",
                "benchmarks.json",
                "system_health",
                "function_analysis_report",
                "model_test_results",
            ],
            FileCategory.DATASETS: ["sentiment_dataset", "music_industry_sentiment", "enhanced_music", "failed_cases"],
            FileCategory.EXPORTS: ["data/", "exports/", "export_data"],
            FileCategory.TEMPORARY: ["temp", "tmp", ".cache", ".mypy_cache"],
        }

    def discover_files(self, workspace_root: Path) -> List[DataFileInfo]:
        """
        Discover all CSV and JSON files in the workspace.

        Args:
            workspace_root: Root directory to search

        Returns:
            List of discovered data files
        """
        self.workspace_root = workspace_root
        discovered_files = []

        # Search for CSV and JSON files
        for pattern in ["**/*.csv", "**/*.json"]:
            for file_path in workspace_root.rglob(pattern):
                # Skip hidden directories and common ignore patterns
                if any(part.startswith(".") for part in file_path.parts):
                    continue
                if "node_modules" in file_path.parts:
                    continue
                if "__pycache__" in file_path.parts:
                    continue

                try:
                    stat = file_path.stat()
                    file_type = file_path.suffix[1:].lower()  # Remove the dot

                    # Categorize the file
                    category = self._categorize_file(file_path)

                    file_info = DataFileInfo(
                        path=file_path,
                        name=file_path.name,
                        size_bytes=stat.st_size,
                        file_type=file_type,
                        category=category,
                        last_modified=datetime.fromtimestamp(stat.st_mtime),
                    )

                    discovered_files.append(file_info)

                except (OSError, PermissionError):
                    # Skip files we can't access
                    continue

        self.discovered_files = discovered_files
        return discovered_files

    def categorize_files(self, files: List[DataFileInfo]) -> Dict[FileCategory, List[DataFileInfo]]:
        """
        Categorize files by type and location.

        Args:
            files: List of discovered files

        Returns:
            Dictionary mapping categories to file lists
        """
        categorized = {}

        for file_info in files:
            category = file_info.category
            if category not in categorized:
                categorized[category] = []
            categorized[category].append(file_info)

        self.file_categories = categorized
        return categorized

    def validate_files(self, files: List[DataFileInfo]) -> List[ValidationResult]:
        """
        Validate file integrity and format.

        Args:
            files: List of files to validate

        Returns:
            List of validation results
        """
        validation_results = []

        for file_info in files:
            result = ValidationResult(file_path=file_info.path, is_valid=True, file_size_bytes=file_info.size_bytes)

            # Check if file exists and is readable
            if not file_info.path.exists():
                result.add_error("File does not exist")
            elif not file_info.path.is_file():
                result.add_error("Path is not a file")
            else:
                # Validate file format
                if not file_info.is_valid_format():
                    result.add_error(f"Invalid file format: {file_info.file_type}")

                # Try to read and validate content
                try:
                    if file_info.file_type == "json":
                        with open(file_info.path, "r", encoding="utf-8") as f:
                            json.load(f)
                    elif file_info.file_type == "csv":
                        # Try to read with pandas to validate CSV format
                        pd.read_csv(file_info.path, nrows=1)
                except Exception as e:
                    result.add_error(f"Content validation failed: {str(e)}")

                # Check file size warnings
                if file_info.size_bytes == 0:
                    result.add_warning("File is empty")
                elif file_info.size_bytes > 100 * 1024 * 1024:  # 100MB
                    result.add_warning("File is very large (>100MB)")

            validation_results.append(result)

        return validation_results

    def create_organized_structure(self, workspace_root: Path) -> OrganizationResult:
        """
        Create organized directory structure.

        Args:
            workspace_root: Root directory for organization

        Returns:
            Result of directory creation
        """
        start_time = time.time()
        created_directories = []
        errors = []

        try:
            # Create main organized data directory
            organized_root = workspace_root / "organized_data"
            organized_root.mkdir(exist_ok=True)
            created_directories.append("organized_data")

            # Create category directories
            for category in FileCategory:
                if category == FileCategory.UNKNOWN:
                    continue  # Skip unknown category

                category_dir = organized_root / category.value
                category_dir.mkdir(exist_ok=True)
                created_directories.append(f"organized_data/{category.value}")

            duration = time.time() - start_time

            return OrganizationResult(
                success=True,
                files_moved=0,  # No files moved yet, just directories created
                created_directories=created_directories,
                errors=errors,
                duration_seconds=duration,
            )

        except Exception as e:
            duration = time.time() - start_time
            errors.append(f"Failed to create directory structure: {str(e)}")

            return OrganizationResult(
                success=False,
                files_moved=0,
                created_directories=created_directories,
                errors=errors,
                duration_seconds=duration,
            )

    def move_files_to_organized_structure(
        self, workspace_root: Path, categorized_files: Dict[FileCategory, List[DataFileInfo]]
    ) -> OrganizationResult:
        """
        Move files to organized directory structure.

        Args:
            workspace_root: Root directory
            categorized_files: Files categorized by type

        Returns:
            Result of file moving operation
        """
        start_time = time.time()
        files_moved = 0
        errors = []

        try:
            organized_root = workspace_root / "organized_data"

            for category, files in categorized_files.items():
                if category == FileCategory.UNKNOWN:
                    continue  # Skip unknown files

                category_dir = organized_root / category.value

                for file_info in files:
                    try:
                        # Create destination path
                        dest_path = category_dir / file_info.name

                        # Handle name conflicts
                        counter = 1
                        while dest_path.exists():
                            name_parts = file_info.name.rsplit(".", 1)
                            if len(name_parts) == 2:
                                dest_path = category_dir / f"{name_parts[0]}_{counter}.{name_parts[1]}"
                            else:
                                dest_path = category_dir / f"{file_info.name}_{counter}"
                            counter += 1

                        # Copy file (don't move original yet, for safety)
                        shutil.copy2(file_info.path, dest_path)
                        files_moved += 1

                    except Exception as e:
                        errors.append(f"Failed to move {file_info.name}: {str(e)}")

            duration = time.time() - start_time

            return OrganizationResult(
                success=len(errors) == 0,
                files_moved=files_moved,
                created_directories=[],  # Already created in previous step
                errors=errors,
                duration_seconds=duration,
            )

        except Exception as e:
            duration = time.time() - start_time
            errors.append(f"Failed to move files: {str(e)}")

            return OrganizationResult(
                success=False, files_moved=files_moved, created_directories=[], errors=errors, duration_seconds=duration
            )

    def generate_inventory_report(
        self, discovered_files: List[DataFileInfo], categorized_files: Dict[FileCategory, List[DataFileInfo]]
    ) -> Dict[str, Any]:
        """
        Generate comprehensive file inventory report.

        Args:
            discovered_files: All discovered files
            categorized_files: Files categorized by type

        Returns:
            Inventory report dictionary
        """
        total_size = sum(f.size_bytes for f in discovered_files)

        # File type distribution
        file_types = {}
        for file_info in discovered_files:
            file_type = file_info.file_type
            if file_type not in file_types:
                file_types[file_type] = {"count": 0, "size_bytes": 0}
            file_types[file_type]["count"] += 1
            file_types[file_type]["size_bytes"] += file_info.size_bytes

        # Category distribution
        categories = {}
        for category, files in categorized_files.items():
            categories[category.value] = {"count": len(files), "size_bytes": sum(f.size_bytes for f in files)}

        # Largest files
        largest_files = sorted(discovered_files, key=lambda f: f.size_bytes, reverse=True)[:10]

        return {
            "total_files": len(discovered_files),
            "total_size_bytes": total_size,
            "total_size_mb": round(total_size / (1024 * 1024), 2),
            "file_types": file_types,
            "categories": categories,
            "largest_files": [
                {
                    "name": f.name,
                    "path": str(f.path),
                    "size_bytes": f.size_bytes,
                    "size_mb": round(f.size_bytes / (1024 * 1024), 2),
                    "category": f.category.value,
                }
                for f in largest_files
            ],
            "generated_at": datetime.now().isoformat(),
        }

    def detect_duplicate_files(self, files: List[DataFileInfo]) -> List[DuplicateGroup]:
        """
        Detect duplicate files by content hash.

        Args:
            files: List of files to check for duplicates

        Returns:
            List of duplicate groups
        """
        hash_groups = {}

        # Group files by content hash
        for file_info in files:
            content_hash = file_info.calculate_content_hash()
            if content_hash not in hash_groups:
                hash_groups[content_hash] = []
            hash_groups[content_hash].append(file_info)

        # Find groups with more than one file (duplicates)
        duplicate_groups = []
        for content_hash, file_list in hash_groups.items():
            if len(file_list) > 1:
                duplicate_group = DuplicateGroup(
                    files=file_list,
                    content_hash=content_hash,
                    total_size_bytes=0,  # Will be calculated in __post_init__
                )
                duplicate_groups.append(duplicate_group)

        return duplicate_groups

    def create_backup(self, workspace_root: Path, files: List[DataFileInfo]) -> BackupResult:
        """
        Create backup of original files before organization.

        Args:
            workspace_root: Root directory
            files: Files to backup

        Returns:
            Result of backup operation
        """
        try:
            # Create backup directory with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_dir = workspace_root / f"data_backup_{timestamp}"
            backup_dir.mkdir(exist_ok=True)

            files_backed_up = 0
            errors = []

            for file_info in files:
                try:
                    # Create relative path structure in backup
                    relative_path = file_info.path.relative_to(workspace_root)
                    backup_path = backup_dir / relative_path

                    # Create parent directories if needed
                    backup_path.parent.mkdir(parents=True, exist_ok=True)

                    # Copy file
                    shutil.copy2(file_info.path, backup_path)
                    files_backed_up += 1

                except Exception as e:
                    errors.append(f"Failed to backup {file_info.name}: {str(e)}")

            return BackupResult(
                success=len(errors) == 0,
                backup_location=str(backup_dir),
                files_backed_up=files_backed_up,
                errors=errors,
            )

        except Exception as e:
            return BackupResult(
                success=False, backup_location=None, files_backed_up=0, errors=[f"Failed to create backup: {str(e)}"]
            )

    def _categorize_file(self, file_path: Path) -> FileCategory:
        """
        Categorize a file based on its path and name.

        Args:
            file_path: Path to the file

        Returns:
            File category
        """
        path_str = str(file_path).lower()
        file_name = file_path.name.lower()

        # Check each category pattern
        for category, patterns in self.category_patterns.items():
            for pattern in patterns:
                if pattern.lower() in path_str or pattern.lower() in file_name:
                    return category

        return FileCategory.UNKNOWN


# --------------------------- Convenience Functions ---------------------------


def organize_workspace_files(workspace_root: Path) -> Dict[str, Any]:
    """
    Convenience function to organize all files in a workspace.

    Args:
        workspace_root: Root directory to organize

    Returns:
        Summary of organization results
    """
    organizer = DataFileOrganizer()

    # Discover files
    discovered_files = organizer.discover_files(workspace_root)

    # Categorize files
    categorized_files = organizer.categorize_files(discovered_files)

    # Validate files
    validation_results = organizer.validate_files(discovered_files)

    # Create backup
    backup_result = organizer.create_backup(workspace_root, discovered_files)

    # Create organized structure
    structure_result = organizer.create_organized_structure(workspace_root)

    # Move files
    move_result = organizer.move_files_to_organized_structure(workspace_root, categorized_files)

    # Generate inventory
    inventory = organizer.generate_inventory_report(discovered_files, categorized_files)

    # Detect duplicates
    duplicates = organizer.detect_duplicate_files(discovered_files)

    return {
        "discovered_files": len(discovered_files),
        "validation_results": {
            "valid_files": len([r for r in validation_results if r.is_valid]),
            "invalid_files": len([r for r in validation_results if not r.is_valid]),
            "total_errors": sum(len(r.errors) for r in validation_results),
        },
        "backup_result": backup_result,
        "organization_result": {
            "structure_created": structure_result.success,
            "files_moved": move_result.files_moved,
            "errors": len(move_result.errors),
        },
        "inventory": inventory,
        "duplicates_found": len(duplicates),
        "duplicate_groups": duplicates,
    }


if __name__ == "__main__":
    # Example usage
    import sys

    if len(sys.argv) > 1:
        workspace_path = Path(sys.argv[1])
    else:
        workspace_path = Path(".")

    print(f"🗂️  Organizing files in: {workspace_path}")
    results = organize_workspace_files(workspace_path)

    print(f"📊 Results:")
    print(f"  Discovered files: {results['discovered_files']}")
    print(f"  Valid files: {results['validation_results']['valid_files']}")
    print(f"  Files moved: {results['organization_result']['files_moved']}")
    print(f"  Duplicates found: {results['duplicates_found']}")
    print(f"  Backup created: {results['backup_result'].success}")
