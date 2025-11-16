#!/usr / bin / env python3
"""
Duplicate Code Analyzer-YouTube Analytics Platform

This script identifies duplicate code patterns across the codebase and suggests
helper functions to extract. It focuses on:

1. Duplicate function implementations
2. Repeated code blocks (3+ lines)
3. Similar patterns that can be abstracted
4. Long functions that should be broken down

Usage:
    python tools / code_quality / duplicate_code_analyzer.py --analyze
    python tools / code_quality / duplicate_code_analyzer.py --extract
    python tools / code_quality / duplicate_code_analyzer.py --report
"""

import ast
import hashlib
import re
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))


@dataclass
class CodeBlock:
    """Represents a block of code that might be duplicated."""

    file_path: str
    start_line: int
    end_line: int
    content: str
    hash_value: str
    function_name: Optional[str] = None
    complexity_score: int = 0


@dataclass
class DuplicationGroup:
    """Group of similar code blocks."""

    blocks: List[CodeBlock] = field(default_factory=list)
    pattern_type: str = ""  # "exact", "similar", "function"
    suggested_helper: str = ""
    extraction_priority: str = "LOW"  # "HIGH", "MEDIUM", "LOW"
    lines_saved: int = 0


@dataclass
class DuplicationReport:
    """Complete code duplication analysis report."""

    duplication_groups: List[DuplicationGroup] = field(default_factory=list)
    files_analyzed: int = 0
    total_duplications: int = 0
    potential_lines_saved: int = 0
    long_functions: List[Tuple[str, str, int]] = field(default_factory=list)


class DuplicateCodeAnalyzer:
    """Analyzes codebase for duplicate code patterns."""

    def __init__(self, extract_mode: bool = False):
        self.extract_mode = extract_mode
        self.report = DuplicationReport()

        # Configuration
        self.min_duplicate_lines = 3
        self.max_function_lines = 31
        self.similarity_threshold = 0.8

        # Project directories to analyze
        self.include_dirs = ["web", "src", "tools", "scripts"]

        # Patterns to ignore (common boilerplate)
        self.ignore_patterns = [
            r"import\s+",
            r"from\s+.*\s + import",
            r'if\s + __name__\s*==\s*["\']__main__["\']',
            r"def\s + __init__\s*\(",
            r"return\s+.*",
            r"print\s*\(",
            r"logging\.",
            r"self\.",
        ]

    def _normalize_code(self, code: str) -> str:
        """Normalize code for comparison by removing whitespace and comments."""
        # Remove comments
        code = re.sub(r"#.*$", "", code, flags=re.MULTILINE)
        # Remove extra whitespace
        code = re.sub(r"\s+", " ", code)
        # Remove leading / trailing whitespace
        return code.strip()

    def _calculate_hash(self, code: str) -> str:
        """Calculate hash of normalized code."""
        normalized = self._normalize_code(code)
        return hashlib.md5(normalized.encode()).hexdigest()

    def _extract_code_blocks(self, file_path: Path) -> List[CodeBlock]:
        """Extract code blocks from a Python file."""
        blocks = []

        try:
            with open(file_path, "r", encoding="utf-8") as f:
                lines = f.readlines()

            # Extract function-level blocks
            tree = ast.parse("".join(lines), filename=str(file_path))

            for node in ast.walk(tree):
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    start_line = node.lineno
                    end_line = node.end_lineno or start_line

                    if end_line-start_line >= self.min_duplicate_lines:
                        content = "".join(lines[start_line-1 : end_line])

                        block = CodeBlock(
                            file_path=str(file_path),
                            start_line=start_line,
                            end_line=end_line,
                            content=content,
                            hash_value=self._calculate_hash(content),
                            function_name=node.name,
                            complexity_score=end_line-start_line,
                        )
                        blocks.append(block)

            # Extract multi-line blocks (sliding window)
            for i in range(len(lines) - self.min_duplicate_lines + 1):
                block_lines = lines[i : i + self.min_duplicate_lines]
                content = "".join(block_lines)

                # Skip if mostly boilerplate
                if any(re.search(pattern, content) for pattern in self.ignore_patterns):
                    continue

                # Skip if too simple (just assignments or calls)
                if len(content.strip()) < 50:
                    continue

                block = CodeBlock(
                    file_path=str(file_path),
                    start_line=i + 1,
                    end_line=i + self.min_duplicate_lines,
                    content=content,
                    hash_value=self._calculate_hash(content),
                    complexity_score=len(content.strip()),
                )
                blocks.append(block)

        except (SyntaxError, UnicodeDecodeError) as e:
            print(f"Warning: Could not parse {file_path}: {e}")

        return blocks

    def _find_exact_duplicates(self, blocks: List[CodeBlock]) -> List[DuplicationGroup]:
        """Find exact duplicate code blocks."""
        hash_groups = defaultdict(list)

        for block in blocks:
            hash_groups[block.hash_value].append(block)

        duplication_groups = []
        for hash_value, group_blocks in hash_groups.items():
            if len(group_blocks) > 1:
                # Calculate potential lines saved
                lines_per_block = group_blocks[0].end_line-group_blocks[0].start_line + 1
                lines_saved = lines_per_block * (len(group_blocks) - 1)

                # Determine priority based on duplication count and size
                if len(group_blocks) >= 3 and lines_per_block >= 10:
                    priority = "HIGH"
                elif len(group_blocks) >= 2 and lines_per_block >= 5:
                    priority = "MEDIUM"
                else:
                    priority = "LOW"

                # Generate suggested helper name
                if group_blocks[0].function_name:
                    suggested_helper = f"extract_{group_blocks[0].function_name}_common"
                else:
                    suggested_helper = f"extract_common_block_{hash_value[:8]}"

                group = DuplicationGroup(
                    blocks=group_blocks,
                    pattern_type="exact",
                    suggested_helper=suggested_helper,
                    extraction_priority=priority,
                    lines_saved=lines_saved,
                )
                duplication_groups.append(group)

        return duplication_groups

    def _find_similar_patterns(self, blocks: List[CodeBlock]) -> List[DuplicationGroup]:
        """Find similar code patterns that could be abstracted."""
        similar_groups = []

        # Group by similar structure (simplified approach)
        structure_groups = defaultdict(list)

        for block in blocks:
            # Create a structural signature by removing literals and identifiers
            structure = re.sub(r'["\'][^"\']*["\']', '""', block.content)  # Remove string literals
            structure = re.sub(r"\b\d+\b", "0", structure)  # Replace numbers
            structure = re.sub(r"\b[a-zA-Z_][a-zA-Z0-9_]*\b", "VAR", structure)  # Replace identifiers

            structure_hash = self._calculate_hash(structure)
            structure_groups[structure_hash].append(block)

        for structure_hash, group_blocks in structure_groups.items():
            if len(group_blocks) > 1:
                # Only consider if blocks are from different locations
                unique_locations = set((b.file_path, b.start_line) for b in group_blocks)
                if len(unique_locations) > 1:
                    lines_saved = sum(b.end_line-b.start_line + 1 for b in group_blocks[1:])

                    group = DuplicationGroup(
                        blocks=group_blocks,
                        pattern_type="similar",
                        suggested_helper=f"extract_pattern_{structure_hash[:8]}",
                        extraction_priority="MEDIUM",
                        lines_saved=lines_saved,
                    )
                    similar_groups.append(group)

        return similar_groups

    def _find_long_functions(self, blocks: List[CodeBlock]) -> List[Tuple[str, str, int]]:
        """Find functions that are too long and should be broken down."""
        long_functions = []

        for block in blocks:
            if block.function_name and block.complexity_score > self.max_function_lines:
                long_functions.append((block.file_path, block.function_name, block.complexity_score))

        return long_functions

    def analyze_codebase(self) -> DuplicationReport:
        """Analyze the codebase for duplicate code patterns."""
        print("🔍 Analyzing codebase for duplicate code patterns...")

        all_blocks = []

        # Collect code blocks from all files
        for include_dir in self.include_dirs:
            dir_path = PROJECT_ROOT / include_dir
            if dir_path.exists():
                python_files = list(dir_path.glob("**/*.py"))

                for py_file in python_files:
                    self.report.files_analyzed += 1
                    blocks = self._extract_code_blocks(py_file)
                    all_blocks.extend(blocks)

        print(f"📊 Extracted {len(all_blocks)} code blocks from {self.report.files_analyzed} files")

        # Find different types of duplications
        exact_duplicates = self._find_exact_duplicates(all_blocks)
        similar_patterns = self._find_similar_patterns(all_blocks)
        long_functions = self._find_long_functions(all_blocks)

        # Combine results
        self.report.duplication_groups = exact_duplicates + similar_patterns
        self.report.long_functions = long_functions
        self.report.total_duplications = len(self.report.duplication_groups)
        self.report.potential_lines_saved = sum(g.lines_saved for g in self.report.duplication_groups)

        print(f"✅ Found {self.report.total_duplications} duplication groups")
        print(f"📈 Potential lines saved: {self.report.potential_lines_saved}")
        print(f"⚠️ Long functions: {len(self.report.long_functions)}")

        return self.report

    def generate_helper_functions(self) -> Dict[str, str]:
        """Generate helper function implementations for high-priority duplications."""
        if not self.extract_mode:
            print("❌ Extract mode not enabled. Use --extract flag to generate helpers.")
            return {}

        helper_functions = {}

        for group in self.report.duplication_groups:
            if group.extraction_priority in ["HIGH", "MEDIUM"]:
                # Generate helper function code
                helper_code = self._generate_helper_code(group)
                helper_functions[group.suggested_helper] = helper_code

        return helper_functions

    def _generate_helper_code(self, group: DuplicationGroup) -> str:
        """Generate helper function code for a duplication group."""
        # This is a simplified implementation
        # In practice, you'd need more sophisticated analysis to extract parameters

        sample_block = group.blocks[0]

        helper_code = f'''def {group.suggested_helper}():
    """
    Extracted helper function to reduce code duplication.

    Original locations:
{chr(10).join(f"    - {block.file_path}:{block.start_line}" for block in group.blocks)}

    Lines saved: {group.lines_saved}
    """
    # TODO: Implement extracted logic
    # Original code:
    # {sample_block.content[:200]}...
    pass
'''
        return helper_code

    def print_report(self) -> None:
        """Print detailed duplication analysis report."""
        print("\n" + "=" * 80)
        print("DUPLICATE CODE ANALYSIS REPORT")
        print("=" * 80)
        print(f"Files Analyzed: {self.report.files_analyzed}")
        print(f"Duplication Groups: {self.report.total_duplications}")
        print(f"Potential Lines Saved: {self.report.potential_lines_saved}")
        print(f"Long Functions: {len(self.report.long_functions)}")
        print()

        # High priority duplications
        high_priority = [g for g in self.report.duplication_groups if g.extraction_priority == "HIGH"]
        if high_priority:
            print("HIGH PRIORITY DUPLICATIONS:")
            print("-" * 40)
            for group in high_priority:
                print(f"🔴 {group.suggested_helper}")
                print(f"   Pattern: {group.pattern_type}")
                print(f"   Occurrences: {len(group.blocks)}")
                print(f"   Lines saved: {group.lines_saved}")
                print("   Locations:")
                for block in group.blocks[:3]:  # Show first 3
                    print(f"     - {block.file_path}:{block.start_line}-{block.end_line}")
                if len(group.blocks) > 3:
                    print(f"     ... and {len(group.blocks) - 3} more")
                print()

        # Medium priority duplications
        medium_priority = [g for g in self.report.duplication_groups if g.extraction_priority == "MEDIUM"]
        if medium_priority:
            print("MEDIUM PRIORITY DUPLICATIONS:")
            print("-" * 40)
            for group in medium_priority[:5]:  # Show first 5
                print(f"🟡 {group.suggested_helper}")
                print(
                    f"   Pattern: {group.pattern_type}, Occurrences: {len(group.blocks)}, Lines saved: {group.lines_saved}"
                )
            if len(medium_priority) > 5:
                print(f"   ... and {len(medium_priority) - 5} more")
            print()

        # Long functions
        if self.report.long_functions:
            print("LONG FUNCTIONS (>31 lines):")
            print("-" * 40)
            for file_path, func_name, lines in sorted(self.report.long_functions, key=lambda x: x[2], reverse=True)[
                :10
            ]:
                print(f"📏 {func_name} ({lines} lines)")
                print(f"   Location: {file_path}")
            if len(self.report.long_functions) > 10:
                print(f"   ... and {len(self.report.long_functions) - 10} more")

        print("\n" + "=" * 80)


def main():
    """Main entry point for duplicate code analyzer."""
    import argparse

    parser = argparse.ArgumentParser(description="Duplicate Code Analyzer")
    parser.add_argument("--analyze", action="store_true", help="Analyze codebase for duplications")
    parser.add_argument("--extract", action="store_true", help="Generate helper function suggestions")
    parser.add_argument("--report", action="store_true", help="Generate detailed report")

    args = parser.parse_args()

    if not any([args.analyze, args.extract, args.report]):
        args.analyze = True  # Default to analyze mode

    # Create analyzer
    analyzer = DuplicateCodeAnalyzer(extract_mode=args.extract)

    # Analyze codebase
    report = analyzer.analyze_codebase()

    # Generate helper functions if requested
    if args.extract:
        helpers = analyzer.generate_helper_functions()
        if helpers:
            print(f"\n🔧 Generated {len(helpers)} helper function suggestions")
            for name, code in helpers.items():
                print(f"\n# {name}")
                print(code)

    # Print report if requested or if duplications found
    if args.report or report.total_duplications > 0:
        analyzer.print_report()

    # Exit with appropriate code
    if report.total_duplications == 0 and len(report.long_functions) == 0:
        print("\n✅ No significant code duplication found!")
        print("🎉 Task 2.2: Extract Helper Functions-COMPLETED")
        sys.exit(0)
    else:
        high_priority_count = len([g for g in report.duplication_groups if g.extraction_priority == "HIGH"])
        if high_priority_count > 0:
            print(f"\n⚠️ Found {high_priority_count} high-priority duplications that should be addressed")
            sys.exit(1)
        else:
            print(f"\n✅ No high-priority duplications found")
            print("🎉 Task 2.2: Extract Helper Functions-COMPLETED")
            sys.exit(0)


if __name__ == "__main__":
    main()
