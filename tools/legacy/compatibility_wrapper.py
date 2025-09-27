#!/usr / bin / env python3
"""
🔄 Backward Compatibility Wrapper

Provides backward compatibility for legacy tool references while guiding users
to migrate to the new consolidated tools.

This wrapper:
- Maps old tool names to new consolidated tools
- Provides deprecation warnings with migration guidance
- Ensures all previous use cases are supported
- Offers automatic migration suggestions

Usage:
    python tools / legacy / compatibility_wrapper.py old_tool_name [args]
    python tools / legacy / compatibility_wrapper.py --list - mappings
    python tools / legacy / compatibility_wrapper.py --migration - guide
"""

import argparse
from datetime import datetime
import os
from pathlib import Path
import subprocess
import sys
from typing import Dict, List, Optional, Tuple
import warnings

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))


class DeprecationWarning(UserWarning):
    """Custom deprecation warning for legacy tools."""

    pass


class CompatibilityWrapper:
    """
    Backward compatibility wrapper for legacy tool references.

    Maps old tool names to new consolidated tools and provides
    migration guidance to users.
    """

    def __init__(self):
        self.tool_mappings = self._initialize_tool_mappings()
        self.migration_messages = self._initialize_migration_messages()

    def _initialize_tool_mappings(self) -> Dict[str, Dict[str, str]]:
        """Initialize mappings from old tools to new consolidated tools."""
        return {
            # ETL Tools
            "run_etl.py": {
                "new_tool": "tools / core / unified_setup.py",
                "args_mapping": {
                    "--focused": "--run - etl --mode focused",
                    "--comprehensive": "--run - etl --mode comprehensive",
                    "--channels": "--run - etl --channels",
                },
                "category": "etl",
            },
            "youtube_channel_etl.py": {
                "new_tool": "tools / core / unified_setup.py",
                "args_mapping": {
                    "--run": "--run - etl",
                    "--validate": "--validate - etl",
                },
                "category": "etl",
            },
            # Setup Tools
            "setup_system.py": {
                "new_tool": "tools / core / unified_setup.py",
                "args_mapping": {
                    "--database": "--database - setup",
                    "--environment": "--environment - setup",
                    "--full": "--full - setup",
                },
                "category": "setup",
            },
            "create_tables.py": {
                "new_tool": "tools / core / unified_setup.py",
                "args_mapping": {
                    "": "--database - setup",
                },
                "category": "setup",
            },
            # Monitoring Tools
            "monitor.py": {
                "new_tool": "tools / core / unified_monitor.py",
                "args_mapping": {
                    "--health": "--health - check",
                    "--performance": "--performance - check",
                    "--all": "--full - check",
                },
                "category": "monitoring",
            },
            "system_health.py": {
                "new_tool": "tools / core / unified_monitor.py",
                "args_mapping": {
                    "": "--health - check",
                },
                "category": "monitoring",
            },
            # Maintenance Tools
            "cleanup.py": {
                "new_tool": "tools / core / unified_maintenance.py",
                "args_mapping": {
                    "--old - data": "--cleanup - old",
                    "--optimize": "--optimize - database",
                    "--retention": "--retention - cleanup",
                },
                "category": "maintenance",
            },
            "database_maintenance.py": {
                "new_tool": "tools / core / unified_maintenance.py",
                "args_mapping": {
                    "--optimize": "--optimize - database",
                    "--cleanup": "--cleanup - old",
                },
                "category": "maintenance",
            },
            # Analytics Tools
            "sentiment_analysis.py": {
                "new_tool": "tools / specialized / analytics / sentiment_analysis_tool.py",
                "args_mapping": {
                    "--analyze": "--run - analysis",
                    "--batch": "--batch - process",
                    "--compare": "--compare - models",
                },
                "category": "analytics",
            },
            # Benchmarking Tools
            "benchmark.py": {
                "new_tool": "tools / specialized / benchmarking / unified_benchmark_tool.py",
                "args_mapping": {
                    "--models": "--model - benchmark",
                    "--system": "--system - benchmark",
                    "--all": "--full - benchmark",
                },
                "category": "benchmarking",
            },
            "model_benchmark.py": {
                "new_tool": "tools / specialized / benchmarking / model_benchmark_tool.py",
                "args_mapping": {
                    "--run": "--run - benchmark",
                    "--compare": "--compare - models",
                },
                "category": "benchmarking",
            },
            # Migration Tools
            "migrate_data.py": {
                "new_tool": "tools / specialized / migration / storage_migrator.py",
                "args_mapping": {
                    "--to - file": "--db - to - file",
                    "--to - db": "--file - to - db",
                    "--validate": "--validate - migration",
                },
                "category": "migration",
            },
        }

    def _initialize_migration_messages(self) -> Dict[str, str]:
        """Initialize category - specific migration messages."""
        return {
            "etl": """
🔄 ETL Tool Migration:
The ETL functionality has been consolidated into the unified setup tool.
New tool provides better error handling, logging, and configuration management.

Migration steps:
1. Update your scripts to use: tools / core / unified_setup.py
2. Review new command - line options with --help
3. Update any automation scripts or cron jobs
""",
            "setup": """
🔧 Setup Tool Migration:
System setup has been unified into a single comprehensive tool.
New tool provides better validation, error recovery, and progress tracking.

Migration steps:
1. Use: tools / core / unified_setup.py --full - setup
2. Review environment variable requirements
3. Update deployment scripts
""",
            "monitoring": """
📊 Monitoring Tool Migration:
Monitoring capabilities have been enhanced and consolidated.
New tool provides better metrics, alerting, and reporting.

Migration steps:
1. Use: tools / core / unified_monitor.py
2. Review new monitoring options with --help
3. Update monitoring scripts and dashboards
""",
            "maintenance": """
🧹 Maintenance Tool Migration:
Maintenance operations have been consolidated and improved.
New tool provides better safety checks, reporting, and automation.

Migration steps:
1. Use: tools / core / unified_maintenance.py
2. Review new maintenance options
3. Update scheduled maintenance scripts
""",
            "analytics": """
📈 Analytics Tool Migration:
Analytics tools have been enhanced with new models and capabilities.
New tools provide better performance, accuracy, and reporting.

Migration steps:
1. Use tools in: tools / specialized / analytics/
2. Review new model options and configurations
3. Update analysis workflows
""",
            "benchmarking": """
🏃 Benchmarking Tool Migration:
Benchmarking has been expanded with comprehensive test suites.
New tools provide better metrics, reporting, and trend analysis.

Migration steps:
1. Use: tools / specialized / benchmarking / unified_benchmark_tool.py
2. Review new benchmark categories
3. Update performance testing workflows
""",
            "migration": """
🔄 Migration Tool Migration:
Data migration tools have been enhanced with better validation and rollback.
New tools provide safer migrations with comprehensive logging.

Migration steps:
1. Use: tools / specialized / migration / storage_migrator.py
2. Review new migration options and safety features
3. Update data migration workflows
""",
        }

    def show_deprecation_warning(self, old_tool: str, mapping: Dict[str, str]) -> None:
        """Show deprecation warning with migration guidance."""
        category = mapping.get("category", "general")
        new_tool = mapping["new_tool"]

        warning_message = f"""
⚠️  DEPRECATION WARNING ⚠️

The tool '{old_tool}' is deprecated and will be removed in a future version.

🔄 Migration Required:
   Old: {old_tool}
   New: {new_tool}

{self.migration_messages.get(category, "")}

📚 For complete migration guide, run:
   python tools / legacy / compatibility_wrapper.py --migration - guide

🆘 For help with the new tool, run:
   python {new_tool} --help
"""

        print(warning_message)
        warnings.warn(f"Tool '{old_tool}' is deprecated. Use '{new_tool}' instead.", DeprecationWarning, stacklevel=2)

    def map_arguments(self, old_args: List[str], mapping: Dict[str, str]) -> List[str]:
        """Map old arguments to new tool arguments."""
        args_mapping = mapping.get("args_mapping", {})
        new_args = []

        i = 0
        while i < len(old_args):
            arg = old_args[i]

            if arg in args_mapping:
                # Direct mapping
                mapped_arg = args_mapping[arg]
                if mapped_arg:  # Not empty string
                    new_args.extend(mapped_arg.split())
            elif arg.startswith("--"):
                # Try to find partial matches
                found_mapping = False
                for old_pattern, new_pattern in args_mapping.items():
                    if old_pattern and arg.startswith(old_pattern):
                        if new_pattern:
                            new_args.extend(new_pattern.split())
                        found_mapping = True
                        break

                if not found_mapping:
                    # Keep unknown arguments as - is with warning
                    print(f"⚠️  Warning: Argument '{arg}' may not be supported by the new tool")
                    new_args.append(arg)
            else:
                # Keep positional arguments as - is
                new_args.append(arg)

            i += 1

        return new_args

    def execute_new_tool(self, new_tool_path: str, new_args: List[str]) -> int:
        """Execute the new tool with mapped arguments."""
        full_path = project_root / new_tool_path

        if not full_path.exists():
            print(f"❌ Error: New tool not found at {full_path}")
            print("This may indicate an incomplete migration. Please check the tool installation.")
            return 1

        try:
            # Execute the new tool
            cmd = [sys.executable, str(full_path)] + new_args
            print(f"🔄 Executing: {' '.join(cmd)}")

            result = subprocess.run(cmd, cwd=project_root)
            return result.returncode

        except Exception as e:
            print(f"❌ Error executing new tool: {e}")
            return 1

    def handle_legacy_tool(self, tool_name: str, args: List[str]) -> int:
        """Handle execution of a legacy tool."""
        # Remove .py extension if present for lookup
        lookup_name = tool_name
        if not lookup_name.endswith(".py"):
            lookup_name += ".py"

        if lookup_name not in self.tool_mappings:
            print(f"❌ Unknown legacy tool: {tool_name}")
            print("Available legacy tool mappings:")
            self.list_mappings()
            return 1

        mapping = self.tool_mappings[lookup_name]

        # Show deprecation warning
        self.show_deprecation_warning(tool_name, mapping)

        # Map arguments
        new_args = self.map_arguments(args, mapping)

        # Ask user for confirmation
        response = input("\n🤔 Do you want to continue with the new tool? (y / N): ").lower()
        if response not in ["y", "yes"]:
            print("❌ Execution cancelled. Please migrate to the new tool when ready.")
            return 1

        # Execute new tool
        return self.execute_new_tool(mapping["new_tool"], new_args)

    def list_mappings(self) -> None:
        """List all available tool mappings."""
        print("🔄 Legacy Tool Mappings:")
        print("=" * 50)

        by_category = {}
        for old_tool, mapping in self.tool_mappings.items():
            category = mapping.get("category", "other")
            if category not in by_category:
                by_category[category] = []
            by_category[category].append((old_tool, mapping["new_tool"]))

        for category, tools in by_category.items():
            print(f"\n📂 {category.upper()}:")
            for old_tool, new_tool in tools:
                print(f"   {old_tool:<25} → {new_tool}")

    def show_migration_guide(self) -> None:
        """Show comprehensive migration guide."""
        print("🔄 COMPREHENSIVE MIGRATION GUIDE")
        print("=" * 60)
        print(
            """
This guide helps you migrate from legacy tools to the new consolidated tooling system.

## Why Migrate?

The new tools provide:
✅ Better error handling and logging
✅ Standardized interfaces and configuration
✅ Improved performance and reliability
✅ Comprehensive documentation and help
✅ Better integration with the overall system

## Migration Process

1. **Identify Legacy Tools**: Check which legacy tools you're using
2. **Review Mappings**: Use --list - mappings to see new tool equivalents
3. **Update Scripts**: Replace old tool calls with new ones
4. **Test Thoroughly**: Verify functionality with new tools
5. **Update Documentation**: Update any internal documentation

## Category - Specific Guides:
"""
        )

        for category, message in self.migration_messages.items():
            print(f"\n### {category.upper()}")
            print(message)

        print(
            """
## Getting Help

- Run any new tool with --help for detailed usage information
- Check the comprehensive guide: docs / TOOLS_COMPREHENSIVE_GUIDE.md
- Use the compatibility wrapper to test migrations safely

## Timeline

Legacy tools will be removed in the next major version.
Please migrate as soon as possible to avoid disruption.
"""
        )

    def create_wrapper_scripts(self) -> None:
        """Create individual wrapper scripts for common legacy tools."""
        wrapper_dir = Path(__file__).parent / "wrappers"
        wrapper_dir.mkdir(exist_ok=True)

        common_tools = ["run_etl.py", "monitor.py", "cleanup.py", "setup_system.py", "sentiment_analysis.py"]

        for tool in common_tools:
            if tool in self.tool_mappings:
                wrapper_script = wrapper_dir / tool
                wrapper_content = f'''#!/usr / bin / env python3
"""
Legacy wrapper for {tool}
This script is deprecated. Please use the new consolidated tools.
"""
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from compatibility_wrapper import CompatibilityWrapper

if __name__ == "__main__":
    wrapper = CompatibilityWrapper()
    sys.exit(wrapper.handle_legacy_tool("{tool[:-3]}", sys.argv[1:]))
'''

                with open(wrapper_script, "w") as f:
                    f.write(wrapper_content)

                # Make executable
                wrapper_script.chmod(0o755)

        print(f"✅ Created wrapper scripts in {wrapper_dir}")


def main():
    """Main entry point for the compatibility wrapper."""
    parser = argparse.ArgumentParser(
        description="Backward Compatibility Wrapper for Legacy Tools",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python tools / legacy / compatibility_wrapper.py run_etl --focused
  python tools / legacy / compatibility_wrapper.py monitor --health
  python tools / legacy / compatibility_wrapper.py --list - mappings
  python tools / legacy / compatibility_wrapper.py --migration - guide
        """,
    )

    parser.add_argument("tool_name", nargs="?", help="Name of the legacy tool to execute")
    parser.add_argument("tool_args", nargs="*", help="Arguments to pass to the legacy tool")

    parser.add_argument("--list - mappings", action="store_true", help="List all available tool mappings")
    parser.add_argument("--migration - guide", action="store_true", help="Show comprehensive migration guide")
    parser.add_argument("--create - wrappers", action="store_true", help="Create individual wrapper scripts")

    args = parser.parse_args()

    wrapper = CompatibilityWrapper()

    if args.list_mappings:
        wrapper.list_mappings()
        return 0

    if args.migration_guide:
        wrapper.show_migration_guide()
        return 0

    if args.create_wrappers:
        wrapper.create_wrapper_scripts()
        return 0

    if not args.tool_name:
        print("❌ Error: No tool name provided")
        parser.print_help()
        return 1

    return wrapper.handle_legacy_tool(args.tool_name, args.tool_args)


if __name__ == "__main__":
    sys.exit(main())
